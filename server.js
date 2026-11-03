'use strict';
/*
 * DROPKILL server
 *  - Contas: nome unico (sem diferenciar maiusculas, acentos, espacos, _ e -) + ID automatico
 *  - Matchmaking hibrido: toda partida tem exatamente 20 participantes. Humanos entram na fila;
 *    ao completar 20 humanos OU esgotar a janela de espera, as vagas restantes viram bots.
 *  - Relay: o servidor apenas repassa as mensagens da partida entre os jogadores (o 1o humano e o "host" dos bots).
 *
 * Uso:  npm install && node server.js        (PORT=8080, MM_WAIT=10 segundos de espera da fila)
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const MM_WAIT = Math.max(0, +process.env.MM_WAIT || 10);   // segundos de espera apos o 1o jogador entrar na fila
const MATCH_SIZE = 20;
const READY_TIMEOUT = 30;                                  // segundos para todos carregarem a partida
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'accounts.json');
const GAME_FILE = process.env.GAME_FILE || path.join(__dirname, 'DROPKILL_conta_perfil.html');

/* ---------------- contas ---------------- */
let db = { nextId: 100001, accounts: [] };
try { db = Object.assign(db, JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'))); } catch (e) { /* primeira execucao */ }
const byKey = new Map(), byId = new Map();
for (const a of db.accounts) { byKey.set(a.key, a); byId.set(a.id, a); }
let saveTimer = null;
function saveDb() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    const tmp = DATA_FILE + '.tmp';
    fs.writeFile(tmp, JSON.stringify(db, null, 1), err => { if (!err) fs.rename(tmp, DATA_FILE, () => {}); });
  }, 100);
}
function cleanName(s) {
  return String(s == null ? '' : s).normalize('NFC').trim().replace(/\s+/g, ' ').replace(/[^A-Za-zÀ-ÿ0-9 _-]/g, '').slice(0, 18);
}
function nameKey(s) {   // "João", "joao", "J o_a-o" => mesma chave
  return cleanName(s).normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[\s_-]+/g, '');
}
function register(rawName) {
  const name = cleanName(rawName);
  if (name.length < 2) return { err: 'invalid_name', msg: 'Digite pelo menos 2 caracteres para seu nome.' };
  const key = nameKey(name);
  if (key.length < 2) return { err: 'invalid_name', msg: 'Nome inválido. Use letras ou números.' };
  if (byKey.has(key)) return { err: 'name_taken', msg: 'Este nome já está em uso por outra conta. Escolha outro.' };
  const acc = { id: db.nextId++, name, key, token: crypto.randomBytes(24).toString('hex'), created: Date.now() };
  db.accounts.push(acc); byKey.set(key, acc); byId.set(acc.id, acc); saveDb();
  return { acc };
}

/* ---------------- conexoes ---------------- */
const online = new Map();      // account id -> client
let nextConn = 1;
function send(c, obj) { if (c && c.ws.readyState === 1) c.ws.send(JSON.stringify(obj)); }

/* ---------------- matchmaking ---------------- */
const queues = { casual: newQueue(), ranked: newQueue() };
function newQueue() { return { list: [], timer: null, deadline: 0 }; }
const matches = new Map();
let nextMatch = 1;

function queueBroadcast(q) {
  const secs = q.deadline ? Math.max(0, Math.ceil((q.deadline - Date.now()) / 1000)) : 0;
  for (const c of q.list) send(c, { t: 'queue', humans: q.list.length, total: MATCH_SIZE, secs });
}
function joinQueue(c, mode) {
  leaveQueue(c);
  if (c.match) return;
  mode = mode === 'ranked' ? 'ranked' : 'casual';
  const q = queues[mode];
  c.queue = q;
  q.list.push(c);
  if (q.list.length === 1) {
    q.deadline = Date.now() + MM_WAIT * 1000;
    q.timer = setTimeout(() => startMatch(mode), MM_WAIT * 1000);
  }
  if (q.list.length >= MATCH_SIZE) return startMatch(mode);
  queueBroadcast(q);
}
function leaveQueue(c) {
  const q = c.queue; if (!q) return;
  c.queue = null;
  const i = q.list.indexOf(c); if (i >= 0) q.list.splice(i, 1);
  if (!q.list.length) { clearTimeout(q.timer); q.timer = null; q.deadline = 0; }
  else queueBroadcast(q);
}
function startMatch(mode) {
  const q = queues[mode];
  clearTimeout(q.timer); q.timer = null; q.deadline = 0;
  const humans = q.list.splice(0, MATCH_SIZE);
  if (!humans.length) return;
  if (q.list.length) {   // sobrou gente (so ocorre se > 20 na fila): abre nova janela
    q.deadline = Date.now() + MM_WAIT * 1000;
    q.timer = setTimeout(() => startMatch(mode), MM_WAIT * 1000);
    queueBroadcast(q);
  }
  const m = {
    id: nextMatch++, mode, seed: crypto.randomInt(1, 2 ** 31) >>> 0,
    players: new Map(), host: 0, ready: new Set(), started: false, timer: null
  };
  humans.forEach((c, i) => {
    c.queue = null; c.match = m; c.slot = i;
    m.players.set(i, { slot: i, id: c.acc.id, name: c.acc.name, skin: 1 + crypto.randomInt(0, 7), c });
  });
  matches.set(m.id, m);
  const roster = [...m.players.values()].map(p => ({ slot: p.slot, id: p.id, name: p.name, skin: p.skin }));
  const bots = MATCH_SIZE - roster.length;
  for (const p of m.players.values()) {
    send(p.c, { t: 'match', id: m.id, mode, seed: m.seed, slot: p.slot, host: m.host, humans: roster, bots, size: MATCH_SIZE });
  }
  log(`partida #${m.id} (${mode}): ${roster.length} humanos + ${bots} bots = ${roster.length + bots}`);
  m.timer = setTimeout(() => goMatch(m), READY_TIMEOUT * 1000);
}
function goMatch(m) {
  if (m.started) return;
  m.started = true; clearTimeout(m.timer);
  for (const p of m.players.values()) send(p.c, { t: 'go' });
}
function leaveMatch(c, notify = true) {
  const m = c.match; if (!m) return;
  c.match = null;
  const slot = c.slot; m.players.delete(slot); m.ready.delete(slot);
  if (!m.players.size) { clearTimeout(m.timer); matches.delete(m.id); log(`partida #${m.id} encerrada`); return; }
  if (notify) for (const p of m.players.values()) send(p.c, { t: 'left', slot });
  if (m.host === slot) {      // migra o host dos bots
    m.host = Math.min(...m.players.keys());
    for (const p of m.players.values()) send(p.c, { t: 'host', slot: m.host });
  }
  if (!m.started && [...m.players.keys()].every(s => m.ready.has(s))) goMatch(m);
}

/* ---------------- HTTP (serve o jogo) + WebSocket ---------------- */
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, { 'content-type': 'application/json' }); return res.end(JSON.stringify({ ok: true, online: online.size, matches: matches.size })); }
  fs.readFile(GAME_FILE, (err, buf) => {
    if (err) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); return res.end('Coloque DROPKILL_conta_perfil.html na pasta do servidor.'); }
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' }); res.end(buf);
  });
});
const wss = new WebSocketServer({ server, maxPayload: 256 * 1024 });

wss.on('connection', (ws) => {
  const c = { ws, cid: nextConn++, acc: null, queue: null, match: null, slot: -1, alive: true, rate: { n: 0, t: Date.now() } };
  ws.on('pong', () => { c.alive = true; });
  send(c, { t: 'hello', matchSize: MATCH_SIZE, wait: MM_WAIT });
  ws.on('message', (data) => {
    const now = Date.now();                       // limite simples de mensagens por segundo
    if (now - c.rate.t > 1000) { c.rate.t = now; c.rate.n = 0; }
    if (++c.rate.n > 400) return;
    let m; try { m = JSON.parse(data); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    handle(c, m);
  });
  ws.on('close', () => drop(c));
  ws.on('error', () => {});
});
function drop(c) {
  leaveQueue(c); leaveMatch(c);
  if (c.acc && online.get(c.acc.id) === c) online.delete(c.acc.id);
}
function authed(c, acc) {
  const old = online.get(acc.id);
  if (old && old !== c) { send(old, { t: 'err', code: 'kicked', msg: 'Sua conta entrou em outro lugar.' }); try { old.ws.close(); } catch (e) {} drop(old); }
  c.acc = acc; online.set(acc.id, c);
}
function handle(c, m) {
  switch (m.t) {
    case 'register': {
      if (c.acc) return;
      const r = register(m.name);
      if (r.err) return send(c, { t: 'err', code: r.err, msg: r.msg, for: 'register' });
      authed(c, r.acc);
      log(`conta criada: ${r.acc.name} (ID ${r.acc.id})`);
      return send(c, { t: 'auth', id: r.acc.id, name: r.acc.name, token: r.acc.token, created: true });
    }
    case 'login': {
      const acc = byId.get(+m.id);
      if (!acc || acc.token !== m.token) return send(c, { t: 'err', code: 'bad_login', msg: 'Conta não encontrada neste servidor. Crie a conta novamente.', for: 'login' });
      authed(c, acc);
      return send(c, { t: 'auth', id: acc.id, name: acc.name, token: acc.token, created: false });
    }
    case 'check': {   // verifica disponibilidade do nome enquanto digita
      const key = nameKey(m.name);
      return send(c, { t: 'check', name: cleanName(m.name), free: key.length >= 2 && !byKey.has(key) });
    }
    case 'queue': if (c.acc && !c.match) joinQueue(c, m.mode); return;
    case 'cancel': leaveQueue(c); return;
    case 'ready': {
      const mt = c.match; if (!mt) return;
      mt.ready.add(c.slot);
      if (!mt.started && [...mt.players.keys()].every(s => mt.ready.has(s))) goMatch(mt);
      return;
    }
    case 'leave': leaveMatch(c); return;
    case 'm': {        // broadcast para os outros humanos da partida
      const mt = c.match; if (!mt) return;
      const out = JSON.stringify({ t: 'm', f: c.slot, d: m.d });
      for (const p of mt.players.values()) if (p.c !== c && p.c.ws.readyState === 1) p.c.ws.send(out);
      return;
    }
    case 'to': {       // mensagem direta para um slot
      const mt = c.match; if (!mt) return;
      const p = mt.players.get(m.to);
      if (p) send(p.c, { t: 'm', f: c.slot, d: m.d });
      return;
    }
    case 'ping': return send(c, { t: 'pong', n: m.n, online: online.size });
  }
}
setInterval(() => {
  for (const ws of wss.clients) { /* heartbeat */ }
  for (const c of online.values()) { if (!c.alive) { try { c.ws.terminate(); } catch (e) {} continue; } c.alive = false; try { c.ws.ping(); } catch (e) {} }
  for (const q of Object.values(queues)) if (q.list.length) queueBroadcast(q);
}, 1000);

function log(s) { console.log(new Date().toISOString().slice(11, 19), s); }
server.listen(PORT, '0.0.0.0', () => {
  const ifs = Object.values(require('os').networkInterfaces()).flat().filter(i => i && i.family === 'IPv4' && !i.internal).map(i => i.address);
  log(`DROPKILL server em http://localhost:${PORT}  (rede local: ${ifs.map(a => `http://${a}:${PORT}`).join(', ') || '-'})`);
  log(`Janela de matchmaking: ${MM_WAIT}s · ${MATCH_SIZE} participantes por partida (humanos + bots)`);
});
