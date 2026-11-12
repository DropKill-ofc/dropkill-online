'use strict';
/*
 * DROPKILL server
 *  - Contas: nome unico (sem diferenciar maiusculas, acentos, espacos, _ e -) + ID automatico
 *  - Amigos: busca por nome ou ID, pedido de amizade na caixa de correio, lista de amigos
 *  - Grupos: SOLO / DUO / SQUAD, convite de amigos; vagas vazias da equipe viram bots
 *  - Matchmaking hibrido: toda partida tem exatamente 30 participantes (humanos + bots)
 *  - Relay: o servidor repassa as mensagens da partida entre os jogadores
 *  - Conta oficial "dropKILLOFC": nome reservado (exige a chave do dono) e marcada como verificada
 *
 * Uso:  npm install && node server.js        (PORT=8080, MM_WAIT=10 segundos de espera da fila)
 * Dados: com DATABASE_URL as contas ficam no PostgreSQL (Render); sem ela, em accounts.json (modo local).
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { openStore } = require('./store');

const PORT = +process.env.PORT || 8080;
const MM_WAIT = Math.max(0, +process.env.MM_WAIT || 10);   // segundos de espera apos o 1o grupo entrar na fila
const MATCH_SIZE = 30;
const ZOMBIE_MAX = 4;          // Modo Zumbi: co-op de ate 4 jogadores, sem bots
const PRE_SECS = Math.max(0, Math.min(120, +process.env.PRE_SECS || 20));   // segundos do MINI LOBBY (casual/ranqueada) antes do carregamento
const READY_TIMEOUT = 30;                                  // segundos para todos carregarem a partida
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'accounts.json');
const GAME_FILE = process.env.GAME_FILE || path.join(__dirname, 'DROPKILL_conta_perfil.html');
const OWNER_FILE = process.env.OWNER_FILE || path.join(__dirname, 'owner.key');
const OWNER_NAME_KEY = 'dropkillofc';
const TEAM_SIZE = { solo: 1, duo: 2, squad: 4 };

/* ---------------- armazenamento (PostgreSQL no Render, arquivo no modo local) ---------------- */
const store = openStore(process.env, DATA_FILE);
let OWNER_KEY = process.env.OWNER_KEY || '';
let ready = false;

/* ---------------- contas ---------------- */
let db = { nextId: 100001, accounts: [] };
const byKey = new Map(), byId = new Map();
store.bind(() => db);
function saveDb() { store.schedule(); }
function adopt(data) {
  db = { nextId: data.nextId || 100001, accounts: data.accounts || [] };
  byKey.clear(); byId.clear();
  for (const a of db.accounts) { a.friends = a.friends || []; a.mail = a.mail || []; byKey.set(a.key, a); byId.set(a.id, a); if (a.id >= db.nextId) db.nextId = a.id + 1; }
}
function cleanName(s) {
  return String(s == null ? '' : s).normalize('NFC').trim().replace(/\s+/g, ' ').replace(/[^A-Za-zÀ-ÿ0-9 _-]/g, '').slice(0, 18);
}
function nameKey(s) {   // "João", "joao", "J o_a-o" => mesma chave
  return cleanName(s).normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[\s_-]+/g, '');
}
const isVerified = a => !!a && a.key === OWNER_NAME_KEY;
function pub(a) { return { id: a.id, name: a.name, v: isVerified(a) ? 1 : 0, lvl: levelOf(a.xp || 0), lg: a.legend ? 1 : 0, rk: a.rk | 0 }; }
function register(rawName, ownerKey) {
  const name = cleanName(rawName);
  if (name.length < 2) return { err: 'invalid_name', msg: 'Digite pelo menos 2 caracteres para seu nome.' };
  const key = nameKey(name);
  if (key.length < 2) return { err: 'invalid_name', msg: 'Nome inválido. Use letras ou números.' };
  if (byKey.has(key)) return { err: 'name_taken', msg: 'Este nome já está em uso por outra conta. Escolha outro.' };
  if (key === OWNER_NAME_KEY && String(ownerKey || '') !== OWNER_KEY)
    return { err: 'reserved', msg: 'Este nome é reservado à conta oficial do jogo. Informe a chave do dono ou escolha outro nome.' };
  const acc = { id: db.nextId++, name, key, token: crypto.randomBytes(24).toString('hex'), created: Date.now(), friends: [], mail: [] };
  db.accounts.push(acc); byKey.set(key, acc); byId.set(acc.id, acc); saveDb();
  return { acc };
}

/* ---------------- niveis 1..100 (XP no servidor) ---------------- */
// Custo crescente por nivel; soma exata = 6.000.000 XP para o nivel 100.
const MAX_LVL = 100, TOTAL_XP = 6000000;
const LV_COST = (() => {
  const raw = []; let sum = 0;
  for (let n = 1; n < MAX_LVL; n++) { const v = Math.pow(n, 1.55) + 9; raw.push(v); sum += v; }
  const c = raw.map(v => Math.round(v * TOTAL_XP / sum));
  c[c.length - 1] += TOTAL_XP - c.reduce((a, b) => a + b, 0);
  return c;                         // c[n-1] = XP para sair do nivel n
})();
const LV_CUM = [0]; for (const c of LV_COST) LV_CUM.push(LV_CUM[LV_CUM.length - 1] + c);   // LV_CUM[n-1] = XP total para ter o nivel n
/* HALL DA FAMA: as 3 contas humanas reais com maior nivel (desempate: mais XP, depois conta mais antiga). Bots nunca entram. */
function top3() {
  return db.accounts.slice().sort((a, b) => (b.xp || 0) - (a.xp || 0) || a.id - b.id).slice(0, 3)
    .map((a, i) => ({ pos: i + 1, id: a.id, name: a.name, lvl: levelOf(a.xp || 0), xp: a.xp | 0, lg: a.legend ? 1 : 0, v: isVerified(a) ? 1 : 0 }));
}
function levelOf(xp) { let l = 1; while (l < MAX_LVL && xp >= LV_CUM[l]) l++; return l; }
// Teto de ganho: 4.000 XP/hora (balde com recarga). 6.000.000 / 4.000 = 1.500 h = 62,5 dias sem parar.
const XP_PER_HOUR = 4000, XP_BUCKET = 1800;
function profOf(a) {
  const xp = a.xp || 0, lvl = levelOf(xp);
  return { xp, lvl, base: LV_CUM[lvl - 1], next: lvl >= MAX_LVL ? LV_CUM[MAX_LVL - 1] : LV_CUM[lvl], legend: a.legend ? 1 : 0 };
}
function broadcastAll(obj) { for (const c of online.values()) send(c, obj); }
function grantXP(c, raw, why) {
  const a = c.acc; if (!a || !(raw > 0)) return null;
  const now = Date.now(), b = a.xb || (a.xb = { t: now, v: XP_BUCKET });
  b.v = Math.min(XP_BUCKET, b.v + (now - b.t) / 1000 * XP_PER_HOUR / 3600); b.t = now;
  const before = levelOf(a.xp || 0);
  const gain = Math.max(0, Math.min(Math.floor(raw), Math.floor(b.v), TOTAL_XP - (a.xp || 0)));
  b.v -= gain; a.xp = (a.xp || 0) + gain;
  const lvl = levelOf(a.xp);
  let newLegend = false;
  if (lvl >= MAX_LVL && !a.legend) { a.legend = now; newLegend = true; }
  saveDb();
  send(c, Object.assign({ t: 'xp', gain, raw: Math.floor(raw), capped: gain < Math.floor(raw) ? 1 : 0, why, up: lvl > before ? 1 : 0, from: before, new_legend: newLegend ? 1 : 0 }, profOf(a)));
  if (newLegend) { log(`LENDARIO: ${a.name}`); broadcastAll({ t: 'announce', kind: 'legend', name: a.name, id: a.id }); }
  return gain;
}
// XP de partida (valores calculados AQUI a partir do que o cliente reporta, com limites de plausibilidade)
function matchXP(m, r, mt) {
  const el = Math.max(0, (Date.now() - (mt.t0 || Date.now())) / 1000);
  const time = Math.min(+r.time || 0, el + 5, 1800);
  const kills = Math.max(0, Math.min(+r.kills || 0, MATCH_SIZE - 1, 20));
  const dmg = Math.max(0, Math.min(+r.dmg || 0, kills * 320 + 900, 4000));
  const place = Math.max(1, Math.min(+r.place || MATCH_SIZE, MATCH_SIZE));
  const win = !!r.win && place === 1;
  let xp = 30 + Math.min(time, 900) / 12 + kills * 22 + dmg / 32 + (win ? 160 : place <= 3 ? 80 : place <= 10 ? 35 : 0);
  if (time < 40) xp *= time / 40;
  if (mt.mode === 'ranked' || mt.ranked) xp *= 1.15;
  return Math.round(xp);
}
const ZXP = { inf: 3, run: 4, brt: 14, crw: 4, jmp: 5, exp: 6, alfa: 150 };
function zombieXP(r, mt) {
  const el = Math.max(0, (Date.now() - (mt.t0 || Date.now())) / 1000);
  const waves = Math.max(0, Math.min(+r.waves || 0, 5)), time = Math.min(+r.time || 0, el + 5);
  const n = Math.max(1, mt.size || 1);
  let xp = 0;
  const k = r.kt || {};
  for (const t of Object.keys(ZXP)) xp += Math.max(0, Math.min(+k[t] || 0, t === 'alfa' ? 1 : 60 * n)) * ZXP[t];
  for (let w = 1; w <= waves; w++) xp += 40 * w;
  const win = !!r.win && waves >= 5 && time >= 5 * 45;
  if (win) xp += 400;
  if (time < waves * 40) xp *= Math.max(0.1, time / (waves * 40));    // hordas rapidas demais = suspeito
  return { xp: Math.round(xp), win };
}

/* ---------------- conexoes ---------------- */
const online = new Map();      // account id -> client
let nextConn = 1;
function send(c, obj) { if (c && c.ws.readyState === 1) c.ws.send(JSON.stringify(obj)); }
const state = c => (c ? (c.match ? 'match' : c.entry ? 'queue' : 'lobby') : 'offline');

/* ---------------- grupos ---------------- */
const parties = new Map();
let nextParty = 1;
const cap = p => TEAM_SIZE[p.mode] || 1;
function newParty(c) {
  const p = { id: nextParty++, leader: c.acc.id, members: [c.acc.id], mode: 'solo' };
  parties.set(p.id, p); c.party = p; return p;
}
function partyClients(p) { return p.members.map(id => online.get(id)).filter(Boolean); }
function partyState(p) {
  return {
    t: 'party', id: p.id, mode: p.mode, leader: p.leader,
    members: p.members.map(id => { const a = byId.get(id); return Object.assign(pub(a), { online: online.has(id), state: state(online.get(id)) }); })
  };
}
function partyBroadcast(p) { const s = partyState(p); for (const c of partyClients(p)) send(c, s); }
function leaveParty(c, notify = true) {
  const p = c.party; if (!p) return;
  const id = c.acc.id;
  if (p.members.includes(id) && p.members.length > 1) {
    cancelEntry(c.entry);
    p.members = p.members.filter(x => x !== id);
    if (p.leader === id) p.leader = p.members[0];
    if (cap(p) < p.members.length) p.mode = p.members.length > 2 ? 'squad' : 'duo';
    partyBroadcast(p);
  } else parties.delete(p.id);
  c.party = null;
  if (notify && online.get(id) === c && c.acc) { newParty(c); send(c, partyState(c.party)); }
}

/* ---------------- matchmaking (filas por modo + tamanho de equipe) ---------------- */
const queues = new Map();     // chave "casual:duo" -> { list:[entry], timer, deadline, T }
function getQueue(key, T) { let q = queues.get(key); if (!q) { q = { list: [], timer: null, deadline: 0, T }; queues.set(key, q); } return q; }
const matches = new Map();
let nextMatch = 1;

function queueHumans(q) { return q.list.reduce((n, e) => n + e.clients.length, 0); }
function queueBroadcast(q) {
  const secs = q.deadline ? Math.max(0, Math.ceil((q.deadline - Date.now()) / 1000)) : 0;
  const humans = queueHumans(q), zomb = q.list[0] && q.list[0].mode === 'zombie';
  for (const e of q.list) for (const c of e.clients) send(c, { t: 'queue', humans, total: zomb ? ZOMBIE_MAX : MATCH_SIZE, secs, team: q.T, key: e.key, zombie: zomb ? 1 : 0 });
}
function joinQueue(leader, mode) {
  const p = leader.party;
  if (!p || p.leader !== leader.acc.id) return send(leader, { t: 'err', code: 'not_leader', msg: 'Só o líder do grupo inicia a partida.' });
  if (leader.match || leader.entry) return;
  const clients = partyClients(p);
  if (clients.length !== p.members.length || clients.some(c => c.match || c.entry))
    return send(leader, { t: 'err', code: 'party_busy', msg: 'Um integrante do grupo está offline ou em partida.' });
  mode = mode === 'ranked' ? 'ranked' : mode === 'zombie' ? 'zombie' : 'casual';
  const zomb = mode === 'zombie';
  if (zomb && p.members.length > ZOMBIE_MAX) return send(leader, { t: 'err', code: 'party_full', msg: `O Modo Zumbi comporta até ${ZOMBIE_MAX} jogadores.` });
  const T = zomb ? ZOMBIE_MAX : cap(p), key = zomb ? 'zombie:coop' : mode + ':' + p.mode;
  const q = getQueue(key, T);
  const entry = { party: p, clients, key, mode, T };
  for (const c of clients) c.entry = entry;
  q.list.push(entry);
  if (q.list.length === 1) { q.deadline = Date.now() + MM_WAIT * 1000; q.timer = setTimeout(() => startMatch(key), MM_WAIT * 1000); }
  if (queueHumans(q) >= (zomb ? ZOMBIE_MAX : MATCH_SIZE)) return startMatch(key);
  queueBroadcast(q);
}
function cancelEntry(entry) {
  if (!entry) return;
  const q = queues.get(entry.key); if (!q) return;
  const i = q.list.indexOf(entry); if (i >= 0) q.list.splice(i, 1);
  for (const c of entry.clients) if (c.entry === entry) { c.entry = null; send(c, { t: 'queue_cancel' }); }
  if (!q.list.length) { clearTimeout(q.timer); q.timer = null; q.deadline = 0; } else queueBroadcast(q);
}
function startMatch(key) {
  const q = queues.get(key); if (!q || !q.list.length) return;
  clearTimeout(q.timer); q.timer = null; q.deadline = 0;
  if (key === 'zombie:coop') return startZombie(key, q);
  const T = q.T, maxTeams = Math.floor(MATCH_SIZE / T);
  // monta as equipes: grupos maiores primeiro; grupos parciais se juntam ate fechar T
  const teams = [], placed = [];
  for (const e of q.list.slice().sort((a, b) => b.clients.length - a.clients.length)) {
    let t = teams.find(t => t.n + e.clients.length <= T);
    if (!t) { if (teams.length >= maxTeams) continue; t = { n: 0, entries: [] }; teams.push(t); }
    t.entries.push(e); t.n += e.clients.length; placed.push(e);
  }
  q.list = q.list.filter(e => !placed.includes(e));
  if (q.list.length) { q.deadline = Date.now() + MM_WAIT * 1000; q.timer = setTimeout(() => startMatch(key), MM_WAIT * 1000); queueBroadcast(q); }
  const m = {
    id: nextMatch++, mode: placed[0].mode, seed: crypto.randomInt(1, 2 ** 31) >>> 0,
    players: new Map(), host: 0, ready: new Set(), started: false, timer: null, T
  };
  const roster = [], teamOf = [];
  let slot = 0;
  teams.forEach((t, ti) => {
    for (const e of t.entries) for (const c of e.clients) {
      c.entry = null; c.match = m; c.slot = slot;
      const a = c.acc;
      const p = { slot, id: a.id, name: a.name, skin: a.legend ? 88 : 1 + crypto.randomInt(0, 7), v: isVerified(a) ? 1 : 0, team: ti, c, lvl: levelOf(a.xp || 0), lg: a.legend ? 1 : 0 };
      m.players.set(slot, p); roster.push({ slot, id: p.id, name: p.name, skin: p.skin, v: p.v, team: ti, lvl: p.lvl, lg: p.lg });
      teamOf[slot] = ti; slot++;
    }
  });
  const H = slot;
  let botId = H;
  teams.forEach((t, ti) => { for (let k = t.n; k < T; k++) teamOf[botId++] = ti; });     // bots completam as equipes humanas
  let nt = teams.length;
  while (botId < MATCH_SIZE) { for (let k = 0; k < T && botId < MATCH_SIZE; k++) teamOf[botId++] = nt; nt++; }   // equipes so de bots
  matches.set(m.id, m);
  const bots = MATCH_SIZE - H;
  const hof = PRE_SECS > 0 ? top3() : [];
  for (const p of m.players.values()) {
    send(p.c, { t: 'match', id: m.id, mode: m.mode, seed: m.seed, slot: p.slot, host: m.host, humans: roster, bots, size: MATCH_SIZE, teamSize: T, teams: teamOf, pre: PRE_SECS, hof });
    p.c.party && send(p.c, partyState(p.c.party));
  }
  log(`partida #${m.id} (${key}): ${H} humanos + ${bots} bots = ${H + bots} · equipes de ${T}${PRE_SECS ? ` · mini lobby ${PRE_SECS}s` : ''}`);
  m.timer = setTimeout(() => goMatch(m), (PRE_SECS + READY_TIMEOUT) * 1000);   // o relogio de "pronto" so conta depois do mini lobby
}
function startZombie(key, q) {
  const placed = []; let n = 0;
  for (const e of q.list.slice().sort((a, b) => b.clients.length - a.clients.length)) if (n + e.clients.length <= ZOMBIE_MAX) { placed.push(e); n += e.clients.length; }
  q.list = q.list.filter(e => !placed.includes(e));
  if (q.list.length) { q.deadline = Date.now() + MM_WAIT * 1000; q.timer = setTimeout(() => startMatch(key), MM_WAIT * 1000); queueBroadcast(q); }
  const m = { id: nextMatch++, mode: 'zombie', seed: crypto.randomInt(1, 2 ** 31) >>> 0, players: new Map(), host: 0, ready: new Set(), started: false, timer: null, T: 1 };
  const roster = []; let slot = 0;
  for (const e of placed) for (const c of e.clients) {
    c.entry = null; c.match = m; c.slot = slot;
    const a = c.acc, p = { slot, id: a.id, name: a.name, skin: a.legend ? 88 : 1 + crypto.randomInt(0, 7), v: isVerified(a) ? 1 : 0, team: 0, c, lvl: levelOf(a.xp || 0), lg: a.legend ? 1 : 0 };
    m.players.set(slot, p); roster.push({ slot, id: p.id, name: p.name, skin: p.skin, v: p.v, team: 0, lvl: p.lvl, lg: p.lg }); slot++;
  }
  m.size = slot; matches.set(m.id, m);
  for (const p of m.players.values()) {
    send(p.c, { t: 'match', id: m.id, mode: 'zombie', zombie: 1, seed: m.seed, slot: p.slot, host: m.host, humans: roster, bots: 0, size: slot, teamSize: 1, teams: null });
    p.c.party && send(p.c, partyState(p.c.party));
  }
  log(`partida #${m.id} (ZUMBI): ${slot} humanos`);
  m.timer = setTimeout(() => goMatch(m), READY_TIMEOUT * 1000);
}
function goMatch(m) {
  if (m.started) return;
  m.started = true; m.t0 = Date.now(); clearTimeout(m.timer);
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

/* ---------------- amizades / correio ---------------- */
const isFriend = (a, b) => a.friends.includes(b.id);
function relation(me, a) {
  if (a.id === me.id) return 'self';
  if (isFriend(me, a)) return 'friend';
  if (a.mail.some(x => x.type === 'friend' && x.from === me.id)) return 'sent';
  if (me.mail.some(x => x.type === 'friend' && x.from === a.id)) return 'received';
  return 'none';
}
function mailItems(c) {
  const acc = c.acc, items = [];
  for (const m of acc.mail) { const f = byId.get(m.from); if (f) items.push({ mid: m.mid, type: 'friend', from: pub(f), ts: m.ts }); }
  for (const inv of (c.invites || [])) {
    const p = parties.get(inv.pid), f = byId.get(inv.from);
    if (p && f && p.members.includes(inv.from)) items.push({ mid: 'p' + inv.pid, type: 'party', pid: inv.pid, mode: p.mode, from: pub(f), ts: inv.ts });
  }
  return items.sort((a, b) => b.ts - a.ts);
}
function pushMail(c) { send(c, { t: 'mail', items: mailItems(c) }); }
function friendList(c) {
  return c.acc.friends.map(id => byId.get(id)).filter(Boolean).map(a => Object.assign(pub(a), { online: online.has(a.id), state: state(online.get(a.id)) }));
}
function makeFriends(a, b) {
  if (!a.friends.includes(b.id)) a.friends.push(b.id);
  if (!b.friends.includes(a.id)) b.friends.push(a.id);
  a.mail = a.mail.filter(x => !(x.type === 'friend' && x.from === b.id));
  b.mail = b.mail.filter(x => !(x.type === 'friend' && x.from === a.id));
  saveDb();
  for (const x of [a, b]) { const c = online.get(x.id); if (c) { send(c, { t: 'friends', list: friendList(c) }); pushMail(c); } }
}

/* ---------------- HTTP (serve o jogo) + WebSocket ---------------- */
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/healthz') { res.writeHead(200, { 'content-type': 'application/json' }); return res.end(JSON.stringify({ ok: true, ready, db: store.kind, accounts: db.accounts.length, online: online.size, matches: matches.size })); }
  fs.readFile(GAME_FILE, (err, buf) => {
    if (err) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); return res.end('Coloque DROPKILL_conta_perfil.html na pasta do servidor.'); }
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' }); res.end(buf);
  });
});
const wss = new WebSocketServer({ server, maxPayload: 256 * 1024 });

wss.on('connection', (ws) => {
  if (!ready) { try { ws.send(JSON.stringify({ t: 'err', code: 'starting', msg: 'Servidor iniciando, tente novamente em instantes.' })); } catch (e) {} return ws.close(1013); }
  const c = { ws, cid: nextConn++, acc: null, party: null, entry: null, match: null, slot: -1, invites: [], alive: true, rate: { n: 0, t: Date.now() } };
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
  cancelEntry(c.entry); leaveMatch(c);
  if (c.acc) {
    if (c.party) leaveParty(c, false);
    if (online.get(c.acc.id) === c) online.delete(c.acc.id);
  }
}
function authed(c, acc) {
  const old = online.get(acc.id);
  if (old && old !== c) { send(old, { t: 'err', code: 'kicked', msg: 'Sua conta entrou em outro lugar.' }); try { old.ws.close(); } catch (e) {} drop(old); }
  c.acc = acc; online.set(acc.id, c);
  newParty(c); send(c, partyState(c.party));
}
function authMsg(acc, created) { return { t: 'auth', id: acc.id, name: acc.name, token: acc.token, created, v: isVerified(acc) ? 1 : 0, lvl: levelOf(acc.xp || 0), lg: acc.legend ? 1 : 0 }; }
const profMsg = acc => Object.assign({ t: 'prof' }, profOf(acc));

function handle(c, m) {
  const me = c.acc;
  switch (m.t) {
    case 'register': {
      if (me) return;
      const r = register(m.name, m.ownerKey);
      if (r.err) return send(c, { t: 'err', code: r.err, msg: r.msg, for: 'register' });
      authed(c, r.acc);
      log(`conta criada: ${r.acc.name} (ID ${r.acc.id})${isVerified(r.acc) ? ' [OFICIAL]' : ''}`);
      send(c, authMsg(r.acc, true)); return send(c, profMsg(r.acc));
    }
    case 'login': {
      const acc = byId.get(+m.id);
      if (!acc || acc.token !== m.token) return send(c, { t: 'err', code: 'bad_login', msg: 'Conta não encontrada neste servidor. Crie a conta novamente.', for: 'login' });
      authed(c, acc);
      send(c, authMsg(acc, false)); send(c, profMsg(acc));
      pushMail(c); send(c, { t: 'friends', list: friendList(c) });
      return;
    }
    case 'check': {   // verifica disponibilidade do nome enquanto digita
      const key = nameKey(m.name);
      return send(c, { t: 'check', name: cleanName(m.name), free: key.length >= 2 && !byKey.has(key) });
    }
  }
  if (!me) return;
  switch (m.t) {
    /* ---- fila ---- */
    case 'queue': if (!c.match) joinQueue(c, m.mode); return;
    case 'cancel': cancelEntry(c.entry); return;
    case 'ready': {
      const mt = c.match; if (!mt) return;
      mt.ready.add(c.slot);
      if (!mt.started && [...mt.players.keys()].every(s => mt.ready.has(s))) goMatch(mt);
      return;
    }
    case 'leave': leaveMatch(c); return;
    case 'result': {          // fim de partida (casual/ranqueada): servidor calcula o XP
      const mt = c.match; if (!mt || !mt.started || c.resDone === mt.id || mt.mode === 'zombie') return;
      c.resDone = mt.id; grantXP(c, matchXP(m, m, mt), 'partida'); return;
    }
    case 'zresult': {         // fim do Modo Zumbi
      const mt = c.match; if (!mt || !mt.started || c.resDone === mt.id || mt.mode !== 'zombie') return;
      c.resDone = mt.id; const z = zombieXP(m, mt); grantXP(c, z.xp, z.win ? 'zumbi_vitoria' : 'zumbi'); return;
    }
    case 'prof': return send(c, profMsg(me));
    /* pontos de patente (ranqueada) informados pelo cliente: apenas exibicao no lobby/grupo (campo extra da conta, sem mudar o banco) */
    case 'rank': {
      const pts = Math.max(0, Math.min(100000, Math.floor(+m.pts) || 0));
      if (pts !== (me.rk | 0)) { me.rk = pts; saveDb(); if (c.party && !c.entry && !c.match) partyBroadcast(c.party); }
      return;
    }

    /* ---- busca / amigos / correio ---- */
    case 'search': {
      const q = String(m.q || '').trim(); if (!q) return send(c, { t: 'search', q, results: [] });
      let list = [];
      const idm = q.replace(/^#|^id\s*/i, '');
      if (/^\d{3,}$/.test(idm)) { const a = byId.get(+idm); if (a) list.push(a); }
      const k = nameKey(q);
      if (k.length >= 1) for (const a of db.accounts) if (a.key.includes(k) && !list.includes(a)) list.push(a);
      list = list.filter(a => a.id !== me.id).sort((a, b) => (a.key === k ? -1 : 0) - (b.key === k ? -1 : 0) || a.name.localeCompare(b.name)).slice(0, 8);
      return send(c, { t: 'search', q, results: list.map(a => Object.assign(pub(a), { online: online.has(a.id), rel: relation(me, a) })) });
    }
    case 'friend_req': {
      const to = byId.get(+m.to);
      if (!to || to.id === me.id) return send(c, { t: 'err', code: 'bad_target', msg: 'Jogador não encontrado.' });
      const rel = relation(me, to);
      if (rel === 'friend') return send(c, { t: 'info', msg: 'Vocês já são amigos.' });
      if (rel === 'sent') return send(c, { t: 'info', msg: 'Pedido já enviado.' });
      if (rel === 'received') { makeFriends(me, to); return send(c, { t: 'info', msg: `Agora você e ${to.name} são amigos!` }); }
      to.mail.push({ mid: crypto.randomBytes(5).toString('hex'), type: 'friend', from: me.id, ts: Date.now() });
      saveDb();
      const tc = online.get(to.id); if (tc) { pushMail(tc); send(tc, { t: 'notify', msg: `${me.name} enviou um pedido de amizade` }); }
      return send(c, { t: 'info', msg: `Pedido de amizade enviado para ${to.name}.`, sent: to.id });
    }
    case 'mail': return pushMail(c);
    case 'friends': return send(c, { t: 'friends', list: friendList(c) });
    case 'mail_act': {
      const it = me.mail.find(x => x.mid === m.mid && x.type === 'friend'); if (!it) return;
      const from = byId.get(it.from);
      if (m.ok && from) { makeFriends(me, from); send(c, { t: 'info', msg: `Agora você e ${from.name} são amigos!` }); const fc = online.get(from.id); if (fc) send(fc, { t: 'notify', msg: `${me.name} aceitou seu pedido de amizade` }); }
      else { me.mail = me.mail.filter(x => x !== it); saveDb(); pushMail(c); }
      return;
    }
    case 'unfriend': {
      const o = byId.get(+m.id); if (!o) return;
      me.friends = me.friends.filter(x => x !== o.id); o.friends = o.friends.filter(x => x !== me.id); saveDb();
      send(c, { t: 'friends', list: friendList(c) }); const oc = online.get(o.id); if (oc) send(oc, { t: 'friends', list: friendList(oc) });
      return;
    }

    /* ---- grupo ---- */
    case 'party': return send(c, partyState(c.party));
    case 'party_mode': {
      const p = c.party; if (!p || c.entry || c.match) return;
      if (p.leader !== me.id) return send(c, { t: 'err', code: 'not_leader', msg: 'Só o líder do grupo muda o modo.' });
      const mode = TEAM_SIZE[m.mode] ? m.mode : 'solo';
      if (p.members.length > TEAM_SIZE[mode]) return send(c, { t: 'err', code: 'party_full', msg: 'O grupo tem mais jogadores do que esse modo comporta.' });
      p.mode = mode; return partyBroadcast(p);
    }
    case 'party_invite': {
      const p = c.party, to = byId.get(+m.to), tc = to && online.get(to.id);
      if (!p || c.entry || c.match) return;
      if (p.leader !== me.id) return send(c, { t: 'err', code: 'not_leader', msg: 'Só o líder do grupo convida.' });
      if (!to || !isFriend(me, to)) return send(c, { t: 'err', code: 'not_friend', msg: 'Você só pode convidar amigos.' });
      if (!tc) return send(c, { t: 'err', code: 'offline', msg: `${to.name} está offline.` });
      if (tc.match || tc.entry) return send(c, { t: 'err', code: 'busy', msg: `${to.name} está em partida ou na fila.` });
      if (p.members.includes(to.id)) return;
      if (p.mode === 'solo') p.mode = 'duo';
      if (p.members.length >= cap(p)) {
        if (p.mode === 'duo' && p.members.length < 4 && m.upgrade) p.mode = 'squad';
        else return send(c, { t: 'err', code: 'party_full', msg: 'O grupo já está completo. Mude para SQUAD para chamar mais gente.' });
      }
      tc.invites = (tc.invites || []).filter(i => i.pid !== p.id);
      tc.invites.push({ pid: p.id, from: me.id, ts: Date.now() });
      partyBroadcast(p); pushMail(tc); send(tc, { t: 'notify', msg: `${me.name} chamou você para um grupo (${p.mode.toUpperCase()})`, invite: p.id });
      return send(c, { t: 'info', msg: `Convite enviado para ${to.name}.` });
    }
    case 'party_accept': {
      const inv = (c.invites || []).find(i => i.pid === +m.pid), p = parties.get(+m.pid);
      c.invites = (c.invites || []).filter(i => i.pid !== +m.pid);
      if (!m.ok) return pushMail(c);
      if (!inv || !p || !p.members.includes(inv.from)) { pushMail(c); return send(c, { t: 'err', code: 'gone', msg: 'Esse convite expirou.' }); }
      if (c.entry || c.match) return;
      if (p.members.length >= cap(p)) { pushMail(c); return send(c, { t: 'err', code: 'party_full', msg: 'O grupo já está completo.' }); }
      leaveParty(c, false);
      p.members.push(me.id); c.party = p;
      partyBroadcast(p); pushMail(c);
      return;
    }
    case 'party_leave': if (!c.entry && !c.match) leaveParty(c, true); return;
    case 'party_kick': {
      const p = c.party; if (!p || p.leader !== me.id || c.entry || c.match) return;
      const t = online.get(+m.id); if (!t || t === c || t.party !== p) return;
      leaveParty(t, true); partyBroadcast(p); return;
    }

    /* ---- relay da partida ---- */
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
  for (const c of online.values()) { if (!c.alive) { try { c.ws.terminate(); } catch (e) {} continue; } c.alive = false; try { c.ws.ping(); } catch (e) {} }
  for (const q of queues.values()) if (q.list.length) queueBroadcast(q);
}, 1000);

function log(s) { console.log(new Date().toISOString().slice(11, 19), s); }
async function main() {
  server.listen(PORT, '0.0.0.0', () => log(`DROPKILL ouvindo na porta ${PORT} (armazenamento: ${store.kind})`));   // responde /health ja no boot
  await store.init();
  // migracao: accounts.json antigo -> PostgreSQL (nao apaga nem sobrescreve nada que ja esteja no banco)
  if (store.kind === 'postgres' && store.importAccounts) {
    let legacy = null;
    try { legacy = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (e) {}
    if (legacy && Array.isArray(legacy.accounts) && legacy.accounts.length) {
      const r = await store.importAccounts(legacy.accounts, legacy.nextId);
      log(`migracao de ${path.basename(DATA_FILE)}: ${r.added} contas importadas, ${r.skipped} ja existiam no banco`);
      try { fs.renameSync(DATA_FILE, DATA_FILE + '.migrado'); } catch (e) {}
    }
  }
  adopt(await store.load());
  if (!OWNER_KEY) {
    OWNER_KEY = (await store.getMeta('owner_key')) || '';
    if (!OWNER_KEY) { try { OWNER_KEY = fs.readFileSync(OWNER_FILE, 'utf8').trim(); } catch (e) {} }
    if (!OWNER_KEY) OWNER_KEY = crypto.randomBytes(6).toString('hex');
    await store.setMeta('owner_key', OWNER_KEY);
    if (store.kind !== 'postgres') { try { fs.writeFileSync(OWNER_FILE, OWNER_KEY + '\n'); } catch (e) {} }
  }
  ready = true;
  const ifs = Object.values(require('os').networkInterfaces()).flat().filter(i => i && i.family === 'IPv4' && !i.internal).map(i => i.address);
  log(`DROPKILL server em http://localhost:${PORT}  (rede local: ${ifs.map(a => `http://${a}:${PORT}`).join(', ') || '-'})`);
  log(`${db.accounts.length} contas carregadas (${store.kind === 'postgres' ? 'PostgreSQL' : DATA_FILE})`);
  if (store.kind !== 'postgres') log('AVISO: sem DATABASE_URL os dados ficam em arquivo local; no Render eles seriam perdidos a cada reinicio.');
  log(`Janela de matchmaking: ${MM_WAIT}s · ${MATCH_SIZE} participantes por partida (humanos + bots) · SOLO/DUO/SQUAD`);
  log(`Conta oficial "dropKILLOFC": para criá-la use a chave do dono: ${OWNER_KEY}${process.env.OWNER_KEY ? '  (variável OWNER_KEY)' : '  (salva no banco/arquivo)'}`);
}

let stopping = false;
async function shutdown(sig) {
  if (stopping) return; stopping = true;
  log(`${sig}: salvando dados e encerrando…`);
  ready = false;
  setTimeout(() => process.exit(1), 12000).unref();
  try { for (const ws of wss.clients) { try { ws.close(1012, 'restart'); } catch (e) {} } } catch (e) {}
  try { await store.close(); } catch (e) { log('erro ao salvar:', e.message); }
  log('dados salvos. Ate logo.');
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
main().catch(e => { console.error('Falha ao iniciar:', e); process.exit(1); });
