'use strict';
/*
 * Armazenamento das contas do DROPKILL.
 *  - Com DATABASE_URL  -> PostgreSQL (persistente; use no Render)
 *  - Sem DATABASE_URL  -> arquivo accounts.json (apenas para rodar na sua maquina / rede local)
 *
 * O servidor mantem as contas em memoria (logica sincrona) e este modulo grava cada mudanca
 * no banco em segundo plano ("write-through" com tentativas automaticas), alem de fazer um
 * flush final quando o processo recebe SIGTERM/SIGINT (reinicios e deploys do Render).
 */
const fs = require('fs');

const LOCK_ID = 727274;           // trava (advisory lock) para so uma instancia escrever por vez
const FLUSH_DELAY = 150;          // ms de espera para agrupar gravacoes
const RETRY_DELAY = 3000;

const log = (...a) => console.log(new Date().toISOString().slice(11, 19), '[db]', ...a);

/* Linha <-> objeto de conta (campos desconhecidos vao para "extra", nada se perde) */
const KNOWN = new Set(['id', 'name', 'key', 'token', 'created', 'friends', 'mail']);
function toRow(a) {
  const extra = {};
  for (const k of Object.keys(a)) if (!KNOWN.has(k)) extra[k] = a[k];
  return [
    a.id, a.name, a.key, a.token, a.created || Date.now(),
    JSON.stringify(a.friends || []), JSON.stringify(a.mail || []), JSON.stringify(extra),
  ];
}
function fromRow(r) {
  const parse = v => (typeof v === 'string' ? JSON.parse(v) : v);
  return Object.assign({}, parse(r.extra) || {}, {
    id: Number(r.id), name: r.name, key: r.key, token: r.token, created: Number(r.created),
    friends: parse(r.friends) || [], mail: parse(r.mail) || [],
  });
}
const sigOf = a => JSON.stringify(toRow(a));

/* ======================================================================== PostgreSQL */
class PgStore {
  constructor(url) {
    const { Pool } = require('pg');
    let host = '';
    try { host = new URL(url).hostname; } catch (e) {}
    const local = !host || host === 'localhost' || host === '127.0.0.1' || host === '::1';
    const mode = (process.env.DATABASE_SSL || process.env.PGSSLMODE || '').toLowerCase();
    const ssl = mode === 'disable' ? false : (mode === 'require' || !local ? { rejectUnauthorized: false } : false);
    this.pool = new Pool({ connectionString: url, ssl, max: 5, idleTimeoutMillis: 30000, connectionTimeoutMillis: 15000, keepAlive: true });
    this.pool.on('error', e => log('erro numa conexao ociosa:', e.message));
    this.kind = 'postgres';
    this.sigs = new Map();      // id -> assinatura gravada
    this.known = new Set();     // ids que ja existem no banco
    this.metaSigs = new Map();
    this.timer = null; this.flushing = false; this.again = false; this.getDb = null;
    this.lockClient = null;
  }
  q(text, params) { return this.pool.query(text, params); }

  async connectWithRetry(tries = 20) {
    for (let i = 1; ; i++) {
      try { await this.q('SELECT 1'); return; }
      catch (e) {
        if (i >= tries) throw e;
        log(`banco indisponivel (${e.message}); nova tentativa ${i}/${tries - 1} em 3s`);
        await new Promise(r => setTimeout(r, 3000));
      }
    }
  }
  /* So uma instancia grava por vez: no deploy do Render a nova espera a antiga encerrar. */
  async acquireLock(maxWaitMs = 90000) {
    try {
      this.lockClient = await this.pool.connect();
      const t0 = Date.now();
      for (;;) {
        const r = await this.lockClient.query('SELECT pg_try_advisory_lock($1) AS ok', [LOCK_ID]);
        if (r.rows[0] && (r.rows[0].ok === true || r.rows[0].ok === 't')) return;
        if (Date.now() - t0 > maxWaitMs) { log('AVISO: outra instancia segue conectada; continuando mesmo assim.'); return; }
        if (!this._warned) { this._warned = true; log('aguardando a instancia anterior encerrar…'); }
        await new Promise(r => setTimeout(r, 1000));
      }
    } catch (e) { log('nao foi possivel obter a trava de instancia unica:', e.message); }
  }
  async init() {
    await this.connectWithRetry();
    await this.acquireLock();
    await this.q(`CREATE TABLE IF NOT EXISTS accounts (
      id         INTEGER PRIMARY KEY,
      name       TEXT    NOT NULL,
      key        TEXT    NOT NULL UNIQUE,
      token      TEXT    NOT NULL,
      created    BIGINT  NOT NULL,
      friends    JSONB   NOT NULL DEFAULT '[]'::jsonb,
      mail       JSONB   NOT NULL DEFAULT '[]'::jsonb,
      extra      JSONB   NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
    await this.q(`CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)`);
  }
  async load() {
    const r = await this.q('SELECT id, name, key, token, created, friends, mail, extra FROM accounts ORDER BY id');
    const accounts = r.rows.map(fromRow);
    const nid = await this.q(`SELECT v FROM meta WHERE k = 'next_id'`);
    let nextId = nid.rows[0] ? Number(nid.rows[0].v) : 100001;
    for (const a of accounts) { this.sigs.set(a.id, sigOf(a)); this.known.add(a.id); if (a.id >= nextId) nextId = a.id + 1; }
    this.metaSigs.set('next_id', String(nextId));
    return { nextId, accounts };
  }
  async getMeta(k) { const r = await this.q('SELECT v FROM meta WHERE k = $1', [k]); return r.rows[0] ? r.rows[0].v : null; }
  async setMeta(k, v) {
    await this.q('INSERT INTO meta (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v', [k, String(v)]);
    this.metaSigs.set(k, String(v));
  }
  /* Importa contas de um accounts.json sem apagar nem sobrescrever o que ja existe no banco. */
  async importAccounts(list, nextId) {
    let added = 0, skipped = 0;
    for (const a of list) {
      a.friends = a.friends || []; a.mail = a.mail || []; a.created = a.created || Date.now();
      const r = await this.q(
        `INSERT INTO accounts (id, name, key, token, created, friends, mail, extra)
         VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb) ON CONFLICT DO NOTHING`, toRow(a));
      if (r.rowCount) added++; else skipped++;
    }
    const max = list.reduce((m, a) => Math.max(m, a.id), 0);
    const want = Math.max(Number(nextId) || 0, max + 1);
    await this.q(`INSERT INTO meta (k, v) VALUES ('next_id', $1)
                  ON CONFLICT (k) DO UPDATE SET v = GREATEST(meta.v::bigint, EXCLUDED.v::bigint)::text`, [String(want)]);
    return { added, skipped };
  }
  async count() { const r = await this.q('SELECT count(*)::int AS n FROM accounts'); return Number(r.rows[0].n); }

  /* ---- gravacao ---- */
  bind(getDb) { this.getDb = getDb; }
  schedule() {
    clearTimeout(this.timer);
    this.timer = setTimeout(() => this.flush().catch(() => {}), FLUSH_DELAY);
  }
  async flush() {
    clearTimeout(this.timer);
    if (this.flushing) { this.again = true; return; }
    this.flushing = true;
    let failed = false;
    try {
      do {
        this.again = false;
        const db = this.getDb();
        for (const a of db.accounts) {
          const sig = sigOf(a);
          if (this.sigs.get(a.id) === sig) continue;
          try {
            if (this.known.has(a.id)) {
              await this.q(
                `UPDATE accounts SET name=$2, key=$3, token=$4, created=$5, friends=$6::jsonb, mail=$7::jsonb, extra=$8::jsonb, updated_at=now() WHERE id=$1`, toRow(a));
            } else {
              const r = await this.q(
                `INSERT INTO accounts (id, name, key, token, created, friends, mail, extra)
                 VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb) ON CONFLICT DO NOTHING`, toRow(a));
              if (!r.rowCount) throw Object.assign(new Error(`conta ${a.id} (${a.name}) conflita com outra ja existente no banco`), { code: 'CONFLICT' });
              this.known.add(a.id);
            }
            this.sigs.set(a.id, sig);
          } catch (e) {
            if (e.code === 'CONFLICT' || e.code === '23505') { log('ERRO (nao sera repetido):', e.message); this.sigs.set(a.id, sig); }
            else { failed = true; log(`falha ao gravar a conta ${a.id}:`, e.message); }
          }
        }
        const nid = String(db.nextId);
        if (this.metaSigs.get('next_id') !== nid) {
          try { await this.setMeta('next_id', nid); } catch (e) { failed = true; log('falha ao gravar next_id:', e.message); }
        }
      } while (this.again);
    } finally { this.flushing = false; }
    if (failed) { clearTimeout(this.timer); this.timer = setTimeout(() => this.flush().catch(() => {}), RETRY_DELAY); }
    return !failed;
  }
  async close() {
    try { await this.flush(); } catch (e) {}
    try { if (this.lockClient) { await this.lockClient.query('SELECT pg_advisory_unlock($1)', [LOCK_ID]); this.lockClient.release(); } } catch (e) {}
    try { await this.pool.end(); } catch (e) {}
  }
}

/* ======================================================================== Arquivo (modo local) */
class FileStore {
  constructor(file) { this.file = file; this.kind = 'arquivo'; this.timer = null; this.getDb = null; this.meta = {}; }
  async init() {}
  async load() {
    let raw = {};
    try { raw = JSON.parse(fs.readFileSync(this.file, 'utf8')); } catch (e) { /* primeira execucao */ }
    this.meta = raw.meta || {};
    return { nextId: raw.nextId || 100001, accounts: raw.accounts || [] };
  }
  async getMeta(k) { return this.meta[k] == null ? null : this.meta[k]; }
  async setMeta(k, v) { this.meta[k] = String(v); this.schedule(); }
  bind(getDb) { this.getDb = getDb; }
  schedule() {
    clearTimeout(this.timer);
    this.timer = setTimeout(() => { this.flush(); }, 100);
  }
  flush() {
    clearTimeout(this.timer);
    const db = this.getDb(), tmp = this.file + '.tmp';
    try { fs.writeFileSync(tmp, JSON.stringify(Object.assign({}, db, { meta: this.meta }), null, 1)); fs.renameSync(tmp, this.file); return true; }
    catch (e) { log('falha ao gravar', this.file, e.message); return false; }
  }
  async close() { this.flush(); }
}

function openStore(env = process.env, dataFile) {
  const url = env.DATABASE_URL || env.POSTGRES_URL || '';
  return url ? new PgStore(url) : new FileStore(dataFile);
}

module.exports = { openStore, PgStore, FileStore, fromRow, toRow };
