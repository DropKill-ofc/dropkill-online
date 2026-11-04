'use strict';
/*
 * Migra um accounts.json antigo para o PostgreSQL, sem apagar nem sobrescrever contas ja existentes no banco.
 *   DATABASE_URL=postgres://... node migrate.js [caminho/accounts.json]
 * (O servidor tambem faz isso sozinho na inicializacao quando encontra accounts.json ao lado do server.js.)
 */
const fs = require('fs');
const path = require('path');
const { PgStore } = require('./store');
const url = process.env.DATABASE_URL;
if (!url) { console.error('Defina DATABASE_URL (use a "External Database URL" do Render para rodar na sua maquina).'); process.exit(1); }
const file = path.resolve(process.argv[2] || path.join(__dirname, 'accounts.json'));
(async () => {
  const data = JSON.parse(fs.readFileSync(file, 'utf8'));
  const st = new PgStore(url);
  await st.init();
  const r = await st.importAccounts(data.accounts || [], data.nextId);
  console.log(`${r.added} contas importadas, ${r.skipped} ja existiam. Total no banco: ${await st.count()}`);
  await st.close();
})().catch(e => { console.error('Falha:', e.message); process.exit(1); });
