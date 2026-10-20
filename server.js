// Serveur relais de « Boule de feu » : il ne fait que transmettre les messages
// entre l'hôte d'une partie et ses invités (la partie elle-même tourne chez l'hôte).
const http = require('http');
const { WebSocketServer } = require('ws');

// ---- palmarès : victoires par nom, gravées dans un fichier et (si configuré) dans Upstash Redis ----
const fs = require('fs');
const path = require('path');
const WINS_FILE = path.join(__dirname, 'wins.json');
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL || '';
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const wins = new Map();                  // nom en minuscules -> { n, w }
const lastWin = new Map();               // ip -> date de la dernière victoire enregistrée
function redis(cmds) {
  return fetch(REDIS_URL + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + REDIS_TOKEN, 'content-type': 'application/json' },
    body: JSON.stringify(cmds)
  }).then((r) => r.json());
}
function pairs(a) { const o = {}; for (let i = 0; i + 1 < (a || []).length; i += 2) o[a[i]] = a[i + 1]; return o; }
function loadWins() {
  if (REDIS_URL && REDIS_TOKEN) {
    redis([['HGETALL', 'bdf:wins'], ['HGETALL', 'bdf:names']]).then((r) => {
      const w = pairs(r[0] && r[0].result), n = pairs(r[1] && r[1].result);
      Object.keys(w).forEach((k) => wins.set(k, { n: n[k] || k, w: parseInt(w[k], 10) || 0 }));
      console.log('Palmarès chargé depuis Redis : ' + wins.size + ' joueurs');
    }).catch((e) => console.log('Redis indisponible : ' + e));
    return;
  }
  try {
    JSON.parse(fs.readFileSync(WINS_FILE, 'utf8')).forEach((x) => wins.set(String(x.n).toLowerCase(), { n: String(x.n), w: x.w | 0 }));
  } catch (e) {}
}
let saveTm = 0;
function saveWins() {
  if (REDIS_URL && REDIS_TOKEN) return;
  if (saveTm) return;
  saveTm = setTimeout(() => {
    saveTm = 0;
    try { fs.writeFileSync(WINS_FILE, JSON.stringify(Array.from(wins.values()))); } catch (e) {}
  }, 1000);
}
function addWin(name) {
  const n = String(name || '').replace(/[<>&"]/g, '').trim().slice(0, 12);
  if (!n) return;
  const k = n.toLowerCase();
  if (!wins.has(k) && wins.size >= 500) return;
  const e = wins.get(k) || { n, w: 0 };
  e.w++;
  wins.set(k, e);
  if (REDIS_URL && REDIS_TOKEN) redis([['HINCRBY', 'bdf:wins', k, 1], ['HSET', 'bdf:names', k, e.n]]).catch(() => {});
  else saveWins();
}
function topWins() {
  return Array.from(wins.values()).sort((a, b) => b.w - a.w).slice(0, 10);
}
loadWins();

const server = http.createServer((req, res) => {
  const url0 = (req.url || '').split('?')[0];
  if (req.method === 'OPTIONS') {
    res.writeHead(204, { 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type', 'access-control-allow-methods': 'GET,POST' });
    res.end();
    return;
  }
  if (url0 === '/wins') {
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'cache-control': 'no-store' });
    res.end(JSON.stringify(topWins()));
    return;
  }
  if (url0 === '/win' && req.method === 'POST') {
    let body = '';
    req.on('data', (c) => { body += c; if (body.length > 500) req.destroy(); });
    req.on('end', () => {
      try {
        const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
        const now = Date.now();
        if (now - (lastWin.get(ip) || 0) >= 5000) {   // une victoire toutes les 5 s au maximum par adresse
          lastWin.set(ip, now);
          if (lastWin.size > 2000) lastWin.clear();
          addWin(JSON.parse(body).n);
        }
      } catch (e) {}
      res.writeHead(204, { 'access-control-allow-origin': '*' });
      res.end();
    });
    return;
  }
  if (url0 === '/rooms') {
    // parties ouvertes (pas encore lancées, avec de la place)
    const list = [];
    rooms.forEach((r, code) => {
      const p = r.hostPres || {};
      if (!p.ph && r.guests.size < MAX_GUESTS_LOBBY) {
        list.push({ room: code, n: String(p.n || '').slice(0, 12), count: 1 + r.guests.size });
      }
    });
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'cache-control': 'no-store' });
    res.end(JSON.stringify(list));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'access-control-allow-origin': '*' });
  res.end('Boule de feu : serveur en marche\n');
});
const MAX_GUESTS_LOBBY = 7;               // 8 joueurs au maximum, hôte compris
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
const rooms = new Map();                 // code -> { host, hostPres, guests: Map(id -> ws) }
const MAX_GUESTS = 7;

function send(ws, m) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(m)); }
function listRooms() {
  const out = [];
  rooms.forEach((r, code) => {
    const p = r.hostPres || {};
    out.push({ code, name: typeof p.n === 'string' ? p.n.slice(0, 12) : 'Joueur', count: 1 + r.guests.size, started: !!p.ph });
  });
  return out.slice(0, 30);
}
function rid() { return Math.random().toString(36).slice(2, 8); }

wss.on('connection', (ws) => {
  ws.alive = true;
  ws.joined = false;
  ws.room = null; ws.role = null; ws.id = null;
  ws.count = 0;
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', (raw) => {
    if (++ws.count > 200) { ws.close(); return; }       // plus de 200 messages par seconde : coupé
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (!ws.joined) {
      if (m.t === 'list') { send(ws, { t: 'rooms', rooms: listRooms() }); setTimeout(() => ws.close(), 200); return; }
      if (m.t !== 'join' || typeof m.room !== 'string' || !/^[a-z0-9]{3,12}$/.test(m.room)) { send(ws, { t: 'err', code: 'bad' }); return; }
      let r = rooms.get(m.room);
      if (m.role === 'h') {
        if (r) { send(ws, { t: 'err', code: 'taken' }); return; }
        r = { host: ws, hostPres: {}, guests: new Map() };
        rooms.set(m.room, r);
        ws.id = 'host';
        send(ws, { t: 'ok', id: 'host' });
      } else if (m.role === 'g') {
        if (!r) { send(ws, { t: 'err', code: 'nohost' }); return; }
        if (r.guests.size >= MAX_GUESTS) { send(ws, { t: 'err', code: 'full' }); return; }
        ws.id = rid();
        r.guests.set(ws.id, ws);
        send(ws, { t: 'ok', id: ws.id, pr: r.hostPres });
        send(r.host, { t: 'join', id: ws.id });
      } else { send(ws, { t: 'err', code: 'bad' }); return; }
      ws.room = m.room; ws.role = m.role; ws.joined = true;
      return;
    }

    const r = rooms.get(ws.room);
    if (!r || m.t !== 'pr' || !m.pr || typeof m.pr !== 'object') return;
    if (ws.role === 'h') {
      Object.assign(r.hostPres, m.pr);
      const out = JSON.stringify({ t: 'pr', pr: m.pr });
      r.guests.forEach((g) => { if (g.readyState === 1) g.send(out); });
    } else {
      send(r.host, { t: 'g', id: ws.id, pr: m.pr });
    }
  });

  ws.on('close', () => {
    if (!ws.joined) return;
    const r = rooms.get(ws.room);
    if (!r) return;
    if (ws.role === 'h') {
      r.guests.forEach((g) => { send(g, { t: 'hostleft' }); g.close(); });
      rooms.delete(ws.room);
    } else {
      r.guests.delete(ws.id);
      send(r.host, { t: 'leave', id: ws.id });
    }
  });
  ws.on('error', () => {});
});

// remet les compteurs à zéro chaque seconde ; ping régulier pour garder les connexions ouvertes
setInterval(() => { wss.clients.forEach((c) => { c.count = 0; }); }, 1000);
setInterval(() => {
  wss.clients.forEach((c) => {
    if (!c.alive) { c.terminate(); return; }
    c.alive = false;
    try { c.ping(); } catch (e) {}
  });
}, 25000);

const port = process.env.PORT || 8080;
server.listen(port, () => console.log('Serveur prêt sur le port ' + port));
