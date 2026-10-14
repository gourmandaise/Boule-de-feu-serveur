// Serveur relais de « Boule de feu » : il ne fait que transmettre les messages
// entre l'hôte d'une partie et ses invités (la partie elle-même tourne chez l'hôte).
const http = require('http');
const { WebSocketServer } = require('ws');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Boule de feu : serveur en marche\n');
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
const rooms = new Map();                 // code -> { host, hostPres, guests: Map(id -> ws) }
const MAX_GUESTS = 8;

function send(ws, m) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(m)); }
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
