'use strict';
/*
  Server for the math game.
  - GET  /api/ping      health check
  - GET  /api/leaders   top 50 players by cups
  - POST /api/score     {name, cups, token} saves a player's cups
  - WS   /ws?room=CODE  relays messages between two players of an online duel
  - everything else serves the game from ./public (index.html)
*/
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const PUBLIC_DIR = path.join(__dirname, 'public');
const CUP_MAX = 100000;

/* ---------- storage: one JSON file ---------- */
let db = { players: {} };
try { db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (e) { /* first start */ }
if (!db.players) db.players = {};
let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    try { fs.writeFileSync(DATA_FILE, JSON.stringify(db)); } catch (e) { console.error('save failed:', e.message); }
  }, 500);
}
const sha = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');

/* ---------- tiny rate limit: 60 requests per minute per IP ---------- */
const hits = new Map();
function limited(ip) {
  const now = Date.now();
  const rec = hits.get(ip) || { n: 0, t: now };
  if (now - rec.t > 60000) { rec.n = 0; rec.t = now; }
  rec.n++;
  hits.set(ip, rec);
  return rec.n > 60;
}
setInterval(() => { const now = Date.now(); hits.forEach((v, k) => { if (now - v.t > 120000) hits.delete(k); }); }, 60000).unref();

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type'
};
function send(res, code, obj) {
  res.writeHead(code, Object.assign({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }, CORS));
  res.end(JSON.stringify(obj));
}
function readBody(req, cb) {
  let body = '';
  req.on('data', (c) => { body += c; if (body.length > 2000) req.destroy(); });
  req.on('end', () => { try { cb(JSON.parse(body)); } catch (e) { cb(null); } });
}

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.json': 'application/json', '.ico': 'image/x-icon' };

const server = http.createServer((req, res) => {
  const ip = req.headers['x-forwarded-for'] ? String(req.headers['x-forwarded-for']).split(',')[0].trim() : req.socket.remoteAddress;
  const url = new URL(req.url, 'http://localhost');

  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); return res.end(); }

  if (url.pathname === '/api/ping') return send(res, 200, { ok: true });

  if (url.pathname === '/api/leaders' && req.method === 'GET') {
    const top = Object.values(db.players)
      .sort((a, b) => b.cups - a.cups)
      .slice(0, 50)
      .map((p) => ({ name: p.name, cups: p.cups }));
    return send(res, 200, { ok: true, leaders: top });
  }

  if (url.pathname === '/api/score' && req.method === 'POST') {
    if (limited(ip)) return send(res, 429, { ok: false, error: 'too_many_requests' });
    return readBody(req, (b) => {
      if (!b) return send(res, 400, { ok: false, error: 'bad_json' });
      const name = String(b.name || '').trim().slice(0, 16);
      const token = String(b.token || '');
      const cups = Math.floor(Number(b.cups));
      if (!name || token.length < 8 || token.length > 64 || !Number.isFinite(cups) || cups < 0 || cups > CUP_MAX) {
        return send(res, 400, { ok: false, error: 'bad_input' });
      }
      const key = name.toLowerCase();
      const old = db.players[key];
      /* a name belongs to the device that took it first */
      if (old && old.h !== sha(token)) return send(res, 403, { ok: false, error: 'name_taken' });
      db.players[key] = { name, cups, h: old ? old.h : sha(token), t: Date.now() };
      save();
      return send(res, 200, { ok: true });
    });
  }

  /* static files: the game itself */
  let file = url.pathname === '/' ? '/index.html' : url.pathname;
  file = path.normalize(file).replace(/^(\.\.[\/\\])+/, '');
  const full = path.join(PUBLIC_DIR, file);
  if (!full.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end(); }
  fs.readFile(full, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(full)] || 'application/octet-stream' });
    res.end(data);
  });
});

/* ---------- online duel: relay between two sockets in a room ---------- */
/* A minimal WebSocket implementation (RFC 6455), so the server needs no npm packages. */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_PAYLOAD = 8192;
const rooms = new Map();

function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  return Buffer.concat([head, payload]);
}

class Sock {
  constructor(socket) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.open = true;
    this.alive = true;
    this.onmessage = null;
    this.onclose = null;
    socket.on('data', (d) => this.read(d));
    socket.on('close', () => this.finish());
    socket.on('error', () => this.finish());
  }
  send(text) { if (this.open) this.socket.write(frame(0x1, Buffer.from(text))); }
  ping() { if (this.open) this.socket.write(frame(0x9, Buffer.alloc(0))); }
  close() { if (this.open) { try { this.socket.write(frame(0x8, Buffer.alloc(0))); } catch (e) {} this.socket.end(); } }
  terminate() { this.socket.destroy(); }
  finish() {
    if (!this.open) return;
    this.open = false;
    if (this.onclose) this.onclose();
  }
  read(d) {
    this.buf = Buffer.concat([this.buf, d]);
    while (this.buf.length >= 2) {
      const b0 = this.buf[0], b1 = this.buf[1];
      const opcode = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { this.terminate(); return; } /* too large for this game */
      if (len > MAX_PAYLOAD || !masked) { this.terminate(); return; }
      if (this.buf.length < off + 4 + len) return;
      const mask = this.buf.slice(off, off + 4);
      const payload = Buffer.from(this.buf.slice(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) payload[i] ^= mask[i % 4];
      this.buf = this.buf.slice(off + 4 + len);
      if (opcode === 0x8) { this.close(); return; }
      if (opcode === 0x9) { this.socket.write(frame(0xA, payload)); continue; }
      if (opcode === 0xA) { this.alive = true; continue; }
      if (opcode === 0x1 && this.onmessage) this.onmessage(payload.toString('utf8'));
    }
  }
}

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://localhost');
  const key = req.headers['sec-websocket-key'];
  const room = String(url.searchParams.get('room') || '').toUpperCase();
  if (url.pathname !== '/ws' || !key || !/^[A-Z0-9]{4,8}$/.test(room)) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');

  const ws = new Sock(socket);
  const set = rooms.get(room) || new Set();
  if (set.size >= 2) { ws.send(JSON.stringify({ t: 'full' })); ws.close(); return; }
  set.add(ws);
  rooms.set(room, set);
  ws.send(JSON.stringify({ t: 'joined', n: set.size }));
  if (set.size === 2) set.forEach((s) => s.send(JSON.stringify({ t: 'ready' })));

  ws.onmessage = (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || m.t !== 'msg') return;
    set.forEach((s) => { if (s !== ws) s.send(JSON.stringify({ t: 'msg', d: m.d })); });
  };
  ws.onclose = () => {
    set.delete(ws);
    set.forEach((s) => s.send(JSON.stringify({ t: 'left' })));
    if (set.size === 0) rooms.delete(room);
  };
});

/* drop dead connections */
setInterval(() => {
  rooms.forEach((set) => set.forEach((ws) => {
    if (!ws.alive) return ws.terminate();
    ws.alive = false;
    ws.ping();
  }));
}, 30000).unref();

server.listen(PORT, () => console.log('Game server listening on port ' + PORT));
