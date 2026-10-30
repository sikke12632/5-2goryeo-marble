// 사회 마블 온라인 방 서버 — Cloudflare Worker + Durable Object 하나(Hub)가 모든 방을 맡는다.
// 게임 규칙은 모른다. 차례인 사람(또는 응답이 없을 때 대신 하는 사람)이 보낸 판 상태를
// 차례 번호(ver)로 확인해 받아 두고 모두에게 퍼뜨린다. 시간은 이 서버 시각 하나로 잰다.
import { DurableObject } from 'cloudflare:workers';

const MAX_PLAYERS = 4;
const IDLE_MS = 45000;    // 차례인 사람이 이만큼 아무것도 안 하면 다른 사람 화면이 대신 진행한다
const AWAY_MS = 4000;     // 차례인 사람이 나갔거나 연결이 끊겼을 때
const AUTO_MS = 2500;     // 대신 진행하는 중에는 한 단계씩 이 간격으로
const WAIT_GONE_MS = 3 * 60000;   // 대기실에서 연결이 끊긴 채 이만큼 지나면 자리에서 뺀다
const VOTE_MS = 20000;            // 방장 제안(되돌리기·돈 조정 등)에 답할 시간
const ANIMALS = ['cat','dog','rabbit','tiger','lion','panda','fox','bear','koala','penguin',
  'monkey','pig','chick','frog','owl','horse','hamster','whale','elephant','octopus'];

const clean = s => String(s ?? '').replace(/[\u0000-\u001f<>&"'`]/g, '').trim().slice(0, 8);
const bool = (v, d) => typeof v === 'boolean' ? v : d;
function settingsOf(x = {}) {
  const m = Math.round(Number(x.minutes) / 5) * 5;
  return {
    minutes: m >= 5 && m <= 30 ? m : 10,
    sets: Array.isArray(x.sets) ? x.sets.filter(s => typeof s === 'string' && s.length < 20).slice(0, 80) : [],
    catchup: bool(x.catchup, true), share: bool(x.share, true),
    diceOrder: bool(x.diceOrder, true), bankrupt: bool(x.bankrupt, false),
  };
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/ws') {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('websocket only', { status: 426 });
      return env.HUB.get(env.HUB.idFromName('hub')).fetch(req);
    }
    return new Response('사회 마블 서버가 켜져 있어요.', { headers: { 'content-type': 'text/plain; charset=utf-8' } });
  },
};

export class Hub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.rooms = new Map();
    ctx.blockConcurrencyWhile(async () => {
      const all = await ctx.storage.list({ prefix: 'room:' });
      for (const r of all.values()) this.rooms.set(r.id, r);
    });
    // 연결 유지용 핑은 깨우지 않고 바로 답한다
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"t":"ping"}', '{"t":"pong"}'));
  }

  async fetch() {
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ pid: null, room: null });
    return new Response(null, { status: 101, webSocket: client });
  }

  /* ---------- 도우미 ---------- */
  me(ws) { return ws.deserializeAttachment() || {}; }
  send(ws, o) { try { ws.send(JSON.stringify({ ...o, now: Date.now() })); } catch (e) {} }
  socketsIn(id) { return this.ctx.getWebSockets().filter(ws => ws.readyState === 1 && this.me(ws).room === id); }
  onlineIn(id) { return new Set(this.socketsIn(id).map(ws => this.me(ws).pid)); }
  store(r) { r.active = Date.now(); this.ctx.storage.put('room:' + r.id, r); }
  drop(r) { this.rooms.delete(r.id); this.ctx.storage.delete('room:' + r.id); }
  async ensureAlarm() { if (!(await this.ctx.storage.getAlarm())) await this.ctx.storage.setAlarm(Date.now() + 60000); }
  roomOf(pid) { for (const r of this.rooms.values()) if (r.players.some(p => p.pid === pid && !p.left)) return r; return null; }

  // 받는 사람마다 '나'의 자리 번호가 다르다. 다른 사람의 pid(신분 열쇠)는 보내지 않는다.
  view(r, pid) {
    const on = this.onlineIn(r.id), v = r.vote;
    return {
      id: r.id, title: r.title, status: r.status, settings: r.settings, max: MAX_PLAYERS, pw: r.pw || '',
      host: r.players.findIndex(p => p.pid === r.host), you: r.players.findIndex(p => p.pid === pid),
      players: r.players.map(p => ({ name: p.name, animal: p.animal, left: !!p.left, on: on.has(p.pid) })),
      ver: r.ver, state: r.state, endAt: r.endAt, deadline: r.deadline, pausedAt: r.pausedAt || 0,
      vote: v ? { id: v.id, text: v.text, until: v.until, n: this.voters(r).size, yes: v.yes.length, no: v.no.length,
        mine: v.yes.includes(pid) ? 'yes' : v.no.includes(pid) ? 'no' : null } : null,
    };
  }
  // 방장 제안에 답할 사람: 지금 연결돼 있고 나가지 않은 사람 (방장 포함)
  voters(r) { const on = this.onlineIn(r.id); return new Set(r.players.filter(p => !p.left && on.has(p.pid)).map(p => p.pid)); }
  // 과반이 동의하면 통과, 과반이 될 수 없으면 취소
  tally(r) {
    const v = r.vote, who = this.voters(r), n = who.size;
    const yes = v.yes.filter(p => who.has(p)).length, no = v.no.filter(p => who.has(p)).length;
    if (yes * 2 > n) return this.resolve(r, true);
    if (no * 2 >= n || Date.now() >= v.until) return this.resolve(r, false);
    this.store(r); this.roomcast(r);
  }
  resolve(r, pass) {
    const v = r.vote; r.vote = null; this.store(r);
    for (const ws of this.socketsIn(r.id)) this.send(ws, { t: 'voted', pass, act: v.act, text: v.text });
    this.roomcast(r);
  }
  summary(r) {
    return { id: r.id, title: r.title, status: r.status, max: MAX_PLAYERS, lock: !!r.pw,
      players: r.players.filter(p => !p.left).map(p => ({ name: p.name, animal: p.animal })) };
  }
  roomcast(r) { for (const ws of this.socketsIn(r.id)) this.send(ws, { t: 'room', room: this.view(r, this.me(ws).pid) }); }
  lobby() {
    const list = [...this.rooms.values()].filter(r => r.players.some(p => !p.left))
      .sort((a, b) => (a.status === 'wait' ? 0 : 1) - (b.status === 'wait' ? 0 : 1) || b.created - a.created)
      .map(r => this.summary(r));
    for (const ws of this.ctx.getWebSockets()) { const m = this.me(ws); if (m.pid && !m.room) this.send(ws, { t: 'rooms', list }); }
  }
  enter(ws, r) { try { const m = this.me(ws); m.room = r ? r.id : null; ws.serializeAttachment(m); } catch (e) {} }

  // 차례인 사람이 없으면(나감·끊김) 빨리 넘긴다
  turnPlayer(r) { return r.state && r.players[r.state.turn]; }
  awayNow(r, p) { return !p || p.left || !this.onlineIn(r.id).has(p.pid); }

  /* ---------- 메시지 ---------- */
  async webSocketMessage(ws, raw) {
    if (typeof raw !== 'string' || raw.length > 300000) return;
    let msg; try { msg = JSON.parse(raw); } catch (e) { return; }
    const m = this.me(ws), now = Date.now();
    if (msg.t !== 'hello' && !m.pid) return;
    const r = m.room ? this.rooms.get(m.room) : null;
    const err = text => this.send(ws, { t: 'err', msg: text });

    switch (msg.t) {
      case 'hello': {
        if (!/^[a-z0-9]{8,32}$/.test(msg.pid || '')) return;
        m.pid = msg.pid; m.name = clean(msg.name) || '친구'; m.animal = ANIMALS.includes(msg.animal) ? msg.animal : 'cat';
        ws.serializeAttachment(m);
        const mine = this.roomOf(m.pid);
        if (mine) {
          const p = mine.players.find(x => x.pid === m.pid);
          if (mine.status === 'wait') { p.name = m.name; p.animal = m.animal; }
          delete p.off;
          this.enter(ws, mine); this.store(mine); this.roomcast(mine); this.lobby();
        } else { this.enter(ws, null); this.send(ws, { t: 'hello' }); this.lobby(); }
        break;
      }
      case 'rooms': this.enter(ws, null); this.lobby(); break;
      case 'create': {
        if (r) this.leave(ws, r);
        let id; do { id = String(1000 + Math.floor(Math.random() * 9000)); } while (this.rooms.has(id));
        // 비밀번호: 숫자 4자리 (안 쓰면 빈칸). 방 안 사람들에게는 보여 주고, 목록에는 자물쇠만
        const room = { id, title: `${m.name}의 방`, host: m.pid, status: 'wait', created: now, pw: /^\d{4}$/.test(msg.pw || '') ? msg.pw : '',
          players: [{ pid: m.pid, name: m.name, animal: m.animal }], settings: settingsOf(msg.settings),
          ver: 0, state: null, endAt: 0, deadline: 0 };
        this.rooms.set(id, room); this.enter(ws, room); this.store(room);
        this.roomcast(room); this.lobby(); await this.ensureAlarm();
        break;
      }
      case 'join': {
        const room = this.rooms.get(String(msg.id));
        if (!room) return err('방이 없어졌어요.');
        if (r && r !== room) this.leave(ws, r);
        const p = room.players.find(x => x.pid === m.pid);
        if (p) { p.left = false; delete p.off; }
        else {
          if (room.status !== 'wait') return err('이미 게임을 시작한 방이에요.');
          if (room.players.length >= MAX_PLAYERS) return err('방이 꽉 찼어요.');
          if (room.pw && String(msg.pw || '') !== room.pw) { this.send(ws, { t: 'err', msg: '비밀번호가 틀렸어요.', code: 'pw' }); return; }
          room.players.push({ pid: m.pid, name: m.name, animal: m.animal });
        }
        this.enter(ws, room); this.store(room); this.roomcast(room); this.lobby();
        break;
      }
      case 'leave': if (r) this.leave(ws, r); this.lobby(); break;
      case 'settings': {
        if (!r || r.host !== m.pid || r.status !== 'wait') return;
        r.settings = settingsOf(msg.settings); this.store(r); this.roomcast(r);
        break;
      }
      case 'start': {
        if (!r || r.host !== m.pid || r.status !== 'wait') return;
        const st = msg.state;
        if (r.players.length < 2) return err('2명 이상 모여야 시작할 수 있어요.');
        if (!st || !Array.isArray(st.teams) || st.teams.length !== r.players.length) return err('시작하지 못했어요. 다시 눌러 주세요.');
        r.status = 'play'; r.state = st; r.ver = 1;
        r.endAt = now + r.settings.minutes * 60000;
        r.deadline = now + (this.awayNow(r, this.turnPlayer(r)) ? AWAY_MS : IDLE_MS);
        this.store(r); this.roomcast(r); this.lobby();
        break;
      }
      case 'push': {
        if (!r || r.status === 'wait' || !r.state) return;
        const st = msg.state;
        if (!st || !Array.isArray(st.teams) || st.teams.length !== r.players.length) return;
        const owner = this.turnPlayer(r), isOwner = owner && owner.pid === m.pid;
        const allowed = isOwner || r.host === m.pid || (now >= r.deadline && !r.pausedAt) || (st.phase === 'over' && now >= r.endAt);
        if (msg.base !== r.ver || !allowed) { this.send(ws, { t: 'room', room: this.view(r, m.pid), resync: true }); return; }
        const sameTurn = r.state.turn === st.turn;
        r.state = st; r.ver++;
        if (st.phase === 'over') r.status = 'over';
        const next = this.turnPlayer(r);
        r.deadline = now + (this.awayNow(r, next) ? AWAY_MS : (!isOwner && sameTurn ? AUTO_MS : IDLE_MS));
        this.store(r);
        this.send(ws, { t: 'ack', ver: r.ver, deadline: r.deadline });
        for (const o of this.socketsIn(r.id)) if (o !== ws)
          this.send(o, { t: 'state', ver: r.ver, state: st, ev: Array.isArray(msg.ev) ? msg.ev.slice(0, 20) : [], deadline: r.deadline, endAt: r.endAt });
        if (r.status === 'over') this.lobby();
        break;
      }
      case 'propose': {
        if (!r || r.host !== m.pid || r.status !== 'play' || r.vote) return;
        const act = msg.act && typeof msg.act === 'object' && JSON.stringify(msg.act).length < 300 ? msg.act : null;
        if (!act) return;
        r.vote = { id: now, act, text: String(msg.text || '').replace(/[<>]/g, '').slice(0, 80), yes: [m.pid], no: [], until: now + VOTE_MS };
        this.tally(r);
        break;
      }
      case 'vote': {
        if (!r || !r.vote || r.vote.id !== msg.id) return;
        const v = r.vote;
        v.yes = v.yes.filter(p => p !== m.pid); v.no = v.no.filter(p => p !== m.pid);
        (msg.yes ? v.yes : v.no).push(m.pid);
        this.tally(r);
        break;
      }
      case 'voteCheck': if (r && r.vote) this.tally(r); break;
      case 'pause': {
        // 멈추기는 동의를 받은 뒤 방장 화면이 보낸다. 다시 시작은 방장 혼자 할 수 있다.
        if (!r || r.host !== m.pid || r.status !== 'play') return;
        if (msg.on && !r.pausedAt) r.pausedAt = now;
        if (!msg.on && r.pausedAt) { const d = now - r.pausedAt; r.endAt += d; r.deadline += d; r.pausedAt = 0; }
        this.store(r); this.roomcast(r);
        break;
      }
      case 'again': {
        if (!r || r.host !== m.pid || r.status !== 'over') return;
        r.status = 'wait'; r.state = null; r.ver = 0; r.endAt = 0; r.deadline = 0; r.pausedAt = 0; r.vote = null;
        r.players = r.players.filter(p => !p.left);
        this.store(r); this.roomcast(r); this.lobby();
        break;
      }
    }
  }

  leave(ws, r) {
    const m = this.me(ws), p = r.players.find(x => x.pid === m.pid);
    // 같은 사람이 연 다른 화면도 함께 방에서 나간다
    for (const o of this.socketsIn(r.id)) if (this.me(o).pid === m.pid) { this.enter(o, null); }
    if (p) {
      if (r.status === 'wait') r.players = r.players.filter(x => x !== p);
      else {
        p.left = true;
        if (this.turnPlayer(r) === p) r.deadline = Math.min(r.deadline, Date.now() + AWAY_MS);
      }
    }
    const live = r.players.filter(x => !x.left);
    if (!live.length) { this.drop(r); return; }
    if (r.host === m.pid) r.host = live[0].pid;
    this.store(r); this.roomcast(r);
  }

  async webSocketClose(ws) { this.gone(ws); }
  async webSocketError(ws) { this.gone(ws); }
  gone(ws) {
    const m = this.me(ws); if (!m.room) return;
    const r = this.rooms.get(m.room); if (!r) return;
    this.enter(ws, null);
    if (this.onlineIn(r.id).has(m.pid)) return;      // 다른 화면으로 아직 들어와 있다
    const p = r.players.find(x => x.pid === m.pid); if (!p) return;
    p.off = Date.now();
    if (r.status === 'play' && this.turnPlayer(r) === p) r.deadline = Math.min(r.deadline, Date.now() + AWAY_MS);
    if (r.vote) { this.tally(r); return; }      // 답할 사람 수가 줄었다
    this.store(r); this.roomcast(r);
  }

  // 1분마다: 대기실에서 오래 끊긴 사람 빼기, 빈 방·끝난 방·너무 오래된 방 지우기
  async alarm() {
    const now = Date.now();
    let changed = false;
    for (const r of [...this.rooms.values()]) {
      const on = this.onlineIn(r.id);
      if (r.status === 'wait') {
        const before = r.players.length;
        r.players = r.players.filter(p => on.has(p.pid) || !(p.off && now - p.off > WAIT_GONE_MS));
        if (r.players.length !== before) {
          changed = true;
          if (!r.players.length) { this.drop(r); continue; }
          if (!r.players.some(p => p.pid === r.host)) r.host = r.players[0].pid;
          this.store(r); this.roomcast(r);
        }
      }
      const idle = now - (r.active || r.created);
      if ((!on.size && idle > 15 * 60000) || (r.status === 'over' && idle > 30 * 60000) || now - r.created > 4 * 3600000) {
        for (const ws of this.socketsIn(r.id)) { this.enter(ws, null); this.send(ws, { t: 'gone' }); }
        this.drop(r); changed = true;
      }
    }
    if (changed) this.lobby();
    if (this.rooms.size) await this.ctx.storage.setAlarm(now + 60000);
  }
}
