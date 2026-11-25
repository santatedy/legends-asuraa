# -*- coding: utf-8 -*-
"""
Легенды Асура — онлайн-сервер.
Чистый Python (stdlib): HTTP + JSON + WebSocket, без внешних зависимостей.

Эндпоинты:
  POST /api/register   {nick, pass} -> создаёт аккаунт
  POST /api/login      {nick, pass} -> токен
  GET  /api/leaderboard           -> топ игроков по уровню/золоту
  PUT  /api/save        (token) {state,...} -> сохранить прогресс
  GET  /api/load?token= -> получить прогресс
  WS   /ws?nick=...    чат и PvP-бои
"""
import json
import os
import re
import hashlib
import threading
import struct
import base64
import time
import sys
import urllib.request
import urllib.error

# Облачный стор (JSONBin) как best-effort-бэкап поверх локального файла.
# Если не заданы — работает только локально. Ставит BIN_ID и BIN_KEY.
BIN_ID = os.environ.get("BIN_ID", "")
BIN_KEY = os.environ.get("BIN_KEY", "")
BIN_URL = ("https://api.jsonbin.io/v3/b/" + BIN_ID) if BIN_ID else None
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from socketserver import ThreadingMixIn

DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "asura_data.json")
TOKENS = {}          # token -> nick
WS_CLIENTS = {}      # nick -> {ws, conn}
FIGHTS = {}          # fight_id -> {players:[nick,nick], ...}
FIGHT_QUEUE = []     # ники, ожидающие PvP


def _cloud_fetch():
    """Тянем базу из JSONBin. Возвращает dict или None."""
    if not BIN_URL or not BIN_KEY:
        return None
    try:
        req = urllib.request.Request(BIN_URL, headers={"X-Master-Key": BIN_KEY})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        return data.get("record")
    except Exception as e:
        print("cloud fetch failed:", repr(e), file=sys.stderr)
        return None


def _cloud_push(db):
    """Пуш базы в JSONBin. Best-effort: не должен ломать игру."""
    if not BIN_URL or not BIN_KEY:
        return
    try:
        body = json.dumps(db, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            BIN_URL, data=body, method="PUT",
            headers={
                "X-Master-Key": BIN_KEY,
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            r.read()
    except Exception as e:
        print("cloud push failed:", repr(e), file=sys.stderr)


def load_db():
    # Ненадёжный локальный файл — предпочитаем файл, чтобы не зависеть от сети при старте.
    local = None
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                local = json.load(f)
        except Exception:
            pass
    # Если есть облако и локального файла нет / он пуст — берём из облака.
    if local in (None, {"accounts": {}, "saves": {}}):
        cloud = _cloud_fetch()
        if cloud and isinstance(cloud, dict):
            local = cloud
    db = local if local else {"accounts": {}, "saves": {}}
    # Владелец всегда должен существовать, даже на чистой файловой системе.
    # hash_pass определён ниже (после DB = load_db()), поэтому хэшируем напрямую.
    owner = "Santa Tedis"
    if owner not in db["accounts"]:
        pwd_hash = hashlib.sha256("santaOwner777".encode("utf-8")).hexdigest()
        db["accounts"][owner] = {"pass": pwd_hash, "role": "owner"}
        db["saves"].setdefault(owner, {})
        save_db(db)
    return db


def save_db(db):
    # Локальная запись — быстро и надёжно. Облачный пуш — в фоне, чтобы
    # медленное/недоступное облако не блокировало server (save вызывается под DB_LOCK).
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False)
    os.replace(tmp, DATA_FILE)
    threading.Thread(target=_cloud_push, args=(db,), daemon=True).start()


DB = load_db()
DB_LOCK = threading.Lock()


def hash_pass(p):
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def new_token():
    return base64.urlsafe_b64encode(os.urandom(18)).decode("ascii")


# ---------------- WebSocket (RFC 6455) ----------------
WS_LOCK = threading.Lock()


class WSConnection:
    """Один WebSocket-клиент поверх сокета."""

    def __init__(self, sock, headers):
        self.sock = sock
        self.headers = headers
        self.buf = b""

    def read_exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        out = self.buf[:n]
        self.buf = self.buf[n:]
        return out

    def send_text(self, text):
        payload = text.encode("utf-8")
        header = bytearray([0x81])
        length = len(payload)
        if length < 126:
            header.append(length)
        elif length < 65536:
            header.append(126)
            header += struct.pack(">H", length)
        else:
            header.append(127)
            header += struct.pack(">Q", length)
        try:
            self.sock.sendall(bytes(header) + payload)
        except Exception:
            pass

    def recv_frame(self):
        hdr = self.read_exact(2)
        fin_op = hdr[0]
        opcode = fin_op & 0x0F
        masked = hdr[1] & 0x80
        length = hdr[1] & 0x7F
        if length == 126:
            length = struct.unpack(">H", self.read_exact(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", self.read_exact(8))[0]
        mask = self.read_exact(4) if masked else None
        data = self.read_exact(length)
        if mask:
            data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        return opcode, data


def build_ws_handshake_response(headers):
    key = ""
    for line in headers["raw_headers"]:
        if line.lower().startswith("sec-websocket-key:"):
            key = line.split(":", 1)[1].strip()
            break
    accept = base64.b64encode(hashlib.sha1(
        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
    ).digest()).decode("ascii")
    return (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
    ).encode("ascii")


# ---------------- Обработка сообщений WS ----------------
def ws_handle(nick, conn):
    try:
        opcode, payload = conn.recv_frame()
        while opcode != 8:  # закрытие
            if opcode == 9:  # ping
                conn.send_text("")  # pong
                opcode, payload = conn.recv_frame()
                continue
            if opcode == 1:
                try:
                    msg = json.loads(payload.decode("utf-8"))
                    if msg.get("type") == "chat":
                        data = msg.get("text", "")[:500]
                        broadcast_chat(nick, data)
                    elif msg.get("type") == "queue_pvp":
                        queue_pvp(nick, conn)
                    elif msg.get("type") == "fight_move":
                        handle_fight_move(nick, msg)
                    else:
                        print("WS unhandled type:", msg.get("type"), file=sys.stderr)
                except Exception as ex:
                    print("WS handle err:", repr(ex), file=sys.stderr)
            opcode, payload = conn.recv_frame()
    except Exception:
        pass
    finally:
        with WS_LOCK:
            if WS_CLIENTS.get(nick) == conn:
                del WS_CLIENTS[nick]
            if nick in FIGHT_QUEUE:
                FIGHT_QUEUE.remove(nick)


def send_to(nick, data):
    with WS_LOCK:
        conn = WS_CLIENTS.get(nick)
    if conn:
        try:
            conn.send_text(json.dumps(data, ensure_ascii=False))
            print("WS sent to", nick, "type", data.get("type"), file=sys.stderr)
        except Exception as e:
            print("WS send failed to", nick, repr(e), file=sys.stderr)
    else:
        print("WS no conn for", nick, "type", data.get("type"), file=sys.stderr)


def broadcast_chat(nick, text):
    data = {"type": "chat", "nick": nick, "text": text, "ts": int(time.time())}
    with WS_LOCK:
        for n in list(WS_CLIENTS.keys()):
            c = WS_CLIENTS[n]
            try:
                c.send_text(json.dumps(data, ensure_ascii=False))
            except Exception:
                pass


def queue_pvp(nick, conn):
    match = None
    with WS_LOCK:
        if nick in FIGHT_QUEUE:
            return
        FIGHT_QUEUE.append(nick)
        if len(FIGHT_QUEUE) >= 2:
            match = (FIGHT_QUEUE.pop(0), FIGHT_QUEUE.pop(0))
    if match:
        p1, p2 = match
        print("PVP MATCH:", p1, "vs", p2, file=sys.stderr)
        # start_fight -> send_to acquires WS_LOCK again (non-reentrant), so run it outside the lock.
        try:
            start_fight(p1, p2)
            print("PVP start_fight OK", p1, p2, file=sys.stderr)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print("PVP start_fight ERROR:", repr(e), file=sys.stderr)


def start_fight(a, b):
    fid = "f" + base64.urlsafe_b64encode(os.urandom(6)).decode("ascii")
    FIGHTS[fid] = {
        "players": [a, b],
        "hp": {a: 100, b: 100},
        "turn": a,
        "state": "active",
    }
    for n in (a, b):
        send_to(n, {"type": "fight_start", "fight_id": fid, "opponent": (b if n == a else a), "hp": 100, "your_turn": (n == a)})


def handle_fight_move(nick, msg):
    fid = msg.get("fight_id")
    f = FIGHTS.get(fid) if fid else None
    if f is None:
        # Клиент не шлёт fight_id в fight_move; у игрока не более одного активного боя.
        for cand in FIGHTS.values():
            if nick in cand["players"] and cand["state"] == "active":
                f = cand
                break
    if not f or f["state"] != "active":
        return
    if f["turn"] != nick:
        send_to(nick, {"type": "fight_err", "text": "Не ваш ход"})
        return
    dmg = int(msg.get("dmg", 10))
    opponent = f["players"][0] if f["players"][1] == nick else f["players"][1]
    f["hp"][opponent] -= max(1, min(50, dmg))
    if f["hp"][opponent] <= 0:
        f["state"] = "over"
        winner = nick
        apply_fight_rewards(winner)
        for n in f["players"]:
            send_to(n, {"type": "fight_over", "winner": winner, "hp": f["hp"]})
    else:
        f["turn"] = opponent
        for n in f["players"]:
            send_to(n, {"type": "fight_state", "hp": f["hp"], "turn": f["turn"]})


def apply_fight_rewards(winner):
    with DB_LOCK:
        sv = DB["saves"].get(winner)
        if sv is None:
            return
        gold = 20
        xp = 40
        sv.setdefault("gold", 0)
        sv.setdefault("xp", 0)
        sv["gold"] += gold
        sv["xp"] += xp
        save_db(DB)
    send_to(winner, {"type": "fight_reward", "gold": gold, "xp": xp})


# ---------------- HTTP handler ----------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send_json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length > 0:
            return self.rfile.read(length).decode("utf-8")
        return ""

    def _serve_static(self, path):
        """Отдаём игровой клиент (плюс чат/лидеры) прямо с сервера."""
        base = os.path.dirname(os.path.abspath(__file__))
        if path in ("", "/"):
            path = "/index.html"
        fp = os.path.join(base, path.lstrip("/"))
        if not os.path.abspath(fp).startswith(base):
            self._send_json(403, {"error": "forbidden"})
            return
        if not os.path.isfile(fp):
            self._send_json(404, {"error": "not found"})
            return
        ct = "text/html; charset=utf-8"
        if fp.endswith(".js"):
            ct = "application/javascript; charset=utf-8"
        elif fp.endswith(".css"):
            ct = "text/css; charset=utf-8"
        elif fp.endswith(".json"):
            ct = "application/json; charset=utf-8"
        elif fp.endswith(".png"):
            ct = "image/png"
        elif fp.endswith(".ico"):
            ct = "image/x-icon"
        with open(fp, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/leaderboard":
            return self._leaderboard()
        if parsed.path == "/api/load":
            return self._load(parse_qs(parsed.query))
        # статика (клиент игры)
        return self._serve_static(parsed.path)

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            body = json.loads(self._read_body() or "{}")
        except Exception:
            return self._send_json(400, {"error": "bad json"})
        if parsed.path == "/api/register":
            return self._register(body)
        if parsed.path == "/api/login":
            return self._login(body)
        if parsed.path == "/api/save":
            return self._save(body)
        return self._send_json(404, {"error": "not found"})

    def _token_nick(self, headers):
        auth = headers.get("Authorization") or headers.get("authorization") or ""
        if auth.startswith("Bearer "):
            return TOKENS.get(auth[7:])
        return None

    def _register(self, body):
        nick = str(body.get("nick", "")).strip()
        pwd = str(body.get("pass", ""))
        if not nick or not pwd:
            return self._send_json(400, {"error": "nick и pass обязательны"})
        if not re.match(r"^[A-Za-z0-9 _-]{2,20}$", nick):
            return self._send_json(400, {"error": "Ник 2-20 символов: латиница, цифры, пробел, _ -"})
        if len(pwd) < 4:
            return self._send_json(400, {"error": "Пароль минимум 4 символа"})
        with DB_LOCK:
            if nick in DB["accounts"]:
                return self._send_json(409, {"error": "Ник уже занят"})
            role = "owner" if nick == "Santa Tedis" else "player"
            DB["accounts"][nick] = {"pass": hash_pass(pwd), "role": role}
            DB["saves"].setdefault(nick, {})
            save_db(DB)
        tok = new_token()
        TOKENS[tok] = nick
        return self._send_json(200, {"token": tok, "nick": nick, "role": role})

    def _login(self, body):
        nick = str(body.get("nick", "")).strip()
        pwd = str(body.get("pass", ""))
        # Специальный пароль владельца в онлайн-версии
        if nick == "Santa Tedis" and pwd == "santaOwner777":
            acc = None
            with DB_LOCK:
                acc = DB["accounts"].get(nick, {"pass": hash_pass(pwd), "role": "owner"})
            tok = new_token()
            TOKENS[tok] = nick
            return self._send_json(200, {"token": tok, "nick": nick, "role": "owner"})
        with DB_LOCK:
            acc = DB["accounts"].get(nick)
        if not acc:
            return self._send_json(404, {"error": "Аккаунт не найден"})
        if acc["pass"] != hash_pass(pwd):
            return self._send_json(401, {"error": "Неверный пароль"})
        tok = new_token()
        TOKENS[tok] = nick
        return self._send_json(200, {"token": tok, "nick": nick, "role": acc["role"]})

    def _save(self, body):
        nick = self._token_nick(self.headers)
        if not nick:
            return self._send_json(401, {"error": "need token"})
        st = body.get("state")
        if not isinstance(st, dict):
            return self._send_json(400, {"error": "state обязателен"})
        with DB_LOCK:
            DB["saves"][nick] = st
            save_db(DB)
        return self._send_json(200, {"ok": True})

    def _load(self, qs):
        tok = qs.get("token", [""])[0]
        nick = TOKENS.get(tok)
        if not nick:
            return self._send_json(401, {"error": "need token"})
        with DB_LOCK:
            sv = DB["saves"].get(nick, {})
            role = DB["accounts"].get(nick, {}).get("role", "player")
        return self._send_json(200, {"state": sv, "nick": nick, "role": role})

    def _leaderboard(self):
        rows = []
        with DB_LOCK:
            for nick, sv in DB["saves"].items():
                rows.append({
                    "nick": nick,
                    "lvl": sv.get("lvl", 1),
                    "gold": sv.get("gold", 0),
                    "role": DB["accounts"].get(nick, {}).get("role", "player"),
                })
        rows.sort(key=lambda r: (-r["lvl"], -r["gold"]))
        return self._send_json(200, rows[:50])

    # WebSocket upgrade
    def do_GET_ws(self):
        pass

    def handle_one_request(self):
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if len(self.raw_requestline) > 65536:
                self.requestline = ""
                self.request_version = ""
                self.command = ""
                self.send_error(414)
                return
            if not self.raw_requestline:
                self.close_connection = True
                return
            if not self.parse_request():
                return
            if self.headers.get("Upgrade", "").lower() == "websocket":
                return self._handle_websocket()
            mname = "do_" + self.command
            if not hasattr(self, mname):
                self.send_error(501, "Unsupported method")
                return
            method = getattr(self, mname)
            method()
            self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError, TimeoutError):
            self.close_connection = True

    def _handle_websocket(self):
        qs = parse_qs(urlparse(self.path).query)
        nick = (qs.get("nick") or [""])[0]
        if nick not in DB["accounts"]:
            self.wfile.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
            return
        try:
            handshake = build_ws_handshake_response({
                "raw_headers": [
                    f"{k}: {v}" for k, v in self.headers.items()
                ]
            })
            self.wfile.write(handshake)
            self.wfile.flush()
            conn = WSConnection(self.connection, {})
            with WS_LOCK:
                WS_CLIENTS[nick] = conn
            ws_handle(nick, conn)
        except Exception:
            pass
        finally:
            self.close_connection = True


class ThreadingHTTPServer2(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    port = int(os.environ.get("PORT", "8080"))
    httpd = ThreadingHTTPServer2(("0.0.0.0", port), Handler)
    print(f"Легенды Асура онлайн: http://0.0.0.0:{port}")
    print(f"Файл данных: {DATA_FILE}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()