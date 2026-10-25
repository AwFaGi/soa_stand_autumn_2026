"""Orders — оформление заказов маркетплейса.

Задание 1: деньги не списываются дважды.

Как оформляется заказ:

1. POST /orders сразу записывает заказ в базу (состояние new) и запускает его
   обработку в фоне. Ответ ждёт обработку не дольше RESPONSE_WAIT: успели —
   отдаём paid/rejected, нет — pending. Так ответ укладывается в 1 секунду
   при любом поведении Payment.
2. Фоновая обработка — конечный автомат, каждый шаг которого записан в базе.
   Каждый шаг идемпотентен: резерв один на order_id, списание идёт с
   Idempotency-Key = order_id, снятие резерва можно повторять. Поэтому шаг
   можно повторять сколько угодно раз — после ошибки, таймаута, потерянного
   ответа или перезапуска сервиса (незавершённые заказы подхватываются при
   старте и периодически).
3. Если исход списания неизвестен (таймаут, оборванное соединение), перед
   повтором спрашиваем Payment, есть ли списание по заказу
   (GET /payments?order_id=): это решает и случай «ответ дольше нашего
   таймаута».
4. Перед Payment стоит предохранитель (circuit breaker): после нескольких
   отказов подряд запросы не идут, раз в cooldown проходит один пробный.
   Пока Payment не отвечает, Orders его не заваливает, а когда он вернулся —
   заказы снова оплачиваются.

Задание 2: заказ не теряется.

5. Исходящие события — transactional outbox. Переход в paid и строка в
   outbox (с заранее выбранным event_id) пишутся одной транзакцией;
   отдельный поток публикует неопубликованные строки в шину, пока она не
   ответит 202, и только потом отмечает их. Падение в любой момент не теряет
   событие: в худшем случае оно уйдёт ещё раз с тем же id.
6. Входящие delivery.confirmed — идемпотентный получатель. id события
   записывается в delivery_events в той же транзакции, что и начисление
   бонусов; копия упирается в первичный ключ и ничего не начисляет, но
   получает 200.
"""

import http.client
import json
import logging
import os
import random
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
from psycopg_pool import ConnectionPool

PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")
# Шина событий; пусто — события не публикуются.
EVENTS_URL = os.environ.get("EVENTS_URL", "").rstrip("/")

RESPONSE_WAIT = 0.5 # Сколько POST /orders ждёт фоновую обработку, прежде чем ответить pending.
PAYMENT_TIMEOUT = 10.0 # Таймаут POST /payments.
LOOKUP_TIMEOUT = 3.0 # GET /payments отвечает сразу; дольше — Payment недоступен.
INVENTORY_TIMEOUT = 3.0
EVENTS_TIMEOUT = 2.0
# Как часто искать незавершённые заказы без обработчика.
SWEEP_EVERY = 5.0
# Сколько хранить уже опубликованные события outbox, с.
OUTBOX_KEEP = 24 * 3600

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    state        TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_unfinished ON orders (created_at)
    WHERE state NOT IN ('paid', 'rejected');

CREATE TABLE IF NOT EXISTS outbox (
    event_id     TEXT PRIMARY KEY,
    order_id     TEXT NOT NULL UNIQUE REFERENCES orders (id),
    occurred_at  TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (created_at)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS delivery_events (
    event_id    TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL,
    points      BIGINT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points  BIGINT NOT NULL
);
"""

FINAL = ("paid", "rejected")

pool = None


def init_db():
    global pool
    # База может подниматься дольше сервиса — ждём её.
    for _ in range(120):
        try:
            with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
                conn.execute(SCHEMA)
            break
        except psycopg.OperationalError as e:
            log.info("база недоступна (%s), жду", e)
            time.sleep(1)
    else:
        raise SystemExit("не дождался базы")
    pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=20, open=True,
                          kwargs={"autocommit": True})
    pool.wait()


def public_status(state):
    return state if state in FINAL else "pending"


# ---------- HTTP-клиент ----------

class Unknown(Exception):
    """Ответа нет: таймаут или соединение оборвалось. Исход неизвестен."""

    def __init__(self, msg, timeout=False):
        super().__init__(msg)
        self.timeout = timeout


def http_call(method, url, body=None, headers=None, timeout=3.0):
    """HTTP-вызов соседа. Возвращает (код, тело); Unknown — ответа нет."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read() or b"null")
        except (ValueError, OSError, http.client.HTTPException):
            payload = None
        return e.code, payload
    except (OSError, http.client.HTTPException, ValueError) as e:
        # URLError, socket.timeout, ConnectionReset, RemoteDisconnected,
        # IncompleteRead, обрезанный JSON — запрос мог и выполниться.
        timeout = isinstance(e, TimeoutError) or isinstance(getattr(e, "reason", None), TimeoutError)
        raise Unknown(f"{type(e).__name__}: {e}", timeout) from None


# ---------- предохранитель перед Payment ----------

class Breaker:
    """Circuit breaker с ограничением одновременных запросов.

    closed    — запросы идут, но не больше max_parallel одновременно и не
                новые, пока какой-то запрос висит дольше slow_after: сосед,
                похоже, завис — не добавляем ему висящих соединений. После
                таймаута — по одному запросу до первого ответа;
    open      — запросы не идут cooldown секунд;
    half-open — по истечении cooldown проходит один пробный запрос: ответ
                закрывает предохранитель, отказ открывает снова.

    Отказы двух видов:
    - жёсткий — таймаут: сосед не отвечает вовсе. hard_threshold таких
      подряд открывают предохранитель, cooldown растёт вдвое до max_cooldown;
    - мягкий — 5xx или оборванное соединение: сосед жив, но ошибается.
      Открывает только серия из soft_threshold отказов подряд, и ненадолго
      (soft_cooldown): повторы по заказам и так идут с паузами.
    Любой ответ, кроме 5xx (2xx, 402, 4xx), значит, что сосед жив.
    """

    def __init__(self, name, hard_threshold=2, soft_threshold=6, cooldown=1.0,
                 max_cooldown=5.0, soft_cooldown=0.5, slow_after=1.0, max_parallel=10):
        self.name = name
        self.hard_threshold = hard_threshold
        self.soft_threshold = soft_threshold
        self.base_cooldown = cooldown
        self.max_cooldown = max_cooldown
        self.soft_cooldown = soft_cooldown
        self.slow_after = slow_after
        self.max_parallel = max_parallel
        self.lock = threading.Lock()
        self.failures = 0  # отказы подряд, любые
        self.hard = 0      # таймауты подряд
        self.is_open = False
        self.cooldown = cooldown
        self.open_until = 0.0
        self.probe_in_flight = False
        self.inflight = {}  # токен → начало запроса
        self.seq = 0

    def tripped(self):
        with self.lock:
            return self.is_open

    def acquire(self):
        """Ждёт разрешения на запрос. Возвращает (токен, пробный ли)."""
        while True:
            with self.lock:
                now = time.monotonic()
                if self.is_open:
                    if now >= self.open_until and not self.probe_in_flight:
                        self.probe_in_flight = True
                        return self._start(now), True
                else:
                    # После таймаута — по одному запросу: накопившиеся заказы
                    # не бросаются на зависшего соседа разом.
                    limit = self.max_parallel if self.hard == 0 else 1
                    if len(self.inflight) < limit and not any(
                            now - t > self.slow_after for t in self.inflight.values()):
                        return self._start(now), False
            time.sleep(0.05)

    def _start(self, now):
        self.seq += 1
        self.inflight[self.seq] = now
        return self.seq

    def release(self, token, probe, outcome):
        """outcome: 'ok' — ответ, 'soft' — 5xx/обрыв, 'hard' — таймаут."""
        with self.lock:
            self.inflight.pop(token, None)
            if probe:
                self.probe_in_flight = False
            if outcome == "ok":
                if self.is_open:
                    log.info("%s: снова отвечает, предохранитель закрыт", self.name)
                self.is_open = False
                self.failures = self.hard = 0
                self.cooldown = self.base_cooldown
                return
            self.failures += 1
            self.hard = self.hard + 1 if outcome == "hard" else 0
            trip = self.hard >= self.hard_threshold or self.failures >= self.soft_threshold
            # Новое окно — при срабатывании и после неудачной пробы. Отказы
            # запросов, ушедших до срабатывания, окно не продлевают.
            if probe or (not self.is_open and trip):
                self.is_open = True
                if outcome == "hard":
                    pause = self.cooldown
                    self.cooldown = min(self.cooldown * 2, self.max_cooldown)
                else:
                    pause = self.soft_cooldown
                self.open_until = time.monotonic() + pause
                log.warning("%s: отказ (%s), предохранитель открыт на %.1f с", self.name, outcome, pause)

    def call(self, method, url, body=None, headers=None, timeout=3.0):
        token, probe = self.acquire()
        outcome = "soft"
        try:
            code, payload = http_call(method, url, body, headers, timeout)
            outcome = "ok" if code < 500 else "soft"
            return code, payload
        except Unknown as e:
            outcome = "hard" if e.timeout else "soft"
            raise
        finally:
            self.release(token, probe, outcome)


payment = Breaker("payment")


# ---------- соседи ----------

def backoff_sleep(attempt, base=0.2, cap=2.0):
    time.sleep(min(cap, base * (2 ** attempt)) * random.uniform(0.5, 1.0))


def reserve(order):
    """Резерв товара. Повтор по тому же order_id отдаёт тот же резерв."""
    body = {"order_id": order["id"],
            "items": [{"sku": i["sku"], "qty": i["qty"]} for i in order["items"]]}
    attempt = 0
    while True:
        try:
            code, _ = http_call("POST", f"{INVENTORY_URL}/reservations", body, timeout=INVENTORY_TIMEOUT)
            if code in (200, 201):
                return
            log.warning("order %s: резерв не удался (%s), повторю", order["id"], code)
        except Unknown as e:
            log.warning("order %s: резерв без ответа (%s), повторю", order["id"], e)
        backoff_sleep(attempt)
        attempt += 1


def release(order_id):
    """Снять резерв. Повторный вызов тоже 204."""
    attempt = 0
    while True:
        try:
            code, _ = http_call("DELETE", f"{INVENTORY_URL}/reservations/{urllib.parse.quote(order_id)}",
                                timeout=INVENTORY_TIMEOUT)
            if code in (200, 204, 404):
                return
            log.warning("order %s: снять резерв не удалось (%s), повторю", order_id, code)
        except Unknown as e:
            log.warning("order %s: снятие резерва без ответа (%s), повторю", order_id, e)
        backoff_sleep(attempt)
        attempt += 1


def find_charge(order_id):
    """Есть ли списание по заказу. True/False; Unknown — Payment не ответил."""
    code, payload = payment.call("GET", f"{PAYMENT_URL}/payments?order_id={urllib.parse.quote(order_id)}",
                                 timeout=LOOKUP_TIMEOUT)
    if code != 200 or not isinstance(payload, list):
        raise Unknown(f"GET /payments: {code}")
    return any(c.get("order_id") == order_id for c in payload if isinstance(c, dict))


def charge(order):
    """Списывает деньги, пока не станет известен исход: 'paid' или 'declined'.

    Все попытки — с Idempotency-Key = order_id и одним и тем же телом:
    Payment выполняет списание по ключу не больше одного раза, а повтор
    отдаёт результат той же операции. Поэтому повтор после 500, таймаута или
    потерянного ответа не спишет деньги второй раз.
    """
    oid = order["id"]
    body = {"order_id": oid, "amount_cents": order["amount_cents"], "currency": "RUB"}
    headers = {"Idempotency-Key": oid}
    unknown = False
    attempt = 0
    while True:
        try:
            if unknown:
                # Прошлая попытка могла списать деньги — сначала спросим.
                if find_charge(oid):
                    return "paid"
                unknown = False
            code, _ = payment.call("POST", f"{PAYMENT_URL}/payments", body, headers, PAYMENT_TIMEOUT)
            if code in (200, 201):
                return "paid"
            if code == 402:
                return "declined"
            if code == 422:
                # Ключ занят другим телом — так быть не должно; выясняем по факту.
                unknown = True
            log.warning("order %s: Payment ответил %s, повторю", oid, code)
        except Unknown as e:
            unknown = True
            log.warning("order %s: исход оплаты неизвестен (%s), выясню", oid, e)
        backoff_sleep(attempt)
        attempt = min(attempt + 1, 4)


# ---------- обработка заказа ----------

def load_order(order_id):
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT id, user_id, state, amount_cents, items FROM orders WHERE id = %s", (order_id,)
        ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "user_id": row[1], "state": row[2], "amount_cents": row[3], "items": row[4]}


def set_state(order_id, frm, to):
    """Переход автомата; условие на прежнее состояние защищает от гонок."""
    with pool.connection() as conn:
        cur = conn.execute(
            "UPDATE orders SET state = %s, updated_at = now() WHERE id = %s AND state = %s",
            (to, order_id, frm),
        )
        return cur.rowcount == 1


def mark_paid(order_id):
    """reserved → paid и событие order.paid в outbox — одной транзакцией.

    Либо заказ paid и событие ждёт публикации, либо ни того ни другого:
    упасть «между» нельзя. Публикует событие отдельный поток (Publisher).
    """
    with pool.connection() as conn, conn.transaction():
        cur = conn.execute(
            "UPDATE orders SET state = 'paid', updated_at = now() WHERE id = %s AND state = 'reserved'",
            (order_id,),
        )
        if cur.rowcount != 1:
            return False
        conn.execute(
            "INSERT INTO outbox (event_id, order_id, occurred_at) VALUES (%s, %s, now())",
            (str(uuid.uuid4()), order_id),
        )
    publisher.wake()
    return True


def advance(order):
    """Один шаг автомата. Возвращает новое состояние."""
    oid, st = order["id"], order["state"]
    if st == "new":
        reserve(order)
        set_state(oid, "new", "reserved")
    elif st == "reserved":
        result = charge(order)
        if result == "paid":
            if mark_paid(oid):
                log.info("order %s: paid", oid)
        else:
            set_state(oid, "reserved", "declined")
    elif st == "declined":
        release(oid)
        if set_state(oid, "declined", "rejected"):
            log.info("order %s: rejected (банк отклонил оплату)", oid)
    order = load_order(oid)
    return order["state"] if order else None


class Processor:
    """Фоновые обработчики заказов: не больше одного на заказ."""

    def __init__(self):
        self.lock = threading.Lock()
        self.active = {}  # order_id → threading.Event (заказ завершён)

    def start(self, order_id):
        with self.lock:
            ev = self.active.get(order_id)
            if ev is not None:
                return ev
            ev = threading.Event()
            self.active[order_id] = ev
        threading.Thread(target=self._run, args=(order_id, ev), daemon=True).start()
        return ev

    def _run(self, order_id, ev):
        attempt = 0
        try:
            while True:
                try:
                    order = load_order(order_id)
                    if order is None or order["state"] in FINAL:
                        return
                    advance(order)
                    attempt = 0
                except Exception:
                    log.exception("order %s: ошибка обработки, повторю", order_id)
                    backoff_sleep(attempt, base=0.5, cap=5.0)
                    attempt = min(attempt + 1, 4)
        finally:
            with self.lock:
                self.active.pop(order_id, None)
            ev.set()

    def sweep_forever(self):
        """Подхватывает незавершённые заказы: при старте и потом раз в SWEEP_EVERY."""
        while True:
            try:
                with pool.connection() as conn:
                    rows = conn.execute(
                        "SELECT id FROM orders WHERE state NOT IN ('paid', 'rejected') ORDER BY created_at"
                    ).fetchall()
                for (oid,) in rows:
                    self.start(oid)
                cleanup()
            except Exception:
                log.exception("поиск незавершённых заказов")
            time.sleep(SWEEP_EVERY)


processor = Processor()


def create_order(body):
    order_id = str(uuid.uuid4())
    amount = sum(i["qty"] * i["price_cents"] for i in body["items"])
    items = [{"sku": i["sku"], "qty": i["qty"], "price_cents": i["price_cents"]} for i in body["items"]]
    # Сначала заказ в базе, потом соседи: деньги не спишутся за заказ,
    # которого нет.
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO orders (id, user_id, state, amount_cents, items) VALUES (%s, %s, 'new', %s, %s)",
            (order_id, body["user_id"], amount, json.dumps(items)),
        )
    done = processor.start(order_id)
    # Пока Payment недоступен, ждать бесполезно — отвечаем сразу.
    if not payment.tripped():
        done.wait(RESPONSE_WAIT)
    order = load_order(order_id)
    return {"id": order_id, "status": public_status(order["state"])}


def get_order(order_id):
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT id, state, amount_cents FROM orders WHERE id = %s", (order_id,)
        ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "status": public_status(row[1]), "amount_cents": row[2]}


def valid(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False
    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False
    for i in items:
        if not isinstance(i, dict):
            return False
        if not isinstance(i.get("sku"), str) or not i["sku"]:
            return False
        for k, minimum in (("qty", 1), ("price_cents", 0)):
            v = i.get(k)
            if not isinstance(v, int) or isinstance(v, bool) or v < minimum:
                return False
    return True


# ---------- задание 2: исходящие события (transactional outbox) ----------

class Publisher:
    """Публикует события из outbox в шину, пока каждое не примут (202).

    Строка outbox появляется в той же транзакции, что и paid (mark_paid), и
    помечается published_at только после 202. Упали после публикации, но до
    отметки — событие уйдёт ещё раз с тем же event_id: доставка «как минимум
    один раз», подписчик отличит копию по id. Перед шиной — свой
    предохранитель: пока шина лежит, идёт один пробный запрос, а не все.
    """

    def __init__(self):
        self.bus = Breaker("events", max_parallel=8)
        self.kick = threading.Event()

    def wake(self):
        self.kick.set()

    def run_forever(self):
        if not EVENTS_URL:
            return
        while True:
            self.kick.clear()
            try:
                with pool.connection() as conn:
                    rows = conn.execute(
                        "SELECT event_id, order_id, occurred_at FROM outbox "
                        "WHERE published_at IS NULL ORDER BY created_at LIMIT 100"
                    ).fetchall()
            except Exception:
                log.exception("outbox: чтение")
                rows = []
            if not rows:
                self.kick.wait(1.0)
                continue
            # Партия — параллельно (ограничивает предохранитель), следующая —
            # когда разобрана эта: одно событие не уходит в два потока сразу.
            workers = [threading.Thread(target=self.publish, args=row, daemon=True) for row in rows]
            for t in workers:
                t.start()
            for t in workers:
                t.join()

    def publish(self, event_id, order_id, occurred_at):
        try:
            code, _ = self.bus.call("POST", f"{EVENTS_URL}/events", {
                "id": event_id,
                "type": "order.paid",
                "order_id": order_id,
                "occurred_at": occurred_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }, timeout=EVENTS_TIMEOUT)
        except Unknown as e:
            log.warning("order %s: событие не опубликовано (%s), повторю", order_id, e)
            time.sleep(0.5)
            return
        if code != 202:
            log.warning("order %s: событие не опубликовано (%s), повторю", order_id, code)
            time.sleep(0.5)
            return
        try:
            with pool.connection() as conn:
                conn.execute("UPDATE outbox SET published_at = now() WHERE event_id = %s", (event_id,))
        except Exception:
            log.exception("outbox: отметка %s", event_id)


publisher = Publisher()


def cleanup():
    """Опубликованные события старше OUTBOX_KEEP больше не нужны."""
    with pool.connection() as conn:
        conn.execute("DELETE FROM outbox WHERE published_at < now() - %s * interval '1 second'",
                     (OUTBOX_KEEP,))


# ---------- задание 2: входящие события (идемпотентный получатель) ----------

def delivery_confirmed(event):
    """Начисляет бонусы за подтверждённую доставку — ровно один раз на event id.

    Запись «событие обработано» и начисление — одна транзакция. Копии
    события, пришедшие одновременно, упираются в первичный ключ: вторая
    ждёт, пока первая зафиксируется, и ничего не делает. Убили посреди
    транзакции — откатится целиком, шина пришлёт событие снова.
    """
    with pool.connection() as conn, conn.transaction():
        cur = conn.execute(
            "INSERT INTO delivery_events (event_id, user_id, points) VALUES (%s, %s, %s) "
            "ON CONFLICT (event_id) DO NOTHING",
            (event["id"], event["user_id"], event["bonus_points"]),
        )
        if cur.rowcount != 1:
            return False
        conn.execute(
            "INSERT INTO bonuses (user_id, points) VALUES (%s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET points = bonuses.points + EXCLUDED.points",
            (event["user_id"], event["bonus_points"]),
        )
    return True


def get_bonus(user_id):
    with pool.connection() as conn:
        row = conn.execute("SELECT points FROM bonuses WHERE user_id = %s", (user_id,)).fetchone()
    return {"points": row[0] if row else 0}


# ---------- HTTP-сервер ----------

ORDER_PATH = re.compile(r"^/orders/([^/]+)$")
BONUS_PATH = re.compile(r"^/users/([^/]+)/bonus$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urllib.parse.urlsplit(self.path).path
        try:
            if path == "/healthz":
                return self.send_json(200, {"status": "ok"})
            m = ORDER_PATH.match(path)
            if m:
                order = get_order(urllib.parse.unquote(m.group(1)))
                if order is None:
                    return self.send_json(404, {"error": "not_found"})
                return self.send_json(200, order)
            m = BONUS_PATH.match(path)
            if m:
                return self.send_json(200, get_bonus(urllib.parse.unquote(m.group(1))))
            self.send_json(404, {"error": "not_found"})
        except Exception as e:
            log.exception("GET %s", path)
            self.send_json(500, {"error": "internal", "message": str(e)})

    def do_POST(self):
        path = urllib.parse.urlsplit(self.path).path
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if path not in ("/orders", "/webhooks/delivery"):
            return self.send_json(404, {"error": "not_found"})
        try:
            body = json.loads(raw or b"null")
        except ValueError:
            body = None
        if path == "/webhooks/delivery":
            return self.webhook(body)
        if not valid(body):
            return self.send_json(400, {"error": "bad_request"})
        try:
            self.send_json(201, create_order(body))
        except Exception as e:
            log.exception("POST /orders")
            self.send_json(500, {"error": "internal", "message": str(e)})

    def webhook(self, body):
        if not isinstance(body, dict) or not isinstance(body.get("id"), str) or not body["id"] \
                or not isinstance(body.get("user_id"), str) or not body["user_id"] \
                or not isinstance(body.get("bonus_points"), int) or isinstance(body["bonus_points"], bool) \
                or body["bonus_points"] <= 0:
            return self.send_json(400, {"error": "bad_request"})
        try:
            if not delivery_confirmed(body):
                log.info("delivery %s: копия, уже начислено", body["id"])
        except Exception as e:
            log.exception("POST /webhooks/delivery")
            return self.send_json(500, {"error": "internal", "message": str(e)})
        self.send_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        pass


class Server(ThreadingHTTPServer):
    daemon_threads = True
    # Раннер шлёт заказы потоком: очередь accept по умолчанию (5) мала.
    request_queue_size = 128


if __name__ == "__main__":
    init_db()
    threading.Thread(target=processor.sweep_forever, daemon=True).start()
    threading.Thread(target=publisher.run_forever, daemon=True).start()
    server = Server(("0.0.0.0", PORT), Handler)
    log.info("orders: слушаю :%d", PORT)
    server.serve_forever()
