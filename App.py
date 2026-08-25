import csv
import hashlib
import hmac
import io
import json
import math
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests
from flask import (
    Flask,
    jsonify,
    redirect,
    render_template_string,
    request,
    send_file,
)


def env_bool(name, default=False):
    value = os.getenv(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "on"}


# ============================================================
# CONFIGURACIÓN
# Las claves reales se colocarán en Railway, nunca aquí.
# ============================================================
BOT_NAME = "BOT GOLD BINGX"
BINGX_BASE_URL = "https://open-api.bingx.com"

# TradingView muestra GOLDXAUUSDT.P.
# BingX utiliza NCCOGOLD2USD-USDT en su API.
BINGX_SYMBOL = os.getenv(
    "BINGX_SYMBOL",
    "NCCOGOLD2USD-USDT",
).strip()

TV_SYMBOLS = {
    item.strip().upper()
    for item in os.getenv(
        "TV_SYMBOLS",
        "GOLDXAUUSDT.P,BINGX:GOLDXAUUSDT.P,"
        "NCCOGOLD2USD-USDT,GOLD(XAU)-USDT",
    ).split(",")
    if item.strip()
}

BINGX_API_KEY = os.getenv("BINGX_API_KEY", "").strip()
BINGX_API_SECRET = (
    os.getenv("BINGX_API_SECRET", "").strip()
    or os.getenv("BINGX_SECRET_KEY", "").strip()
)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
CONTROL_SECRET = os.getenv(
    "CONTROL_SECRET",
    WEBHOOK_SECRET,
).strip()
MONITOR_SECRET = os.getenv("MONITOR_SECRET", "").strip()

BALANCE_PERCENT = float(
    os.getenv("BALANCE_PERCENT", "90")
)
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.0005"))
QTY_STEP = float(os.getenv("QTY_STEP", "0.0001"))
MIN_QTY = float(os.getenv("MIN_QTY", "0.0005"))

POSITION_MODE = os.getenv(
    "POSITION_MODE",
    "HEDGE",
).strip().upper()

# Seguridad: inicialmente no envía órdenes reales.
DRY_RUN = env_bool("DRY_RUN", True)
DRY_BALANCE = float(os.getenv("DRY_BALANCE", "1000"))

UPSTASH_REDIS_REST_URL = os.getenv(
    "UPSTASH_REDIS_REST_URL",
    "",
).strip()

UPSTASH_REDIS_REST_TOKEN = os.getenv(
    "UPSTASH_REDIS_REST_TOKEN",
    "",
).strip()

STATE_PREFIX = os.getenv(
    "STATE_PREFIX",
    "bot_gold_1h",
).strip()

DATA_DIR = os.getenv("DATA_DIR", ".").strip()

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    "",
).strip()

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    "",
).strip()

VALID_MODES = [
    "OFF",
    "LONG_ONLY",
    "SHORT_ONLY",
    "CLOSE_ONLY",
    "BOTH",
]

SIGNAL_LOCK = threading.RLock()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def secret_matches(received, expected):
    return bool(received and expected) and hmac.compare_digest(
        str(received),
        str(expected),
    )


def notify(message):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return

    try:
        requests.post(
            (
                "https://api.telegram.org/bot"
                f"{TELEGRAM_BOT_TOKEN}/sendMessage"
            ),
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message[:3900],
            },
            timeout=10,
        )
    except Exception:
        pass


# ============================================================
# ALMACENAMIENTO
# Usa Upstash si está configurado.
# Si no, guarda archivos JSON locales.
# ============================================================
class Store:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.lock = threading.RLock()

    @property
    def redis_enabled(self):
        return bool(
            UPSTASH_REDIS_REST_URL
            and UPSTASH_REDIS_REST_TOKEN
        )

    def _key(self, name):
        return f"{STATE_PREFIX}:{name}"

    def _path(self, name):
        return os.path.join(
            DATA_DIR,
            f"{STATE_PREFIX}_{name}.json",
        )

    def _redis(self, command):
        response = requests.post(
            UPSTASH_REDIS_REST_URL.rstrip("/"),
            headers={
                "Authorization": (
                    f"Bearer {UPSTASH_REDIS_REST_TOKEN}"
                ),
                "Content-Type": "application/json",
            },
            json=command,
            timeout=12,
        )

        payload = response.json()

        if response.status_code >= 400 or payload.get("error"):
            raise RuntimeError(
                f"Error de almacenamiento: {payload}"
            )

        return payload.get("result")

    def get(self, name, default):
        with self.lock:
            if self.redis_enabled:
                raw = self._redis(
                    ["GET", self._key(name)]
                )
                return default if not raw else json.loads(raw)

            path = self._path(name)

            if not os.path.exists(path):
                return default

            with open(
                path,
                "r",
                encoding="utf-8",
            ) as handle:
                return json.load(handle)

    def set(self, name, value):
        with self.lock:
            raw = json.dumps(
                value,
                ensure_ascii=False,
            )

            if self.redis_enabled:
                self._redis(
                    ["SET", self._key(name), raw]
                )
                return

            with open(
                self._path(name),
                "w",
                encoding="utf-8",
            ) as handle:
                json.dump(
                    value,
                    handle,
                    ensure_ascii=False,
                    indent=2,
                )

    def get_mode(self):
        state = self.get("mode", {})
        mode = state.get("mode", "OFF")

        if mode in VALID_MODES:
            return mode

        return "OFF"

    def set_mode(self, mode):
        if mode not in VALID_MODES:
            raise ValueError("Modo inválido")

        self.set(
            "mode",
            {
                "mode": mode,
                "updated_at": utc_now(),
            },
        )

    def get_active_trade(self):
        return self.get("active_trade", None)

    def set_active_trade(self, trade):
        self.set("active_trade", trade)

    def clear_active_trade(self):
        self.set("active_trade", None)

    def get_trades(self):
        trades = self.get("trades", [])

        if isinstance(trades, list):
            return trades

        return []

    def append_trade(self, trade):
        with self.lock:
            trades = self.get_trades()
            trades.append(trade)
            self.set("trades", trades)


store = Store()


# ============================================================
# CONEXIÓN CON BINGX
# ============================================================
def floor_step(value, step=QTY_STEP):
    decimals = max(
        0,
        len(
            f"{step:.12f}"
            .rstrip("0")
            .split(".")[-1]
        ),
    )

    quantity = (
        math.floor((value + 1e-12) / step) * step
    )

    return round(quantity, decimals)


class BingX:
    def _request(
        self,
        method,
        path,
        params=None,
        private=True,
    ):
        params = dict(params or {})
        headers = {}

        if private:
            if not (
                BINGX_API_KEY
                and BINGX_API_SECRET
            ):
                raise RuntimeError(
                    "Las API de BingX no están configuradas"
                )

            params["timestamp"] = int(
                time.time() * 1000
            )

            query = urlencode(
                sorted(params.items())
            )

            signature = hmac.new(
                BINGX_API_SECRET.encode(),
                query.encode(),
                hashlib.sha256,
            ).hexdigest()

            url = (
                f"{BINGX_BASE_URL}{path}"
                f"?{query}&signature={signature}"
            )

            headers["X-BX-APIKEY"] = BINGX_API_KEY

        else:
            query = urlencode(params)

            url = f"{BINGX_BASE_URL}{path}"

            if query:
                url += f"?{query}"

        response = requests.request(
            method,
            url,
            headers=headers,
            timeout=20,
        )

        payload = response.json()

        if str(payload.get("code")) != "0":
            raise RuntimeError(
                f"Error BingX: {payload}"
            )

        return payload

    def price(self):
        payload = self._request(
            "GET",
            "/openApi/swap/v2/quote/price",
            {"symbol": BINGX_SYMBOL},
            private=False,
        )

        data = payload.get("data", {})

        return float(
            data.get("price")
            or data.get("lastPrice")
        )

    def available_balance(self):
        if (
            DRY_RUN
            and not (
                BINGX_API_KEY
                and BINGX_API_SECRET
            )
        ):
            return DRY_BALANCE

        payload = self._request(
            "GET",
            "/openApi/swap/v2/user/balance",
        )

        data = payload.get("data", {})

        if isinstance(data, dict):
            data = data.get("balance", data)

        elif isinstance(data, list):
            data = data[0] if data else {}

        return float(
            data.get("availableMargin")
            or data.get("availableBalance")
            or data.get("balance")
            or 0
        )

    def set_leverage(self):
        if DRY_RUN:
            return

        for side in ("LONG", "SHORT"):
            self._request(
                "POST",
                "/openApi/swap/v2/trade/leverage",
                {
                    "symbol": BINGX_SYMBOL,
                    "side": side,
                    "leverage": LEVERAGE,
                },
            )

    def quantity(self):
        balance = self.available_balance()
        price = self.price()

        margin = (
            balance
            * BALANCE_PERCENT
            / 100
        )

        quantity = floor_step(
            margin * LEVERAGE / price
        )

        if quantity < MIN_QTY:
            raise RuntimeError(
                "La cantidad calculada es menor que el mínimo"
            )

        return {
            "balance": balance,
            "price": price,
            "margin": margin,
            "quantity": quantity,
        }

    def _position_side(self, direction):
        if POSITION_MODE == "HEDGE":
            return direction

        return "BOTH"

    def market_order(
        self,
        order_side,
        direction,
        quantity,
        closing=False,
    ):
        reference_price = self.price()

        params = {
            "symbol": BINGX_SYMBOL,
            "side": order_side,
            "positionSide": self._position_side(
                direction
            ),
            "type": "MARKET",
            "quantity": floor_step(quantity),
        }

        if POSITION_MODE != "HEDGE":
            params["reduceOnly"] = (
                "true" if closing else "false"
            )

        if DRY_RUN:
            return {
                "order_id": (
                    f"dry-{int(time.time() * 1000)}"
                ),
                "price": reference_price,
                "quantity": floor_step(quantity),
            }

        payload = self._request(
            "POST",
            "/openApi/swap/v2/trade/order",
            params,
        )

        order = (
            payload.get("data", {}).get(
                "order",
                payload.get("data", {}),
            )
        )

        order_id = (
            order.get("orderId")
            or order.get("orderID")
        )

        details = order

        if order_id:
            for _ in range(5):
                time.sleep(0.35)

                try:
                    check = self._request(
                        "GET",
                        "/openApi/swap/v2/trade/order",
                        {
                            "symbol": BINGX_SYMBOL,
                            "orderId": order_id,
                        },
                    )

                    details = (
                        check.get("data", {}).get(
                            "order",
                            check.get("data", {}),
                        )
                    )

                    if (
                        details.get("avgPrice")
                        or details.get("executedQty")
                    ):
                        break

                except Exception:
                    pass

        price = float(
            details.get("avgPrice")
            or details.get("price")
            or reference_price
        )

        executed = float(
            details.get("executedQty")
            or details.get("quantity")
            or quantity
        )

        return {
            "order_id": order_id,
            "price": price,
            "quantity": executed,
        }


bingx = BingX()


# ============================================================
# ESTADÍSTICAS
# ============================================================
def parse_time(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


def filter_trades(
    trades,
    period="all",
    month="",
):
    now = datetime.now(timezone.utc)

    if period == "last_month":
        start = now - timedelta(days=30)

        return [
            trade
            for trade in trades
            if parse_time(trade["closed_at"]) >= start
        ]

    if period == "last_3_months":
        start = now - timedelta(days=90)

        return [
            trade
            for trade in trades
            if parse_time(trade["closed_at"]) >= start
        ]

    if period == "specific_month" and month:
        return [
            trade
            for trade in trades
            if trade.get("closed_at", "")[:7] == month
        ]

    return list(trades)


def trade_summary(trades):
    wins = sum(
        1
        for trade in trades
        if float(trade.get("net_pnl", 0)) > 0
    )

    losses = len(trades) - wins

    gross = sum(
        float(trade.get("gross_pnl", 0))
        for trade in trades
    )

    fees = sum(
        float(trade.get("total_fees", 0))
        for trade in trades
    )

    net = sum(
        float(trade.get("net_pnl", 0))
        for trade in trades
    )

    compounded = 1.0
    peak = 1.0
    max_drawdown = 0.0

    ordered = sorted(
        trades,
        key=lambda item: item.get(
            "closed_at",
            "",
        ),
    )

    for trade in ordered:
        impact = float(
            trade.get(
                "balance_impact_pct",
                0,
            )
        )

        compounded *= 1 + impact / 100
        peak = max(peak, compounded)

        if peak:
            drawdown = (
                (peak - compounded)
                / peak
                * 100
            )

            max_drawdown = max(
                max_drawdown,
                drawdown,
            )

    gains = sum(
        float(trade.get("net_pnl", 0))
        for trade in trades
        if float(trade.get("net_pnl", 0)) > 0
    )

    negative = abs(
        sum(
            float(trade.get("net_pnl", 0))
            for trade in trades
            if float(trade.get("net_pnl", 0)) <= 0
        )
    )

    if negative:
        profit_factor = gains / negative
    elif gains:
        profit_factor = 999
    else:
        profit_factor = 0

    return {
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "winrate": (
            wins / len(trades) * 100
            if trades
            else 0
        ),
        "gross_pnl": gross,
        "fees": fees,
        "net_pnl": net,
        "return_pct": (
            (compounded - 1) * 100
            if trades
            else 0
        ),
        "max_drawdown_pct": max_drawdown,
        "profit_factor": profit_factor,
        "biggest_win": max(
            (
                float(
                    trade.get("net_pnl", 0)
                )
                for trade in trades
            ),
            default=0,
        ),
        "biggest_loss": min(
            (
                float(
                    trade.get("net_pnl", 0)
                )
                for trade in trades
            ),
            default=0,
        ),
    }


def csv_bytes(trades):
    fields = [
        "opened_at",
        "closed_at",
        "side",
        "symbol",
        "quantity",
        "entry_price",
        "exit_price",
        "leverage",
        "margin_used",
        "price_move_pct",
        "gross_roe_pct",
        "net_roe_pct",
        "balance_impact_pct",
        "gross_pnl",
        "entry_fee",
        "exit_fee",
        "total_fees",
        "net_pnl",
        "close_reason",
        "open_order_id",
        "close_order_id",
    ]

    buffer = io.StringIO()

    writer = csv.DictWriter(
        buffer,
        fieldnames=fields,
        extrasaction="ignore",
    )

    writer.writeheader()
    writer.writerows(trades)

    return buffer.getvalue().encode("utf-8")


# ============================================================
# OPERACIONES
# ============================================================
def active_trade():
    return store.get_active_trade()


def open_trade(direction):
    if active_trade():
        return {
            "status": "skipped",
            "reason": "position_already_open",
        }

    calculation = bingx.quantity()
    bingx.set_leverage()

    side = (
        "BUY"
        if direction == "LONG"
        else "SELL"
    )

    fill = bingx.market_order(
        side,
        direction,
        calculation["quantity"],
        closing=False,
    )

    quantity = fill["quantity"]
    entry_price = fill["price"]

    entry_fee = (
        entry_price
        * quantity
        * FEE_RATE
    )

    trade = {
        "id": str(uuid.uuid4()),
        "opened_at": utc_now(),
        "side": direction,
        "symbol": BINGX_SYMBOL,
        "quantity": quantity,
        "entry_price": entry_price,
        "leverage": LEVERAGE,
        "balance_before": calculation["balance"],
        "margin_used": calculation["margin"],
        "entry_fee": entry_fee,
        "open_order_id": fill.get("order_id"),
    }

    store.set_active_trade(trade)

    notify(
        f"APERTURA {direction} {BOT_NAME}\n"
        f"Precio: {entry_price}\n"
        f"Cantidad: {quantity}\n"
        f"Margen: {calculation['margin']:.2f} USDT\n"
        f"Apalancamiento: {LEVERAGE}x"
    )

    return {
        "status": "opened",
        "trade": trade,
    }


def close_trade(reason):
    trade = active_trade()

    if not trade:
        return {
            "status": "skipped",
            "reason": "no_position_to_close",
        }

    direction = trade["side"]

    side = (
        "SELL"
        if direction == "LONG"
        else "BUY"
    )

    fill = bingx.market_order(
        side,
        direction,
        trade["quantity"],
        closing=True,
    )

    exit_price = fill["price"]

    quantity = min(
        float(fill["quantity"]),
        float(trade["quantity"]),
    )

    sign = 1 if direction == "LONG" else -1

    gross = (
        (
            exit_price
            - float(trade["entry_price"])
        )
        * quantity
        * sign
    )

    exit_fee = (
        exit_price
        * quantity
        * FEE_RATE
    )

    entry_fee = float(
        trade.get("entry_fee", 0)
    )

    total_fees = entry_fee + exit_fee
    net = gross - total_fees

    margin = float(
        trade.get("margin_used", 0)
    )

    balance = float(
        trade.get("balance_before", 0)
    )

    if trade.get("entry_price"):
        price_move = (
            (
                exit_price
                / float(trade["entry_price"])
                - 1
            )
            * sign
            * 100
        )
    else:
        price_move = 0

    closed = {
        **trade,
        "closed_at": utc_now(),
        "exit_price": exit_price,
        "quantity": quantity,
        "price_move_pct": price_move,
        "gross_roe_pct": (
            gross / margin * 100
            if margin
            else 0
        ),
        "net_roe_pct": (
            net / margin * 100
            if margin
            else 0
        ),
        "balance_impact_pct": (
            net / balance * 100
            if balance
            else 0
        ),
        "gross_pnl": gross,
        "exit_fee": exit_fee,
        "total_fees": total_fees,
        "net_pnl": net,
        "close_reason": reason,
        "close_order_id": fill.get("order_id"),
    }

    store.append_trade(closed)
    store.clear_active_trade()

    notify(
        f"CIERRE {direction} {BOT_NAME}\n"
        f"Movimiento: {price_move:.3f}%\n"
        f"ROE neto: {closed['net_roe_pct']:.2f}%\n"
        f"PNL neto: {net:.2f} USDT"
    )

    return {
        "status": "closed",
        "trade": closed,
    }


def process_signal(side):
    with SIGNAL_LOCK:
        mode = store.get_mode()
        trade = active_trade()

        current_side = (
            trade.get("side")
            if trade
            else None
        )

        if mode == "OFF":
            return {
                "status": "ignored",
                "reason": "mode_off",
                "mode": mode,
            }

        result = {
            "status": "processed",
            "mode": mode,
            "closed": None,
            "opened": None,
        }

        if (
            side == "BUY"
            and current_side == "SHORT"
        ):
            result["closed"] = close_trade(
                "opposite_buy_signal"
            )

            current_side = None

        elif (
            side == "SELL"
            and current_side == "LONG"
        ):
            result["closed"] = close_trade(
                "opposite_sell_signal"
            )

            current_side = None

        if mode == "CLOSE_ONLY":
            return result

        if (
            side == "BUY"
            and mode in {"LONG_ONLY", "BOTH"}
            and current_side is None
        ):
            result["opened"] = open_trade("LONG")

        if (
            side == "SELL"
            and mode in {"SHORT_ONLY", "BOTH"}
            and current_side is None
        ):
            result["opened"] = open_trade("SHORT")

        return result


# ============================================================
# PANEL DE CONTROL
# ============================================================
PANEL_HTML = r"""
<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport"
content="width=device-width,initial-scale=1">
<title>BOT GOLD BINGX</title>
<style>
:root{
--bg:#080808;
--card:#1b1b1b;
--muted:#aaa;
--green:#00e889;
--red:#ff4b6a;
--blue:#2986ff;
--gold:#d9a900
}
*{box-sizing:border-box}
body{
margin:0;
padding:18px;
background:var(--bg);
color:#fff;
font-family:Arial,sans-serif
}
main{
max-width:1150px;
margin:auto
}
h1{
text-align:center;
letter-spacing:2px;
font-size:34px
}
.card{
background:var(--card);
padding:18px;
border-radius:18px;
margin:16px 0;
box-shadow:0 0 18px #0008
}
.mode{
text-align:center;
color:var(--green);
font-size:42px;
font-weight:800
}
.muted{
color:var(--muted);
font-size:13px;
text-align:center
}
.buttons{
display:grid;
grid-template-columns:
repeat(auto-fit,minmax(180px,1fr));
gap:12px
}
button,.download{
width:100%;
border:0;
border-radius:15px;
padding:17px;
color:#fff;
font-size:18px;
font-weight:800;
cursor:pointer;
text-decoration:none;
text-align:center;
display:block
}
.off{background:#555}
.long{background:#008f39}
.short{background:#b00020}
.close{background:#0057b8}
.both{background:#d4a000}
.grid{
display:grid;
grid-template-columns:
repeat(auto-fit,minmax(145px,1fr));
gap:12px
}
.stat{
background:#101010;
padding:14px;
border-radius:14px;
text-align:center
}
.label{
color:var(--muted);
font-size:12px
}
.value{
margin-top:6px;
font-size:21px;
font-weight:800
}
.positive{color:var(--green)}
.negative{color:var(--red)}
form.filter{
display:grid;
grid-template-columns:2fr 1fr auto;
gap:10px
}
select,input{
background:#101010;
color:#fff;
border:1px solid #444;
border-radius:10px;
padding:12px
}
.apply{
background:var(--blue);
padding:12px
}
.table-wrap{overflow:auto}
table{
width:100%;
border-collapse:collapse;
font-size:13px;
white-space:nowrap
}
th,td{
padding:10px 8px;
border-bottom:1px solid #333;
text-align:right
}
th:first-child,
td:first-child,
th:nth-child(2),
td:nth-child(2){
text-align:left
}
@media(max-width:650px){
form.filter{grid-template-columns:1fr}
h1{font-size:27px}
}
</style>
</head>
<body>
<main>

<h1>BOT GOLD BINGX</h1>

<section class="card">
<div class="muted">Modo manual actual</div>
<div class="mode">{{ mode }}</div>
<div class="muted">
{{ 'SIMULACIÓN: no envía órdenes reales'
if dry_run else 'OPERACIÓN REAL HABILITADA' }}
</div>
</section>

<section class="buttons">
{% for item in modes %}
<form method="post"
action="/setmode/{{ item }}?secret={{ secret }}">
<button class="{{
'off' if item=='OFF'
else 'long' if item=='LONG_ONLY'
else 'short' if item=='SHORT_ONLY'
else 'close' if item=='CLOSE_ONLY'
else 'both'
}}">
{{
{
'OFF':'OFF',
'LONG_ONLY':'SOLO LONG',
'SHORT_ONLY':'SOLO SHORT',
'CLOSE_ONLY':'SOLO CERRAR',
'BOTH':'AMBOS'
}[item]
}}
</button>
</form>
{% endfor %}
</section>

<section class="card">
<h2>Posición registrada</h2>

{% if active %}
<div class="grid">

<div class="stat">
<div class="label">Dirección</div>
<div class="value">{{ active.side }}</div>
</div>

<div class="stat">
<div class="label">Entrada</div>
<div class="value">
{{ '%.2f'|format(active.entry_price) }}
</div>
</div>

<div class="stat">
<div class="label">Cantidad</div>
<div class="value">{{ active.quantity }}</div>
</div>

<div class="stat">
<div class="label">Margen</div>
<div class="value">
${{ '%.2f'|format(active.margin_used) }}
</div>
</div>

</div>
{% else %}
<div class="muted">
Sin posición abierta registrada
</div>
{% endif %}
</section>

<section class="card">
<form class="filter"
method="get"
action="/control">

<input type="hidden"
name="secret"
value="{{ secret }}">

<select name="period">
<option value="all"
{{ 'selected' if period=='all' }}>
Historial completo
</option>

<option value="last_month"
{{ 'selected' if period=='last_month' }}>
Últimos 30 días
</option>

<option value="last_3_months"
{{ 'selected' if period=='last_3_months' }}>
Últimos 3 meses
</option>

<option value="specific_month"
{{ 'selected' if period=='specific_month' }}>
Mes específico
</option>
</select>

<input type="month"
name="month"
value="{{ month }}">

<button class="apply">
VER PERIODO
</button>
</form>
</section>

<section class="card">
<h2>Estadísticas del periodo</h2>

<div class="grid">

<div class="stat">
<div class="label">Operaciones</div>
<div class="value">{{ stats.trades }}</div>
</div>

<div class="stat">
<div class="label">Winrate</div>
<div class="value">
{{ '%.2f'|format(stats.winrate) }}%
</div>
</div>

<div class="stat">
<div class="label">Ganadas / Perdidas</div>
<div class="value">
{{ stats.wins }} / {{ stats.losses }}
</div>
</div>

<div class="stat">
<div class="label">PNL bruto</div>
<div class="value">
${{ '%.2f'|format(stats.gross_pnl) }}
</div>
</div>

<div class="stat">
<div class="label">Comisiones</div>
<div class="value">
${{ '%.2f'|format(stats.fees) }}
</div>
</div>

<div class="stat">
<div class="label">PNL neto</div>
<div class="value {{
'positive' if stats.net_pnl>=0
else 'negative'
}}">
${{ '%.2f'|format(stats.net_pnl) }}
</div>
</div>

<div class="stat">
<div class="label">Rentabilidad</div>
<div class="value {{
'positive' if stats.return_pct>=0
else 'negative'
}}">
{{ '%.2f'|format(stats.return_pct) }}%
</div>
</div>

<div class="stat">
<div class="label">Drawdown máximo</div>
<div class="value negative">
{{ '%.2f'|format(stats.max_drawdown_pct) }}%
</div>
</div>

<div class="stat">
<div class="label">Profit factor</div>
<div class="value">
{{ '%.2f'|format(stats.profit_factor) }}
</div>
</div>

<div class="stat">
<div class="label">Mayor ganancia</div>
<div class="value positive">
${{ '%.2f'|format(stats.biggest_win) }}
</div>
</div>

<div class="stat">
<div class="label">Mayor pérdida</div>
<div class="value negative">
${{ '%.2f'|format(stats.biggest_loss) }}
</div>
</div>

</div>
</section>

<a class="download close"
href="/download?secret={{ secret }}&period={{ period }}&month={{ month }}">
DESCARGAR ESTE PERIODO
</a>

<section class="card table-wrap">
<table>
<thead>
<tr>
<th>Cierre</th>
<th>Lado</th>
<th>Entrada</th>
<th>Salida</th>
<th>Precio %</th>
<th>ROE bruto %</th>
<th>ROE neto %</th>
<th>Cuenta %</th>
<th>Comisión</th>
<th>PNL neto</th>
</tr>
</thead>

<tbody>
{% for t in trades %}
<tr>

<td>
{{ t.closed_at[:19].replace('T',' ') }}
</td>

<td>{{ t.side }}</td>

<td>
{{ '%.2f'|format(t.entry_price) }}
</td>

<td>
{{ '%.2f'|format(t.exit_price) }}
</td>

<td>
{{ '%.3f'|format(t.price_move_pct) }}%
</td>

<td>
{{ '%.2f'|format(t.gross_roe_pct) }}%
</td>

<td class="{{
'positive' if t.net_roe_pct>=0
else 'negative'
}}">
{{ '%.2f'|format(t.net_roe_pct) }}%
</td>

<td>
{{ '%.2f'|format(t.balance_impact_pct) }}%
</td>

<td>
${{ '%.2f'|format(t.total_fees) }}
</td>

<td class="{{
'positive' if t.net_pnl>=0
else 'negative'
}}">
${{ '%.2f'|format(t.net_pnl) }}
</td>

</tr>

{% else %}

<tr>
<td colspan="10" class="muted">
No hay operaciones en este periodo
</td>
</tr>

{% endfor %}
</tbody>
</table>
</section>

</main>
</body>
</html>
"""


app = Flask(__name__)


def control_authorized():
    return secret_matches(
        request.args.get("secret", ""),
        CONTROL_SECRET,
    )


@app.get("/")
def home():
    return (
        f"{BOT_NAME} activo | "
        f"DRY_RUN={DRY_RUN}",
        200,
    )


@app.get("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "bot": BOT_NAME,
            "bingx_symbol": BINGX_SYMBOL,
            "accepted_tv_symbols": sorted(
                TV_SYMBOLS
            ),
            "mode": store.get_mode(),
            "dry_run": DRY_RUN,
            "balance_percent": BALANCE_PERCENT,
            "leverage": LEVERAGE,
            "fee_rate_per_side": FEE_RATE,
            "persistent_redis": (
                store.redis_enabled
            ),
            "position_registered": bool(
                active_trade()
            ),
        }
    )


@app.get("/control")
def control():
    if not control_authorized():
        return "Clave de panel inválida", 403

    period = request.args.get(
        "period",
        "all",
    )

    month = request.args.get(
        "month",
        "",
    )

    trades = filter_trades(
        store.get_trades(),
        period,
        month,
    )

    return render_template_string(
        PANEL_HTML,
        bot_name=BOT_NAME,
        secret=request.args["secret"],
        mode=store.get_mode(),
        modes=VALID_MODES,
        dry_run=DRY_RUN,
        active=active_trade(),
        period=period,
        month=month,
        trades=list(reversed(trades)),
        stats=trade_summary(trades),
    )


@app.post("/setmode/<mode>")
def set_mode(mode):
    if not control_authorized():
        return "Clave de panel inválida", 403

    mode = mode.upper()

    if mode not in VALID_MODES:
        return jsonify(
            {"error": "Modo inválido"}
        ), 400

    store.set_mode(mode)

    notify(
        f"{BOT_NAME}: modo manual "
        f"cambiado a {mode}"
    )

    return redirect(
        "/control?secret="
        f"{request.args['secret']}"
    )


@app.get("/download")
def download():
    if not control_authorized():
        return "Clave de panel inválida", 403

    period = request.args.get(
        "period",
        "all",
    )

    month = request.args.get(
        "month",
        "",
    )

    trades = filter_trades(
        store.get_trades(),
        period,
        month,
    )

    filename = f"gold_trades_{period}"

    if month:
        filename += f"_{month}"

    filename += ".csv"

    return send_file(
        io.BytesIO(csv_bytes(trades)),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.post("/webhook")
def webhook():
    payload = request.get_json(
        silent=True
    ) or {}

    if not secret_matches(
        payload.get("secret"),
        WEBHOOK_SECRET,
    ):
        return jsonify(
            {
                "error": (
                    "Clave de webhook inválida"
                )
            }
        ), 403

    side = str(
        payload.get("side")
        or payload.get("action")
        or ""
    ).upper().strip()

    symbol = str(
        payload.get("symbol", "")
    ).upper().strip()

    timeframe = str(
        payload.get("timeframe", "")
    ).lower().strip()

    if side not in {"BUY", "SELL"}:
        return jsonify(
            {"error": "Usa BUY o SELL"}
        ), 400

    if (
        symbol not in TV_SYMBOLS
        and symbol != BINGX_SYMBOL.upper()
    ):
        return jsonify(
            {
                "error": (
                    "Símbolo de oro inválido"
                )
            }
        ), 400

    if timeframe not in {
        "1h",
        "60",
        "60m",
    }:
        return jsonify(
            {
                "error": (
                    "Solo se aceptan señales "
                    "1H cerradas"
                )
            }
        ), 400

    try:
        result = process_signal(side)

        return jsonify(
            {
                "ok": True,
                "result": result,
            }
        )

    except Exception as exc:
        notify(
            f"ERROR {BOT_NAME}: {exc}"
        )

        return jsonify(
            {
                "ok": False,
                "error": str(exc),
            }
        ), 500


@app.get("/monitor")
def monitor():
    if not secret_matches(
        request.args.get("token", ""),
        MONITOR_SECRET,
    ):
        return jsonify(
            {"error": "No autorizado"}
        ), 401

    return jsonify(
        {
            "checked_at": utc_now(),
            "mode": store.get_mode(),
            "active_trade": active_trade(),
            "stats_all": trade_summary(
                store.get_trades()
            ),
        }
    )


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv("PORT", "10000")
        ),
    )
