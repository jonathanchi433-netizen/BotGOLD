from flask import Flask, request, jsonify, send_file
import os
import time
import math
import hmac
import hashlib
import urllib.parse
import requests
import csv
import json
from datetime import datetime

app = Flask(__name__)

# =========================
# VARIABLES DE ENTORNO
# =========================
API_KEY = os.getenv("BINGX_API_KEY", "").strip()
SECRET_KEY = os.getenv("BINGX_SECRET_KEY", "").strip()

SYMBOL = os.getenv("SYMBOL", "XAUT-USDT").strip()
LEVERAGE = int(os.getenv("LEVERAGE", "5"))
QTY_BUFFER = float(os.getenv("QTY_BUFFER", "0.95"))

# Riesgo fijo
RISK_PERCENT_FIXED = 80.0

# Parciales
TP1_CLOSE_RATIO = 0.30
TP2_CLOSE_RATIO = 0.30

BASE_URL = "https://open-api.bingx.com"

print(
    f"GOLD BOT CONFIG -> SYMBOL={SYMBOL}, LEVERAGE={LEVERAGE}, "
    f"QTY_BUFFER={QTY_BUFFER}, RISK_PERCENT_FIXED={RISK_PERCENT_FIXED}, "
    f"TP1={TP1_CLOSE_RATIO}, TP2={TP2_CLOSE_RATIO}",
    flush=True
)

# =========================
# ARCHIVOS
# =========================
TRADES_LOG_FILE = "trades_log.csv"
EVENTS_LOG_FILE = "bot_events.csv"
STATE_FILE = "position_state.json"


# =========================
# UTILIDADES
# =========================
def utc_now():
    return datetime.utcnow().isoformat()


def now_ms():
    return int(time.time() * 1000)


def round_down(value, decimals=3):
    factor = 10 ** decimals
    return math.floor(value * factor) / factor


def safe_float(value, default=None):
    try:
        return float(value)
    except Exception:
        return default


# =========================
# FIRMA BINGX PRIVADO
# =========================
def sign_params(params: dict) -> str:
    params["timestamp"] = now_ms()
    sorted_params = sorted(params.items())
    query = urllib.parse.urlencode(sorted_params)
    signature = hmac.new(
        SECRET_KEY.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return f"{query}&signature={signature}"


def bingx_private_request(method, path, params=None):
    if params is None:
        params = {}

    query = sign_params(params)
    url = f"{BASE_URL}{path}?{query}"
    headers = {"X-BX-APIKEY": API_KEY}

    if method.upper() == "GET":
        response = requests.get(url, headers=headers, timeout=20)
    else:
        response = requests.post(url, headers=headers, timeout=20)

    try:
        data = response.json()
    except Exception:
        raise Exception(f"Respuesta no JSON de BingX privado: {response.text}")

    return data


# =========================
# LOGS / ESTADO
# =========================
def ensure_files():
    if not os.path.exists(TRADES_LOG_FILE):
        with open(TRADES_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "opened_at",
                "closed_at",
                "side",
                "symbol",
                "leverage",
                "risk_percent",
                "qty",
                "entry_price",
                "exit_price",
                "pnl_gross",
                "close_reason"
            ])

    if not os.path.exists(EVENTS_LOG_FILE):
        with open(EVENTS_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp",
                "action",
                "symbol",
                "message",
                "details"
            ])


def append_event_log(action, message, details):
    ensure_files()
    with open(EVENTS_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            utc_now(),
            action,
            SYMBOL,
            message,
            json.dumps(details, ensure_ascii=False)
        ])


def append_trade_log(opened_at, closed_at, side, qty, entry_price, exit_price, pnl_gross, close_reason):
    ensure_files()
    with open(TRADES_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            opened_at,
            closed_at,
            side,
            SYMBOL,
            LEVERAGE,
            RISK_PERCENT_FIXED,
            qty,
            entry_price,
            exit_price,
            pnl_gross,
            close_reason
        ])


def load_state():
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def clear_state():
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)


# =========================
# BINGX PRIVADO - CUENTA
# =========================
def get_price():
    data = bingx_private_request("GET", "/openApi/swap/v2/quote/price", {
        "symbol": SYMBOL
    })
    price = data.get("data", {}).get("price")
    if not price:
        raise Exception(f"No se pudo obtener el precio: {data}")
    return float(price)


def get_balance():
    data = bingx_private_request("GET", "/openApi/swap/v2/user/balance")
    balance_data = data.get("data", {})

    if isinstance(balance_data, dict):
        if "balance" in balance_data and isinstance(balance_data["balance"], dict):
            bal = balance_data["balance"].get("availableBalance") or balance_data["balance"].get("balance")
            if bal is not None:
                return float(bal)

        bal = balance_data.get("availableBalance") or balance_data.get("balance")
        if bal is not None:
            return float(bal)

    raise Exception(f"No se pudo obtener el balance: {data}")


def get_positions():
    data = bingx_private_request("GET", "/openApi/swap/v2/user/positions", {
        "symbol": SYMBOL
    })
    positions = data.get("data", [])
    if isinstance(positions, dict):
        positions = [positions]
    return positions


def get_current_position_info():
    positions = get_positions()

    for pos in positions:
        pos_symbol = str(pos.get("symbol", "")).strip()
        if pos_symbol != SYMBOL:
            continue

        amount = pos.get("positionAmt") or pos.get("positionAmount") or pos.get("availableAmt") or 0
        try:
            amount = float(amount)
        except Exception:
            amount = 0.0

        if amount == 0:
            continue

        side = str(pos.get("positionSide", "")).upper()
        avg_price_raw = (
            pos.get("avgPrice")
            or pos.get("averagePrice")
            or pos.get("positionAvgPrice")
            or pos.get("avgOpenPrice")
        )

        avg_price = None
        try:
            if avg_price_raw is not None:
                avg_price = float(avg_price_raw)
        except Exception:
            avg_price = None

        if side in ["LONG", "SHORT"]:
            return {
                "side": side,
                "qty": abs(amount),
                "entry_price": avg_price
            }

        if amount > 0:
            return {
                "side": "LONG",
                "qty": abs(amount),
                "entry_price": avg_price
            }
        elif amount < 0:
            return {
                "side": "SHORT",
                "qty": abs(amount),
                "entry_price": avg_price
            }

    return {
        "side": "NONE",
        "qty": 0.0,
        "entry_price": None
    }


# =========================
# LÓGICA HTF DESDE ALERTA
# =========================
def determine_alignment(action: str, htf_signal: str):
    htf_signal = str(htf_signal or "").upper().strip()

    if htf_signal not in ["BUY", "SELL"]:
        return "neutral_htf"

    if action == "buy":
        return "with_htf" if htf_signal == "BUY" else "against_htf"

    if action == "sell":
        return "with_htf" if htf_signal == "SELL" else "against_htf"

    return "neutral_htf"


# =========================
# SINCRONIZACIÓN
# =========================
def sync_state_with_exchange():
    state = load_state()
    current = get_current_position_info()

    if current["side"] == "NONE":
        if state is not None:
            clear_state()
        return None, current

    if state is None:
        inferred_state = {
            "side": current["side"],
            "qty": current["qty"],
            "entry_price": current["entry_price"],
            "opened_at": utc_now(),
            "symbol": SYMBOL,
            "leverage": LEVERAGE,
            "risk_percent": RISK_PERCENT_FIXED,
            "tp1_done": False,
            "tp2_done": False
        }
        save_state(inferred_state)
        state = inferred_state

    # Asegurar campos nuevos
    if "tp1_done" not in state:
        state["tp1_done"] = False
    if "tp2_done" not in state:
        state["tp2_done"] = False

    return state, current


# =========================
# ÓRDENES
# =========================
def calculate_order_quantity():
    balance = get_balance()
    price = get_price()

    margin_to_use = balance * (RISK_PERCENT_FIXED / 100.0)
    notional = margin_to_use * LEVERAGE
    qty = notional / price
    qty = qty * QTY_BUFFER
    qty = round_down(qty, 3)

    print(
        f"DEBUG QTY -> balance={balance}, price={price}, risk_percent={RISK_PERCENT_FIXED}, "
        f"margin_to_use={margin_to_use}, notional={notional}, qty_buffered={qty}",
        flush=True
    )

    if qty <= 0:
        raise Exception("La cantidad calculada es 0. Revisa balance, leverage o precio.")

    return qty


def extract_order_data(order_response):
    order = order_response.get("data", {}).get("order", {})
    avg_price_raw = order.get("avgPrice")
    executed_qty_raw = order.get("executedQty") or order.get("quantity")

    avg_price = None
    executed_qty = None

    try:
        if avg_price_raw is not None:
            avg_price = float(avg_price_raw)
    except Exception:
        avg_price = None

    try:
        if executed_qty_raw is not None:
            executed_qty = float(executed_qty_raw)
    except Exception:
        executed_qty = None

    return avg_price, executed_qty


def place_order(side, quantity, reduce_only=False):
    params = {
        "symbol": SYMBOL,
        "side": side.upper(),
        "positionSide": "BOTH",
        "type": "MARKET",
        "quantity": quantity,
        "reduceOnly": "true" if reduce_only else "false"
    }

    print(f"ENVIANDO ORDEN -> side={side}, quantity={quantity}, reduce_only={reduce_only}", flush=True)

    data = bingx_private_request("POST", "/openApi/swap/v2/trade/order", params)

    if str(data.get("code")) != "0":
        raise Exception(f"Error BingX: {data}")

    return data


def close_position(current_side, current_qty):
    if current_side == "LONG":
        return place_order("SELL", round_down(current_qty, 3), reduce_only=True)
    elif current_side == "SHORT":
        return place_order("BUY", round_down(current_qty, 3), reduce_only=True)
    return None


def close_partial_position(current_side, current_qty, ratio):
    qty_to_close = round_down(current_qty * ratio, 3)

    # Si el parcial sale muy pequeño, cerrar el mínimo posible o todo lo restante
    if qty_to_close <= 0:
        qty_to_close = round_down(current_qty, 3)

    if current_side == "LONG":
        return place_order("SELL", qty_to_close, reduce_only=True), qty_to_close
    elif current_side == "SHORT":
        return place_order("BUY", qty_to_close, reduce_only=True), qty_to_close

    return None, 0.0


def open_new_position(action, qty):
    if action == "buy":
        return place_order("BUY", qty, reduce_only=False)
    elif action == "sell":
        return place_order("SELL", qty, reduce_only=False)
    else:
        raise Exception("Acción inválida para abrir posición.")


def calc_gross_pnl(side, qty, entry_price, exit_price):
    if entry_price is None or exit_price is None:
        return None

    if side == "LONG":
        return round((exit_price - entry_price) * qty, 6)
    elif side == "SHORT":
        return round((entry_price - exit_price) * qty, 6)

    return None


# =========================
# ABRIR / CERRAR
# =========================
def execute_open(action):
    state, current = sync_state_with_exchange()
    current_side = current["side"]

    if action == "buy" and current_side == "LONG":
        return {"message": "Ya estás en LONG, no se abre otra posición."}

    if action == "sell" and current_side == "SHORT":
        return {"message": "Ya estás en SHORT, no se abre otra posición."}

    if action == "buy" and current_side == "SHORT":
        return {"message": "Hay SHORT abierto. BUY nuevo no se abre hasta cierre previo."}

    if action == "sell" and current_side == "LONG":
        return {"message": "Hay LONG abierto. SELL nuevo no se abre hasta cierre previo."}

    new_qty = calculate_order_quantity()

    open_result = open_new_position(action, new_qty)
    open_price, open_qty = extract_order_data(open_result)
    open_qty = open_qty if open_qty is not None else new_qty

    new_state = {
        "side": "LONG" if action == "buy" else "SHORT",
        "qty": open_qty,
        "entry_price": open_price,
        "opened_at": utc_now(),
        "symbol": SYMBOL,
        "leverage": LEVERAGE,
        "risk_percent": RISK_PERCENT_FIXED,
        "tp1_done": False,
        "tp2_done": False
    }
    save_state(new_state)

    return {
        "message": "BUY ejecutado" if action == "buy" else "SELL ejecutado",
        "risk_percent_used": RISK_PERCENT_FIXED,
        "sent_qty": open_qty,
        "opened_entry_price": open_price,
        "result": open_result
    }


def execute_close_by_opposite_signal(action):
    state, current = sync_state_with_exchange()
    current_side = current["side"]
    current_qty = current["qty"]

    if action == "buy" and current_side == "SHORT":
        close_result = close_position(current_side, current_qty)
        close_price, closed_qty = extract_order_data(close_result)
        closed_qty = closed_qty if closed_qty is not None else current_qty

        entry_price = state.get("entry_price") if state else None
        opened_at = state.get("opened_at") if state else ""
        pnl_gross = calc_gross_pnl("SHORT", closed_qty, entry_price, close_price)

        append_trade_log(
            opened_at=opened_at,
            closed_at=utc_now(),
            side="SHORT",
            qty=closed_qty,
            entry_price=entry_price,
            exit_price=close_price,
            pnl_gross=pnl_gross,
            close_reason="opposite_buy_signal_close_only"
        )

        clear_state()

        return {
            "message": "SHORT cerrado por señal BUY contraria",
            "closed_qty": closed_qty,
            "closed_entry_price": entry_price,
            "closed_exit_price": close_price,
            "closed_pnl_gross": pnl_gross,
            "close_result": close_result
        }

    if action == "sell" and current_side == "LONG":
        close_result = close_position(current_side, current_qty)
        close_price, closed_qty = extract_order_data(close_result)
        closed_qty = closed_qty if closed_qty is not None else current_qty

        entry_price = state.get("entry_price") if state else None
        opened_at = state.get("opened_at") if state else ""
        pnl_gross = calc_gross_pnl("LONG", closed_qty, entry_price, close_price)

        append_trade_log(
            opened_at=opened_at,
            closed_at=utc_now(),
            side="LONG",
            qty=closed_qty,
            entry_price=entry_price,
            exit_price=close_price,
            pnl_gross=pnl_gross,
            close_reason="opposite_sell_signal_close_only"
        )

        clear_state()

        return {
            "message": "LONG cerrado por señal SELL contraria",
            "closed_qty": closed_qty,
            "closed_entry_price": entry_price,
            "closed_exit_price": close_price,
            "closed_pnl_gross": pnl_gross,
            "close_result": close_result
        }

    return None


def execute_explicit_close(action):
    state, current = sync_state_with_exchange()
    current_side = current["side"]
    current_qty = current["qty"]

    if action == "close_long":
        if current_side != "LONG":
            return {"message": "No hay LONG abierto para cerrar."}

        close_result = close_position(current_side, current_qty)
        close_price, closed_qty = extract_order_data(close_result)
        closed_qty = closed_qty if closed_qty is not None else current_qty

        entry_price = state.get("entry_price") if state else None
        opened_at = state.get("opened_at") if state else ""
        pnl_gross = calc_gross_pnl("LONG", closed_qty, entry_price, close_price)

        append_trade_log(
            opened_at=opened_at,
            closed_at=utc_now(),
            side="LONG",
            qty=closed_qty,
            entry_price=entry_price,
            exit_price=close_price,
            pnl_gross=pnl_gross,
            close_reason="explicit_close_long"
        )

        clear_state()

        return {
            "message": "LONG cerrado por señal CLOSE_LONG",
            "closed_qty": closed_qty,
            "closed_entry_price": entry_price,
            "closed_exit_price": close_price,
            "closed_pnl_gross": pnl_gross,
            "close_result": close_result
        }

    if action == "close_short":
        if current_side != "SHORT":
            return {"message": "No hay SHORT abierto para cerrar."}

        close_result = close_position(current_side, current_qty)
        close_price, closed_qty = extract_order_data(close_result)
        closed_qty = closed_qty if closed_qty is not None else current_qty

        entry_price = state.get("entry_price") if state else None
        opened_at = state.get("opened_at") if state else ""
        pnl_gross = calc_gross_pnl("SHORT", closed_qty, entry_price, close_price)

        append_trade_log(
            opened_at=opened_at,
            closed_at=utc_now(),
            side="SHORT",
            qty=closed_qty,
            entry_price=entry_price,
            exit_price=close_price,
            pnl_gross=pnl_gross,
            close_reason="explicit_close_short"
        )

        clear_state()

        return {
            "message": "SHORT cerrado por señal CLOSE_SHORT",
            "closed_qty": closed_qty,
            "closed_entry_price": entry_price,
            "closed_exit_price": close_price,
            "closed_pnl_gross": pnl_gross,
            "close_result": close_result
        }

    return None


def execute_partial_close(action):
    state, current = sync_state_with_exchange()
    current_side = current["side"]
    current_qty = current["qty"]

    if state is None or current_side == "NONE" or current_qty <= 0:
        return {"message": "No hay posición abierta para parcial."}

    entry_price = state.get("entry_price")
    opened_at = state.get("opened_at", "")
    tp1_done = state.get("tp1_done", False)
    tp2_done = state.get("tp2_done", False)

    if action == "tp1_long":
        if current_side != "LONG":
            return {"message": "No hay LONG abierto para TP1_LONG."}
        if tp1_done:
            return {"message": "TP1_LONG ya ejecutado."}

        close_result, qty_closed = close_partial_position(current_side, current_qty, TP1_CLOSE_RATIO)
        close_price, executed_qty = extract_order_data(close_result)
        executed_qty = executed_qty if executed_qty is not None else qty_closed

        pnl_gross = calc_gross_pnl("LONG", executed_qty, entry_price, close_price)

        state["tp1_done"] = True
        state["qty"] = round_down(max(current_qty - executed_qty, 0.0), 3)
        save_state(state)

        append_trade_log(
            opened_at=opened_at,
            closed_at=utc_now(),
            side="LONG",
            qty=executed_qty,
            entry_price=entry_price,
            exit_price=close_price,
            pnl_gross=pnl_gross,
            close_reason="tp1_long_30pct"
        )

        return {
            "message": "TP1_LONG ejecutado (30%)",
            "closed_qty": executed_qty,
            "remaining_qty_state": state["qty"],
            "exit_price": close_price,
            "pnl_gross": pnl_gross,
            "close_result": close_result
        }

    if action == "tp2_long":
        if current_side != "LONG":
            return {"message": "No hay LONG abierto para TP2_LONG."}
        if not tp1_done:
            return {"message": "TP2_LONG bloqueado: primero debe ejecutarse TP1_LONG."}
        if tp2_done:
            return {"message": "TP2_LONG ya ejecutado."}

        close_result, qty_closed = close_partial_position(current_side, current_qty, TP2_CLOSE_RATIO)
        close_price, executed_qty = extract_order_data(close_result)
        executed_qty = executed_qty if executed_qty is not None else qty_closed

        pnl_gross = calc_gross_pnl("LONG", executed_qty, entry_price, close_price)

        state["tp2_done"] = True
        state["qty"] = round_down(max(current_qty - executed_qty, 0.0), 3)
        save_state(state)

        append_trade_log(
            opened_at=opened_at,
            closed_at=utc_now(),
            side="LONG",
            qty=executed_qty,
            entry_price=entry_price,
            exit_price=close_price,
            pnl_gross=pnl_gross,
            close_reason="tp2_long_30pct"
        )

        return {
            "message": "TP2_LONG ejecutado (30%)",
            "closed_qty": executed_qty,
            "remaining_qty_state": state["qty"],
            "exit_price": close_price,
            "pnl_gross": pnl_gross,
            "close_result": close_result
        }

    if action == "tp1_short":
        if current_side != "SHORT":
            return {"message": "No hay SHORT abierto para TP1_SHORT."}
        if tp1_done:
            return {"message": "TP1_SHORT ya ejecutado."}

        close_result, qty_closed = close_partial_position(current_side, current_qty, TP1_CLOSE_RATIO)
        close_price, executed_qty = extract_order_data(close_result)
        executed_qty = executed_qty if executed_qty is not None else qty_closed

        pnl_gross = calc_gross_pnl("SHORT", executed_qty, entry_price, close_price)

        state["tp1_done"] = True
        state["qty"] = round_down(max(current_qty - executed_qty, 0.0), 3)
        save_state(state)

        append_trade_log(
            opened_at=opened_at,
            closed_at=utc_now(),
            side="SHORT",
            qty=executed_qty,
            entry_price=entry_price,
            exit_price=close_price,
            pnl_gross=pnl_gross,
            close_reason="tp1_short_30pct"
        )

        return {
            "message": "TP1_SHORT ejecutado (30%)",
            "closed_qty": executed_qty,
            "remaining_qty_state": state["qty"],
            "exit_price": close_price,
            "pnl_gross": pnl_gross,
            "close_result": close_result
        }

    if action == "tp2_short":
        if current_side != "SHORT":
            return {"message": "No hay SHORT abierto para TP2_SHORT."}
        if not tp1_done:
            return {"message": "TP2_SHORT bloqueado: primero debe ejecutarse TP1_SHORT."}
        if tp2_done:
            return {"message": "TP2_SHORT ya ejecutado."}

        close_result, qty_closed = close_partial_position(current_side, current_qty, TP2_CLOSE_RATIO)
        close_price, executed_qty = extract_order_data(close_result)
        executed_qty = executed_qty if executed_qty is not None else qty_closed

        pnl_gross = calc_gross_pnl("SHORT", executed_qty, entry_price, close_price)

        state["tp2_done"] = True
        state["qty"] = round_down(max(current_qty - executed_qty, 0.0), 3)
        save_state(state)

        append_trade_log(
            opened_at=opened_at,
            closed_at=utc_now(),
            side="SHORT",
            qty=executed_qty,
            entry_price=entry_price,
            exit_price=close_price,
            pnl_gross=pnl_gross,
            close_reason="tp2_short_30pct"
        )

        return {
            "message": "TP2_SHORT ejecutado (30%)",
            "closed_qty": executed_qty,
            "remaining_qty_state": state["qty"],
            "exit_price": close_price,
            "pnl_gross": pnl_gross,
            "close_result": close_result
        }

    return None


# =========================
# RUTAS
# =========================
@app.route("/", methods=["GET"])
def home():
    return "GOLD BOT TEST ACTIVO - PARCIALES 30/30/40", 200


@app.route("/logs", methods=["GET"])
def download_trade_logs():
    ensure_files()
    return send_file(TRADES_LOG_FILE, as_attachment=True)


@app.route("/events", methods=["GET"])
def download_event_logs():
    ensure_files()
    return send_file(EVENTS_LOG_FILE, as_attachment=True)


@app.route("/state", methods=["GET"])
def get_state():
    state = load_state()
    current = get_current_position_info()
    return jsonify({
        "saved_state": state,
        "exchange_position": current
    }), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("Señal recibida:", data, flush=True)

    action_raw = str(data.get("action", "")).upper().strip()
    htf_signal = str(data.get("htf_signal", "")).upper().strip()
    incoming_symbol = str(data.get("symbol", "")).strip()

    if incoming_symbol != SYMBOL:
        msg = f"Señal ignorada: símbolo recibido {incoming_symbol} != {SYMBOL}"
        print(msg, flush=True)
        append_event_log("ignored", msg, {"received": data})
        return jsonify({"ok": True, "ignored": True, "message": msg}), 200

    action_map = {
        "BUY": "buy",
        "SELL": "sell",
        "CLOSE_LONG": "close_long",
        "CLOSE_SHORT": "close_short",
        "TP1_LONG": "tp1_long",
        "TP2_LONG": "tp2_long",
        "TP1_SHORT": "tp1_short",
        "TP2_SHORT": "tp2_short",
    }

    action = action_map.get(action_raw, "")

    if action == "":
        append_event_log(action_raw, "Acción inválida", {"received": data})
        return jsonify({
            "ok": False,
            "error": "Acción inválida",
            "received": data
        }), 400

    try:
        # 1) Parciales
        if action in ["tp1_long", "tp2_long", "tp1_short", "tp2_short"]:
            partial_result = execute_partial_close(action)
            print("PARCIAL ->", partial_result, flush=True)
            append_event_log(action, partial_result.get("message", "Parcial ejecutado"), partial_result)
            return jsonify({"ok": True, "result": partial_result}), 200

        # 2) Cierres explícitos finales
        if action in ["close_long", "close_short"]:
            close_result = execute_explicit_close(action)
            print("CIERRE EXPLÍCITO ->", close_result, flush=True)
            append_event_log(action, close_result.get("message", "Cierre explícito"), close_result)
            return jsonify({"ok": True, "result": close_result}), 200

        # 3) Si hay posición contraria abierta, cerrarla
        close_result = execute_close_by_opposite_signal(action)
        if close_result is not None:
            print("CIERRE POR SEÑAL CONTRARIA ->", close_result, flush=True)
            append_event_log(action, close_result.get("message", "Cierre por señal contraria"), close_result)

            alignment = determine_alignment(action, htf_signal)
            if alignment != "with_htf":
                msg = f"Posición cerrada, pero nueva entrada bloqueada: acción {action_raw} no está a favor del 15m ({htf_signal})"
                print(msg, flush=True)
                append_event_log(action, msg, {"received": data, "alignment": alignment})
                return jsonify({
                    "ok": True,
                    "result": close_result,
                    "filtered": True,
                    "message": msg
                }), 200

            open_result = execute_open(action)
            print("REVERSAL EJECUTADO ->", open_result, flush=True)
            append_event_log(action, open_result.get("message", "Reversal ejecutado"), open_result)

            return jsonify({
                "ok": True,
                "closed_result": close_result,
                "opened_result": open_result
            }), 200

        # 4) Si no había contraria, abrir solo a favor del 15m
        alignment = determine_alignment(action, htf_signal)
        if alignment != "with_htf":
            msg = f"Trade bloqueado: acción {action_raw} no está a favor del 15m ({htf_signal})"
            print(msg, flush=True)
            append_event_log(action, msg, {"received": data, "alignment": alignment})
            return jsonify({
                "ok": True,
                "filtered": True,
                "message": msg
            }), 200

        result = execute_open(action)
        print("RESULTADO TRADE ->", result, flush=True)
        append_event_log(action, result.get("message", "Trade ejecutado"), result)

        return jsonify({"ok": True, "result": result}), 200

    except Exception as e:
        print("ERROR webhook:", str(e), flush=True)
        try:
            append_event_log(action, f"ERROR webhook: {str(e)}", {"received": data})
        except Exception:
            pass

        return jsonify({
            "ok": False,
            "error": str(e),
            "received": data
        }), 500


if __name__ == "__main__":
    ensure_files()
    app.run(host="0.0.0.0", port=10000)