from flask import Flask, request, jsonify, send_file
from openai import OpenAI
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

# Símbolo de trading real en BingX
SYMBOL = os.getenv("SYMBOL", "BTC-USDT").strip()

# Símbolo chart / quote para velas
CHART_SYMBOL = os.getenv("CHART_SYMBOL", SYMBOL).strip()

LEVERAGE = int(os.getenv("LEVERAGE", "5"))
QTY_BUFFER = float(os.getenv("QTY_BUFFER", "0.95"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4").strip()
AI_FILTER_ENABLED = os.getenv("AI_FILTER_ENABLED", "true").strip().lower() == "true"

# Porcentajes internos
RISK_LOW_PERCENT = 30.0
RISK_MEDIUM_PERCENT = 55.0
RISK_HIGH_PERCENT = 85.0

# Respaldo si algo falla
RISK_PERCENT_FALLBACK = 30.0

BASE_URL = "https://open-api.bingx.com"

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

print(
    f"BOT CONFIG -> SYMBOL={SYMBOL}, CHART_SYMBOL={CHART_SYMBOL}, LEVERAGE={LEVERAGE}, "
    f"QTY_BUFFER={QTY_BUFFER}, AI_FILTER_ENABLED={AI_FILTER_ENABLED}, OPENAI_MODEL={OPENAI_MODEL}",
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


def safe_int(value, default=None):
    try:
        return int(float(value))
    except Exception:
        return default


def extract_json_from_text(text: str):
    if not text:
        return None

    cleaned = text.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(cleaned)
    except Exception:
        pass

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = cleaned[start:end + 1]
        try:
            return json.loads(candidate)
        except Exception:
            return None

    return None


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


def append_trade_log(opened_at, closed_at, side, qty, entry_price, exit_price, pnl_gross, close_reason, risk_percent_used):
    ensure_files()
    with open(TRADES_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            opened_at,
            closed_at,
            side,
            SYMBOL,
            LEVERAGE,
            risk_percent_used,
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
# BINGX PÚBLICO - VELAS
# =========================
def get_public_klines(symbol: str, interval: str, limit: int = 260):
    """
    Intenta leer velas públicas de BingX.
    """
    endpoints = [
        f"{BASE_URL}/openApi/swap/v3/quote/klines",
        f"{BASE_URL}/openApi/swap/v2/quote/klines",
    ]

    last_error = None

    for endpoint in endpoints:
        try:
            response = requests.get(
                endpoint,
                params={"symbol": symbol, "interval": interval, "limit": limit},
                timeout=20
            )
            data = response.json()

            payload = data.get("data", data)

            # casos posibles: lista directa o dict con list
            if isinstance(payload, list):
                klines = payload
            elif isinstance(payload, dict):
                klines = (
                    payload.get("klines")
                    or payload.get("list")
                    or payload.get("items")
                    or payload.get("data")
                    or []
                )
            else:
                klines = []

            if not klines:
                last_error = f"Sin velas útiles en {endpoint}: {data}"
                continue

            parsed = []
            for item in klines:
                if isinstance(item, dict):
                    o = safe_float(item.get("open"))
                    h = safe_float(item.get("high"))
                    l = safe_float(item.get("low"))
                    c = safe_float(item.get("close"))
                    t = item.get("time") or item.get("openTime") or item.get("timestamp")
                elif isinstance(item, list) and len(item) >= 5:
                    # [time, open, high, low, close, ...]
                    t = item[0]
                    o = safe_float(item[1])
                    h = safe_float(item[2])
                    l = safe_float(item[3])
                    c = safe_float(item[4])
                else:
                    continue

                if None in [o, h, l, c]:
                    continue

                parsed.append({
                    "time": t,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                })

            if parsed:
                return parsed

            last_error = f"No se pudieron parsear velas en {endpoint}: {data}"

        except Exception as e:
            last_error = str(e)

    raise Exception(f"Error obteniendo velas públicas de BingX: {last_error}")


# =========================
# INDICADORES
# =========================
def sma(values, length):
    out = [None] * len(values)
    if length <= 0:
        return out

    running = 0.0
    for i, v in enumerate(values):
        running += v
        if i >= length:
            running -= values[i - length]
        if i >= length - 1:
            out[i] = running / length
    return out


def ema(values, length):
    out = [None] * len(values)
    if not values or length <= 0:
        return out

    alpha = 2 / (length + 1)
    prev = values[0]
    out[0] = prev

    for i in range(1, len(values)):
        prev = (values[i] * alpha) + (prev * (1 - alpha))
        out[i] = prev

    return out


def atr(highs, lows, closes, length=14):
    tr = []
    for i in range(len(closes)):
        if i == 0:
            tr.append(highs[i] - lows[i])
        else:
            tr.append(max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1])
            ))
    return ema(tr, length)


def stochastic(highs, lows, closes, k_length=14, k_smooth=3, d_length=3):
    raw_k = [None] * len(closes)

    for i in range(len(closes)):
        if i < k_length - 1:
            continue

        hh = max(highs[i - k_length + 1:i + 1])
        ll = min(lows[i - k_length + 1:i + 1])

        if hh == ll:
            raw_k[i] = 50.0
        else:
            raw_k[i] = ((closes[i] - ll) / (hh - ll)) * 100.0

    raw_k_clean = [x if x is not None else 50.0 for x in raw_k]
    k = sma(raw_k_clean, k_smooth)
    k_clean = [x if x is not None else 50.0 for x in k]
    d = sma(k_clean, d_length)

    return k, d


def compute_context_from_klines(klines_5m, klines_15m):
    closes_5 = [x["close"] for x in klines_5m]
    highs_5 = [x["high"] for x in klines_5m]
    lows_5 = [x["low"] for x in klines_5m]

    closes_15 = [x["close"] for x in klines_15m]
    highs_15 = [x["high"] for x in klines_15m]
    lows_15 = [x["low"] for x in klines_15m]

    ema13_5 = ema(closes_5, 13)[-1]
    ema62_5 = ema(closes_5, 62)[-1]
    ema200_5 = ema(closes_5, 200)[-1]
    atr_5 = atr(highs_5, lows_5, closes_5, 14)[-1]
    k_5, d_5 = stochastic(highs_5, lows_5, closes_5, 14, 3, 3)
    stoch_k_5 = k_5[-1]
    stoch_d_5 = d_5[-1]

    ema13_15 = ema(closes_15, 13)[-1]
    ema62_15 = ema(closes_15, 62)[-1]
    ema200_15 = ema(closes_15, 200)[-1]
    atr_15 = atr(highs_15, lows_15, closes_15, 14)[-1]
    k_15, d_15 = stochastic(highs_15, lows_15, closes_15, 14, 3, 3)
    stoch_k_15 = k_15[-1]
    stoch_d_15 = d_15[-1]

    close_5 = closes_5[-1]
    close_15 = closes_15[-1]

    atr_pct_5 = (atr_5 / close_5 * 100.0) if atr_5 and close_5 else None
    atr_pct_15 = (atr_15 / close_15 * 100.0) if atr_15 and close_15 else None

    trend_15m = "neutral"
    if close_15 > ema62_15 and ema13_15 > ema62_15 and ema62_15 > ema200_15:
        trend_15m = "bullish"
    elif close_15 < ema62_15 and ema13_15 < ema62_15 and ema62_15 < ema200_15:
        trend_15m = "bearish"

    return {
        "close_5m": close_5,
        "ema13_5m": ema13_5,
        "ema62_5m": ema62_5,
        "ema200_5m": ema200_5,
        "atr_5m": atr_5,
        "atr_pct_5m": atr_pct_5,
        "stoch_k_5m": stoch_k_5,
        "stoch_d_5m": stoch_d_5,

        "close_15m": close_15,
        "ema13_15m": ema13_15,
        "ema62_15m": ema62_15,
        "ema200_15m": ema200_15,
        "atr_15m": atr_15,
        "atr_pct_15m": atr_pct_15,
        "stoch_k_15m": stoch_k_15,
        "stoch_d_15m": stoch_d_15,

        "trend_15m": trend_15m
    }


def get_market_context():
    klines_5m = get_public_klines(CHART_SYMBOL, "5m", 260)
    klines_15m = get_public_klines(CHART_SYMBOL, "15m", 260)
    return compute_context_from_klines(klines_5m, klines_15m)


# =========================
# LÓGICA IA / PROBABILIDAD
# =========================
def determine_alignment(action: str, trend_15m: str):
    if action == "buy":
        if trend_15m == "bullish":
            return "with_htf"
        elif trend_15m == "bearish":
            return "against_htf"
        return "neutral_htf"

    if action == "sell":
        if trend_15m == "bearish":
            return "with_htf"
        elif trend_15m == "bullish":
            return "against_htf"
        return "neutral_htf"

    return "neutral_htf"


def probability_to_risk(probability: int, alignment: str):
    if alignment == "neutral_htf":
        return "REJECT", 0.0

    # Contra 15m: solo 30%, solo si >50
    if alignment == "against_htf":
        if probability <= 50:
            return "REJECT", 0.0
        return "COUNTER", RISK_LOW_PERCENT

    # A favor del 15m
    if probability < 30:
        return "REJECT", 0.0
    elif 30 <= probability <= 59:
        return "LOW", RISK_LOW_PERCENT
    elif 60 <= probability <= 80:
        return "MEDIUM", RISK_MEDIUM_PERCENT
    else:
        return "HIGH", RISK_HIGH_PERCENT


def ai_filter_signal(action, payload, market_context):
    if not AI_FILTER_ENABLED:
        alignment = determine_alignment(action, market_context["trend_15m"])
        tier, risk_percent = probability_to_risk(100, alignment)
        return {
            "decision": "APPROVE" if risk_percent > 0 else "REJECT",
            "probability": 100,
            "tier": tier,
            "risk_percent": risk_percent,
            "reason": "AI filter disabled",
            "alignment": alignment
        }

    if client is None:
        return {
            "decision": "REJECT",
            "probability": 0,
            "tier": "REJECT",
            "risk_percent": 0.0,
            "reason": "OPENAI_API_KEY missing",
            "alignment": "neutral_htf"
        }

    try:
        source = str(payload.get("source", "unknown")).lower().strip()
        alignment = determine_alignment(action, market_context["trend_15m"])

        signal_context = {
            "action": str(action).upper(),
            "source": source,

            "alignment": alignment,
            "trend_15m": market_context["trend_15m"],

            "close_5m": market_context["close_5m"],
            "ema13_5m": market_context["ema13_5m"],
            "ema62_5m": market_context["ema62_5m"],
            "ema200_5m": market_context["ema200_5m"],
            "atr_5m": market_context["atr_5m"],
            "atr_pct_5m": market_context["atr_pct_5m"],
            "stoch_k_5m": market_context["stoch_k_5m"],
            "stoch_d_5m": market_context["stoch_d_5m"],

            "close_15m": market_context["close_15m"],
            "ema13_15m": market_context["ema13_15m"],
            "ema62_15m": market_context["ema62_15m"],
            "ema200_15m": market_context["ema200_15m"],
            "atr_15m": market_context["atr_15m"],
            "atr_pct_15m": market_context["atr_pct_15m"],
            "stoch_k_15m": market_context["stoch_k_15m"],
            "stoch_d_15m": market_context["stoch_d_15m"],

            "symbol": payload.get("symbol", CHART_SYMBOL),
            "timeframe": payload.get("timeframe", "5"),
            "htf": payload.get("htf", "15"),
            "utc_time": utc_now()
        }

        system_prompt = """
You are a conservative BTCUSDT signal quality evaluator.

The trading signal already exists from the user's indicator.
Your job is NOT to create trades.
Your job is ONLY to score the quality of the existing setup from 0 to 100.

Priorities:
1) 15m structural direction
2) ATR / expansion quality
3) stochastic timing quality
4) signal source quality ("filtered" is stronger than "original")
5) whether the setup looks clean vs noisy

Important:
- If the signal is weak, stretched, noisy, or poor quality, give a low score.
- Return ONLY valid JSON.
- Do not explain anything outside JSON.

Return exactly:
{
  "decision": "APPROVE" or "REJECT",
  "probability": 0,
  "reason": "short explanation"
}
"""

        user_prompt = f"Evaluate this BTCUSDT setup:\n{json.dumps(signal_context, ensure_ascii=False)}"

        response = client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )

        raw_text = response.output_text.strip()
        parsed = extract_json_from_text(raw_text)

        if not parsed:
            return {
                "decision": "REJECT",
                "probability": 0,
                "tier": "REJECT",
                "risk_percent": 0.0,
                "reason": f"Invalid AI output: {raw_text[:300]}",
                "alignment": alignment
            }

        decision = str(parsed.get("decision", "REJECT")).upper().strip()
        probability = safe_int(parsed.get("probability", 0), 0)
        reason = str(parsed.get("reason", "")).strip()

        probability = max(0, min(100, probability))

        tier, risk_percent = probability_to_risk(probability, alignment)

        if decision != "APPROVE" or risk_percent <= 0:
            return {
                "decision": "REJECT",
                "probability": probability,
                "tier": "REJECT",
                "risk_percent": 0.0,
                "reason": reason or "Probability below execution threshold",
                "alignment": alignment
            }

        return {
            "decision": "APPROVE",
            "probability": probability,
            "tier": tier,
            "risk_percent": risk_percent,
            "reason": reason,
            "alignment": alignment
        }

    except Exception as e:
        return {
            "decision": "REJECT",
            "probability": 0,
            "tier": "REJECT",
            "risk_percent": 0.0,
            "reason": f"AI filter error: {str(e)}",
            "alignment": "neutral_htf"
        }


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
            "risk_percent": RISK_PERCENT_FALLBACK
        }
        save_state(inferred_state)
        state = inferred_state

    return state, current


# =========================
# ORDENES
# =========================
def calculate_order_quantity(risk_percent_override=None):
    balance = get_balance()
    price = get_price()

    selected_risk_percent = risk_percent_override if risk_percent_override is not None else RISK_PERCENT_FALLBACK

    margin_to_use = balance * (selected_risk_percent / 100.0)
    notional = margin_to_use * LEVERAGE
    qty = notional / price

    qty = qty * QTY_BUFFER
    qty = round_down(qty, 3)

    print(
        f"DEBUG QTY -> balance={balance}, price={price}, risk_percent={selected_risk_percent}, "
        f"margin_to_use={margin_to_use}, notional={notional}, qty_buffered={qty}",
        flush=True
    )

    if qty <= 0:
        raise Exception("La cantidad calculada es 0. Revisa balance, leverage o precio.")

    return qty, selected_risk_percent


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
def execute_open(action, risk_percent_override=None):
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

    new_qty, selected_risk_percent = calculate_order_quantity(risk_percent_override)

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
        "risk_percent": selected_risk_percent
    }
    save_state(new_state)

    return {
        "message": "BUY ejecutado" if action == "buy" else "SELL ejecutado",
        "risk_percent_used": selected_risk_percent,
        "sent_qty": open_qty,
        "opened_entry_price": open_price,
        "result": open_result
    }


def execute_close_by_opposite_signal(action):
    state, current = sync_state_with_exchange()
    current_side = current["side"]
    current_qty = current["qty"]

    # Señal BUY cierra SHORT
    if action == "buy" and current_side == "SHORT":
        close_result = close_position(current_side, current_qty)
        close_price, closed_qty = extract_order_data(close_result)
        closed_qty = closed_qty if closed_qty is not None else current_qty

        entry_price = state.get("entry_price") if state else None
        opened_at = state.get("opened_at") if state else ""
        prev_risk_percent = state.get("risk_percent", RISK_PERCENT_FALLBACK) if state else RISK_PERCENT_FALLBACK
        pnl_gross = calc_gross_pnl("SHORT", closed_qty, entry_price, close_price)

        append_trade_log(
            opened_at=opened_at,
            closed_at=utc_now(),
            side="SHORT",
            qty=closed_qty,
            entry_price=entry_price,
            exit_price=close_price,
            pnl_gross=pnl_gross,
            close_reason="opposite_buy_signal_close_only",
            risk_percent_used=prev_risk_percent
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

    # Señal SELL cierra LONG
    if action == "sell" and current_side == "LONG":
        close_result = close_position(current_side, current_qty)
        close_price, closed_qty = extract_order_data(close_result)
        closed_qty = closed_qty if closed_qty is not None else current_qty

        entry_price = state.get("entry_price") if state else None
        opened_at = state.get("opened_at") if state else ""
        prev_risk_percent = state.get("risk_percent", RISK_PERCENT_FALLBACK) if state else RISK_PERCENT_FALLBACK
        pnl_gross = calc_gross_pnl("LONG", closed_qty, entry_price, close_price)

        append_trade_log(
            opened_at=opened_at,
            closed_at=utc_now(),
            side="LONG",
            qty=closed_qty,
            entry_price=entry_price,
            exit_price=close_price,
            pnl_gross=pnl_gross,
            close_reason="opposite_sell_signal_close_only",
            risk_percent_used=prev_risk_percent
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


# =========================
# RUTAS
# =========================
@app.route("/", methods=["GET"])
def home():
    return "BOT V4 ACTIVO - INDICADOR VIEJO + IA EN EL BOT", 200


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
    source = str(data.get("source", "unknown")).lower().strip()

    action = "buy" if action_raw == "BUY" else "sell" if action_raw == "SELL" else ""

    if action not in ["buy", "sell"]:
        append_event_log(action_raw, "Acción inválida", {"received": data})
        return jsonify({
            "ok": False,
            "error": "Acción inválida",
            "received": data
        }), 400

    try:
        # 1) Si la señal es contraria a la posición, primero cerrar
        close_result = execute_close_by_opposite_signal(action)
        if close_result is not None:
            print("CIERRE POR SEÑAL CONTRARIA ->", close_result, flush=True)
            append_event_log(action, close_result.get("message", "Cierre por señal contraria"), close_result)
            return jsonify({
                "ok": True,
                "result": close_result
            }), 200

        # 2) Si no hay cierre, calcular contexto real desde BingX
        market_context = get_market_context()

        # 3) IA calcula probabilidad
        ai_result = ai_filter_signal(action, data, market_context)

        print(
            f"MARKET CONTEXT -> trend_15m={market_context.get('trend_15m')}, "
            f"stoch_5m=({market_context.get('stoch_k_5m')}, {market_context.get('stoch_d_5m')}), "
            f"atr_pct_5m={market_context.get('atr_pct_5m')}, source={source}",
            flush=True
        )

        print(
            f"AI RESULT -> decision={ai_result.get('decision')}, "
            f"probability={ai_result.get('probability')}, "
            f"tier={ai_result.get('tier')}, "
            f"risk_percent={ai_result.get('risk_percent')}, "
            f"alignment={ai_result.get('alignment')}, "
            f"reason={ai_result.get('reason')}",
            flush=True
        )

        append_event_log(action, "AI filter evaluated", {
            "payload": data,
            "market_context": market_context,
            "ai_result": ai_result
        })

        if ai_result["decision"] != "APPROVE":
            print("TRADE BLOQUEADO -> probabilidad insuficiente", flush=True)
            return jsonify({
                "ok": True,
                "filtered": True,
                "message": "Trade bloqueado por probabilidad insuficiente",
                "ai_result": ai_result,
                "market_context": market_context,
                "received": data
            }), 200

        # 4) Si aprueba, abrir posición
        result = execute_open(
            action,
            risk_percent_override=ai_result["risk_percent"]
        )
        result["ai_result"] = ai_result
        result["market_context"] = market_context

        print("RESULTADO TRADE ->", result, flush=True)
        append_event_log(action, result.get("message", "Trade ejecutado"), result)

        return jsonify({
            "ok": True,
            "result": result
        }), 200

    except Exception as e:
        print("ERROR webhook:", str(e), flush=True)

        try:
            append_event_log(action, f"ERROR webhook: {str(e)}", {"received": data})
        except Exception as log_error:
            print("Error guardando event log:", log_error, flush=True)

        return jsonify({
            "ok": False,
            "error": str(e),
            "received": data
        }), 500


if __name__ == "__main__":
    ensure_files()
    app.run(host="0.0.0.0", port=10000)