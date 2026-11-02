#!/usr/bin/env python3
"""Binance USD-M BTCUSDT research-only forward collector: no trades or account APIs."""
import argparse
import hashlib
import hmac
import json
import math
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

BASE = "https://fapi.binance.com"
SYMBOL = "BTCUSDT"
TF_MS = {"15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}
ENDPOINTS = {
    "OI": "/futures/data/openInterestHist",
    "GLOBAL": "/futures/data/globalLongShortAccountRatio",
    "TOP_ACCOUNT": "/futures/data/topLongShortAccountRatio",
    "TOP_POSITION": "/futures/data/topLongShortPositionRatio",
    "TAKER": "/futures/data/takerlongshortRatio",
}
USER_AGENT = "BTC-Pesquisa-Forward/1.0 (read-only)"


def utc(ms):
    return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def request_json(path, params=None, timeout=20):
    url = BASE + path + ("?" + urlencode(params) if params else "")
    req = Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}, method="GET")
    try:
        with urlopen(req, timeout=timeout) as response:
            if response.status != 200:
                raise RuntimeError("BINANCE_HTTP_{} {}".format(response.status, path))
            obj = json.loads(response.read(1200000).decode("utf-8"))
    except HTTPError as exc:
        if exc.code in (403, 418, 451):
            raise RuntimeError("BINANCE_BLOCKED_HTTP_{} {}; no bypass".format(exc.code, path)) from exc
        raise RuntimeError("BINANCE_HTTP_{} {}".format(exc.code, path)) from exc
    except (URLError, UnicodeError, ValueError) as exc:
        raise RuntimeError("BINANCE_REQUEST_ERROR {}: {}".format(path, exc)) from exc
    if isinstance(obj, dict) and "code" in obj and obj["code"] not in (0, 200):
        raise RuntimeError("BINANCE_API_ERROR {}: {}".format(path, obj["code"]))
    return obj


def number(value, label):
    value = float(value)
    if not math.isfinite(value) or abs(value) >= 1e20:
        raise ValueError("invalid number: " + label)
    return value


def valid_list(obj, label):
    if not isinstance(obj, list) or not obj:
        raise RuntimeError("empty/malformed Binance response: " + label)
    return obj


def collect(now_ms):
    window_limits = {"15m": 24, "1h": 12, "4h": 8, "1d": 4}
    rows, stats = [], {}
    for tf, period_ms in TF_MS.items():
        for kind, path in ENDPOINTS.items():
            source = valid_list(request_json(path, {"symbol": SYMBOL, "period": tf, "limit": window_limits[tf]}), kind + "/" + tf)
            current = []
            for item in source:
                ts = int(item["timestamp"])
                # Binance OI/LS timestamps are bucket ends, taker timestamps are starts.
                effective_end = ts + period_ms if kind == "TAKER" else ts
                if effective_end > now_ms - 120000 or effective_end < now_ms - 35 * 86400000:
                    continue
                row = {"kind": kind, "tf": tf, "source_ms": ts, "effective_end_ms": effective_end,
                       "quality": "CAPTURADO_SEM_AUDITORIA", "source": BASE + path}
                if kind == "OI":
                    row["oi_btc"] = number(item["sumOpenInterest"], "OI_BTC")
                    row["oi_usdt"] = number(item["sumOpenInterestValue"], "OI_USDT")
                elif kind == "TAKER":
                    row["buy_btc"] = number(item["buyVol"], "TAKER_BUY")
                    row["sell_btc"] = number(item["sellVol"], "TAKER_SELL")
                    row["ratio"] = number(item["buySellRatio"], "TAKER_RATIO")
                else:
                    row["long"] = number(item["longAccount"], "LONG")
                    row["short"] = number(item["shortAccount"], "SHORT")
                    row["ratio"] = number(item["longShortRatio"], "RATIO")
                    if abs(row["long"] + row["short"] - 1) > .003 or row["short"] <= 0:
                        raise RuntimeError("invalid long/short fractions: " + kind + "/" + tf)
                current.append(row)
            current.sort(key=lambda r: r["source_ms"])
            if kind == "OI":
                previous = None
                for row in current:
                    row["delta_oi"] = None
                    if previous is not None and row["source_ms"] - previous["source_ms"] == period_ms and previous["oi_btc"] > 0:
                        row["delta_oi"] = row["oi_btc"] / previous["oi_btc"] - 1
                    previous = row
            rows += current
            stats[kind + "_" + tf] = len(current)
    funding = valid_list(request_json("/fapi/v1/fundingRate", {"symbol": SYMBOL, "limit": 6}), "funding")
    for item in funding:
        ts = int(item["fundingTime"])
        if ts <= now_ms - 120000:
            rows.append({"kind": "FUNDING", "source_ms": ts,
                         "rate": number(item["fundingRate"], "FUNDING"),
                         "mark": number(item.get("markPrice", 0), "FUNDING_MARK"),
                         "source": BASE + "/fapi/v1/fundingRate", "quality": "CAPTURADO_SEM_AUDITORIA"})
    premium = request_json("/fapi/v1/premiumIndex", {"symbol": SYMBOL})
    oi = request_json("/fapi/v1/openInterest", {"symbol": SYMBOL})
    rows.append({"kind": "SNAPSHOT", "source_ms": now_ms,
                 "mark": number(premium["markPrice"], "MARK"),
                 "index": number(premium["indexPrice"], "INDEX"),
                 "funding_predicted": number(premium["lastFundingRate"], "FUNDING_PREDICTED"),
                 "next_funding_ms": int(premium["nextFundingTime"]),
                 "oi_btc": number(oi["openInterest"], "INSTANT_OI"),
                 "oi_source_ms": int(oi["time"]),
                 "quality": "CAPTURADO_SEM_AUDITORIA",
                 "source": BASE + "/fapi/v1/premiumIndex + /fapi/v1/openInterest"})
    if not 1 <= len(rows) <= 400:
        raise RuntimeError("invalid batch size: {}".format(len(rows)))
    return rows, stats


def submit(rows, server_ms):
    url = os.environ.get("BTC_BIN_WEBAPP_URL", "")
    secret = os.environ.get("BTC_BIN_INGEST_SECRET", "")
    if not (url.startswith("https://script.google.com/macros/s/") and url.endswith("/exec")):
        raise RuntimeError("BTC_BIN_WEBAPP_URL must be a Google Apps Script /exec endpoint")
    if len(secret) < 40:
        raise RuntimeError("BTC_BIN_INGEST_SECRET missing/too short")
    batch = {"schema": "BTC_BINANCE_FORWARD_V1", "batch_id": str(uuid.uuid4()),
             "created_utc": utc(int(time.time() * 1000)), "binance_server_utc": utc(server_ms),
             "symbol": SYMBOL, "contract": "BINANCE_USDM_BTCUSDT_PERPETUAL",
             "evidence": "CAPTURADO_PROSPECTIVO_NAO_AUDITADO", "items": rows}
    raw = json.dumps(batch, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    sig = hmac.new(secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()
    payload = json.dumps({"raw": raw, "sig": sig}, separators=(",", ":")).encode("utf-8")
    req = Request(url, data=payload, headers={"Content-Type": "application/json",
                   "User-Agent": USER_AGENT}, method="POST")
    try:
        with urlopen(req, timeout=45) as response:
            result = json.loads(response.read(8192).decode("utf-8"))
    except (HTTPError, URLError, ValueError) as exc:
        raise RuntimeError("Apps Script POST failed, no ACK: {}".format(type(exc).__name__)) from exc
    if result.get("ok") is not True or result.get("status") not in ("ACK_PERSISTIDO_NO_SHEETS", "BATCH_REPLAY_NOOP"):
        raise RuntimeError("Apps Script rejected batch: {}".format(result.get("error", "missing ACK")))
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true", help="Test 7 public endpoints, NO Sheet write")
    args = ap.parse_args()
    now_ms = int(request_json("/fapi/v1/time")["serverTime"])
    skew_sec = abs(int(time.time() * 1000) - now_ms) / 1000
    if skew_sec > 90:
        raise RuntimeError("machine/Binance clock skew > 90s")
    print("BINANCE_HTTP_200_TIME; clock skew {}s".format(round(skew_sec, 2)))
    if args.smoke:
        samples = [(path, {"symbol": SYMBOL, "period": "15m", "limit": 2})
                   for path in ENDPOINTS.values()]
        samples.append(("/fapi/v1/fundingRate", {"symbol": SYMBOL, "limit": 2}))
        for path, params in samples:
            valid_list(request_json(path, params), path)
            print("HTTP_200", path)
        print("SMOKE_OK_7_ENDPOINTS; NO_SHEETS_WRITE; NO_TRADES")
        return
    rows, stats = collect(now_ms)
    print("VALIDATED_BATCH", len(rows), stats)
    ack = submit(rows, now_ms)
    print("SHEETS_INGEST_ACK", json.dumps({k: ack.get(k) for k in ("ok", "batch_id", "inserted", "duplicates", "status")}))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, KeyError, TypeError) as exc:
        print("FAILED:", str(exc), file=sys.stderr)
        sys.exit(1)
