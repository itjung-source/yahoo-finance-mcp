import asyncio
import datetime
import json
import math
import sqlite3
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

DB_PATH = "C:/work/AI/SET/set_stocks.db"
EXECUTOR = ThreadPoolExecutor(max_workers=20)

app = Server("yahoo-finance")


# ─── Helpers ────────────────────────────────────────────────

def _load_symbols(market: str | None = None) -> list[str]:
    con = sqlite3.connect(DB_PATH)
    if market:
        rows = con.execute(
            "SELECT symbol FROM stocks WHERE LOWER(market)=? ORDER BY symbol",
            (market.lower(),)
        ).fetchall()
    else:
        rows = con.execute("SELECT symbol FROM stocks ORDER BY symbol").fetchall()
    con.close()
    return [r[0] for r in rows]


def _calc_ema(prices: list[float], period: int) -> float | None:
    if len(prices) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema


def _fetch_yahoo(sym: str, range_: str = "2y") -> dict:
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}.BK?interval=1d&range={range_}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible)"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}


def _parse_history(data: dict, today: str) -> tuple[list[dict], float | None, int]:
    """Returns (past_rows, today_price, today_volume)"""
    try:
        result = data["chart"]["result"][0]
        meta = result["meta"]
        timestamps = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        volumes = result["indicators"]["quote"][0]["volume"]
    except Exception:
        return [], None, 0

    rows = []
    for ts, c, v in zip(timestamps, closes, volumes):
        if c is None or v is None:
            continue
        dt = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%d")
        rows.append({"dt": dt, "c": float(c), "v": int(v)})

    today_p = meta.get("regularMarketPrice") or meta.get("currentPrice")
    today_v = int(meta.get("regularMarketVolume") or 0)

    last = rows[-1] if rows else None
    if last and last["dt"] == today:
        past = [r for r in rows[:-1] if r["v"] > 0]
    elif last and today_p and abs(last["c"] - today_p) / today_p < 0.03:
        past = [r for r in rows[:-1] if r["v"] > 0]
    else:
        past = [r for r in rows if r["v"] > 0]

    return past, today_p, today_v


def _scan_one(sym: str, today: str) -> dict:
    data = _fetch_yahoo(sym)
    if "error" in data:
        return {"sym": sym, "status": "fetch_error", "error": data["error"]}

    past, today_p, today_v = _parse_history(data, today)

    if len(past) < 203:
        return {"sym": sym, "status": "insufficient_data"}

    d1, d2, d3 = past[-1], past[-2], past[-3]
    c2 = (d3["c"] >= d2["c"]) and (d2["c"] >= d1["c"]) and (d3["c"] > d1["c"])
    if not c2:
        return {"sym": sym, "status": "fail_c2"}

    closes = [r["c"] for r in past]
    ema200 = _calc_ema(closes, 200)
    ema90 = _calc_ema(closes, 90)
    if not ema200 or not ema90:
        return {"sym": sym, "status": "ema_error"}

    if not today_p:
        today_p = d1["c"]

    c1 = today_p > ema200 and today_p > ema90
    c3p = today_p >= d1["c"]
    c3v = today_v > d1["v"]

    if c1 and c2 and c3p and c3v:
        status = "pass"
    elif c1 and c2 and c3p:
        status = "pending"
    else:
        status = "fail"

    return {
        "sym": sym, "status": status,
        "c1": c1, "c2": c2, "c3p": c3p, "c3v": c3v,
        "today_p": round(today_p, 4), "today_v": today_v,
        "ema200": round(ema200, 4), "ema90": round(ema90, 4),
        "d1": {"dt": d1["dt"], "c": round(d1["c"], 4), "v": d1["v"]},
        "d2": {"dt": d2["dt"], "c": round(d2["c"], 4)},
        "d3": {"dt": d3["dt"], "c": round(d3["c"], 4)},
    }


# ─── Statement formatter ────────────────────────────────────

def _fmt_statement(sym: str) -> str:
    """Fetch income statement and format as markdown with YoY and QoQ."""
    tk = yf.Ticker(f"{sym}.BK")
    q_df = tk.quarterly_income_stmt
    a_df = tk.income_stmt

    if (q_df is None or q_df.empty) and (a_df is None or a_df.empty):
        return f"ไม่พบข้อมูลงบการเงินของ {sym}"

    # Fetch price, P/E, P/BV from info
    try:
        info = tk.info or {}
    except Exception:
        info = {}
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    pe    = info.get("trailingPE")
    pbv   = info.get("priceToBook")

    def safe(df, row, col):
        try:
            v = df.loc[row, col]
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return None
            return float(v)
        except Exception:
            return None

    def to_m(v):
        return round(v / 1e6, 1) if v is not None else None

    def fmt_ni(v):
        if v is None:
            return "—"
        return f"({abs(v):,.1f})" if v < 0 else f"{v:,.1f}"

    def fmt_eps(v):
        return "—" if v is None else f"{v:.2f}"

    def fmt_price(v):
        return "—" if v is None else f"{v:,.2f}"

    def fmt_ratio(v):
        return "—" if v is None else f"{v:.2f}"

    def change(curr, prev):
        if curr is None or prev is None or prev == 0:
            return "—", "—"
        if prev < 0 and curr >= 0:
            return "↑", "พลิกกำไร"
        if prev >= 0 and curr < 0:
            return "↓", "พลิกขาดทุน"
        pct = (curr - prev) / abs(prev) * 100
        return ("↑" if pct >= 0 else "↓"), f"{pct:+.1f}%"

    # Parse quarterly (newest first)
    qtrs = []
    if q_df is not None and not q_df.empty:
        for col in q_df.columns[:7]:
            q = (col.month - 1) // 3 + 1
            qtrs.append({
                "year": col.year, "q": q,
                "ni": to_m(safe(q_df, "Net Income", col)),
                "eps": safe(q_df, "Basic EPS", col),
            })

    # Parse annual (newest first)
    annuals = []
    if a_df is not None and not a_df.empty:
        for col in a_df.columns[:5]:
            annuals.append({
                "year": col.year,
                "ni": to_m(safe(a_df, "Net Income", col)),
                "eps": safe(a_df, "Basic EPS", col),
            })

    qmap = {(d["year"], d["q"]): d for d in qtrs}
    amap = {d["year"]: d for d in annuals}

    def yoy(d):
        prev = qmap.get((d["year"] - 1, d["q"]))
        return change(d["ni"], prev["ni"] if prev else None)

    def qoq(d):
        pq = d["q"] - 1 if d["q"] > 1 else 4
        py = d["year"] if d["q"] > 1 else d["year"] - 1
        prev = qmap.get((py, pq))
        return change(d["ni"], prev["ni"] if prev else None)

    lines = [f"## {sym} — Net Income & EPS (หน่วย: ล้านบาท)", ""]

    # Latest quarter summary
    if qtrs:
        lt = qtrs[0]
        label = f"Q{lt['q']}/{lt['year']}"
        ya, yp = yoy(lt)
        qa, qp = qoq(lt)
        lines += [
            f"### ล่าสุด: {label}", "",
            f"| | | | **{label}** | **EPS** |",
            "|---|---|---|---:|---:|",
            f"| **Net Income (M)** | {ya} | YoY {yp} | **{fmt_ni(lt['ni'])}** | **{fmt_eps(lt['eps'])}** |",
            f"| | {qa} | QoQ {qp} | | |",
            f"| **ราคา (บาท)** | | | **{fmt_price(price)}** | |",
            f"| **P/E** | | | **{fmt_ratio(pe)}** | |",
            f"| **P/BV** | | | **{fmt_ratio(pbv)}** | |",
            "",
        ]

    # Quarterly table (Q4→Q1 within each year)
    lines += [
        "### รายไตรมาส", "",
        "| ไตรมาส | | YoY% | QoQ% | Net Income | EPS |",
        "|---|---|---:|---:|---:|---:|",
    ]

    years = sorted(set(d["year"] for d in qtrs), reverse=True)
    shown_ann = set()

    for yr in years:
        yr_qtrs = sorted([d for d in qtrs if d["year"] == yr], key=lambda x: x["q"], reverse=True)
        lines.append(f"| **— {yr} —** | | | | | |")
        for d in yr_qtrs:
            ya, yp = yoy(d)
            qa, qp = qoq(d)
            arrow = ya if ya != "—" else qa
            lines.append(f"| Q{d['q']} | {arrow} | {yp} | {qp} | {fmt_ni(d['ni'])} | {fmt_eps(d['eps'])} |")
        # Annual total for this year
        a = amap.get(yr)
        if a:
            prev_a = amap.get(yr - 1)
            aa, ap = change(a["ni"], prev_a["ni"] if prev_a else None)
            lines.append(f"| **{yr} รวมปี** | {aa} | **{ap}** | | **{fmt_ni(a['ni'])}** | **{fmt_eps(a['eps'])}** |")
            shown_ann.add(yr)

    # Annual-only years (older years with no quarterly data)
    for a in annuals:
        if a["year"] not in shown_ann:
            prev_a = amap.get(a["year"] - 1)
            aa, ap = change(a["ni"], prev_a["ni"] if prev_a else None)
            lines.append(f"| **{a['year']} รวมปี** | {aa} | **{ap}** | | **{fmt_ni(a['ni'])}** | **{fmt_eps(a['eps'])}** |")

    return "\n".join(lines)


# ─── Tools ──────────────────────────────────────────────────

@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="scan_uptrend_pullback",
            description=(
                "สแกนหุ้นไทย SET/MAI ทั้งตลาดหา uptrend 3-day pullback pattern\n"
                "เงื่อนไข: (1) ราคา > EMA200 และ > EMA90  "
                "(2) ปิดลง 3 วันทำการติดต่อกัน (d3>d2>d1)  "
                "(3) วันนี้ราคา >= เมื่อวาน และ volume > เมื่อวาน\n"
                "คืนค่า: passed (ผ่านครบ) และ pending (รอ volume)"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "market": {
                        "type": "string",
                        "enum": ["SET", "mai", "all"],
                        "default": "all",
                        "description": "กรองเฉพาะ SET, mai หรือ all"
                    }
                }
            }
        ),
        Tool(
            name="get_stock_quote",
            description=(
                "ดึงราคาและข้อมูลล่าสุดของหุ้นไทยรายตัว\n"
                "คืนค่า: ราคาปัจจุบัน, EMA200, EMA90, volume, % เปลี่ยนแปลง, ราคา 3 วันล่าสุด"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "สัญลักษณ์หุ้น เช่น PTT, KGEN, AOT"
                    }
                },
                "required": ["symbol"]
            }
        ),
        Tool(
            name="get_stock_history",
            description=(
                "ดึงข้อมูลราคาย้อนหลังของหุ้นไทยรายตัว\n"
                "คืนค่า: ราคาปิด+volume รายวัน N วันล่าสุด พร้อม EMA200, EMA90"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "สัญลักษณ์หุ้น เช่น PTT, KGEN, AOT"
                    },
                    "days": {
                        "type": "integer",
                        "default": 20,
                        "description": "จำนวนวันย้อนหลัง (สูงสุด 500)"
                    }
                },
                "required": ["symbol"]
            }
        ),
        Tool(
            name="list_stocks",
            description="แสดงรายชื่อหุ้นทั้งหมดใน DB",
            inputSchema={
                "type": "object",
                "properties": {
                    "market": {
                        "type": "string",
                        "enum": ["SET", "mai", "all"],
                        "default": "all"
                    }
                }
            }
        ),
        Tool(
            name="get_income_statement",
            description=(
                "ดึงงบกำไรขาดทุน (Income Statement) ของหุ้นไทยรายตัว\n"
                "รองรับทั้งรายไตรมาส (quarterly) และรายปี (annual)\n"
                "ข้อมูลที่ได้: รายได้รวม, กำไรขั้นต้น, กำไรจากการดำเนินงาน, กำไรสุทธิ, EBITDA, EPS\n"
                "ข้อจำกัด: รายไตรมาสได้สูงสุด ~7 ไตรมาส, รายปีได้สูงสุด ~5 ปี\n"
                "ใช้ tool นี้เมื่อถามเรื่อง: รายได้, กำไร, ขาดทุน, งบ P&L, EPS"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "สัญลักษณ์หุ้น เช่น AOT, PTT, KBANK"
                    },
                    "period": {
                        "type": "string",
                        "enum": ["quarterly", "annual"],
                        "default": "quarterly",
                        "description": "quarterly = รายไตรมาส, annual = รายปี"
                    },
                    "periods": {
                        "type": "integer",
                        "default": 5,
                        "description": "จำนวนงวดย้อนหลัง (quarterly สูงสุด 7, annual สูงสุด 5)"
                    }
                },
                "required": ["symbol"]
            }
        ),
        Tool(
            name="get_balance_sheet",
            description=(
                "ดึงงบดุล (Balance Sheet) ของหุ้นไทยรายตัว\n"
                "รองรับทั้งรายไตรมาส (quarterly) และรายปี (annual)\n"
                "ข้อมูลที่ได้: สินทรัพย์รวม, หนี้สินรวม, ส่วนของผู้ถือหุ้น, เงินสด, หนี้สินระยะยาว\n"
                "ใช้ tool นี้เมื่อถามเรื่อง: สินทรัพย์, หนี้สิน, ทุน, งบดุล, D/E ratio"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "สัญลักษณ์หุ้น เช่น AOT, PTT, KBANK"
                    },
                    "period": {
                        "type": "string",
                        "enum": ["quarterly", "annual"],
                        "default": "quarterly",
                        "description": "quarterly = รายไตรมาส, annual = รายปี"
                    },
                    "periods": {
                        "type": "integer",
                        "default": 5,
                        "description": "จำนวนงวดย้อนหลัง"
                    }
                },
                "required": ["symbol"]
            }
        ),
        Tool(
            name="get_stock_statement",
            description=(
                "แสดงงบกำไรขาดทุนหุ้นไทย พร้อม YoY และ QoQ ในรูปแบบตาราง\n"
                "คืนค่า: Net Income, EPS รายไตรมาส (Q4→Q1) + รวมปี พร้อม % เปลี่ยนแปลง YoY และ QoQ\n"
                "ใช้ tool นี้เมื่อต้องการดูภาพรวมงบกำไรขาดทุนแบบ compact\n"
                "ใช้แทน get_income_statement เมื่อถามเรื่อง: งบหุ้น, แสดงงบ, กำไร YoY QoQ"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "สัญลักษณ์หุ้น เช่น AOT, PTT, KBANK"
                    }
                },
                "required": ["symbol"]
            }
        ),
        Tool(
            name="get_cash_flow",
            description=(
                "ดึงงบกระแสเงินสด (Cash Flow Statement) ของหุ้นไทยรายตัว\n"
                "รองรับทั้งรายไตรมาส (quarterly) และรายปี (annual)\n"
                "ข้อมูลที่ได้: กระแสเงินสดจากการดำเนินงาน, ลงทุน, จัดหาเงิน, Capex, Free Cash Flow\n"
                "ใช้ tool นี้เมื่อถามเรื่อง: กระแสเงินสด, cash flow, FCF, capex"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "สัญลักษณ์หุ้น เช่น AOT, PTT, KBANK"
                    },
                    "period": {
                        "type": "string",
                        "enum": ["quarterly", "annual"],
                        "default": "quarterly",
                        "description": "quarterly = รายไตรมาส, annual = รายปี"
                    },
                    "periods": {
                        "type": "integer",
                        "default": 5,
                        "description": "จำนวนงวดย้อนหลัง"
                    }
                },
                "required": ["symbol"]
            }
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    today = datetime.date.today().strftime("%Y-%m-%d")

    # ── scan_uptrend_pullback ────────────────────────────────
    if name == "scan_uptrend_pullback":
        market = arguments.get("market", "all")
        syms = _load_symbols(None if market == "all" else market)
        import time
        t0 = time.time()

        passed, pending = [], []
        loop = asyncio.get_event_loop()

        futures_map = {
            loop.run_in_executor(EXECUTOR, _scan_one, sym, today): sym
            for sym in syms
        }
        results = await asyncio.gather(*futures_map.keys(), return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                continue
            if r["status"] == "pass":
                passed.append(r)
            elif r["status"] == "pending":
                pending.append(r)

        elapsed = round(time.time() - t0, 1)

        def fmt(r):
            chg = ((r["today_p"] - r["d1"]["c"]) / r["d1"]["c"] * 100) if r["d1"]["c"] else 0
            vr = r["today_v"] / r["d1"]["v"] * 100 if r["d1"]["v"] else 0
            return (
                f"{r['sym']}  p={r['today_p']} ({chg:+.2f}%)  "
                f"vol={r['today_v']:,} ({vr:.0f}%)  "
                f"ema200={r['ema200']}  ema90={r['ema90']}\n"
                f"  d3={r['d3']['dt']}:{r['d3']['c']} > "
                f"d2={r['d2']['dt']}:{r['d2']['c']} > "
                f"d1={r['d1']['dt']}:{r['d1']['c']}"
            )

        lines = [
            f"สแกน {len(syms)} ตัว ใช้เวลา {elapsed}s  (today={today})",
            f"\n=== ผ่านครบ 4 เงื่อนไข: {len(passed)} ตัว ===",
        ]
        for r in sorted(passed, key=lambda x: x["sym"]):
            lines.append(fmt(r))

        lines.append(f"\n=== รอ Volume: {len(pending)} ตัว ===")
        for r in sorted(pending, key=lambda x: x["sym"]):
            vr = r["today_v"] / r["d1"]["v"] * 100 if r["d1"]["v"] else 0
            lines.append(
                f"{r['sym']}  p={r['today_p']}  "
                f"vol={r['today_v']:,} ({vr:.0f}%  need>{r['d1']['v']:,})"
            )

        return [TextContent(type="text", text="\n".join(lines))]

    # ── get_stock_quote ─────────────────────────────────────
    elif name == "get_stock_quote":
        sym = arguments["symbol"].upper()
        data = _fetch_yahoo(sym)
        if "error" in data:
            return [TextContent(type="text", text=f"Error fetching {sym}: {data['error']}")]

        past, today_p, today_v = _parse_history(data, today)
        if not past or not today_p:
            return [TextContent(type="text", text=f"No data for {sym}")]

        d1 = past[-1]
        closes = [r["c"] for r in past]
        ema200 = _calc_ema(closes, 200)
        ema90 = _calc_ema(closes, 90)
        chg = (today_p - d1["c"]) / d1["c"] * 100 if d1["c"] else 0
        vr = today_v / d1["v"] * 100 if d1["v"] else 0

        text = (
            f"{sym}\n"
            f"  ราคา:     {today_p} ({chg:+.2f}%)\n"
            f"  Volume:   {today_v:,} ({vr:.0f}% ของเมื่อวาน)\n"
            f"  EMA200:   {round(ema200, 4) if ema200 else 'N/A'}\n"
            f"  EMA90:    {round(ema90, 4) if ema90 else 'N/A'}\n"
            f"  ราคาเมื่อวาน ({d1['dt']}): {d1['c']}\n"
        )
        if len(past) >= 3:
            d2, d3 = past[-2], past[-3]
            trend = "ลง" if d3["c"] > d2["c"] > d1["c"] else ("ขึ้น" if d1["c"] > d2["c"] > d3["c"] else "ผสม")
            text += (
                f"  3 วันล่าสุด: {d3['dt']}:{d3['c']} -> {d2['dt']}:{d2['c']} -> {d1['dt']}:{d1['c']}  ({trend})\n"
            )
        above = today_p > (ema200 or 0) and today_p > (ema90 or 0)
        text += f"  เหนือ EMA200+EMA90: {'✅' if above else '❌'}"

        return [TextContent(type="text", text=text)]

    # ── get_stock_history ───────────────────────────────────
    elif name == "get_stock_history":
        sym = arguments["symbol"].upper()
        days = min(int(arguments.get("days", 20)), 500)
        data = _fetch_yahoo(sym, range_="2y")
        if "error" in data:
            return [TextContent(type="text", text=f"Error: {data['error']}")]

        past, today_p, today_v = _parse_history(data, today)
        if not past:
            return [TextContent(type="text", text=f"No data for {sym}")]

        closes = [r["c"] for r in past]
        ema200 = _calc_ema(closes, 200)
        ema90 = _calc_ema(closes, 90)

        recent = past[-days:]
        lines = [
            f"{sym} — {days} วันล่าสุด",
            f"EMA200={round(ema200,4) if ema200 else 'N/A'}  EMA90={round(ema90,4) if ema90 else 'N/A'}",
            f"{'วันที่':12s}  {'ปิด':>10s}  {'Volume':>14s}",
            "-" * 42,
        ]
        for r in recent:
            lines.append(f"{r['dt']:12s}  {r['c']:>10.4f}  {r['v']:>14,}")

        return [TextContent(type="text", text="\n".join(lines))]

    # ── list_stocks ─────────────────────────────────────────
    elif name == "list_stocks":
        market = arguments.get("market", "all")
        syms = _load_symbols(None if market == "all" else market)
        con = sqlite3.connect(DB_PATH)
        rows = con.execute(
            "SELECT symbol, market FROM stocks ORDER BY market, symbol"
        ).fetchall()
        con.close()
        if market != "all":
            rows = [r for r in rows if r[1].lower() == market.lower()]

        set_syms = [r[0] for r in rows if r[1].upper() == "SET"]
        mai_syms = [r[0] for r in rows if r[1].lower() == "mai"]
        text = (
            f"รวม {len(rows)} ตัว\n"
            f"SET ({len(set_syms)}): {', '.join(set_syms)}\n"
            f"mai ({len(mai_syms)}): {', '.join(mai_syms)}"
        )
        return [TextContent(type="text", text=text)]

    # ── get_stock_statement ─────────────────────────────────
    elif name == "get_stock_statement":
        sym = arguments["symbol"].upper()
        text = _fmt_statement(sym)
        return [TextContent(type="text", text=text)]

    # ── get_income_statement ────────────────────────────────
    elif name == "get_income_statement":
        sym = arguments["symbol"].upper()
        period = arguments.get("period", "quarterly")
        n = min(int(arguments.get("periods", 5)), 7 if period == "quarterly" else 5)

        tk = yf.Ticker(f"{sym}.BK")
        df = tk.quarterly_income_stmt if period == "quarterly" else tk.income_stmt
        if df is None or df.empty:
            return [TextContent(type="text", text=f"ไม่พบข้อมูลงบกำไรขาดทุนของ {sym}")]

        cols = df.columns[:n]
        rows_map = {
            "Total Revenue":       "รายได้รวม",
            "Cost Of Revenue":     "ต้นทุนขาย",
            "Gross Profit":        "กำไรขั้นต้น",
            "Operating Income":    "กำไรจากการดำเนินงาน",
            "Net Income":          "กำไรสุทธิ",
            "EBITDA":              "EBITDA",
            "Basic EPS":           "EPS (บาท)",
        }

        period_label = "รายไตรมาส" if period == "quarterly" else "รายปี"
        lines = [f"งบกำไรขาดทุน {sym} ({period_label}) — หน่วย: ล้านบาท"]
        header = f"{'รายการ':28s}" + "".join(f"{str(c)[:10]:>16s}" for c in cols)
        lines.append(header)
        lines.append("-" * (28 + 16 * len(cols)))

        for eng, thai in rows_map.items():
            if eng not in df.index:
                continue
            label = f"{thai} ({eng})" if eng == "Basic EPS" else thai
            line = f"{label:28s}"
            for c in cols:
                val = df.loc[eng, c]
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    line += f"{'N/A':>16s}"
                elif eng == "Basic EPS":
                    line += f"{float(val):>16.2f}"
                else:
                    line += f"{float(val)/1e6:>15,.1f} "
            lines.append(line)

        return [TextContent(type="text", text="\n".join(lines))]

    # ── get_balance_sheet ───────────────────────────────────
    elif name == "get_balance_sheet":
        sym = arguments["symbol"].upper()
        period = arguments.get("period", "quarterly")
        n = min(int(arguments.get("periods", 5)), 7 if period == "quarterly" else 5)

        tk = yf.Ticker(f"{sym}.BK")
        df = tk.quarterly_balance_sheet if period == "quarterly" else tk.balance_sheet
        if df is None or df.empty:
            return [TextContent(type="text", text=f"ไม่พบข้อมูลงบดุลของ {sym}")]

        cols = df.columns[:n]
        rows_map = {
            "Total Assets":                "สินทรัพย์รวม",
            "Total Liabilities Net Minority Interest": "หนี้สินรวม",
            "Stockholders Equity":         "ส่วนของผู้ถือหุ้น",
            "Cash And Cash Equivalents":   "เงินสด",
            "Long Term Debt":              "หนี้สินระยะยาว",
            "Current Assets":              "สินทรัพย์หมุนเวียน",
            "Current Liabilities":         "หนี้สินหมุนเวียน",
        }

        period_label = "รายไตรมาส" if period == "quarterly" else "รายปี"
        lines = [f"งบดุล {sym} ({period_label}) — หน่วย: ล้านบาท"]
        header = f"{'รายการ':28s}" + "".join(f"{str(c)[:10]:>16s}" for c in cols)
        lines.append(header)
        lines.append("-" * (28 + 16 * len(cols)))

        for eng, thai in rows_map.items():
            if eng not in df.index:
                continue
            line = f"{thai:28s}"
            for c in cols:
                val = df.loc[eng, c]
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    line += f"{'N/A':>16s}"
                else:
                    line += f"{float(val)/1e6:>15,.1f} "
            lines.append(line)

        # คำนวณ D/E ratio
        if "Total Liabilities Net Minority Interest" in df.index and "Stockholders Equity" in df.index:
            lines.append("")
            lines.append("D/E Ratio:")
            de_line = f"{'':28s}"
            for c in cols:
                debt = df.loc["Total Liabilities Net Minority Interest", c]
                equity = df.loc["Stockholders Equity", c]
                if (debt is None or equity is None or
                        (isinstance(debt, float) and math.isnan(debt)) or
                        (isinstance(equity, float) and math.isnan(equity)) or
                        float(equity) == 0):
                    de_line += f"{'N/A':>16s}"
                else:
                    de_line += f"{float(debt)/float(equity):>15.2f}x"
            lines.append(de_line)

        return [TextContent(type="text", text="\n".join(lines))]

    # ── get_cash_flow ───────────────────────────────────────
    elif name == "get_cash_flow":
        sym = arguments["symbol"].upper()
        period = arguments.get("period", "quarterly")
        n = min(int(arguments.get("periods", 5)), 7 if period == "quarterly" else 5)

        tk = yf.Ticker(f"{sym}.BK")
        df = tk.quarterly_cashflow if period == "quarterly" else tk.cashflow
        if df is None or df.empty:
            return [TextContent(type="text", text=f"ไม่พบข้อมูลงบกระแสเงินสดของ {sym}")]

        cols = df.columns[:n]
        rows_map = {
            "Operating Cash Flow":         "เงินสดจากการดำเนินงาน",
            "Investing Cash Flow":         "เงินสดจากการลงทุน",
            "Financing Cash Flow":         "เงินสดจากการจัดหาเงิน",
            "Capital Expenditure":         "Capex",
            "Free Cash Flow":              "Free Cash Flow",
            "End Cash Position":           "เงินสดสิ้นงวด",
        }

        period_label = "รายไตรมาส" if period == "quarterly" else "รายปี"
        lines = [f"งบกระแสเงินสด {sym} ({period_label}) — หน่วย: ล้านบาท"]
        header = f"{'รายการ':28s}" + "".join(f"{str(c)[:10]:>16s}" for c in cols)
        lines.append(header)
        lines.append("-" * (28 + 16 * len(cols)))

        for eng, thai in rows_map.items():
            if eng not in df.index:
                continue
            line = f"{thai:28s}"
            for c in cols:
                val = df.loc[eng, c]
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    line += f"{'N/A':>16s}"
                else:
                    line += f"{float(val)/1e6:>15,.1f} "
            lines.append(line)

        return [TextContent(type="text", text="\n".join(lines))]

    return [TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    import sys
    sys.stderr.write("yahoo-finance MCP server starting...\n")
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
