"""
สแกนหุ้นทุกตัวในตลาด SET+mai
หาราคาปิดวันศุกร์ 2 สัปดาห์ล่าสุด แล้วคำนวณ % เปลี่ยนแปลง
กรองเฉพาะหุ้นที่เปลี่ยนแปลง > threshold%
"""
import sqlite3
import urllib.request
import json
import datetime
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "C:/work/AI/SET/set_stocks.db"
THRESHOLD = 5.0    # %
MAX_WORKERS = 30
RANGE = "1mo"      # ดึงย้อนหลัง 1 เดือน

# ──────────────────────────────────────────────
# หา 2 วันศุกร์ล่าสุด (ไม่นับวันนี้ถ้าเป็น Sat/Sun)
# ──────────────────────────────────────────────
def last_fridays(n=3):
    today = datetime.date.today()
    days_since_fri = (today.weekday() - 4) % 7
    last_fri = today - datetime.timedelta(days=days_since_fri)
    return [(last_fri - datetime.timedelta(weeks=i)) for i in range(n - 1, -1, -1)]

fridays = last_fridays(3)
fri_prev2, fri_prev, fri_curr = fridays[0], fridays[1], fridays[2]
print(f"วันศุกร์ -2 สัปดาห์ : {fri_prev2}")
print(f"วันศุกร์ก่อนหน้า   : {fri_prev}")
print(f"วันศุกร์ล่าสุด     : {fri_curr}")
print(f"กำลังสแกน {DB_PATH} ...")

# โหลด symbols
con = sqlite3.connect(DB_PATH)
symbols = [r[0] for r in con.execute("SELECT symbol FROM stock_list ORDER BY symbol").fetchall()]
con.close()
print(f"หุ้นทั้งหมด: {len(symbols)} ตัว\n")

# โหลด sector map
con2 = sqlite3.connect(DB_PATH)
sector_map = {r[0]: r[1] for r in con2.execute("SELECT symbol, sector FROM stock_list").fetchall()}
con2.close()

# ──────────────────────────────────────────────
# ดึงราคาจาก Yahoo Finance (direct HTTP)
# ──────────────────────────────────────────────
def fetch_closes(sym: str) -> dict | None:
    """คืน {date_str: close_price} สำหรับหุ้นนั้น"""
    ticker = f"{sym}.BK"
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range={RANGE}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        result = data["chart"]["result"]
        if not result:
            return None
        timestamps = result[0]["timestamp"]
        closes = result[0]["indicators"]["quote"][0]["close"]
        out = {}
        for ts, cl in zip(timestamps, closes):
            if cl is None:
                continue
            d = datetime.date.fromtimestamp(ts)
            out[d] = round(cl, 2)
        return out
    except Exception:
        return None

# ──────────────────────────────────────────────
# ดึงราคาปิดวันศุกร์จาก SET scraper (fallback)
# ──────────────────────────────────────────────
def fetch_set_quote(sym: str) -> float | None:
    """ดึงราคาล่าสุดจาก SET API"""
    url = f"https://www.set.or.th/api/set/stock/quotation/{sym}/info"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.set.or.th/"
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        return data.get("priorPrice") or data.get("last") or data.get("closePrice")
    except Exception:
        return None

# ──────────────────────────────────────────────
# หาราคาปิดวันศุกร์จาก dict ของราคา
# ยืดหยุ่น ±2 วัน (กรณีหยุดกลางสัปดาห์)
# ──────────────────────────────────────────────
def get_friday_close(closes: dict, target: datetime.date, tolerance=2) -> float | None:
    for delta in range(0, tolerance + 1):
        d = target - datetime.timedelta(days=delta)
        if d in closes:
            return closes[d]
    return None

# ──────────────────────────────────────────────
# สแกน
# ──────────────────────────────────────────────
results = []
errors = []
total = len(symbols)

def process(sym):
    closes = fetch_closes(sym)
    if closes is None:
        return sym, None, None, None, "no_data", "no_data"

    prev2_close = get_friday_close(closes, fri_prev2)
    prev_close  = get_friday_close(closes, fri_prev)
    curr_close  = get_friday_close(closes, fri_curr)

    # ถ้าวันศุกร์ล่าสุดไม่มีใน Yahoo → ลองดึงจาก SET
    if curr_close is None:
        curr_close = fetch_set_quote(sym)

    # คำนวณ % สัปดาห์นี้
    if prev_close is None or curr_close is None:
        pct_curr = "missing"
    else:
        pct_curr = round((curr_close - prev_close) / prev_close * 100, 2)

    # คำนวณ % สัปดาห์ก่อน
    if prev2_close is None or prev_close is None:
        pct_prev = "missing"
    else:
        pct_prev = round((prev_close - prev2_close) / prev2_close * 100, 2)

    return sym, prev2_close, prev_close, curr_close, pct_prev, pct_curr

print(f"กำลังดึงข้อมูล (workers={MAX_WORKERS}) ...")
done = 0
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
    futs = {exe.submit(process, s): s for s in symbols}
    for fut in as_completed(futs):
        sym, prev2, prev, curr, pct_prev, pct_curr = fut.result()
        done += 1
        if done % 100 == 0:
            print(f"  {done}/{total} ...", flush=True)
        if isinstance(pct_curr, float) and abs(pct_curr) >= THRESHOLD:
            results.append((sym, prev2, prev, curr, pct_prev, pct_curr))

MIN_PRICE = 0.20   # ตัดหุ้นราคาต่ำกว่านี้ออก
TOP_LIMIT = 30     # จำกัดจำนวนหุ้นสูงสุดแต่ละหัวข้อ

def fmt_pct(v):
    if isinstance(v, float):
        return f"{v:>+8.2f}%"
    return f"{'N/A':>9}"

# กรองหุ้นราคาต่ำกว่า MIN_PRICE
results = [r for r in results if r[3] is not None and r[3] >= MIN_PRICE]

# เรียงตาม %W(cur) ก่อน แล้ว %W-1
results.sort(key=lambda x: (x[5], x[4] if isinstance(x[4], float) else 0), reverse=True)
gainers = [r for r in results if r[5] > 0]
losers  = sorted([r for r in results if r[5] < 0],
                 key=lambda x: (x[5], x[4] if isinstance(x[4], float) else 0))

HDR = f"{'#':<4} {'Symbol':<10} {'ราคาปิดล่าสุด':>14}  {'%สัปดาห์นี้':>12}  {'%สัปดาห์ก่อน':>13}  {'Sector':<12}"
SEP = "-" * 75

print(f"\n{'='*75}")
print(f"  สรุปหุ้นที่เปลี่ยนแปลง > {THRESHOLD}%  ({fri_prev} -> {fri_curr})")
print(f"  (ตัดหุ้นราคาต่ำกว่า {MIN_PRICE} บาทออกแล้ว)")
print(f"{'='*75}")
print(f"  ขึ้น: {len(gainers)} ตัว  |  ลง: {len(losers)} ตัว")
print(f"{'='*75}")

print(f"\n📈 TOP GAINERS (แสดง {min(len(gainers),TOP_LIMIT)}/{len(gainers)} ตัว)")
print(HDR)
print(SEP)
for i,(sym,p2,prev,curr,pp,pc) in enumerate(gainers[:TOP_LIMIT],1):
    cs  = f"{curr:>14.2f}" if curr is not None else f"{'N/A':>14}"
    sec = sector_map.get(sym, "")
    print(f"{i:<4} {sym:<10} {cs}  {fmt_pct(pc)}  {fmt_pct(pp)}  {sec:<12}")

print(f"\n📉 TOP LOSERS (แสดง {min(len(losers),TOP_LIMIT)}/{len(losers)} ตัว)")
print(HDR)
print(SEP)
for i,(sym,p2,prev,curr,pp,pc) in enumerate(losers[:TOP_LIMIT],1):
    cs  = f"{curr:>14.2f}" if curr is not None else f"{'N/A':>14}"
    sec = sector_map.get(sym, "")
    print(f"{i:<4} {sym:<10} {cs}  {fmt_pct(pc)}  {fmt_pct(pp)}  {sec:<12}")
