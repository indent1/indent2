import os
import glob
import json
import time
import random
import hashlib
import datetime
import requests
import akshare as ak
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed


# ================= 参数区 =================
# 原参数保留，避免其他地方引用报错
RED_WINDOW_LONG = 10
RED_DAYS_LONG = 8

RED_WINDOW_SHORT = 7
RED_DAYS_SHORT = 6

TOP_N = 25

# 首次建缓存时才会用到新浪历史接口，别太高
MAX_WORKERS = 6

# 拉最近120个自然日，尽量覆盖更多交易日
HIST_CALENDAR_DAYS = 120

POST_FOLDER = "content/post"

CACHE_FOLDER = "stock_cache"
CACHE_FILE = os.path.join(CACHE_FOLDER, "sina_ohlc_cache.csv")

REPORT_PREFIX = "preboom"

# DeepSeek 配置
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash")
DEEPSEEK_API_BASE = os.environ.get("DEEPSEEK_API_BASE", "https://api.deepseek.com")
DEEPSEEK_THINKING = os.environ.get("DEEPSEEK_THINKING", "disabled").strip().lower()

# AI 个股解读缓存
AI_CACHE_FOLDER = "stock_cache"
AI_CACHE_FILE = os.path.join(AI_CACHE_FOLDER, "deepseek_preboom_stock_brief_cache.json")

AI_CACHE_VERSION = "preboom_stock_brief_v2_relaxed"
AI_CACHE_KEEP_DAYS = 180


# ================= 宽松版暴涨前形态参数 =================
PREBOOM_LOOKBACK = 55
PREBOOM_MIN_DAYS = 32

BASE_WINDOW = 28
BASE_SKIP_RECENT = 3

BIG_UP_PCT = 7.0
LIMIT_LIKE_PCT = 9.5

# 宽松版评分门槛
MIN_PATTERN_SCORE = 42.0

# 兜底观察池评分门槛
FALLBACK_PATTERN_SCORE = 32.0

# 过滤“已经明显涨疯”的票，宽松一点
MAX_R5_GAIN = 32.0
MAX_R10_GAIN = 55.0
MAX_R20_GAIN = 85.0
MAX_R60_GAIN = 160.0

# 平台范围放松
MIN_BASE_RANGE = 4.0
MAX_BASE_RANGE = 65.0

# 距离平台高点放松
MIN_CLOSE_TO_BASE_HIGH = -18.0
MAX_BREAKOUT_ABOVE_BASE = 30.0

# 偏离20日线放松
MAX_CLOSE_ABOVE_MA20 = 38.0


# ================= 工具函数 =================
def cn_now():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(hours=8)


def clean_stock_code(code):
    text = str(code).lower()
    text = (
        text
        .replace("sh", "")
        .replace("sz", "")
        .replace("bj", "")
        .replace(".0", "")
        .strip()
    )

    digits = "".join([ch for ch in text if ch.isdigit()])

    if not digits:
        return None

    return digits[-6:].zfill(6)


def get_market_prefix(code):
    code_str = clean_stock_code(code)

    if not code_str:
        return None

    if code_str.startswith("6"):
        return f"sh{code_str}"
    elif code_str.startswith("0") or code_str.startswith("3"):
        return f"sz{code_str}"
    elif code_str.startswith("4") or code_str.startswith("8") or code_str.startswith("9"):
        return f"bj{code_str}"

    return f"sh{code_str}"


def get_sina_chart_html(symbol, stock_name):
    market_code = get_market_prefix(symbol)

    min_chart_url = f"https://image.sinajs.cn/newchart/min/n/{market_code}.gif"
    daily_chart_url = f"https://image.sinajs.cn/newchart/daily/n/{market_code}.gif"

    return f"""
**📊 行情走势图（左：今日分时，右：近期日K）：**

<div style="display: flex; justify-content: space-between; gap: 20px; margin: 18px 0 28px 0; flex-wrap: wrap;">
  <div style="flex: 1; min-width: 280px; text-align: center;">
    <img src="{min_chart_url}" alt="{stock_name} 分时图" style="width: 100%; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.15);">
    <div style="font-size: 14px; color: #666; margin-top: 6px;">今日分时图</div>
  </div>
  <div style="flex: 1; min-width: 280px; text-align: center;">
    <img src="{daily_chart_url}" alt="{stock_name} 日K线图" style="width: 100%; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.15);">
    <div style="font-size: 14px; color: #666; margin-top: 6px;">近期日K线</div>
  </div>
</div>

"""


def get_date_range():
    end_date = cn_now().strftime("%Y%m%d")
    start_date = (cn_now() - datetime.timedelta(days=HIST_CALENDAR_DAYS)).strftime("%Y%m%d")
    return start_date, end_date


def normalize_date(value):
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d")
    except Exception:
        return cn_now().strftime("%Y-%m-%d")


def get_safe_market_date():
    now = cn_now()
    weekday = now.weekday()

    if weekday == 5:
        now = now - datetime.timedelta(days=1)
    elif weekday == 6:
        now = now - datetime.timedelta(days=2)

    return now.strftime("%Y-%m-%d")


def get_random_philosophy():
    url = "https://v1.hitokoto.cn/?c=k&c=d&c=i"

    try:
        response = requests.get(url, timeout=5)
        response.encoding = "utf-8"
        data = response.json()

        text = data.get("hitokoto", "投资的本质是对认知的变现。")
        author = data.get("from_who", "")
        source = data.get("from", "")

        if author and source:
            footer = f"**{author}** 《{source}》"
        elif author:
            footer = f"**{author}**"
        elif source:
            footer = f"《{source}》"
        else:
            footer = "**佚名**"

        return f"> 💡 **投资哲思**：*“{text}”* —— {footer}"

    except Exception:
        return "> 💡 **投资哲思**：*“耐心是一切聪明才智的基础。”* —— **柏拉图**"


def find_column(columns, keywords):
    str_columns = [str(col) for col in columns]

    for keyword in keywords:
        for col in str_columns:
            if keyword in col:
                return col

    for keyword in keywords:
        for col in str_columns:
            if keyword.lower() in col.lower():
                return col

    return None


# ================= 新浪/网易全市场实时行情 =================
def get_all_a_stock_spot_sina():
    print("📈 正在通过【新浪/网易】获取A股全市场实时行情...")

    def clean_code_for_spot(value):
        text = str(value).lower()
        text = (
            text
            .replace("sh", "")
            .replace("sz", "")
            .replace("bj", "")
            .replace(".0", "")
            .strip()
        )

        digits = "".join([ch for ch in text if ch.isdigit()])

        if not digits:
            return None

        return digits[-6:].zfill(6)

    providers = [
        ("新浪", lambda: ak.stock_zh_a_spot()),
        ("网易", lambda: ak.stock_zh_a_spot_netease()),
    ]

    for source_name, fetcher in providers:
        for attempt in range(3):
            try:
                print(f"🔎 尝试获取 {source_name} 实时行情，第 {attempt + 1} 次...")

                spot_df = fetcher()

                if spot_df is None or spot_df.empty:
                    print(f"⚠️ {source_name} 返回为空。")
                    time.sleep(2)
                    continue

                code_col = find_column(spot_df.columns, ["代码", "symbol", "code"])
                name_col = find_column(spot_df.columns, ["名称", "name"])
                close_col = find_column(
                    spot_df.columns,
                    ["最新价", "最新", "现价", "收盘", "trade", "price"]
                )
                open_col = find_column(
                    spot_df.columns,
                    ["今开", "开盘", "open"]
                )

                if not code_col or not name_col or not close_col or not open_col:
                    print(f"❌ {source_name} 实时行情缺少形态筛选所需字段。")
                    print("当前字段：", spot_df.columns.tolist())
                    time.sleep(2)
                    continue

                spot_df = spot_df.copy()

                spot_df["code"] = spot_df[code_col].apply(clean_code_for_spot)
                spot_df = spot_df.dropna(subset=["code"]).copy()

                spot_df["symbol"] = spot_df["code"].apply(get_market_prefix)
                spot_df["name"] = spot_df[name_col].astype(str)

                spot_df["close"] = pd.to_numeric(spot_df[close_col], errors="coerce")
                spot_df["open"] = pd.to_numeric(spot_df[open_col], errors="coerce")

                spot_df = spot_df[
                    spot_df["code"].astype(str).str.match(r"^\d{6}$", na=False)
                ].copy()

                spot_df = spot_df[
                    ~spot_df["name"].str.contains(r"\*?ST|退", regex=True, na=False)
                ].copy()

                spot_df = spot_df.dropna(subset=["open", "close"]).copy()
                spot_df = spot_df[(spot_df["open"] > 0) & (spot_df["close"] > 0)].copy()

                date_col = find_column(spot_df.columns, ["日期", "date"])

                if date_col:
                    spot_df["date"] = spot_df[date_col].apply(normalize_date)
                else:
                    spot_df["date"] = get_safe_market_date()

                result = (
                    spot_df[["symbol", "code", "name", "date", "open", "close"]]
                    .dropna(subset=["symbol", "code", "name", "date", "open", "close"])
                    .drop_duplicates(subset=["symbol"], keep="last")
                    .copy()
                )

                if result.empty:
                    print(f"⚠️ {source_name} 清洗后无可用数据。")
                    time.sleep(2)
                    continue

                print(f"✅ {source_name} 全市场行情获取成功！")
                print(f"🚀 {source_name} 返回可用股票数量：{len(result)}")

                return result

            except AttributeError as e:
                print(f"⚠️ 当前 AkShare 版本可能没有 {source_name} 接口：{str(e)}")
                break

            except Exception as e:
                print(f"⚠️ {source_name} 实时行情获取失败，第 {attempt + 1} 次：{str(e)}")
                time.sleep(3)

    print("❌ 新浪和网易实时行情均获取失败。")
    return None


# ================= 行情缓存读写 =================
def empty_cache_df():
    return pd.DataFrame(columns=["symbol", "code", "name", "date", "open", "close"])


def load_cache():
    if not os.path.exists(CACHE_FILE):
        print("🧊 未发现历史缓存，准备首次全量建立缓存。")
        return empty_cache_df()

    try:
        cache_df = pd.read_csv(CACHE_FILE, dtype={"symbol": str, "code": str})

        required_cols = ["symbol", "code", "name", "date", "open", "close"]
        for col in required_cols:
            if col not in cache_df.columns:
                print(f"⚠️ 缓存缺少字段 {col}，需要重建缓存。")
                return empty_cache_df()

        cache_df["date"] = cache_df["date"].astype(str)
        cache_df["open"] = pd.to_numeric(cache_df["open"], errors="coerce")
        cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
        cache_df = cache_df.dropna(subset=["symbol", "date", "open", "close"])

        print(f"🧊 已加载历史缓存：{len(cache_df)} 行。")
        return cache_df

    except Exception as e:
        print(f"⚠️ 历史缓存读取失败，将重建缓存：{str(e)}")
        return empty_cache_df()


def save_cache(cache_df):
    os.makedirs(CACHE_FOLDER, exist_ok=True)

    if cache_df is None or cache_df.empty:
        print("⚠️ 缓存为空，本次不写入缓存文件。")
        return

    cache_df = cache_df.dropna(subset=["symbol", "date", "open", "close"]).copy()
    cache_df["date"] = cache_df["date"].astype(str)
    cache_df["open"] = pd.to_numeric(cache_df["open"], errors="coerce")
    cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
    cache_df = cache_df.dropna(subset=["open", "close"])

    if cache_df.empty:
        print("⚠️ 清洗后缓存为空，本次不写入缓存文件。")
        return

    cache_df = cache_df.sort_values(["symbol", "date"])
    cache_df = cache_df.groupby("symbol", group_keys=False).tail(120)

    cache_df.to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")
    print(f"✅ OHLC缓存已保存：{CACHE_FILE}，共 {len(cache_df)} 行。")


def cache_too_old(cache_df, spot_trade_date):
    if cache_df is None or cache_df.empty:
        return True

    if "open" not in cache_df.columns or "close" not in cache_df.columns:
        return True

    if cache_df["open"].isna().mean() > 0.2:
        return True

    try:
        latest_cache_date = pd.to_datetime(cache_df["date"]).max()
        spot_date = pd.to_datetime(spot_trade_date)
        gap_days = (spot_date - latest_cache_date).days

        return gap_days > 6

    except Exception:
        return True


# ================= AI解读缓存读写 =================
def load_ai_cache():
    if not os.path.exists(AI_CACHE_FILE):
        return {}

    try:
        with open(AI_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict):
            print(f"💾 已加载 DeepSeek 暴涨前形态解读缓存：{len(data)} 条。")
            return data

        return {}

    except Exception as e:
        print(f"⚠️ DeepSeek 暴涨前形态解读缓存读取失败，将重新创建：{str(e)}")
        return {}


def save_ai_cache(cache_data):
    try:
        os.makedirs(AI_CACHE_FOLDER, exist_ok=True)

        cache_data = prune_ai_cache(cache_data)

        with open(AI_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

        print(f"✅ DeepSeek 暴涨前形态解读缓存已保存：{AI_CACHE_FILE}，共 {len(cache_data)} 条。")

    except Exception as e:
        print(f"⚠️ DeepSeek 暴涨前形态解读缓存保存失败：{str(e)}")


def prune_ai_cache(cache_data):
    if not isinstance(cache_data, dict) or not cache_data:
        return {}

    cutoff = cn_now() - datetime.timedelta(days=AI_CACHE_KEEP_DAYS)
    new_cache = {}

    for key, item in cache_data.items():
        try:
            created_at = item.get("created_at", "")
            created_dt = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")

            if created_dt >= cutoff:
                new_cache[key] = item

        except Exception:
            new_cache[key] = item

    return new_cache


def make_stock_brief_cache_key(stock):
    cache_payload = {
        "report_prefix": REPORT_PREFIX,
        "cache_version": AI_CACHE_VERSION,
        "model": DEEPSEEK_MODEL,
        "code": str(stock.get("code", "")).zfill(6),
        "name": str(stock.get("name", "")),
        "pattern_score": round(float(stock.get("pattern_score", 0)), 2),
        "condition": str(stock.get("condition", "")),
        "r3": round(float(stock.get("r3", 0)), 2),
        "r5": round(float(stock.get("r5", 0)), 2),
        "r10": round(float(stock.get("r10", 0)), 2),
        "r20": round(float(stock.get("r20", 0)), 2),
        "base_range": round(float(stock.get("base_range", 0)), 2),
        "close_to_base_high": round(float(stock.get("close_to_base_high", 0)), 2),
        "risk_note": str(stock.get("risk_note", "")),
    }

    raw = json.dumps(cache_payload, ensure_ascii=False, sort_keys=True)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ================= 首次或缓存过旧时：新浪历史K线建缓存 =================
def fetch_one_history_sina(row, start_date, end_date):
    symbol = row["symbol"]

    try:
        time.sleep(random.uniform(0.08, 0.25))

        hist_df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )

        if hist_df is None or hist_df.empty:
            return []

        if "date" not in hist_df.columns or "open" not in hist_df.columns or "close" not in hist_df.columns:
            return []

        hist_df = hist_df[["date", "open", "close"]].copy()
        hist_df["date"] = hist_df["date"].apply(normalize_date)
        hist_df["open"] = pd.to_numeric(hist_df["open"], errors="coerce")
        hist_df["close"] = pd.to_numeric(hist_df["close"], errors="coerce")
        hist_df = hist_df.dropna(subset=["open", "close"])

        rows = []

        for _, h in hist_df.iterrows():
            rows.append({
                "symbol": row["symbol"],
                "code": str(row["code"]).zfill(6),
                "name": row["name"],
                "date": h["date"],
                "open": float(h["open"]),
                "close": float(h["close"])
            })

        return rows

    except Exception as e:
        print(f"⚠️ 历史K线获取失败：{row.get('name')}({row.get('code')})，原因：{str(e)}")
        return []


def rebuild_history_cache_from_sina(spot_df):
    start_date, end_date = get_date_range()

    print("🧱 开始通过新浪历史K线重建OHLC缓存。")
    print(f"📅 历史数据区间：{start_date} ~ {end_date}")
    print(f"🚀 并发线程数：{MAX_WORKERS}")

    rows = []
    total = len(spot_df)
    finished = 0

    records = spot_df.to_dict("records")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_one_history_sina, row, start_date, end_date): row["symbol"]
            for row in records
        }

        for future in as_completed(futures):
            finished += 1

            if finished % 100 == 0:
                print(f"🔄 历史缓存进度：{finished} / {total}")

            result_rows = future.result()
            if result_rows:
                rows.extend(result_rows)

    if not rows:
        print("⚠️ OHLC历史缓存重建失败，未获取到历史K线。")
        return empty_cache_df()

    cache_df = pd.DataFrame(rows)
    cache_df = cache_df.drop_duplicates(subset=["symbol", "date"], keep="last")

    print(f"✅ OHLC历史缓存重建完成，共 {len(cache_df)} 行。")
    return cache_df


def update_cache_with_spot(cache_df, spot_df):
    if spot_df is None or spot_df.empty:
        return cache_df

    spot_rows = spot_df[["symbol", "code", "name", "date", "open", "close"]].copy()
    spot_rows["date"] = spot_rows["date"].astype(str)
    spot_rows["open"] = pd.to_numeric(spot_rows["open"], errors="coerce")
    spot_rows["close"] = pd.to_numeric(spot_rows["close"], errors="coerce")
    spot_rows = spot_rows.dropna(subset=["open", "close"])

    if spot_rows.empty:
        print("⚠️ 新浪/网易实时行情没有可用 open 字段，本次不更新当天K线。")
        return cache_df

    if cache_df is None or cache_df.empty:
        updated = spot_rows.copy()
    else:
        cache_df = cache_df.copy()

        cache_df["key"] = cache_df["symbol"].astype(str) + "_" + cache_df["date"].astype(str)
        spot_rows["key"] = spot_rows["symbol"].astype(str) + "_" + spot_rows["date"].astype(str)

        cache_df = cache_df[~cache_df["key"].isin(set(spot_rows["key"]))].drop(columns=["key"])
        spot_rows = spot_rows.drop(columns=["key"])

        updated = pd.concat([cache_df, spot_rows], ignore_index=True)

    updated = updated.drop_duplicates(subset=["symbol", "date"], keep="last")
    updated = updated.sort_values(["symbol", "date"])

    print(f"✅ 已用新浪/网易实时行情更新OHLC缓存，当前缓存 {len(updated)} 行。")
    return updated


# ================= 核心筛选：宽松版暴涨前平台启动形态 =================
def screen_from_cache(cache_df):
    """
    宽松版暴涨前形态筛选逻辑。

    目标：
    1. 找平台整理后，刚刚开始转强的股票。
    2. 不要求已经完全突破，不要求均线完美多头。
    3. 允许还在平台高点下方 18% 以内。
    4. 允许短线刚启动，也允许只是开始抬头。
    5. 如果严格入选为空，会输出宽松观察池，避免一个都没有。
    """
    print("🧮 正在从本地缓存中执行【宽松版暴涨前平台启动形态】筛选...")

    results = []
    fallback_results = []

    if cache_df is None or cache_df.empty:
        print("今日未筛选到符合暴涨前形态的股票。")
        return None

    cache_df = cache_df.copy()
    cache_df["date"] = cache_df["date"].astype(str)
    cache_df["open"] = pd.to_numeric(cache_df["open"], errors="coerce")
    cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
    cache_df = cache_df.dropna(subset=["symbol", "date", "open", "close"])
    cache_df = cache_df[(cache_df["open"] > 0) & (cache_df["close"] > 0)].copy()
    cache_df = cache_df.sort_values(["symbol", "date"])

    def pct_change(close_series, days):
        if len(close_series) <= days:
            return 0.0

        latest = float(close_series.iloc[-1])
        base = float(close_series.iloc[-days - 1])

        if base <= 0:
            return 0.0

        return (latest / base - 1) * 100

    def daily_pct_series(close_series):
        return close_series.pct_change() * 100

    def clip_score(value, low, high, reverse=False):
        try:
            value = float(value)
        except Exception:
            return 0.0

        if high == low:
            return 0.0

        if reverse:
            raw = (high - value) / (high - low) * 100
        else:
            raw = (value - low) / (high - low) * 100

        return float(max(0, min(100, raw)))

    def range_score(value):
        """
        平台振幅宽松化。
        8%-40% 比较理想；
        4%-65% 都允许，但边缘区间降分。
        """
        value = float(value)

        if value < MIN_BASE_RANGE:
            return 0.0
        elif value < 8:
            return clip_score(value, MIN_BASE_RANGE, 8)
        elif value <= 40:
            return 100.0
        elif value <= MAX_BASE_RANGE:
            return clip_score(value, MAX_BASE_RANGE, 40, reverse=True)
        else:
            return 0.0

    for symbol, group in cache_df.groupby("symbol"):
        group = group.drop_duplicates(subset=["date"], keep="last").sort_values("date")

        if len(group) < PREBOOM_MIN_DAYS:
            continue

        lookback = group.tail(PREBOOM_LOOKBACK).copy()
        close = lookback["close"].astype(float)

        if len(lookback) < PREBOOM_MIN_DAYS:
            continue

        latest_row = lookback.iloc[-1]
        latest_close = float(latest_row["close"])

        if latest_close <= 0:
            continue

        ma5 = float(close.tail(5).mean())
        ma10 = float(close.tail(10).mean())
        ma20 = float(close.tail(20).mean())
        ma30 = float(close.tail(30).mean()) if len(close) >= 30 else ma20
        ma60 = float(close.tail(55).mean()) if len(close) >= 55 else float(close.mean())

        if ma5 <= 0 or ma10 <= 0 or ma20 <= 0 or ma60 <= 0:
            continue

        r3 = pct_change(close, 3)
        r5 = pct_change(close, 5)
        r10 = pct_change(close, 10)
        r20 = pct_change(close, 20)
        r30 = pct_change(close, 30) if len(close) > 30 else r20
        r60 = pct_change(close, min(54, len(close) - 1))

        daily_pct = daily_pct_series(close)
        last_5_daily = daily_pct.tail(5)
        last_10_daily = daily_pct.tail(10)
        last_20_daily = daily_pct.tail(20)

        big_up_count_5 = int((last_5_daily >= BIG_UP_PCT).sum())
        big_up_count_10 = int((last_10_daily >= BIG_UP_PCT).sum())
        big_up_count_20 = int((last_20_daily >= BIG_UP_PCT).sum())
        limit_like_count_20 = int((last_20_daily >= LIMIT_LIKE_PCT).sum())

        latest_day_pct = 0.0
        if len(last_10_daily.dropna()) > 0:
            latest_day_pct = float(last_10_daily.iloc[-1])

        last_5 = lookback.tail(5).copy()
        last_7 = lookback.tail(7).copy()
        last_10 = lookback.tail(10).copy()

        red_count_5 = int((last_5["close"] > last_5["open"]).sum())
        red_count_7 = int((last_7["close"] > last_7["open"]).sum())
        red_count_10 = int((last_10["close"] > last_10["open"]).sum())

        if len(lookback) >= BASE_WINDOW + BASE_SKIP_RECENT:
            base = lookback.iloc[-BASE_WINDOW - BASE_SKIP_RECENT:-BASE_SKIP_RECENT].copy()
        else:
            base = lookback.iloc[:-BASE_SKIP_RECENT].copy()

        if base is None or base.empty or len(base) < 16:
            continue

        base_close = base["close"].astype(float)
        base_high = float(base_close.max())
        base_low = float(base_close.min())

        if base_low <= 0 or base_high <= 0:
            continue

        base_range = (base_high / base_low - 1) * 100
        close_to_base_high = (latest_close / base_high - 1) * 100
        close_above_ma20 = (latest_close / ma20 - 1) * 100
        close_above_ma60 = (latest_close / ma60 - 1) * 100

        ma_short_max = max(ma5, ma10, ma20)
        ma_short_min = min(ma5, ma10, ma20)
        ma_spread = (ma_short_max / ma_short_min - 1) * 100 if ma_short_min > 0 else 999

        # 宽松趋势判断
        ma_bull = latest_close >= ma5 >= ma10 >= ma20
        ma_turning = (
            latest_close >= ma20 * 0.96 and
            ma5 >= ma10 * 0.96 and
            ma10 >= ma20 * 0.94
        )
        ma_try_stronger = (
            latest_close >= ma5 * 0.98 or
            latest_close >= ma10 * 0.98 or
            ma5 >= ma20 * 0.96
        )
        ma20_not_bad = ma20 >= ma30 * 0.92
        long_trend_ok = latest_close >= ma60 * 0.82

        # 平台位置宽松判断
        near_base_high = close_to_base_high >= MIN_CLOSE_TO_BASE_HIGH
        not_too_far_breakout = close_to_base_high <= MAX_BREAKOUT_ABOVE_BASE

        # 早期启动条件放松
        early_accel = (
            r3 >= -3.0 and
            r5 >= -5.0 and
            (
                r3 >= 0.3 or
                r5 >= 1.0 or
                r10 >= 2.0 or
                close_to_base_high >= -10.0 or
                red_count_10 >= 4
            )
        )

        # 不要已经极端过热
        not_extreme_overheated = (
            r5 <= MAX_R5_GAIN and
            r10 <= MAX_R10_GAIN and
            r20 <= MAX_R20_GAIN and
            r60 <= MAX_R60_GAIN and
            close_above_ma20 <= MAX_CLOSE_ABOVE_MA20 and
            big_up_count_10 <= 3 and
            limit_like_count_20 <= 3
        )

        base_ok = MIN_BASE_RANGE <= base_range <= MAX_BASE_RANGE

        red_ok = red_count_5 >= 2 or red_count_7 >= 3 or red_count_10 >= 4

        # ================= 打分 =================
        base_score = range_score(base_range)

        trend_score = 0.0
        if latest_close >= ma5 * 0.98:
            trend_score += 18
        if ma5 >= ma10 * 0.98:
            trend_score += 22
        if ma10 >= ma20 * 0.96:
            trend_score += 22
        if ma20 >= ma30 * 0.95:
            trend_score += 16
        if latest_close >= ma60 * 0.90:
            trend_score += 22

        if close_to_base_high < -18:
            breakout_score = 0.0
        elif close_to_base_high <= -5:
            breakout_score = clip_score(close_to_base_high, -18, -5) * 0.75
        elif close_to_base_high <= 5:
            breakout_score = 75 + clip_score(close_to_base_high, -5, 5) * 0.25
        elif close_to_base_high <= 15:
            breakout_score = 100.0
        elif close_to_base_high <= MAX_BREAKOUT_ABOVE_BASE:
            breakout_score = clip_score(close_to_base_high, MAX_BREAKOUT_ABOVE_BASE, 15, reverse=True)
        else:
            breakout_score = 0.0

        # 这里放松动量：不要求已经明显涨，只要开始抬头就给分
        accel_score = (
            clip_score(r3, -2, 9) * 0.32 +
            clip_score(r5, -3, 16) * 0.36 +
            clip_score(r10, -5, 30) * 0.32
        )

        compact_score = clip_score(ma_spread, 16, 2, reverse=True)

        red_score = (
            clip_score(red_count_5, 1, 5) * 0.42 +
            clip_score(red_count_10, 3, 8) * 0.58
        )

        # 趋势斜率分：最近20日不要太烂，最好温和向上
        slope_score = (
            clip_score(r20, -10, 35) * 0.60 +
            clip_score(r30, -12, 45) * 0.40
        )

        risk_penalty = 0.0

        if big_up_count_5 >= 2:
            risk_penalty += 6

        if big_up_count_10 >= 3:
            risk_penalty += 7

        if r10 > 40:
            risk_penalty += clip_score(r10, 40, MAX_R10_GAIN) * 0.12

        if r20 > 65:
            risk_penalty += clip_score(r20, 65, MAX_R20_GAIN) * 0.15

        if close_above_ma20 > 28:
            risk_penalty += clip_score(close_above_ma20, 28, MAX_CLOSE_ABOVE_MA20) * 0.15

        if latest_day_pct >= LIMIT_LIKE_PCT:
            risk_penalty += 7

        pattern_score = (
            trend_score * 0.24 +
            breakout_score * 0.24 +
            accel_score * 0.18 +
            base_score * 0.11 +
            compact_score * 0.09 +
            red_score * 0.07 +
            slope_score * 0.07 -
            risk_penalty
        )

        pattern_score = max(0, min(100, pattern_score))

        condition_list = []

        if ma_bull:
            condition_list.append("均线多头")
        elif ma_turning:
            condition_list.append("均线转强")
        elif ma_try_stronger:
            condition_list.append("均线尝试修复")
        else:
            condition_list.append("趋势观察")

        if close_to_base_high >= 0:
            condition_list.append("小幅突破平台")
        elif close_to_base_high >= -8:
            condition_list.append("贴近平台高点")
        else:
            condition_list.append("平台下方蓄势")

        if ma_spread <= 8:
            condition_list.append("均线粘合")
        elif ma_spread <= 14:
            condition_list.append("均线靠拢")

        if r5 >= 4:
            condition_list.append("短线启动")
        elif r10 >= 4:
            condition_list.append("温和抬升")
        else:
            condition_list.append("低位观察")

        if big_up_count_10 <= 1:
            condition_list.append("未明显暴涨")
        elif big_up_count_10 <= 3:
            condition_list.append("已有异动")

        risk_notes = []

        if r10 >= 35:
            risk_notes.append(f"10日涨幅已达{r10:.2f}%")

        if close_above_ma20 >= 25:
            risk_notes.append(f"偏离20日线{close_above_ma20:.2f}%")

        if big_up_count_10 >= 2:
            risk_notes.append(f"近10日已有{big_up_count_10}天涨幅超{BIG_UP_PCT:.0f}%")

        if close_to_base_high < -12:
            risk_notes.append("距离平台高点仍有一定空间，需要观察能否继续走强")

        if not risk_notes:
            risk_notes.append("形态偏早期，需要观察是否放量突破平台")

        red_days_detail = []
        red_days_detail.append(f"形态评分 {pattern_score:.1f}")
        red_days_detail.append(f"平台振幅 {base_range:.2f}%")
        red_days_detail.append(f"距离平台高点 {close_to_base_high:.2f}%")
        red_days_detail.append(f"偏离20日线 {close_above_ma20:.2f}%")
        red_days_detail.append(f"MA5/10/20/60：{ma5:.2f}/{ma10:.2f}/{ma20:.2f}/{ma60:.2f}")
        red_days_detail.append(f"3日/5日/10日/20日涨幅：{r3:.2f}%/{r5:.2f}%/{r10:.2f}%/{r20:.2f}%")
        red_days_detail.append(f"近10日涨幅超{BIG_UP_PCT:.0f}%天数：{big_up_count_10}")

        item = {
            "name": str(latest_row["name"]),
            "code": str(latest_row["code"]).zfill(6),
            "symbol": symbol,

            "red_count_10": red_count_10,
            "red_count_7": red_count_7,
            "condition": "、".join(condition_list),
            "total_change": r10,
            "latest_close": latest_close,
            "red_days_detail": red_days_detail,

            "pattern_score": pattern_score,
            "r3": r3,
            "r5": r5,
            "r10": r10,
            "r20": r20,
            "r30": r30,
            "r60": r60,
            "base_range": base_range,
            "close_to_base_high": close_to_base_high,
            "close_above_ma20": close_above_ma20,
            "close_above_ma60": close_above_ma60,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma60": ma60,
            "ma_spread": ma_spread,
            "big_up_count_5": big_up_count_5,
            "big_up_count_10": big_up_count_10,
            "big_up_count_20": big_up_count_20,
            "risk_note": "；".join(risk_notes),
        }

        # 主筛选：还是要满足基本结构
        main_pass = (
            base_ok and
            near_base_high and
            not_too_far_breakout and
            ma20_not_bad and
            long_trend_ok and
            early_accel and
            not_extreme_overheated and
            red_ok and
            (ma_turning or ma_try_stronger) and
            pattern_score >= MIN_PATTERN_SCORE
        )

        # 兜底池：更宽松，只要别太烂、别太疯狂，就先放入观察
        fallback_pass = (
            base_ok and
            close_to_base_high >= -25.0 and
            close_to_base_high <= 38.0 and
            r10 >= -12.0 and
            r20 >= -20.0 and
            r10 <= 70.0 and
            close_above_ma20 <= 45.0 and
            big_up_count_10 <= 4 and
            red_count_10 >= 3 and
            pattern_score >= FALLBACK_PATTERN_SCORE
        )

        if main_pass:
            results.append(item)
        elif fallback_pass:
            item["condition"] = "宽松观察、" + item["condition"]
            fallback_results.append(item)

    if not results and fallback_results:
        print("⚠️ 严格条件未命中，启用【宽松观察池】。")
        results = fallback_results

    if not results:
        print("今日未筛选到符合宽松版暴涨前平台启动形态的股票。")
        return None

    results = sorted(results, key=lambda x: x["pattern_score"], reverse=True)
    top_results = results[:TOP_N]

    print(f"🎯 筛选完成：共命中 {len(results)} 只，按形态评分截取 TOP {TOP_N}。")

    for item in top_results:
        print(
            f"✅ {item['name']}({item['code']}) "
            f"形态评分 {item['pattern_score']:.1f}，"
            f"{item['condition']}，"
            f"3日 {item['r3']:.2f}% / 5日 {item['r5']:.2f}% / 10日 {item['r10']:.2f}%"
        )

    return top_results


def get_surge_stocks():
    spot_df = get_all_a_stock_spot_sina()

    if spot_df is None or spot_df.empty:
        return "ERROR"

    cache_df = load_cache()

    spot_trade_date = str(spot_df["date"].iloc[0])

    if cache_too_old(cache_df, spot_trade_date):
        print("⚠️ 缓存为空、字段不完整或过旧，将全量重建。首次运行会比较慢。")
        cache_df = rebuild_history_cache_from_sina(spot_df)

    cache_df = update_cache_with_spot(cache_df, spot_df)
    save_cache(cache_df)

    return screen_from_cache(cache_df)


# ================= DeepSeek =================
def ask_deepseek(prompt, system_prompt="", temperature=0.65, timeout=180):
    api_key = os.environ.get("DEEPSEEK_API_KEY")

    if not api_key:
        return "❌ DeepSeek API Key 未配置。请在 GitHub Secrets 中添加 DEEPSEEK_API_KEY。"

    url = f"{DEEPSEEK_API_BASE.rstrip('/')}/chat/completions"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    messages = []

    if system_prompt:
        messages.append({
            "role": "system",
            "content": system_prompt
        })

    messages.append({
        "role": "user",
        "content": prompt
    })

    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": messages,
        "temperature": temperature,
        "stream": False
    }

    if DEEPSEEK_THINKING in ["enabled", "disabled"]:
        payload["thinking"] = {
            "type": DEEPSEEK_THINKING
        }

    for i in range(3):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=timeout)

            if response.status_code != 200:
                print(f"❌ DeepSeek HTTP错误：{response.status_code}")
                print(response.text)
                time.sleep(2 + i * 2)
                continue

            data = response.json()
            choices = data.get("choices", [])

            if not choices:
                print("❌ DeepSeek 没有返回 choices。")
                print(data)
                time.sleep(2 + i * 2)
                continue

            message = choices[0].get("message", {})
            text = (message.get("content") or "").strip()

            if text:
                return text

            print("❌ DeepSeek 返回正文为空。")
            print(data)
            time.sleep(2 + i * 2)

        except Exception as e:
            print(f"❌ DeepSeek 请求失败，第 {i + 1} 次：{str(e)}")
            time.sleep(2 + i * 2)

    return "❌ AI 分析生成失败。"


def ask_deepseek_single_stock_brief(stock, ai_cache=None):
    if ai_cache is None:
        ai_cache = load_ai_cache()

    cache_key = make_stock_brief_cache_key(stock)
    cached_item = ai_cache.get(cache_key)

    if cached_item and cached_item.get("text"):
        print(f"💾 命中 DeepSeek 暴涨前形态解读缓存：{stock['name']}({stock['code']})")
        return cached_item["text"]

    detail_text = "；".join(stock["red_days_detail"])

    system_prompt = """你是一位严谨的A股市场研究员。
请用通俗易懂的大白话解释股票，不要写投资建议，不要承诺上涨。
如果你无法确定某个原因，必须写“可能与……有关”，不要装作确定。
避免使用“必涨”“确定上涨”“强烈推荐”“可以买入”等表述。

你必须严格按照下面格式输出：

**这家公司是做什么的：**
用1-2句话说明主营业务、产品、客户或所处行业。尽量大白话，不要堆术语。

**为什么像启动前形态：**
用1-2条 bullet 分析可能原因，比如平台整理、均线转强、贴近平台高点、短线温和抬升、资金试盘等。

**需要观察什么：**
用1句话说明后续要观察的风险点，比如是否放量突破、是否冲高回落、是否跌回平台。
"""

    user_prompt = f"""请分析这只股票：

股票名称：{stock['name']}
股票代码：{stock['code']}
形态评分：{stock['pattern_score']:.1f}
命中条件：{stock['condition']}
最新收盘价：{stock['latest_close']:.2f}
3日涨幅：{stock['r3']:.2f}%
5日涨幅：{stock['r5']:.2f}%
10日涨幅：{stock['r10']:.2f}%
20日涨幅：{stock['r20']:.2f}%
平台振幅：{stock['base_range']:.2f}%
距离平台高点：{stock['close_to_base_high']:.2f}%
偏离20日均线：{stock['close_above_ma20']:.2f}%
近10日涨幅超7%的天数：{stock['big_up_count_10']}
风险观察：{stock['risk_note']}
形态细节：{detail_text}

请重点讲清楚：
1. 这家公司是做什么的。
2. 为什么它像“暴涨前的蓄势/启动形态”。
3. 还需要观察什么，不能写成投资建议。

总字数控制在180字左右。
"""

    print(f"🤖 DeepSeek 正在生成个股解读：{stock['name']}({stock['code']})")

    text = ask_deepseek(
        prompt=user_prompt,
        system_prompt=system_prompt,
        temperature=0.65,
        timeout=120
    )

    if text and not text.startswith("❌"):
        ai_cache[cache_key] = {
            "created_at": cn_now().strftime("%Y-%m-%d %H:%M:%S"),
            "model": DEEPSEEK_MODEL,
            "cache_version": AI_CACHE_VERSION,
            "stock_code": str(stock["code"]).zfill(6),
            "stock_name": stock["name"],
            "pattern_score": round(float(stock["pattern_score"]), 2),
            "condition": stock["condition"],
            "r3": round(float(stock["r3"]), 2),
            "r5": round(float(stock["r5"]), 2),
            "r10": round(float(stock["r10"]), 2),
            "r20": round(float(stock["r20"]), 2),
            "base_range": round(float(stock["base_range"]), 2),
            "close_to_base_high": round(float(stock["close_to_base_high"]), 2),
            "latest_close": round(float(stock["latest_close"]), 2),
            "risk_note": stock["risk_note"],
            "red_days_detail": stock["red_days_detail"],
            "text": text
        }

        save_ai_cache(ai_cache)

    return text


# ================= 写 Hugo 博客 =================
def write_blog_post(stock_list):
    today_date = cn_now().strftime("%Y-%m-%d")
    post_time = cn_now().strftime("%Y-%m-%dT%H:%M:%S+08:00")

    os.makedirs(POST_FOLDER, exist_ok=True)

    for old_file in glob.glob(os.path.join(POST_FOLDER, f"{REPORT_PREFIX}-*.md")):
        os.remove(old_file)

    md_content = f"""---
title: "🚀 【宽松版暴涨前形态雷达】平台蓄势 / 均线转强 / 启动观察股票扫描 ({today_date})"
date: {post_time}
categories:
    - 量化研报
tags:
    - AI选股
    - 暴涨前形态
    - 宽松观察
    - 平台突破
    - 均线转强
    - 全市场扫描
    - 新浪行情
    - 网易兜底
    - DeepSeek
draft: false
---

# 🚀 宽松版暴涨前形态雷达：平台蓄势 / 均线转强 / 启动观察

本报告由 **Python + 新浪/网易行情接口 + 本地OHLC缓存 + DeepSeek AI** 自动生成。

> ⚠️ 风险提示：本文仅为基于公开行情数据的自动化整理与AI文本生成，不构成任何投资建议。股市有风险，交易需谨慎。

扫描思路：

- 股票范围：A股全市场，剔除 ST、退市、停牌无价格标的
- 目标形态：平台整理或缓慢抬升后，均线开始修复，股价接近平台高点或小幅突破
- 宽松条件：不要求完全突破，不要求均线完美多头，允许仍在平台高点下方蓄势
- 风险过滤：过滤极端暴涨、严重偏离20日线、平台振幅过大的标的
- 兜底机制：如果严格条件未命中，会输出“宽松观察池”
- 当前限制：本程序沿用原接口和缓存，只使用 open / close，暂不判断成交量
- 排名方式：按暴涨前形态评分排序，截取 TOP {TOP_N}
- 数据来源：新浪行情接口为主，网易行情接口兜底
- AI模型：{DEEPSEEK_MODEL}

---

"""

    if stock_list == "ERROR":
        md_content += """
## 今日扫描结果

今日新浪/网易数据抓取失败，未能完成全市场暴涨前形态扫描。

可能原因包括：

- 新浪/网易接口临时不可用
- GitHub Actions 海外网络异常
- AkShare 接口字段变化
- 请求频率过高被临时限制

---

"""

    elif stock_list is None:
        md_content += f"""
## 今日扫描结果

经过全市场扫描，暂时没有股票满足宽松版暴涨前形态。

这通常说明市场短线结构较弱，或者当前数据缓存不足。可以考虑继续放宽参数，或者下一步加入成交量、最高价、最低价字段。

---

{get_random_philosophy()}

---

"""

    else:
        ai_cache = load_ai_cache()

        md_content += "## 今日命中的 TOP 暴涨前形态观察股票\n\n"
        md_content += "| 排名 | 股票 | 代码 | 形态评分 | 命中条件 | 3日涨幅 | 5日涨幅 | 10日涨幅 | 20日涨幅 | 平台振幅 | 距平台高点 | 最新收盘价 |\n"
        md_content += "|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|\n"

        for idx, s in enumerate(stock_list, start=1):
            md_content += (
                f"| {idx} | {s['name']} | {s['code']} | {s['pattern_score']:.1f} | "
                f"{s['condition']} | "
                f"{s['r3']:.2f}% | {s['r5']:.2f}% | {s['r10']:.2f}% | {s['r20']:.2f}% | "
                f"{s['base_range']:.2f}% | {s['close_to_base_high']:.2f}% | {s['latest_close']:.2f} |\n"
            )

        md_content += "\n---\n\n"
        md_content += "## 个股行情与通俗解读\n\n"

        for idx, s in enumerate(stock_list, start=1):
            detail_text = "；".join(s["red_days_detail"])

            md_content += f"### {idx}. {s['name']}（{s['code']}）\n\n"

            md_content += (
                f"**形态数据**：形态评分 **{s['pattern_score']:.1f}**；"
                f"命中条件为 **{s['condition']}**；"
                f"3日涨幅 **{s['r3']:.2f}%**，"
                f"5日涨幅 **{s['r5']:.2f}%**，"
                f"10日涨幅 **{s['r10']:.2f}%**，"
                f"20日涨幅 **{s['r20']:.2f}%**；"
                f"平台振幅 **{s['base_range']:.2f}%**；"
                f"距离平台高点 **{s['close_to_base_high']:.2f}%**；"
                f"偏离20日均线 **{s['close_above_ma20']:.2f}%**；"
                f"最新收盘价 **{s['latest_close']:.2f}**。\n\n"
            )

            md_content += f"**形态细节**：{detail_text}\n\n"
            md_content += f"**风险观察**：{s['risk_note']}\n\n"

            md_content += get_sina_chart_html(s["symbol"], s["name"])

            stock_brief = ask_deepseek_single_stock_brief(s, ai_cache=ai_cache)
            md_content += stock_brief + "\n\n"

            md_content += "---\n\n"

        md_content += get_random_philosophy() + "\n\n"

    md_content += f"""
---

*本文由自动化程序于北京时间 {today_date} 自动发布。*
"""

    file_path = os.path.join(POST_FOLDER, f"{REPORT_PREFIX}-{today_date}.md")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"✅ 博客文章已成功生成：{file_path}")


# ================= 主程序 =================
if __name__ == "__main__":
    stock_list = get_surge_stocks()
    write_blog_post(stock_list)
