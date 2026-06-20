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
# 主筛选窗口（个交易日）
SCREEN_WINDOW = 10
# 短期窗口（用于计算近端动量）
SHORT_WINDOW = 5
# 加速度计算：近端 N 天 vs 之前 M 天
ACCEL_RECENT_DAYS = 3
ACCEL_PREV_DAYS = 4

# 单日涨幅阈值（以收盘价对前一交易日收盘价计算）
BIG_UP_PCT = 5.0       # 大涨日：单日涨幅 > 5%
SUPER_UP_PCT = 7.0     # 超大涨日：单日涨幅 > 7%（接近或触及涨停）

# 条件A：龙头放量型
COND_A_SUPER_DAYS = 2          # 10日内至少 2 天单日涨幅 > SUPER_UP_PCT
COND_A_TOTAL_CHANGE = 15.0     # 且10日累计涨幅 > 15%

# 条件B：持续强势型
COND_B_BIG_DAYS = 3            # 10日内至少 3 天单日涨幅 > BIG_UP_PCT
COND_B_RED_RATIO = 0.6         # 且10日红K率 >= 60%

# 条件C：加速启动型
COND_C_RECENT_CHANGE = 10.0    # 近3日累计涨幅 > 10%
COND_C_PREV_CHANGE = 5.0       # 且之前4日累计涨幅 < 5%（说明刚启动）

# 过滤上限：10日累计涨幅 > 该阈值视为已透支，剔除
MAX_TOTAL_CHANGE = 60.0

TOP_N = 25

# 首次建缓存时才会用到新浪历史接口，别太高
MAX_WORKERS = 6

# 拉最近60个自然日，尽量覆盖10个交易日以及节假日空档
HIST_CALENDAR_DAYS = 60

POST_FOLDER = "content/post"

# 缓存目录：会生成 stock_cache/sina_ohlc_cache.csv
CACHE_FOLDER = "stock_cache"
CACHE_FILE = os.path.join(CACHE_FOLDER, "sina_ohlc_cache.csv")

REPORT_PREFIX = "hot"

# DeepSeek 配置
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash")
DEEPSEEK_API_BASE = os.environ.get("DEEPSEEK_API_BASE", "https://api.deepseek.com")
DEEPSEEK_THINKING = os.environ.get("DEEPSEEK_THINKING", "disabled").strip().lower()

# AI 个股解读缓存
AI_CACHE_FOLDER = "stock_cache"
AI_CACHE_FILE = os.path.join(AI_CACHE_FOLDER, "deepseek_hot_stock_brief_cache.json")

# 改 prompt 时手动改这个版本号，避免继续使用旧口径缓存
AI_CACHE_VERSION = "hot_stock_brief_v1"

# AI 缓存最多保留多少天，防止长期无限增长
AI_CACHE_KEEP_DAYS = 180


# ================= 工具函数 =================
def cn_now():
    """
    返回北京时间，避免 datetime.utcnow() 的 DeprecationWarning。
    保持 naive datetime，避免和旧缓存时间比较时报错。
    """
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(hours=8)


def clean_stock_code(code):
    """
    把 sh600000、sz000001、600000、600000.0 等格式统一清洗成 6 位纯数字代码。
    """
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
    """
    这一段按你提供的可运行代码写：
    直接在函数内部完成字段识别、代码清洗、open/close 校验。
    """
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
                    print(f"❌ {source_name} 实时行情缺少所需字段。")
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
    cache_df = cache_df.groupby("symbol", group_keys=False).tail(80)

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
            print(f"💾 已加载 DeepSeek 热点个股解读缓存：{len(data)} 条。")
            return data

        return {}

    except Exception as e:
        print(f"⚠️ DeepSeek 热点个股解读缓存读取失败，将重新创建：{str(e)}")
        return {}


def save_ai_cache(cache_data):
    try:
        os.makedirs(AI_CACHE_FOLDER, exist_ok=True)

        cache_data = prune_ai_cache(cache_data)

        with open(AI_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

        print(f"✅ DeepSeek 热点个股解读缓存已保存：{AI_CACHE_FILE}，共 {len(cache_data)} 条。")

    except Exception as e:
        print(f"⚠️ DeepSeek 热点个股解读缓存保存失败：{str(e)}")


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
        "screen_window": SCREEN_WINDOW,
        "big_up_pct": BIG_UP_PCT,
        "super_up_pct": SUPER_UP_PCT,
        "code": str(stock.get("code", "")).zfill(6),
        "name": str(stock.get("name", "")),
        "big_up_days": int(stock.get("big_up_days", 0)),
        "super_up_days": int(stock.get("super_up_days", 0)),
        "red_count_10": int(stock.get("red_count_10", 0)),
        "condition": str(stock.get("condition", "")),
        "total_change": round(float(stock.get("total_change", 0)), 2),
        "short_change": round(float(stock.get("short_change", 0)), 2),
        "acceleration": round(float(stock.get("acceleration", 0)), 2),
        "latest_close": round(float(stock.get("latest_close", 0)), 2),
        "big_up_detail": stock.get("big_up_detail", [])
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


# ================= 核心筛选：多因子热点扫描 =================
def screen_from_cache(cache_df):
    print("🧮 正在从本地缓存中执行多因子热点扫描...")
    print(f"📐 筛选规则：")
    print(f"   - 条件A（龙头放量）：10日内 ≥{COND_A_SUPER_DAYS} 天单日涨幅>{SUPER_UP_PCT}%，且10日涨幅>{COND_A_TOTAL_CHANGE}%")
    print(f"   - 条件B（持续强势）：10日内 ≥{COND_B_BIG_DAYS} 天单日涨幅>{BIG_UP_PCT}%，且红K率>={COND_B_RED_RATIO*100:.0f}%")
    print(f"   - 条件C（加速启动）：近3日涨幅>{COND_C_RECENT_CHANGE}%，且之前4日涨幅<{COND_C_PREV_CHANGE}%")
    print(f"   - 过滤上限：10日涨幅 > {MAX_TOTAL_CHANGE}% 视为透支，剔除")

    if cache_df is None or cache_df.empty:
        print("今日未筛选到符合条件的股票。")
        return None

    cache_df = cache_df.copy()
    cache_df["date"] = cache_df["date"].astype(str)
    cache_df["open"] = pd.to_numeric(cache_df["open"], errors="coerce")
    cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
    cache_df = cache_df.dropna(subset=["symbol", "date", "open", "close"])
    cache_df = cache_df.sort_values(["symbol", "date"])

    results = []

    # 多取一天用于计算第一天的"日涨跌幅"（基于前一收盘）
    need_len = SCREEN_WINDOW + 1

    for symbol, group in cache_df.groupby("symbol"):
        group = group.drop_duplicates(subset=["date"], keep="last").sort_values("date")

        if len(group) < need_len:
            continue

        win = group.tail(need_len).copy().reset_index(drop=True)
        win["prev_close"] = win["close"].shift(1)
        # 日涨跌幅：close 相对前一日 close 的变化（标准日收益率）
        win["day_change"] = (win["close"] - win["prev_close"]) / win["prev_close"] * 100

        # 真正的筛选窗口（10 行，都有有效 day_change）
        screen_df = win.iloc[1:].reset_index(drop=True)

        if len(screen_df) < SCREEN_WINDOW:
            continue

        # ----- 核心因子 -----
        # 1. 大涨日 / 超大涨日数量（市场关注度指标）
        big_up_days = int((screen_df["day_change"] > BIG_UP_PCT).sum())
        super_up_days = int((screen_df["day_change"] > SUPER_UP_PCT).sum())

        # 2. 红K率（实体红K，用于辅助判断趋势健康度）
        red_count = int((screen_df["close"] > screen_df["open"]).sum())
        red_ratio = red_count / len(screen_df)

        # 3. 10日累计涨幅（窗口起点的前一日收盘 -> 最新收盘）
        base_close = float(win.iloc[0]["close"])
        latest_close = float(screen_df.iloc[-1]["close"])
        if base_close <= 0 or latest_close <= 0:
            continue
        total_change = (latest_close - base_close) / base_close * 100

        # 过滤：透支股
        if total_change > MAX_TOTAL_CHANGE:
            continue
        # 过滤：明显是下跌中的反弹噪音
        if total_change < -5:
            continue

        # 4. 短期5日涨幅
        if len(screen_df) >= SHORT_WINDOW:
            short_base_idx = len(screen_df) - SHORT_WINDOW - 1
            if short_base_idx >= 0:
                short_base = float(screen_df.iloc[short_base_idx]["close"])
            else:
                short_base = float(win.iloc[max(0, len(win) - SHORT_WINDOW - 1)]["close"])
            short_change = (latest_close - short_base) / short_base * 100 if short_base > 0 else 0.0
        else:
            short_change = total_change

        # 5. 加速度：近 ACCEL_RECENT_DAYS 天涨幅 - 之前 ACCEL_PREV_DAYS 天涨幅
        recent_change_pct = 0.0
        prev_change_pct = 0.0
        acceleration = 0.0

        if len(screen_df) >= ACCEL_RECENT_DAYS + ACCEL_PREV_DAYS:
            split_idx = len(screen_df) - ACCEL_RECENT_DAYS

            recent_start_close = float(screen_df.iloc[split_idx - 1]["close"])
            prev_start_close_idx = split_idx - ACCEL_PREV_DAYS - 1

            if prev_start_close_idx >= 0:
                prev_start_close = float(screen_df.iloc[prev_start_close_idx]["close"])
            else:
                prev_start_close = float(win.iloc[0]["close"])

            if recent_start_close > 0:
                recent_change_pct = (latest_close - recent_start_close) / recent_start_close * 100
            if prev_start_close > 0:
                prev_change_pct = (recent_start_close - prev_start_close) / prev_start_close * 100

            acceleration = recent_change_pct - prev_change_pct

        # ----- 三个条件判定 -----
        condition_hits = []

        # 条件A：龙头放量型
        if super_up_days >= COND_A_SUPER_DAYS and total_change > COND_A_TOTAL_CHANGE:
            condition_hits.append(
                f"龙头放量({super_up_days}个>{SUPER_UP_PCT:.0f}%涨日)"
            )

        # 条件B：持续强势型
        if big_up_days >= COND_B_BIG_DAYS and red_ratio >= COND_B_RED_RATIO:
            condition_hits.append(
                f"持续强势({big_up_days}个>{BIG_UP_PCT:.0f}%涨日+红K率{red_ratio*100:.0f}%)"
            )

        # 条件C：加速启动型
        if (recent_change_pct > COND_C_RECENT_CHANGE
                and prev_change_pct < COND_C_PREV_CHANGE):
            condition_hits.append(
                f"加速启动(近3日+{recent_change_pct:.1f}%)"
            )

        if not condition_hits:
            continue

        # ----- 综合评分 -----
        # 大涨日权重最高，叠加短期动量、加速度、红K健康度
        score = (
            super_up_days * 5.0 +
            big_up_days * 3.0 +
            short_change * 0.5 +
            max(acceleration, 0) * 0.8 +
            red_ratio * 10.0
        )

        # ----- 大涨日明细 -----
        big_up_detail = []
        for _, row in screen_df.iterrows():
            dc = float(row["day_change"])
            if dc > BIG_UP_PCT:
                tag = "🔥" if dc > SUPER_UP_PCT else "💥"
                big_up_detail.append(f"{row['date']} {tag} {dc:+.2f}%")

        latest_row = screen_df.iloc[-1]

        results.append({
            "name": str(latest_row["name"]),
            "code": str(latest_row["code"]).zfill(6),
            "symbol": symbol,
            "big_up_days": big_up_days,
            "super_up_days": super_up_days,
            "red_count_10": red_count,
            "red_ratio_10": red_ratio,
            "condition": " + ".join(condition_hits),
            "total_change": total_change,
            "short_change": short_change,
            "acceleration": acceleration,
            "score": score,
            "latest_close": latest_close,
            "big_up_detail": big_up_detail
        })

    if not results:
        print("今日未筛选到符合条件的股票。")
        return None

    results = sorted(results, key=lambda x: x["score"], reverse=True)
    top_results = results[:TOP_N]

    print(f"🎯 筛选完成：共命中 {len(results)} 只，按综合评分截取 TOP {TOP_N}。")

    for item in top_results:
        print(
            f"✅ {item['name']}({item['code']}) "
            f"{item['condition']}，评分 {item['score']:.1f}，"
            f"10日涨幅 {item['total_change']:.2f}%"
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
        print(f"💾 命中 DeepSeek 热点解读缓存：{stock['name']}({stock['code']})")
        return cached_item["text"]

    detail_text = "；".join(stock["big_up_detail"]) if stock["big_up_detail"] else "无单日大涨"

    system_prompt = """你是一位严谨的A股市场研究员。
请用通俗易懂的大白话解释股票，不要写投资建议，不要承诺上涨。
如果你无法确定某个原因，必须写"可能与……有关"，不要装作确定。
避免使用"必涨""确定上涨""强烈推荐""可以买入"等表述。

你必须严格按照下面格式输出：

**这家公司是做什么的：**
用1-2句话说明主营业务、产品、客户或所处行业。尽量大白话，不要堆术语。

**这波为什么受市场关注：**
用1-2条 bullet 分析可能原因，比如题材催化、业绩预期、政策方向、行业情绪、龙头效应、资金抱团等。结合它最近多次大涨这个现象去推测，不要泛泛而谈。
"""

    user_prompt = f"""请分析这只股票：

股票名称：{stock['name']}
股票代码：{stock['code']}
入选原因：{stock['condition']}
最近10个交易日单日涨幅>5%的天数：{stock['big_up_days']} 天
最近10个交易日单日涨幅>7%的天数：{stock['super_up_days']} 天（接近或触及涨停）
最近10个交易日累计涨幅：{stock['total_change']:.2f}%
最近5个交易日累计涨幅：{stock['short_change']:.2f}%
加速度（近3日涨幅 - 之前4日涨幅）：{stock['acceleration']:+.2f}%
最新收盘价：{stock['latest_close']:.2f}
大涨日明细：{detail_text}

请重点讲清楚：
1. 这家公司是做什么的。
2. 它最近为什么会反复出现单日大涨，资金可能在炒什么题材或预期。

总字数控制在150字左右。
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
            "big_up_days": int(stock["big_up_days"]),
            "super_up_days": int(stock["super_up_days"]),
            "red_count_10": int(stock["red_count_10"]),
            "condition": stock["condition"],
            "total_change": round(float(stock["total_change"]), 2),
            "short_change": round(float(stock["short_change"]), 2),
            "acceleration": round(float(stock["acceleration"]), 2),
            "latest_close": round(float(stock["latest_close"]), 2),
            "big_up_detail": stock["big_up_detail"],
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
title: "🔥 【全市场热点雷达】短线强势 + 高关注度股票扫描 ({today_date})"
date: {post_time}
categories:
    - 量化研报
tags:
    - AI选股
    - 热点扫描
    - 短线强势
    - 全市场扫描
    - 新浪行情
    - 网易兜底
    - DeepSeek
draft: false
---

# 🔥 全市场热点雷达：短线强势 + 高关注度股票扫描

本报告由 **Python + 新浪/网易行情接口 + 本地OHLC缓存 + DeepSeek AI** 自动生成。

> ⚠️ 风险提示：本文仅为基于公开行情数据的自动化整理与AI文本生成，不构成任何投资建议。股市有风险，交易需谨慎。

## 筛选思路

不再单纯计数红K线，而是用**多因子模型**捕捉真正被市场关注的标的：

- **股票范围**：A股全市场，剔除 ST、退市、停牌无价格标的
- **关注度代理指标**：单日涨幅>5% 视为大涨日，>7% 视为超大涨日（接近涨停）。资金真正关注的股票会反复出现单日大涨
- **筛选条件**（满足任一即可入选）：
  - **条件A · 龙头放量型**：最近10日内至少 **{COND_A_SUPER_DAYS}** 天单日涨幅>{SUPER_UP_PCT:.0f}%，且10日累计涨幅>{COND_A_TOTAL_CHANGE:.0f}%
  - **条件B · 持续强势型**：最近10日内至少 **{COND_B_BIG_DAYS}** 天单日涨幅>{BIG_UP_PCT:.0f}%，且红K率≥{COND_B_RED_RATIO*100:.0f}%
  - **条件C · 加速启动型**：最近3日涨幅>{COND_C_RECENT_CHANGE:.0f}%，且之前4日涨幅<{COND_C_PREV_CHANGE:.0f}%（刚启动）
- **风险过滤**：10日累计涨幅 > **{MAX_TOTAL_CHANGE:.0f}%** 视为已透支，剔除
- **排序方式**：综合评分（大涨日数 × 权重 + 短期动量 + 加速度 + 红K健康度），截取 TOP {TOP_N}
- **数据来源**：新浪行情接口为主，网易行情接口兜底
- **AI模型**：{DEEPSEEK_MODEL}

---

"""

    if stock_list == "ERROR":
        md_content += """
## 今日扫描结果

今日新浪/网易数据抓取失败，未能完成全市场热点扫描。

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

经过全市场扫描，暂时没有股票满足任一入选条件。

这通常说明短期内市场缺少强势龙头，或者已有的强势股已经透支被过滤，资金可能处于分歧、休整或切换状态。

---

{get_random_philosophy()}

---

"""

    else:
        ai_cache = load_ai_cache()

        md_content += "## 今日命中的 TOP 热点股票\n\n"
        md_content += "| 排名 | 股票 | 代码 | 命中条件 | 大涨日(>5%) | 超大涨日(>7%) | 10日涨幅 | 5日涨幅 | 加速度 | 评分 | 最新价 |\n"
        md_content += "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|\n"

        for idx, s in enumerate(stock_list, start=1):
            md_content += (
                f"| {idx} | {s['name']} | {s['code']} | {s['condition']} | "
                f"{s['big_up_days']} | {s['super_up_days']} | "
                f"{s['total_change']:+.2f}% | {s['short_change']:+.2f}% | "
                f"{s['acceleration']:+.2f}% | {s['score']:.1f} | "
                f"{s['latest_close']:.2f} |\n"
            )

        md_content += "\n---\n\n"
        md_content += "## 个股行情与通俗解读\n\n"

        for idx, s in enumerate(stock_list, start=1):
            big_up_text = "；".join(s["big_up_detail"]) if s["big_up_detail"] else "近10日无单日大涨（靠加速度入选）"

            md_content += f"### {idx}. {s['name']}（{s['code']}）\n\n"

            md_content += (
                f"**异动数据**：命中条件 **{s['condition']}**；"
                f"近10日大涨日 **{s['big_up_days']}** 天（其中接近涨停的超大涨日 **{s['super_up_days']}** 天）；"
                f"10日累计涨幅 **{s['total_change']:+.2f}%**；"
                f"5日累计涨幅 **{s['short_change']:+.2f}%**；"
                f"加速度 **{s['acceleration']:+.2f}%**；"
                f"综合评分 **{s['score']:.1f}**；"
                f"最新收盘价 **{s['latest_close']:.2f}**。\n\n"
            )

            md_content += f"**大涨日明细**：{big_up_text}\n\n"

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

