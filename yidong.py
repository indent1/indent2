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
RED_WINDOW_LONG = 10
RED_DAYS_LONG = 8

RED_WINDOW_SHORT = 7
RED_DAYS_SHORT = 6

TOP_N = 25

# 首次建缓存时才会用到历史接口，并发别太高，避免被接口限流
MAX_WORKERS = 3

# 拉最近60个自然日，尽量覆盖10个交易日以及节假日空档
HIST_CALENDAR_DAYS = 60

POST_FOLDER = "content/post"

# 缓存目录：会生成 stock_cache/sina_ohlc_cache.csv
CACHE_FOLDER = "stock_cache"
CACHE_FILE = os.path.join(CACHE_FOLDER, "sina_ohlc_cache.csv")

REPORT_PREFIX = "redk"

# DeepSeek 配置
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash")
DEEPSEEK_API_BASE = os.environ.get("DEEPSEEK_API_BASE", "https://api.deepseek.com")
DEEPSEEK_THINKING = os.environ.get("DEEPSEEK_THINKING", "disabled").strip().lower()

# AI 个股解读缓存
AI_CACHE_FOLDER = "stock_cache"
AI_CACHE_FILE = os.path.join(AI_CACHE_FOLDER, "deepseek_redk_stock_brief_cache.json")

# 改 prompt 时手动改这个版本号，避免继续使用旧口径缓存
AI_CACHE_VERSION = "redk_stock_brief_v1"

# AI 缓存最多保留多少天，防止长期无限增长
AI_CACHE_KEEP_DAYS = 180


# ================= 工具函数 =================
def cn_now():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=8)


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
def normalize_spot_df(spot_df, source_name):
    if spot_df is None or spot_df.empty:
        return None

    try:
        code_col = find_column(spot_df.columns, ["代码", "symbol", "code"])
        name_col = find_column(spot_df.columns, ["名称", "name"])
        close_col = find_column(spot_df.columns, ["最新价", "最新", "现价", "收盘", "price", "trade"])
        open_col = find_column(spot_df.columns, ["今开", "开盘", "open"])

        if not code_col or not name_col or not close_col:
            print(f"❌ {source_name} 实时行情缺少关键字段。")
            print(spot_df.columns.tolist())
            return None

        df = spot_df.copy()

        df["code"] = df[code_col].apply(clean_stock_code)
        df = df.dropna(subset=["code"]).copy()

        df["symbol"] = df["code"].apply(get_market_prefix)
        df["name"] = df[name_col].astype(str)
        df["close"] = pd.to_numeric(df[close_col], errors="coerce")

        if open_col:
            df["open"] = pd.to_numeric(df[open_col], errors="coerce")
        else:
            df["open"] = None
            print(f"⚠️ {source_name} 没有识别到今开字段，可能无法更新当天红K。")

        df = df[df["code"].astype(str).str.match(r"^\d{6}$", na=False)].copy()
        df = df[~df["name"].str.contains(r"\*?ST|退", regex=True, na=False)].copy()
        df = df[df["close"] > 0].copy()

        if open_col:
            df = df[(df["open"].isna()) | (df["open"] > 0)].copy()

        date_col = find_column(df.columns, ["日期", "date"])

        if date_col:
            df["date"] = df[date_col].apply(normalize_date)
        else:
            df["date"] = get_safe_market_date()

        result = (
            df[["symbol", "code", "name", "date", "open", "close"]]
            .dropna(subset=["symbol", "code", "name", "date", "close"])
            .drop_duplicates(subset=["symbol"], keep="last")
            .copy()
        )

        if result.empty:
            return None

        print(f"🚀 {source_name} 返回可用股票数量：{len(result)}")
        return result

    except Exception as e:
        print(f"❌ {source_name} 实时行情清洗失败：{str(e)}")
        print(spot_df.columns.tolist())
        return None


def get_all_a_stock_spot_sina():
    print("📈 正在通过【新浪/网易】获取A股全市场实时行情...")

    providers = [
        ("新浪", lambda: ak.stock_zh_a_spot()),
        ("网易", lambda: ak.stock_zh_a_spot_netease()),
    ]

    for source_name, fetcher in providers:
        for attempt in range(3):
            try:
                print(f"🔎 尝试获取 {source_name} 实时行情，第 {attempt + 1} 次...")

                raw_df = fetcher()
                result = normalize_spot_df(raw_df, source_name)

                if result is not None and not result.empty:
                    print(f"✅ {source_name} 全市场行情获取成功！")
                    return result

                print(f"⚠️ {source_name} 返回为空或字段异常。")
                time.sleep(2 + attempt * 2)

            except AttributeError as e:
                print(f"⚠️ 当前 AkShare 版本可能没有 {source_name} 接口：{str(e)}")
                break

            except Exception as e:
                print(f"⚠️ {source_name} 实时行情获取失败，第 {attempt + 1} 次：{str(e)}")
                time.sleep(3 + attempt * 2)

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
            print(f"💾 已加载 DeepSeek 红K个股解读缓存：{len(data)} 条。")
            return data

        return {}

    except Exception as e:
        print(f"⚠️ DeepSeek 红K个股解读缓存读取失败，将重新创建：{str(e)}")
        return {}


def save_ai_cache(cache_data):
    try:
        os.makedirs(AI_CACHE_FOLDER, exist_ok=True)

        cache_data = prune_ai_cache(cache_data)

        with open(AI_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

        print(f"✅ DeepSeek 红K个股解读缓存已保存：{AI_CACHE_FILE}，共 {len(cache_data)} 条。")

    except Exception as e:
        print(f"⚠️ DeepSeek 红K个股解读缓存保存失败：{str(e)}")


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
        "red_window_long": RED_WINDOW_LONG,
        "red_days_long": RED_DAYS_LONG,
        "red_window_short": RED_WINDOW_SHORT,
        "red_days_short": RED_DAYS_SHORT,
        "code": str(stock.get("code", "")).zfill(6),
        "name": str(stock.get("name", "")),
        "red_count_10": int(stock.get("red_count_10", 0)),
        "red_count_7": int(stock.get("red_count_7", 0)),
        "condition": str(stock.get("condition", "")),
        "total_change": round(float(stock.get("total_change", 0)), 2),
        "latest_close": round(float(stock.get("latest_close", 0)), 2),
        "red_days_detail": stock.get("red_days_detail", [])
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

    except Exception:
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
        print("⚠️ 实时行情没有可用 open 字段，本次不更新当天K线。")
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

    print(f"✅ 已用实时行情更新OHLC缓存，当前缓存 {len(updated)} 行。")
    return updated


# ================= 核心筛选：10日8红 或 7日6红 =================
def screen_from_cache(cache_df):
    print("🧮 正在从本地缓存中执行红K量化筛选...")

    results = []

    if cache_df is None or cache_df.empty:
        print("今日未筛选到符合红K条件的股票。")
        return None

    cache_df = cache_df.copy()
    cache_df["date"] = cache_df["date"].astype(str)
    cache_df["open"] = pd.to_numeric(cache_df["open"], errors="coerce")
    cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
    cache_df = cache_df.dropna(subset=["symbol", "date", "open", "close"])
    cache_df = cache_df.sort_values(["symbol", "date"])

    for symbol, group in cache_df.groupby("symbol"):
        group = group.drop_duplicates(subset=["date"], keep="last").sort_values("date")

        if len(group) < RED_WINDOW_LONG:
            continue

        last_10 = group.tail(RED_WINDOW_LONG).copy()
        last_7 = group.tail(RED_WINDOW_SHORT).copy()

        red_count_10 = int((last_10["close"] > last_10["open"]).sum())
        red_count_7 = int((last_7["close"] > last_7["open"]).sum())

        condition_list = []

        if red_count_10 >= RED_DAYS_LONG:
            condition_list.append(f"{RED_WINDOW_LONG}日{red_count_10}红")

        if red_count_7 >= RED_DAYS_SHORT:
            condition_list.append(f"{RED_WINDOW_SHORT}日{red_count_7}红")

        if not condition_list:
            continue

        close_latest = float(last_10.iloc[-1]["close"])
        close_start = float(last_10.iloc[0]["close"])

        if close_start <= 0:
            continue

        total_change = (close_latest - close_start) / close_start * 100

        red_days_detail = []

        for _, row in last_10.iterrows():
            is_red = float(row["close"]) > float(row["open"])
            if is_red:
                day_change = (float(row["close"]) - float(row["open"])) / float(row["open"]) * 100
                red_days_detail.append(f"{row['date']} 红K {day_change:.2f}%")

        latest_row = last_10.iloc[-1]

        results.append({
            "name": str(latest_row["name"]),
            "code": str(latest_row["code"]).zfill(6),
            "symbol": symbol,
            "red_count_10": red_count_10,
            "red_count_7": red_count_7,
            "condition": "、".join(condition_list),
            "total_change": total_change,
            "latest_close": close_latest,
            "red_days_detail": red_days_detail
        })

    if not results:
        print("今日未筛选到符合红K条件的股票。")
        return None

    results = sorted(results, key=lambda x: x["total_change"], reverse=True)
    top_results = results[:TOP_N]

    print(f"🎯 筛选完成：共命中 {len(results)} 只，按10日区间涨幅截取 TOP {TOP_N}。")

    for item in top_results:
        print(
            f"✅ {item['name']}({item['code']}) "
            f"{item['condition']}，10日涨幅 {item['total_change']:.2f}%"
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
        print(f"💾 命中 DeepSeek 红K解读缓存：{stock['name']}({stock['code']})")
        return cached_item["text"]

    detail_text = "；".join(stock["red_days_detail"])

    system_prompt = """你是一位严谨的A股市场研究员。
请用通俗易懂的大白话解释股票，不要写投资建议，不要承诺上涨。
如果你无法确定某个原因，必须写“可能与……有关”，不要装作确定。
避免使用“必涨”“确定上涨”“强烈推荐”“可以买入”等表述。

你必须严格按照下面格式输出：

**这家公司是做什么的：**
用1-2句话说明主营业务、产品、客户或所处行业。尽量大白话，不要堆术语。

**这波为什么会涨：**
用1-2条 bullet 分析可能原因，比如题材催化、业绩预期、政策方向、行业情绪、市场资金偏好等。
"""

    user_prompt = f"""请分析这只股票：

股票名称：{stock['name']}
股票代码：{stock['code']}
最近10个交易日区间涨幅：{stock['total_change']:.2f}%
最新收盘价：{stock['latest_close']:.2f}
最近10个交易日红K情况：{detail_text}

请重点讲清楚：
1. 这家公司是做什么的。
2. 它为什么会连续出现这么多红K，资金可能在炒什么。

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
            "red_count_10": int(stock["red_count_10"]),
            "red_count_7": int(stock["red_count_7"]),
            "condition": stock["condition"],
            "total_change": round(float(stock["total_change"]), 2),
            "latest_close": round(float(stock["latest_close"]), 2),
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
title: "📈 【全市场红K雷达】10日8红 / 7日6红强势股扫描 ({today_date})"
date: {post_time}
categories:
    - 量化研报
tags:
    - AI选股
    - 红K扫描
    - 全市场扫描
    - 新浪行情
    - 网易兜底
    - DeepSeek
draft: false
---

# 📈 全市场红K雷达：10日8红 / 7日6红强势股扫描

本报告由 **Python + 新浪/网易行情接口 + 本地OHLC缓存 + DeepSeek AI** 自动生成。

> ⚠️ 风险提示：本文仅为基于公开行情数据的自动化整理与AI文本生成，不构成任何投资建议。股市有风险，交易需谨慎。

扫描条件：

- 股票范围：A股全市场，剔除 ST、退市、停牌无价格标的
- 条件A：最近 **{RED_WINDOW_LONG}** 个交易日内，至少 **{RED_DAYS_LONG}** 天是红K线
- 条件B：最近 **{RED_WINDOW_SHORT}** 个交易日内，至少 **{RED_DAYS_SHORT}** 天是红K线
- 红K定义：当日 **收盘价 > 开盘价**
- 入选规则：满足条件A或条件B即可入选
- 排名方式：按最近10个交易日区间涨幅排序，截取 TOP {TOP_N}
- 数据来源：新浪行情接口为主，网易行情接口兜底
- AI模型：{DEEPSEEK_MODEL}

---

"""

    if stock_list == "ERROR":
        md_content += """
## 今日扫描结果

今日新浪/网易数据抓取失败，未能完成全市场红K扫描。

可能原因包括：

- 新浪或网易接口临时不可用
- GitHub Actions 海外网络异常
- AkShare 接口字段变化
- 请求频率过高被临时限制

---

"""

    elif stock_list is None:
        md_content += f"""
## 今日扫描结果

经过全市场扫描，暂时没有股票满足：

> 最近 {RED_WINDOW_LONG} 个交易日至少 {RED_DAYS_LONG} 天红K，或最近 {RED_WINDOW_SHORT} 个交易日至少 {RED_DAYS_SHORT} 天红K。

这通常说明短线持续阳线推进的标的较少，市场资金可能处于分歧或休整状态。

---

{get_random_philosophy()}

---

"""

    else:
        ai_cache = load_ai_cache()

        md_content += "## 今日命中的 TOP 红K活跃股票\n\n"
        md_content += "| 排名 | 股票 | 代码 | 命中条件 | 10日红K数 | 7日红K数 | 10日区间涨幅 | 最新收盘价 |\n"
        md_content += "|---|---|---|---|---:|---:|---:|---:|\n"

        for idx, s in enumerate(stock_list, start=1):
            md_content += (
                f"| {idx} | {s['name']} | {s['code']} | {s['condition']} | "
                f"{s['red_count_10']} | {s['red_count_7']} | "
                f"{s['total_change']:.2f}% | {s['latest_close']:.2f} |\n"
            )

        md_content += "\n---\n\n"
        md_content += "## 个股行情与通俗解读\n\n"

        for idx, s in enumerate(stock_list, start=1):
            red_detail_text = "；".join(s["red_days_detail"])

            md_content += f"### {idx}. {s['name']}（{s['code']}）\n\n"

            md_content += (
                f"**异动数据**：命中条件为 **{s['condition']}**；"
                f"最近10个交易日红K数量 **{s['red_count_10']}** 天，"
                f"最近7个交易日红K数量 **{s['red_count_7']}** 天；"
                f"最近10个交易日区间涨幅 **{s['total_change']:.2f}%**；"
                f"最新收盘价 **{s['latest_close']:.2f}**。\n\n"
            )

            md_content += f"**红K日期**：{red_detail_text}\n\n"

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
