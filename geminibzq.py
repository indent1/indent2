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
# 选股策略参数：潜伏蓄势 / 爆发前夕模型 (老鸭头/均线密集)
LOOKBACK_DAYS = 30         # 考察的交易日窗口期
MA_TIGHTNESS_PCT = 5.0     # 均线密集度(%)：MA5与MA20的偏离度不能超过此值，越小代表均线粘合度越高，蓄势越充分
MAX_DAILY_PCT = 6.5        # 窗口期内单日最大涨幅(%)：剔除最近已经出现过暴涨或涨停的个股（寻找未爆发的）
MIN_CLOSE_TO_HIGH = 92.0   # 当前价格距离近30日最高收盘价的比例(%)：保证在近期高点附近横盘，准备突破

TOP_N = 25

# 并发配置
MAX_WORKERS = 6
HIST_CALENDAR_DAYS = 80    # 拉长一点自然日，确保能算足 MA20

# 目录与前缀配置
POST_FOLDER = "content/post"
CACHE_FOLDER = "stock_cache"
CACHE_FILE = os.path.join(CACHE_FOLDER, "sina_ohlc_cache.csv")
REPORT_PREFIX = "sneakk"

# DeepSeek 配置
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash")
DEEPSEEK_API_BASE = os.environ.get("DEEPSEEK_API_BASE", "https://api.deepseek.com")
DEEPSEEK_THINKING = os.environ.get("DEEPSEEK_THINKING", "disabled").strip().lower()

# AI 个股解读缓存
AI_CACHE_FOLDER = "stock_cache"
AI_CACHE_FILE = os.path.join(AI_CACHE_FOLDER, "deepseek_sneak_stock_brief_cache.json")
AI_CACHE_VERSION = "sneak_stock_brief_v1"
AI_CACHE_KEEP_DAYS = 180


# ================= 工具函数 =================
def cn_now():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(hours=8)

def clean_stock_code(code):
    text = str(code).lower().replace("sh", "").replace("sz", "").replace("bj", "").replace(".0", "").strip()
    digits = "".join([ch for ch in text if ch.isdigit()])
    if not digits: return None
    return digits[-6:].zfill(6)

def get_market_prefix(code):
    code_str = clean_stock_code(code)
    if not code_str: return None
    if code_str.startswith("6"): return f"sh{code_str}"
    elif code_str.startswith("0") or code_str.startswith("3"): return f"sz{code_str}"
    elif code_str.startswith("4") or code_str.startswith("8") or code_str.startswith("9"): return f"bj{code_str}"
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
    if weekday == 5: now = now - datetime.timedelta(days=1)
    elif weekday == 6: now = now - datetime.timedelta(days=2)
    return now.strftime("%Y-%m-%d")

def get_random_philosophy():
    return "> 💡 **潜伏信条**：*“善战者，无智名，无勇功。大爆发行情的起点，往往平淡无奇。”*"

def find_column(columns, keywords):
    str_columns = [str(col) for col in columns]
    for keyword in keywords:
        for col in str_columns:
            if keyword in col: return col
    for keyword in keywords:
        for col in str_columns:
            if keyword.lower() in col.lower(): return col
    return None


# ================= 新浪/网易全市场实时行情 =================
def get_all_a_stock_spot_sina():
    print("📈 正在通过【新浪/网易】获取A股全市场实时行情...")
    def clean_code_for_spot(value):
        text = str(value).lower().replace("sh", "").replace("sz", "").replace("bj", "").replace(".0", "").strip()
        digits = "".join([ch for ch in text if ch.isdigit()])
        return digits[-6:].zfill(6) if digits else None

    providers = [("新浪", lambda: ak.stock_zh_a_spot()), ("网易", lambda: ak.stock_zh_a_spot_netease())]

    for source_name, fetcher in providers:
        for attempt in range(3):
            try:
                spot_df = fetcher()
                if spot_df is None or spot_df.empty:
                    time.sleep(2)
                    continue

                code_col = find_column(spot_df.columns, ["代码", "symbol", "code"])
                name_col = find_column(spot_df.columns, ["名称", "name"])
                close_col = find_column(spot_df.columns, ["最新价", "最新", "现价", "收盘", "trade", "price"])
                open_col = find_column(spot_df.columns, ["今开", "开盘", "open"])

                if not code_col or not name_col or not close_col or not open_col: continue

                spot_df = spot_df.copy()
                spot_df["code"] = spot_df[code_col].apply(clean_code_for_spot)
                spot_df = spot_df.dropna(subset=["code"]).copy()
                spot_df["symbol"] = spot_df["code"].apply(get_market_prefix)
                spot_df["name"] = spot_df[name_col].astype(str)
                spot_df["close"] = pd.to_numeric(spot_df[close_col], errors="coerce")
                spot_df["open"] = pd.to_numeric(spot_df[open_col], errors="coerce")

                spot_df = spot_df[spot_df["code"].astype(str).str.match(r"^\d{6}$", na=False)].copy()
                spot_df = spot_df[~spot_df["name"].str.contains(r"\*?ST|退", regex=True, na=False)].copy()
                spot_df = spot_df.dropna(subset=["open", "close"]).copy()
                spot_df = spot_df[(spot_df["open"] > 0) & (spot_df["close"] > 0)].copy()

                date_col = find_column(spot_df.columns, ["日期", "date"])
                if date_col: spot_df["date"] = spot_df[date_col].apply(normalize_date)
                else: spot_df["date"] = get_safe_market_date()

                result = spot_df[["symbol", "code", "name", "date", "open", "close"]].dropna().drop_duplicates(subset=["symbol"], keep="last")

                if not result.empty:
                    print(f"✅ {source_name} 行情获取成功！可用股票数量：{len(result)}")
                    return result
            except Exception as e:
                print(f"⚠️ {source_name} 接口请求失败：{str(e)}")
                time.sleep(3)

    print("❌ 实时行情获取失败。")
    return None


# ================= OHLC缓存构建与管理 =================
def empty_cache_df():
    return pd.DataFrame(columns=["symbol", "code", "name", "date", "open", "close"])

def load_cache():
    if not os.path.exists(CACHE_FILE): return empty_cache_df()
    try:
        cache_df = pd.read_csv(CACHE_FILE, dtype={"symbol": str, "code": str})
        if "close" not in cache_df.columns: return empty_cache_df()
        cache_df["date"] = cache_df["date"].astype(str)
        cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
        return cache_df.dropna(subset=["symbol", "date", "close"])
    except:
        return empty_cache_df()

def save_cache(cache_df):
    os.makedirs(CACHE_FOLDER, exist_ok=True)
    if cache_df is None or cache_df.empty: return
    cache_df = cache_df.dropna(subset=["symbol", "date", "close"])
    cache_df = cache_df.sort_values(["symbol", "date"]).groupby("symbol", group_keys=False).tail(60)
    cache_df.to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")

def cache_too_old(cache_df, spot_trade_date):
    if cache_df is None or cache_df.empty: return True
    try:
        latest_cache_date = pd.to_datetime(cache_df["date"]).max()
        gap_days = (pd.to_datetime(spot_trade_date) - latest_cache_date).days
        return gap_days > 6
    except:
        return True

def fetch_one_history_sina(row, start_date, end_date):
    try:
        time.sleep(random.uniform(0.08, 0.25))
        hist_df = ak.stock_zh_a_daily(symbol=row["symbol"], start_date=start_date, end_date=end_date, adjust="qfq")
        if hist_df is None or hist_df.empty: return []
        
        hist_df = hist_df[["date", "open", "close"]].copy()
        hist_df["date"] = hist_df["date"].apply(normalize_date)
        
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
    except:
        return []

def rebuild_history_cache_from_sina(spot_df):
    start_date, end_date = get_date_range()
    print(f"🧱 开始重建OHLC历史缓存... (使用前复权数据)")
    rows, finished = [], 0
    records = spot_df.to_dict("records")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_one_history_sina, row, start_date, end_date): row for row in records}
        for future in as_completed(futures):
            finished += 1
            if finished % 100 == 0: print(f"🔄 历史缓存进度：{finished} / {len(records)}")
            result_rows = future.result()
            if result_rows: rows.extend(result_rows)

    cache_df = pd.DataFrame(rows).drop_duplicates(subset=["symbol", "date"], keep="last")
    print(f"✅ 缓存重建完成，共 {len(cache_df)} 行。")
    return cache_df

def update_cache_with_spot(cache_df, spot_df):
    if spot_df is None or spot_df.empty: return cache_df
    spot_rows = spot_df[["symbol", "code", "name", "date", "open", "close"]].dropna()
    
    if cache_df is None or cache_df.empty:
        updated = spot_rows
    else:
        cache_df["key"] = cache_df["symbol"].astype(str) + "_" + cache_df["date"].astype(str)
        spot_rows["key"] = spot_rows["symbol"].astype(str) + "_" + spot_rows["date"].astype(str)
        cache_df = cache_df[~cache_df["key"].isin(set(spot_rows["key"]))].drop(columns=["key"])
        updated = pd.concat([cache_df, spot_rows.drop(columns=["key"])], ignore_index=True)
        
    updated = updated.drop_duplicates(subset=["symbol", "date"], keep="last").sort_values(["symbol", "date"])
    return updated


# ================= AI 解读缓存 =================
def load_ai_cache():
    if not os.path.exists(AI_CACHE_FILE): return {}
    try:
        with open(AI_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except: return {}

def save_ai_cache(cache_data):
    try:
        os.makedirs(AI_CACHE_FOLDER, exist_ok=True)
        cutoff = cn_now() - datetime.timedelta(days=AI_CACHE_KEEP_DAYS)
        new_cache = {}
        for k, v in cache_data.items():
            dt = datetime.datetime.strptime(v.get("created_at", "2000-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            if dt >= cutoff: new_cache[k] = v
        with open(AI_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(new_cache, f, ensure_ascii=False, indent=2)
    except: pass

def make_stock_brief_cache_key(stock):
    payload = {
        "report_prefix": REPORT_PREFIX,
        "cache_version": AI_CACHE_VERSION,
        "code": stock["code"],
        "tightness": round(stock["tightness"], 2),
        "close_to_high": round(stock["close_to_high"], 2)
    }
    return hashlib.md5(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


# ================= 核心量化筛选：均线密集 / 潜伏爆发前夕模型 =================
def screen_from_cache(cache_df):
    print("🧮 正在执行【均线密集 / 潜伏爆发前夕】量化筛选...")
    results = []

    if cache_df is None or cache_df.empty: return None

    cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
    cache_df = cache_df.dropna(subset=["symbol", "date", "close"]).sort_values(["symbol", "date"])

    for symbol, group in cache_df.groupby("symbol"):
        group = group.drop_duplicates(subset=["date"], keep="last").sort_values("date")
        
        # 预留20天以上的数据用来计算MA20
        if len(group) < LOOKBACK_DAYS + 20:
            continue

        group = group.copy()
        group["ma5"] = group["close"].rolling(5).mean()
        group["ma10"] = group["close"].rolling(10).mean()
        group["ma20"] = group["close"].rolling(20).mean()
        
        # 计算每日涨跌幅 (使用 shift 规避分母为0等问题)
        group["pre_close"] = group["close"].shift(1)
        group["pct_change"] = (group["close"] - group["pre_close"]) / group["pre_close"] * 100
        
        # 取出近期用于考察的 N 天数据
        last_n = group.tail(LOOKBACK_DAYS).copy()
        if len(last_n) < LOOKBACK_DAYS: continue

        latest = last_n.iloc[-1]

        # ---------------- 过滤条件开始 ----------------

        # 条件1：均线多头排列（即将翘头），5日 > 10日 > 20日
        if not (latest["ma5"] >= latest["ma10"] and latest["ma10"] >= latest["ma20"]):
            continue

        # 条件2：均线极度密集（波动率收缩蓄势）。MA5 和 MA20 距离极小
        tightness = (latest["ma5"] - latest["ma20"]) / latest["ma20"] * 100
        if tightness > MA_TIGHTNESS_PCT or tightness < 0:
            continue

        # 条件3：潜伏期特征，近期不能有暴涨（剔除已被拉爆的股票）
        max_pct_in_window = last_n["pct_change"].max()
        if max_pct_in_window > MAX_DAILY_PCT:
            continue

        # 条件4：整体趋势在慢慢抬高（当前20日线高于15天前的20日线）
        ma20_15_days_ago = last_n.iloc[-15]["ma20"]
        if latest["ma20"] <= ma20_15_days_ago:
            continue

        # 条件5：当前价格必须逼近近期的最高价，呈现随时准备突破的姿态
        max_close = last_n["close"].max()
        if max_close <= 0: continue
        close_to_high_ratio = latest["close"] / max_close * 100
        if close_to_high_ratio < MIN_CLOSE_TO_HIGH:
            continue
            
        # 条件6：强势特征，当前价格必须稳稳踩在5日均线之上
        if latest["close"] < latest["ma5"]:
            continue

        # ---------------- 记录结果 ----------------
        results.append({
            "name": str(latest["name"]),
            "code": str(latest["code"]).zfill(6),
            "symbol": symbol,
            "tightness": float(tightness),
            "max_pct": float(max_pct_in_window),
            "close_to_high": float(close_to_high_ratio),
            "latest_pct_change": float(latest["pct_change"]),
            "latest_close": float(latest["close"]),
            "condition": "均线多头密集/潜伏蓄势"
        })

    if not results: return None

    # 排序：优先按“均线密集度(tightness)”从小到大排（越密集越好），然后按逼近高点比例从大到小排
    results = sorted(results, key=lambda x: (x["tightness"], -x["close_to_high"]))
    top_results = results[:TOP_N]

    print(f"🎯 筛选完成：共命中 {len(results)} 只，截取均线最密集的潜伏 TOP {TOP_N}。")
    return top_results

def get_surge_stocks():
    spot_df = get_all_a_stock_spot_sina()
    if spot_df is None or spot_df.empty: return "ERROR"

    cache_df = load_cache()
    if cache_too_old(cache_df, spot_df["date"].iloc[0]):
        cache_df = rebuild_history_cache_from_sina(spot_df)

    cache_df = update_cache_with_spot(cache_df, spot_df)
    save_cache(cache_df)
    return screen_from_cache(cache_df)


# ================= DeepSeek AI 解析题材 =================
def ask_deepseek(prompt, system_prompt="", temperature=0.65):
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key: return "❌ DeepSeek API Key 未配置。"
    
    url = f"{DEEPSEEK_API_BASE.rstrip('/')}/chat/completions"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": prompt}],
        "temperature": temperature
    }
    
    for _ in range(3):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 200:
                text = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
                if text: return text
        except: time.sleep(2)
    return "❌ AI 题材分析生成失败。"

def ask_deepseek_single_stock_brief(stock, ai_cache):
    cache_key = make_stock_brief_cache_key(stock)
    if cache_key in ai_cache: return ai_cache[cache_key]["text"]

    system_prompt = """你是一位精通A股基本面与技术面结合的【潜伏型】游资专家。
请用大白话简明扼要地解释，绝不要给任何投资建议。如果你不知道某个概念，说明“可能与……有关”，不要捏造假消息。

格式必须严格如下（包含两个小标题）：

**这家公司是做什么的：**
1-2句话说明核心主营业务或所属的热门概念板块。

**资金在潜伏酝酿什么题材：**
1-2句话分析。它目前走势非常平稳且均线密集蓄势，随时可能爆发，请分析近期可能存在的行业催化剂、潜在政策利好，或市场资金偏好它的内在原因。
"""

    user_prompt = f"""股票名称：{stock['name']}（代码：{stock['code']}）
近期走势特征：走势极度平稳（近30天单日最大涨幅仅 {stock['max_pct']:.2f}%），均线呈现极度密集的“老鸭头”多头排列（MA5与MA20偏离度仅 {stock['tightness']:.2f}%），且当前价格非常逼近近期高位，属于典型的“暴涨前夕/蓄势待发”技术形态。

请重点讲清楚：
1. 它的主营业务是什么？
2. 资金在这个位置悄悄托盘吸筹，可能在博弈什么潜在利好逻辑？
字数控制在150字以内。
"""

    text = ask_deepseek(prompt=user_prompt, system_prompt=system_prompt)
    if not text.startswith("❌"):
        ai_cache[cache_key] = {
            "created_at": cn_now().strftime("%Y-%m-%d %H:%M:%S"),
            "text": text
        }
        save_ai_cache(ai_cache)
    return text


# ================= 博客生成 =================
def write_blog_post(stock_list):
    today_date = cn_now().strftime("%Y-%m-%d")
    post_time = cn_now().strftime("%Y-%m-%dT%H:%M:%S+08:00")
    os.makedirs(POST_FOLDER, exist_ok=True)
    
    for old_file in glob.glob(os.path.join(POST_FOLDER, f"{REPORT_PREFIX}-*.md")): os.remove(old_file)

    md = f"""---
title: "🥷 【潜伏雷达】均线密集蓄势，爆发前夕选股扫描 ({today_date})"
date: {post_time}
categories:
    - 量化研报
tags:
    - AI选股
    - 老鸭头
    - 均线多头
    - 潜伏底
    - DeepSeek
draft: false
---

# 🥷 潜伏雷达：均线密集蓄势，爆发前夕选股扫描

本报告由 **Python + 新浪/网易行情接口 + 本地缓存 + DeepSeek AI** 自动生成。

> ⚠️ 风险提示：本文仅为客观的技术面数据统计与AI总结，绝不构成任何投资建议。均线密集虽是启动前兆，但亦有向下破位风险，请结合大盘情绪谨慎对待。

**🎯 选股核心逻辑：寻找“暴涨前夕的潜伏底”。即近期未曾大涨、均线极度密集且多头翘起、股价贴近近期高位蓄势的“老鸭头”标的。**

扫描条件：
- **未曾暴涨**：最近 {LOOKBACK_DAYS} 个交易日内，单日最大涨幅不超过 {MAX_DAILY_PCT}%（剔除已被热炒拉爆的股票）。
- **均线密集多头**：5日线 > 10日线 > 20日线，且5日与20日均线的距离不超过 {MA_TIGHTNESS_PCT}%（距离越小，筹码越集中）。
- **趋势缓慢向上**：20日均线整体斜率为正。
- **高位逼空蓄势**：最新收盘价距离近 {LOOKBACK_DAYS} 天最高价的回撤幅度极小（当前价格 >= 最高价的 {MIN_CLOSE_TO_HIGH}%），且稳站5日均线之上。

---

"""
    if stock_list == "ERROR":
        md += "## ❌ 今日行情数据抓取失败。\n"
    elif not stock_list:
        md += "## ❄️ 今日扫描结果为空。\n\n当前市场可能缺乏横盘蓄势稳健的标的，或者大盘处于单边下跌行情，未能找到符合“均线多头密集”特征的个股。\n\n" + get_random_philosophy()
    else:
        ai_cache = load_ai_cache()
        md += "## 🥇 今日潜伏蓄势标的 TOP 榜（按均线密集度排序）\n\n"
        md += "| 排名 | 股票 | 代码 | 形态描述 | 均线偏离度 | 30日最大涨幅 | 逼近前高比例 | 最新收盘价 |\n"
        md += "|---|---|---|---|---:|---:|---:|---:|\n"
        
        for idx, s in enumerate(stock_list, start=1):
            md += f"| {idx} | **{s['name']}** | {s['code']} | {s['condition']} | {s['tightness']:.2f}% | {s['max_pct']:.2f}% | {s['close_to_high']:.2f}% | {s['latest_close']:.2f} |\n"

        md += "\n---\n\n## 💡 潜伏逻辑剖析与技术走势\n\n"
        
        for idx, s in enumerate(stock_list, start=1):
            md += f"### {idx}. {s['name']}（{s['code']}）\n\n"
            md += f"**形态数据**：5日与20日均线偏离度仅 **{s['tightness']:.2f}%**，筹码极度粘合。近期最高单日涨幅仅 **{s['max_pct']:.2f}%**（未透支），目前股价高达近期最高点的 **{s['close_to_high']:.2f}%**，随时可能突破。\n\n"
            md += get_sina_chart_html(s["symbol"], s["name"])
            md += ask_deepseek_single_stock_brief(s, ai_cache) + "\n\n---\n\n"
            
        md += get_random_philosophy()

    md += f"\n\n*本文由自动化程序于北京时间 {today_date} 自动发布。*"
    
    file_path = os.path.join(POST_FOLDER, f"{REPORT_PREFIX}-{today_date}.md")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"✅ 博客文章已成功生成：{file_path}")

if __name__ == "__main__":
    stock_list = get_surge_stocks()
    write_blog_post(stock_list)
