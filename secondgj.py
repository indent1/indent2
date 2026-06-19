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
# 选股策略参数：强势接力/龙回头模型
WINDOW_DAYS = 10              # 考察的交易日窗口
MIN_LIMIT_UP_COUNT = 1        # 窗口内最少涨停次数(单日涨跌幅 >= 9.5% 视作涨停)
MAX_LIMIT_UP_COUNT = 4        # 窗口内最多涨停次数(防止连续一字板极度透支)

MIN_TOTAL_CHANGE = 15.0       # 窗口内最低累计涨幅(%)
MAX_TOTAL_CHANGE = 55.0       # 窗口内最高累计涨幅(%)

MAX_DRAWDOWN = 12.0           # 最新价距离窗口内最高价的最大回撤(%)，越小越抗跌
MIN_LATEST_PCT_CHANGE = -5.0  # 最新一日跌幅不能超过5%(规避恐慌跌停)

TOP_N = 25

# 并发配置
MAX_WORKERS = 6
HIST_CALENDAR_DAYS = 60

# 目录与前缀配置
POST_FOLDER = "content/post"
CACHE_FOLDER = "stock_cache"
CACHE_FILE = os.path.join(CACHE_FOLDER, "sina_ohlc_cache.csv")
REPORT_PREFIX = "focusk"

# DeepSeek 配置
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash")
DEEPSEEK_API_BASE = os.environ.get("DEEPSEEK_API_BASE", "https://api.deepseek.com")
DEEPSEEK_THINKING = os.environ.get("DEEPSEEK_THINKING", "disabled").strip().lower()

# AI 个股解读缓存
AI_CACHE_FOLDER = "stock_cache"
AI_CACHE_FILE = os.path.join(AI_CACHE_FOLDER, "deepseek_focus_stock_brief_cache.json")
AI_CACHE_VERSION = "focus_stock_brief_v1"
AI_CACHE_KEEP_DAYS = 180


# ================= 工具函数 =================
def cn_now():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(hours=8)

def clean_stock_code(code):
    text = str(code).lower()
    text = text.replace("sh", "").replace("sz", "").replace("bj", "").replace(".0", "").strip()
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
    return "> 💡 **游资信条**：*“买入分歧，卖出一致；顺势而为，方得始终。”*"

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

    providers = [
        ("新浪", lambda: ak.stock_zh_a_spot()),
        ("网易", lambda: ak.stock_zh_a_spot_netease()),
    ]

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

                if not code_col or not name_col or not close_col or not open_col:
                    continue

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
                if date_col:
                    spot_df["date"] = spot_df[date_col].apply(normalize_date)
                else:
                    spot_df["date"] = get_safe_market_date()

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
        # 注意：这里改成了 qfq (前复权)，以保证隔日涨跌幅计算不受除权除息影响！
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
    except:
        return {}

def save_ai_cache(cache_data):
    try:
        os.makedirs(AI_CACHE_FOLDER, exist_ok=True)
        # 清理超期缓存
        cutoff = cn_now() - datetime.timedelta(days=AI_CACHE_KEEP_DAYS)
        new_cache = {}
        for k, v in cache_data.items():
            dt = datetime.datetime.strptime(v.get("created_at", "2000-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            if dt >= cutoff: new_cache[k] = v
                
        with open(AI_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(new_cache, f, ensure_ascii=False, indent=2)
    except:
        pass

def make_stock_brief_cache_key(stock):
    payload = {
        "report_prefix": REPORT_PREFIX,
        "cache_version": AI_CACHE_VERSION,
        "code": stock["code"],
        "limit_up_count": stock["limit_up_count"],
        "total_change": round(stock["total_change"], 2),
        "drawdown": round(stock["drawdown"], 2)
    }
    return hashlib.md5(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


# ================= 核心量化筛选：强势回龙头接力模型 =================
def screen_from_cache(cache_df):
    print("🧮 正在执行【短线资金焦点/强势股】量化筛选...")
    results = []

    if cache_df is None or cache_df.empty: return None

    cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
    cache_df = cache_df.dropna(subset=["symbol", "date", "close"]).sort_values(["symbol", "date"])

    for symbol, group in cache_df.groupby("symbol"):
        group = group.drop_duplicates(subset=["date"], keep="last").sort_values("date")
        
        # 多取一天用来计算第一天的涨幅
        if len(group) < WINDOW_DAYS + 1:
            continue

        last_n_plus_1 = group.tail(WINDOW_DAYS + 1).copy()
        last_n_plus_1["pre_close"] = last_n_plus_1["close"].shift(1)
        
        # 截取最后的 WINDOW_DAYS 天数据
        last_n = last_n_plus_1.dropna(subset=["pre_close"]).copy()
        if len(last_n) < WINDOW_DAYS: continue

        # 计算每日涨跌幅
        last_n["pct_change"] = (last_n["close"] - last_n["pre_close"]) / last_n["pre_close"] * 100
        
        # 统计涨停板数量 (涨幅 >= 9.5%)
        limit_up_count = (last_n["pct_change"] >= 9.5).sum()
        
        # 条件1：涨停数量限制（必须有涨停激活股性，但排除一字连板极高位股）
        if limit_up_count < MIN_LIMIT_UP_COUNT or limit_up_count > MAX_LIMIT_UP_COUNT:
            continue
            
        # 条件2：累计涨幅适中
        first_pre_close = last_n.iloc[0]["pre_close"]
        latest_close = last_n.iloc[-1]["close"]
        if first_pre_close <= 0: continue
            
        total_change = (latest_close - first_pre_close) / first_pre_close * 100
        if total_change < MIN_TOTAL_CHANGE or total_change > MAX_TOTAL_CHANGE:
            continue
            
        # 条件3：高位抗跌，回撤较小
        max_close = last_n["close"].max()
        drawdown = (max_close - latest_close) / max_close * 100
        if drawdown > MAX_DRAWDOWN:
            continue
            
        # 条件4：最近一日没有恐慌性抛售
        latest_pct_change = last_n.iloc[-1]["pct_change"]
        if latest_pct_change < MIN_LATEST_PCT_CHANGE:
            continue

        limit_up_dates = last_n[last_n["pct_change"] >= 9.5]["date"].tolist()

        results.append({
            "name": str(last_n.iloc[-1]["name"]),
            "code": str(last_n.iloc[-1]["code"]).zfill(6),
            "symbol": symbol,
            "limit_up_count": int(limit_up_count),
            "total_change": float(total_change),
            "drawdown": float(drawdown),
            "latest_pct_change": float(latest_pct_change),
            "latest_close": float(latest_close),
            "limit_up_dates": limit_up_dates,
            "condition": f"{WINDOW_DAYS}日{limit_up_count}板"
        })

    if not results: return None

    # 排序：优先按涨停数量降序(股性越活越前)，其次按区间涨幅降序
    results = sorted(results, key=lambda x: (x["limit_up_count"], x["total_change"]), reverse=True)
    top_results = results[:TOP_N]

    print(f"🎯 筛选完成：共命中 {len(results)} 只，截取热度最高 TOP {TOP_N}。")
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

    system_prompt = """你是一位实战派的A股短线游资选手。
请用大白话简明扼要地解释，绝不要给任何投资建议。
如果你不知道某个概念，必须说明“可能与……有关”，不要捏造假消息。

格式必须严格如下（包含两个小标题）：

**这家公司是做什么的：**
1-2句话说明核心主营业务或所属概念板块。

**资金为什么在炒它：**
1-2句话说明这只股票近期爆发的原因。分析可能涉及的政策利好、行业热点、同类龙头带动效应，或市场资金偏好的题材逻辑。
"""

    dates_str = "、".join(stock['limit_up_dates'])
    user_prompt = f"""股票名称：{stock['name']}（代码：{stock['code']}）
近期走势特征：最近10个交易日内爆发了 {stock['limit_up_count']} 次涨停板，区间累计拉升 {stock['total_change']:.2f}%，目前距离近期最高价仅回撤 {stock['drawdown']:.2f}%，在高位非常抗跌。
涨停爆发日：{dates_str}

请重点讲清楚：
1. 它的主营业务及热门题材标签是什么？
2. 近期短线活跃资金到底在它身上博弈什么逻辑/利好？
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
title: "🔥 【市场焦点雷达】短线异动与强势接力股扫描 ({today_date})"
date: {post_time}
categories:
    - 量化研报
tags:
    - AI选股
    - 强势股
    - 涨停分析
    - DeepSeek
draft: false
---

# 🔥 市场焦点雷达：短线异动与强势接力股扫描

本报告由 **Python + 新浪/网易行情接口 + 本地缓存 + DeepSeek AI** 自动生成。

> ⚠️ 风险提示：本文仅为客观的数据统计与AI总结，绝不构成任何投资建议。短线博弈波动巨大，极易出现回撤风险，请谨慎对待。

**🎯 选股核心逻辑：寻找“短线游资强力拉升介入，且当前维持高位横盘抗跌”的焦点标的（即经典的接力/龙回头模型）。**

扫描条件：
- **资金强介入**：最近 {WINDOW_DAYS} 个交易日内，至少出现过 {MIN_LIMIT_UP_COUNT} 次涨停（单日涨跌幅 > 9.5%）。
- **涨幅未透支**：近 {WINDOW_DAYS} 天累计涨幅介于 {MIN_TOTAL_CHANGE}% ~ {MAX_TOTAL_CHANGE}% 之间（排除连续一字板的极端妖股）。
- **高位抗跌蓄势**：最新收盘价距离近期最高价的回撤不超过 {MAX_DRAWDOWN}%。
- **近期无恐慌跌停**：最新一个交易日单日跌幅不超过 {-MIN_LATEST_PCT_CHANGE}%。

---

"""
    if stock_list == "ERROR":
        md += "## ❌ 今日行情数据抓取失败。\n"
    elif not stock_list:
        md += "## ❄️ 今日扫描结果为空。\n\n当前市场可能正处于严重分歧或全面退潮期，未能找到符合高位抗跌强势接力特征的个股。多看少动为主。\n\n" + get_random_philosophy()
    else:
        ai_cache = load_ai_cache()
        md += "## 🥇 今日强势焦点标的 TOP 榜\n\n"
        md += "| 排名 | 股票 | 代码 | 近期爆发情况 | 10日累计涨幅 | 高点回撤 | 今日涨跌幅 | 最新收盘价 |\n"
        md += "|---|---|---|---|---:|---:|---:|---:|\n"
        
        for idx, s in enumerate(stock_list, start=1):
            md += f"| {idx} | **{s['name']}** | {s['code']} | {s['condition']} | {s['total_change']:.2f}% | {s['drawdown']:.2f}% | {s['latest_pct_change']:.2f}% | {s['latest_close']:.2f} |\n"

        md += "\n---\n\n## 💡 资金逻辑剖析与技术走势\n\n"
        
        for idx, s in enumerate(stock_list, start=1):
            md += f"### {idx}. {s['name']}（{s['code']}）\n\n"
            md += f"**爆发异动节点**：在 {', '.join(s['limit_up_dates'])} 录得涨停板。区间涨幅 **{s['total_change']:.2f}%**，今日收盘表现为 **{s['latest_pct_change']:.2f}%**，目前回撤 **{s['drawdown']:.2f}%**，筹码锁定良好。\n\n"
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
