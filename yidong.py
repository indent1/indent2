import os
import glob
import time
import random
import datetime
import requests
import akshare as ak
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed


# ================= 参数区 =================
LOOKBACK_TRADING_DAYS = 12
SURGE_THRESHOLD = 7.0
MIN_SURGE_TIMES = 3
TOP_N = 50

# 首次建缓存时才会用到新浪历史接口，别太高
MAX_WORKERS = 6

# 拉最近60个自然日，保证覆盖12个交易日
HIST_CALENDAR_DAYS = 60

POST_FOLDER = "content/post"

# 缓存目录：会生成 stock_cache/sina_close_cache.csv
CACHE_FOLDER = "stock_cache"
CACHE_FILE = os.path.join(CACHE_FOLDER, "sina_close_cache.csv")

REPORT_PREFIX = "radar"
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")


# ================= 工具函数 =================
def cn_now():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=8)


def get_market_prefix(code):
    code_str = str(code)
    code_str = (
        code_str
        .replace("sh", "")
        .replace("sz", "")
        .replace("bj", "")
        .strip()
        .zfill(6)
    )

    if code_str.startswith("6"):
        return f"sh{code_str}"
    elif code_str.startswith("0") or code_str.startswith("3"):
        return f"sz{code_str}"
    elif code_str.startswith("4") or code_str.startswith("8"):
        return f"bj{code_str}"

    return f"sh{code_str}"


def get_sina_chart_html(symbol, stock_name):
    """
    生成新浪分时图 + 日K图 HTML。
    symbol 示例：sh600000 / sz300905 / bj430017
    """
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
    """
    如果新浪实时行情没有给交易日期，就用北京时间日期。
    周末则回退到上一个周五，避免生成周末假K线。
    """
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


# ================= 新浪全市场行情 =================
def get_all_a_stock_spot_sina():
    print("📈 正在通过新浪获取A股全市场实时行情...")

    spot_df = None

    for attempt in range(3):
        try:
            spot_df = ak.stock_zh_a_spot()
            if spot_df is not None and not spot_df.empty:
                print("✅ 新浪全市场行情获取成功！")
                break
        except Exception as e:
            print(f"⚠️ 新浪实时行情获取失败，第 {attempt + 1} 次：{str(e)}")
            time.sleep(3)

    if spot_df is None or spot_df.empty:
        return None

    try:
        code_col = [col for col in spot_df.columns if "代码" in col or "symbol" in col.lower()][0]
        name_col = [col for col in spot_df.columns if "名称" in col or "name" in col.lower()][0]

        price_cols = [
            col for col in spot_df.columns
            if "最新" in col or "price" in col.lower() or "trade" in col.lower()
        ]

        if not price_cols:
            print("❌ 新浪实时行情没有找到最新价字段。")
            print(spot_df.columns.tolist())
            return None

        price_col = price_cols[0]

        spot_df[code_col] = spot_df[code_col].astype(str)
        spot_df[name_col] = spot_df[name_col].astype(str)

        spot_df["symbol"] = spot_df[code_col].apply(get_market_prefix)
        spot_df["code"] = spot_df["symbol"].str.extract(r"(\d{6})")
        spot_df["name"] = spot_df[name_col]
        spot_df["close"] = pd.to_numeric(spot_df[price_col], errors="coerce")

        spot_df = spot_df[~spot_df["name"].str.contains("ST|退", regex=True, na=False)].copy()
        spot_df = spot_df[spot_df["close"] > 0].copy()

        date_cols = [col for col in spot_df.columns if "日期" in col or "date" in col.lower()]
        if date_cols:
            date_col = date_cols[0]
            spot_df["date"] = spot_df[date_col].apply(normalize_date)
        else:
            spot_df["date"] = get_safe_market_date()

        result = spot_df[["symbol", "code", "name", "date", "close"]].dropna().copy()
        result = result.drop_duplicates(subset=["symbol"], keep="last")

        print(f"🚀 新浪返回可用股票数量：{len(result)}")
        return result

    except Exception as e:
        print(f"❌ 新浪实时行情清洗失败：{str(e)}")
        print(spot_df.columns.tolist())
        return None


# ================= 缓存读写 =================
def load_cache():
    if not os.path.exists(CACHE_FILE):
        print("🧊 未发现历史缓存，准备首次全量建立缓存。")
        return pd.DataFrame(columns=["symbol", "code", "name", "date", "close"])

    try:
        cache_df = pd.read_csv(CACHE_FILE, dtype={"symbol": str, "code": str})
        cache_df["date"] = cache_df["date"].astype(str)
        cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
        cache_df = cache_df.dropna(subset=["symbol", "date", "close"])
        print(f"🧊 已加载历史缓存：{len(cache_df)} 行。")
        return cache_df

    except Exception as e:
        print(f"⚠️ 历史缓存读取失败，将重建缓存：{str(e)}")
        return pd.DataFrame(columns=["symbol", "code", "name", "date", "close"])


def save_cache(cache_df):
    os.makedirs(CACHE_FOLDER, exist_ok=True)

    cache_df = cache_df.dropna(subset=["symbol", "date", "close"]).copy()
    cache_df["date"] = cache_df["date"].astype(str)
    cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
    cache_df = cache_df.dropna(subset=["close"])

    cache_df = cache_df.sort_values(["symbol", "date"])
    cache_df = cache_df.groupby("symbol", group_keys=False).tail(80)

    cache_df.to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")
    print(f"✅ 历史缓存已保存：{CACHE_FILE}，共 {len(cache_df)} 行。")


def cache_too_old(cache_df, spot_trade_date):
    if cache_df.empty:
        return True

    try:
        latest_cache_date = pd.to_datetime(cache_df["date"]).max()
        spot_date = pd.to_datetime(spot_trade_date)
        gap_days = (spot_date - latest_cache_date).days

        return gap_days > 6

    except Exception:
        return True


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

        if "date" not in hist_df.columns or "close" not in hist_df.columns:
            return []

        hist_df = hist_df[["date", "close"]].copy()
        hist_df["date"] = hist_df["date"].apply(normalize_date)
        hist_df["close"] = pd.to_numeric(hist_df["close"], errors="coerce")
        hist_df = hist_df.dropna(subset=["close"])

        rows = []

        for _, h in hist_df.iterrows():
            rows.append({
                "symbol": row["symbol"],
                "code": row["code"],
                "name": row["name"],
                "date": h["date"],
                "close": float(h["close"])
            })

        return rows

    except Exception:
        return []


def rebuild_history_cache_from_sina(spot_df):
    start_date, end_date = get_date_range()

    print("🧱 开始通过新浪历史K线重建缓存。")
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
        return pd.DataFrame(columns=["symbol", "code", "name", "date", "close"])

    cache_df = pd.DataFrame(rows)
    cache_df = cache_df.drop_duplicates(subset=["symbol", "date"], keep="last")

    print(f"✅ 历史缓存重建完成，共 {len(cache_df)} 行。")
    return cache_df


def update_cache_with_spot(cache_df, spot_df):
    if spot_df is None or spot_df.empty:
        return cache_df

    spot_rows = spot_df[["symbol", "code", "name", "date", "close"]].copy()
    spot_rows["date"] = spot_rows["date"].astype(str)
    spot_rows["close"] = pd.to_numeric(spot_rows["close"], errors="coerce")
    spot_rows = spot_rows.dropna(subset=["close"])

    if cache_df.empty:
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

    print(f"✅ 已用新浪实时行情更新缓存，当前缓存 {len(updated)} 行。")
    return updated


# ================= 核心筛选：12交易日内3次涨幅 > 7% =================
def screen_from_cache(cache_df):
    print("🧮 正在从本地缓存中执行量化筛选...")

    results = []

    cache_df = cache_df.copy()
    cache_df["date"] = cache_df["date"].astype(str)
    cache_df["close"] = pd.to_numeric(cache_df["close"], errors="coerce")
    cache_df = cache_df.dropna(subset=["symbol", "date", "close"])
    cache_df = cache_df.sort_values(["symbol", "date"])

    for symbol, group in cache_df.groupby("symbol"):
        group = group.drop_duplicates(subset=["date"], keep="last").sort_values("date")

        if len(group) < LOOKBACK_TRADING_DAYS + 1:
            continue

        recent = group.tail(LOOKBACK_TRADING_DAYS + 1).copy()
        recent["daily_change"] = recent["close"].pct_change() * 100

        check_df = recent.tail(LOOKBACK_TRADING_DAYS).copy()
        check_df["daily_change"] = check_df["daily_change"].fillna(0)

        count_surge = int((check_df["daily_change"] > SURGE_THRESHOLD).sum())

        if count_surge < MIN_SURGE_TIMES:
            continue

        close_start = float(recent.iloc[0]["close"])
        close_latest = float(check_df.iloc[-1]["close"])

        if close_start <= 0:
            continue

        total_change = (close_latest - close_start) / close_start * 100

        surge_days_detail = []

        for _, row in check_df.iterrows():
            day_change = float(row["daily_change"])
            if day_change > SURGE_THRESHOLD:
                surge_days_detail.append(f"{row['date']} 涨幅 {day_change:.2f}%")

        latest_row = check_df.iloc[-1]

        results.append({
            "name": str(latest_row["name"]),
            "code": str(latest_row["code"]).zfill(6),
            "symbol": symbol,
            "times": count_surge,
            "total_change": total_change,
            "latest_close": close_latest,
            "surge_days_detail": surge_days_detail
        })

    if not results:
        print("今日未筛选到符合条件的股票。")
        return None

    results = sorted(results, key=lambda x: x["total_change"], reverse=True)
    top_results = results[:TOP_N]

    print(f"🎯 筛选完成：共命中 {len(results)} 只，截取 TOP {TOP_N}。")

    for item in top_results:
        print(
            f"✅ {item['name']}({item['code']}) "
            f"{LOOKBACK_TRADING_DAYS}日内 {item['times']} 次 > {SURGE_THRESHOLD}% ，"
            f"区间涨幅 {item['total_change']:.2f}%"
        )

    return top_results


def get_surge_stocks():
    spot_df = get_all_a_stock_spot_sina()

    if spot_df is None or spot_df.empty:
        return "ERROR"

    cache_df = load_cache()

    spot_trade_date = str(spot_df["date"].iloc[0])

    if cache_too_old(cache_df, spot_trade_date):
        print("⚠️ 缓存为空或过旧，将全量重建。首次运行会比较慢。")
        cache_df = rebuild_history_cache_from_sina(spot_df)

    cache_df = update_cache_with_spot(cache_df, spot_df)
    save_cache(cache_df)

    return screen_from_cache(cache_df)


# ================= Gemini =================
def ask_gemini(prompt, system_prompt="", temperature=0.35, timeout=180):
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")

    if not api_key:
        return "❌ Gemini API Key 未配置。请在 GitHub Secrets 中添加 GEMINI_API_KEY。"

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key
    }

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": prompt
                    }
                ]
            }
        ],
        "generationConfig": {
            "temperature": temperature
        }
    }

    if system_prompt:
        payload["systemInstruction"] = {
            "parts": [
                {
                    "text": system_prompt
                }
            ]
        }

    for i in range(3):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=timeout)

            if response.status_code != 200:
                print(f"❌ Gemini HTTP错误：{response.status_code}")
                print(response.text)
                time.sleep(2)
                continue

            data = response.json()
            candidates = data.get("candidates", [])

            if not candidates:
                print("❌ Gemini 没有返回 candidates。")
                print(data)
                time.sleep(2)
                continue

            parts = candidates[0].get("content", {}).get("parts", [])

            if not parts:
                print("❌ Gemini 没有返回正文 parts。")
                print(data)
                time.sleep(2)
                continue

            text = parts[0].get("text", "").strip()

            if text:
                return text

            time.sleep(2)

        except Exception as e:
            print(f"❌ Gemini 请求失败，第 {i + 1} 次：{str(e)}")
            time.sleep(2)

    return "❌ AI 分析生成失败。"


def ask_gemini_single_stock_brief(stock):
    """
    只生成单只股票的通俗解读。
    不再生成“全市场异动扫描结果”“总体结论”等大段内容。
    """
    detail_text = "；".join(stock["surge_days_detail"])

    system_prompt = """
你是一位严谨的A股市场研究员。
请用通俗易懂的大白话解释股票。

你必须严格按照下面格式输出：

**这家公司是做什么的：**
用5-7句话说明主营业务、产品、客户或所处行业。尽量大白话，不要堆术语。

**这波为什么会涨：**
用2-4条 bullet 分析可能原因，比如题材催化、资金风格、行业消息、业绩预期、政策方向、市场情绪等。
如果你不确定，要写“可能与……有关”，不要装作确定。
"""

    user_prompt = f"""
请分析这只股票：

股票名称：{stock['name']}
股票代码：{stock['code']}
筛选条件：最近{LOOKBACK_TRADING_DAYS}个交易日内出现 {stock['times']} 次单日涨幅超过 {SURGE_THRESHOLD}%
区间总涨幅：{stock['total_change']:.2f}%
最新收盘价：{stock['latest_close']:.2f}
异动日期：{detail_text}

请重点讲清楚：
1. 这家公司是做什么的。
2. 它这波上涨可能是什么原因。

"""

    print(f"🤖 Gemini 正在生成个股解读：{stock['name']}({stock['code']})")

    return ask_gemini(
        prompt=user_prompt,
        system_prompt=system_prompt,
        temperature=0.35,
        timeout=120
    )


# ================= 写 Hugo 博客 =================
def write_blog_post(stock_list):
    today_date = cn_now().strftime("%Y-%m-%d")
    post_time = cn_now().strftime("%Y-%m-%dT%H:%M:%S+08:00")

    os.makedirs(POST_FOLDER, exist_ok=True)

    for old_file in glob.glob(os.path.join(POST_FOLDER, f"{REPORT_PREFIX}-*.md")):
        os.remove(old_file)

    md_content = f"""---
title: "🚀 【全市场雷达】12日内3次暴涨异动股扫描 ({today_date})"
date: {post_time}
categories:
    - 量化研报
tags:
    - AI选股
    - 全市场扫描
    - 新浪行情
    - Gemini
draft: false
---

# 🚀 全市场雷达：12日内3次暴涨异动股扫描

本报告由 **Python + 新浪行情接口 + 本地K线缓存 + Gemini AI** 自动生成。

扫描条件：

- 股票范围：A股全市场，剔除 ST、退市、停牌无价格标的
- 时间窗口：最近 **{LOOKBACK_TRADING_DAYS}** 个交易日
- 异动标准：至少 **{MIN_SURGE_TIMES}** 次单日涨幅大于 **{SURGE_THRESHOLD}%**
- 排名方式：按最近区间总涨幅排序，截取 TOP {TOP_N}
- 数据来源：新浪行情接口
- AI模型：{GEMINI_MODEL}

---

"""

    if stock_list == "ERROR":
        md_content += """
## 今日扫描结果

今日新浪数据抓取失败，未能完成全市场扫描。

可能原因包括：

- 新浪接口临时不可用
- GitHub Actions 海外网络异常
- AkShare 接口字段变化
- 请求频率过高被临时限制

---

"""

    elif stock_list is None:
        md_content += f"""
## 今日扫描结果

经过全市场扫描，最近 {LOOKBACK_TRADING_DAYS} 个交易日内，暂时没有股票满足：

> 至少 {MIN_SURGE_TIMES} 次单日涨幅大于 {SURGE_THRESHOLD}%

这通常说明短线极端活跃标的较少，市场资金可能处于分散或休整状态。

---

{get_random_philosophy()}

---

"""

    else:
        md_content += "## 今日命中的 TOP 活跃股票\n\n"
        md_content += "| 排名 | 股票 | 代码 | 异动次数 | 区间总涨幅 | 最新收盘价 | 异动日期 |\n"
        md_content += "|---|---|---|---:|---:|---:|---|\n"

        for idx, s in enumerate(stock_list, start=1):
            detail_text = "<br>".join(s["surge_days_detail"])
            md_content += (
                f"| {idx} | {s['name']} | {s['code']} | "
                f"{s['times']} | {s['total_change']:.2f}% | "
                f"{s['latest_close']:.2f} | {detail_text} |\n"
            )

        md_content += "\n---\n\n"
        md_content += "## 个股行情与通俗解读\n\n"

        for idx, s in enumerate(stock_list, start=1):
            md_content += f"### {idx}. {s['name']}（{s['code']}）\n\n"

            md_content += (
                f"**异动数据**：最近 {LOOKBACK_TRADING_DAYS} 个交易日内，"
                f"出现 **{s['times']}** 次单日涨幅大于 **{SURGE_THRESHOLD}%**；"
                f"区间总涨幅 **{s['total_change']:.2f}%**；"
                f"最新收盘价 **{s['latest_close']:.2f}**。\n\n"
            )

            md_content += get_sina_chart_html(s["symbol"], s["name"])

            stock_brief = ask_gemini_single_stock_brief(s)
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
