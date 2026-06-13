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


# ================= 参数区：以后主要改这里 =================
LOOKBACK_TRADING_DAYS = 12          # 最近12个交易日
SURGE_THRESHOLD = 7.0               # 单日涨幅大于7%
MIN_SURGE_TIMES = 3                 # 至少出现3次
TOP_N = 10                          # 最终给AI分析前10名

# 全市场逐只拉历史K线，别开太高，海外IP建议 3~4
MAX_WORKERS = 3

# 拉最近45个自然日，足够覆盖12个交易日
HIST_CALENDAR_DAYS = 45

# Hugo文章目录
POST_FOLDER = "content/post"

REPORT_PREFIX = "12天异动3次"

# DeepSeek 配置
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash")
DEEPSEEK_API_BASE = os.environ.get("DEEPSEEK_API_BASE", "https://api.deepseek.com")
DEEPSEEK_THINKING = os.environ.get("DEEPSEEK_THINKING", "disabled").strip().lower()

# AI 个股解读缓存
AI_CACHE_FOLDER = "stock_cache"
AI_CACHE_FILE = os.path.join(AI_CACHE_FOLDER, "deepseek_12surge_stock_brief_cache.json")

# 改 prompt 时手动改这个版本号，避免继续使用旧口径缓存
AI_CACHE_VERSION = "deepseek_12surge_stock_brief_v1"

# AI 缓存最多保留多少天，防止长期无限增长
AI_CACHE_KEEP_DAYS = 180


# ================= 工具函数：北京时间 =================
def cn_now():
    """
    返回北京时间，避免 datetime.utcnow() 的 DeprecationWarning。
    保持 naive datetime，避免和旧逻辑比较时报错。
    """
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(hours=8)


# ================= 工具函数：获取日期区间 =================
def get_date_range():
    end_date = cn_now().strftime("%Y%m%d")
    start_date = (cn_now() - datetime.timedelta(days=HIST_CALENDAR_DAYS)).strftime("%Y%m%d")
    return start_date, end_date


# ================= 工具函数：清洗股票代码 =================
def clean_stock_code(code):
    """
    把 sh600000、sz000001、bj430047、600000、600000.0 等格式统一成 6 位纯数字代码。
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


# ================= 工具函数：识别新浪市场前缀 =================
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


# ================= 工具函数：新浪走势图 =================
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


# ================= 工具函数：日期标准化 =================
def normalize_date(value):
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d")
    except Exception:
        return cn_now().strftime("%Y-%m-%d")


# ================= 工具函数：字段查找 =================
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


# ================= 工具函数：对接“一言” API，获取哲学盲盒 =================
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


# ================= 核心1-1：清洗全市场股票名单 =================
def normalize_stock_list_df(spot_df, source_name):
    if spot_df is None or spot_df.empty:
        return None

    try:
        code_col = find_column(spot_df.columns, ["代码", "symbol", "code"])
        name_col = find_column(spot_df.columns, ["名称", "name"])

        if not code_col or not name_col:
            print(f"❌ {source_name} 全市场名单缺少关键字段。")
            print("当前字段：")
            print(spot_df.columns.tolist())
            return None

        df = spot_df.copy()

        df["纯数字代码"] = df[code_col].apply(clean_stock_code)
        df = df.dropna(subset=["纯数字代码"]).copy()

        df["市场代码"] = df["纯数字代码"].apply(get_market_prefix)
        df[name_col] = df[name_col].astype(str)

        # 剔除 ST、*ST、退市股
        df = df[~df[name_col].str.contains(r"\*?ST|退", regex=True, na=False)].copy()

        # 如果有最新价字段，剔除停牌或无价格股票
        price_col = find_column(
            df.columns,
            ["最新价", "最新", "现价", "收盘", "price", "trade"]
        )

        if price_col:
            df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
            df = df[df[price_col] > 0].copy()

        df = df.dropna(subset=["市场代码", "纯数字代码"]).copy()
        df = df.drop_duplicates(subset=["市场代码"], keep="last")

        all_symbols = df["市场代码"].dropna().unique().tolist()
        name_dict = dict(zip(df["市场代码"], df[name_col]))
        pure_code_dict = dict(zip(df["市场代码"], df["纯数字代码"]))

        if not all_symbols:
            print(f"⚠️ {source_name} 清洗后股票名单为空。")
            return None

        print(f"🚀 {source_name} 返回全市场名单，剔除ST/退市/无价格后共计 {len(all_symbols)} 只股票。")
        return all_symbols, name_dict, pure_code_dict

    except Exception as e:
        print(f"❌ {source_name} 名单清洗失败：{str(e)}")
        print("当前字段：")
        print(spot_df.columns.tolist())
        return None


# ================= 核心1-1：按A代码接口获取全市场股票名单 =================
def get_all_a_stock_list_sina():
    """
    全市场名单使用新浪 ak.stock_zh_a_spot()；
    网易 ak.stock_zh_a_spot_netease() 作为兜底。
    """
    print("📈 正在获取A股全市场最新名单：新浪优先，网易兜底...")

    providers = [
        ("新浪", lambda: ak.stock_zh_a_spot()),
        ("网易", lambda: ak.stock_zh_a_spot_netease()),
    ]

    for source_name, fetcher in providers:
        for attempt in range(3):
            try:
                print(f"🔎 尝试获取 {source_name} 全市场名单，第 {attempt + 1} 次...")

                raw_df = fetcher()
                result = normalize_stock_list_df(raw_df, source_name)

                if result is not None:
                    print(f"✅ {source_name} 全市场名单获取成功！")
                    return result

                print(f"⚠️ {source_name} 返回为空或字段异常。")
                time.sleep(2 + attempt * 2)

            except AttributeError as e:
                print(f"⚠️ 当前 AkShare 版本可能没有 {source_name} 接口：{str(e)}")
                break

            except Exception as e:
                print(f"⚠️ {source_name} 全市场名单获取失败，第 {attempt + 1} 次：{str(e)}")
                time.sleep(3 + attempt * 2)

    print("❌ 新浪和网易全市场名单均获取失败。")
    return None


# ================= 核心1-2：历史K线标准化 =================
def normalize_hist_df(hist_df):
    if hist_df is None or hist_df.empty:
        return None

    date_col = find_column(hist_df.columns, ["日期", "date"])
    close_col = find_column(hist_df.columns, ["收盘", "close"])

    if not date_col or not close_col:
        return None

    df = hist_df[[date_col, close_col]].copy()
    df.columns = ["date", "close"]

    df["date"] = df["date"].apply(normalize_date)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"])
    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date")

    if df.empty:
        return None

    return df


# ================= 核心1-2：扫描单只股票的最近12个交易日 =================
def scan_one_stock_sina(symbol, name_dict, pure_code_dict, start_date, end_date):
    """
    历史K线使用新浪 ak.stock_zh_a_daily()。
    """
    code = pure_code_dict.get(symbol, clean_stock_code(symbol))
    name = name_dict.get(symbol, "未知名称")

    if not code:
        return None

    hist_df = None

    try:
        time.sleep(random.uniform(0.08, 0.25))

        raw_hist_df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )

        hist_df = normalize_hist_df(raw_hist_df)

    except Exception as e:
        print(f"⚠️ 新浪历史K线失败：{name}({symbol})，{str(e)}")
        return None

    try:
        if hist_df is None or hist_df.empty:
            return None

        # 计算最近12个交易日涨幅，需要至少13根K线
        if len(hist_df) < LOOKBACK_TRADING_DAYS + 1:
            return None

        recent_df = hist_df.tail(LOOKBACK_TRADING_DAYS + 1).copy()

        # 用收盘价计算单日涨幅
        recent_df["单日涨幅"] = recent_df["close"].pct_change() * 100

        # 去掉第一根，只统计最近12个交易日
        check_df = recent_df.tail(LOOKBACK_TRADING_DAYS).copy()
        check_df["单日涨幅"] = check_df["单日涨幅"].fillna(0)

        count_surge_days = int((check_df["单日涨幅"] > SURGE_THRESHOLD).sum())

        if count_surge_days < MIN_SURGE_TIMES:
            return None

        close_latest = float(check_df.iloc[-1]["close"])
        close_start = float(recent_df.iloc[0]["close"])

        if close_start <= 0:
            return None

        total_change = (close_latest - close_start) / close_start * 100

        surge_days_detail = []

        for _, row in check_df.iterrows():
            day_change = float(row["单日涨幅"])

            if day_change > SURGE_THRESHOLD:
                surge_days_detail.append(
                    f"{row['date']} 涨幅 {day_change:.2f}%"
                )

        return {
            "name": name,
            "code": code,
            "symbol": symbol,
            "times": count_surge_days,
            "total_change": total_change,
            "latest_close": close_latest,
            "surge_days_detail": surge_days_detail
        }

    except Exception as e:
        print(f"⚠️ 扫描单股失败：{name}({code})，{str(e)}")
        return None


# ================= 核心1-3：全市场量化扫描 =================
def get_pattern_surge_stocks_all_market():
    stock_info = get_all_a_stock_list_sina()

    if stock_info is None:
        return "ERROR"

    all_symbols, name_dict, pure_code_dict = stock_info
    total_stocks = len(all_symbols)

    start_date, end_date = get_date_range()

    print(f"⏳ 开始扫描最近 {HIST_CALENDAR_DAYS} 个自然日K线。")
    print(f"🎯 条件：最近 {LOOKBACK_TRADING_DAYS} 个交易日内，至少 {MIN_SURGE_TIMES} 次单日涨幅 > {SURGE_THRESHOLD}%。")
    print(f"🚀 当前并发线程数：{MAX_WORKERS}")
    print(f"📅 数据区间：{start_date} ~ {end_date}")
    print("📡 历史K线接口：新浪 stock_zh_a_daily。")

    surge_list_data = []
    finished = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                scan_one_stock_sina,
                symbol,
                name_dict,
                pure_code_dict,
                start_date,
                end_date
            ): symbol
            for symbol in all_symbols
        }

        for future in as_completed(futures):
            finished += 1

            if finished % 100 == 0:
                print(f"🔄 扫描进度：{finished} / {total_stocks}，当前命中：{len(surge_list_data)}")

            result = future.result()

            if result is not None:
                surge_list_data.append(result)

                print(
                    f"✅ 命中：{result['name']}({result['code']}) "
                    f"{LOOKBACK_TRADING_DAYS}日内 {result['times']} 次 > {SURGE_THRESHOLD}% ，"
                    f"区间涨幅 {result['total_change']:.2f}%"
                )

    if not surge_list_data:
        return None

    surge_list_data = sorted(
        surge_list_data,
        key=lambda x: x["total_change"],
        reverse=True
    )

    top_stocks = surge_list_data[:TOP_N]

    print(
        f"🎯 扫描完毕！全市场共选出 {len(surge_list_data)} 只符合条件的股票，"
        f"已截取最强 TOP {TOP_N} 准备提交 DeepSeek 分析。"
    )

    return top_stocks


# 这个函数名保留给 GitHub Actions 自动识别脚本用
def get_surge_stocks():
    return get_pattern_surge_stocks_all_market()


# ================= DeepSeek 缓存读写 =================
def load_ai_cache():
    if not os.path.exists(AI_CACHE_FILE):
        return {}

    try:
        with open(AI_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict):
            print(f"💾 已加载 DeepSeek 12日异动个股解读缓存：{len(data)} 条。")
            return data

        return {}

    except Exception as e:
        print(f"⚠️ DeepSeek 个股解读缓存读取失败，将重新创建：{str(e)}")
        return {}


def save_ai_cache(cache_data):
    try:
        os.makedirs(AI_CACHE_FOLDER, exist_ok=True)

        cache_data = prune_ai_cache(cache_data)

        with open(AI_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

        print(f"✅ DeepSeek 个股解读缓存已保存：{AI_CACHE_FILE}，共 {len(cache_data)} 条。")

    except Exception as e:
        print(f"⚠️ DeepSeek 个股解读缓存保存失败：{str(e)}")


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
        "lookback_trading_days": LOOKBACK_TRADING_DAYS,
        "surge_threshold": SURGE_THRESHOLD,
        "min_surge_times": MIN_SURGE_TIMES,
        "code": str(stock.get("code", "")).zfill(6),
        "name": str(stock.get("name", "")),
        "times": int(stock.get("times", 0)),
        "total_change": round(float(stock.get("total_change", 0)), 2),
        "latest_close": round(float(stock.get("latest_close", 0)), 2),
        "surge_days_detail": stock.get("surge_days_detail", [])
    }

    raw = json.dumps(cache_payload, ensure_ascii=False, sort_keys=True)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ================= DeepSeek API 通用请求函数 =================
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


# ================= DeepSeek 单只股票解析，带缓存 =================
def ask_deepseek_single_stock_brief(stock, ai_cache=None):
    if ai_cache is None:
        ai_cache = load_ai_cache()

    cache_key = make_stock_brief_cache_key(stock)
    cached_item = ai_cache.get(cache_key)

    if cached_item and cached_item.get("text"):
        print(f"💾 命中 DeepSeek 个股解读缓存：{stock['name']}({stock['code']})")
        return cached_item["text"]

    detail_text = "；".join(stock["surge_days_detail"])

    system_prompt = f"""你是一位严谨的A股市场研究员和量化复盘写作者。
请用通俗易懂的大白话解释股票，不要写投资建议，不要承诺上涨。
如果你无法确定某个原因，必须写“可能与……有关”，不要装作确定。
避免使用“必涨”“确定上涨”“强烈推荐”“可以买入”“建议买入”等表述。

这只股票是通过机器条件筛选出来的：
最近 {LOOKBACK_TRADING_DAYS} 个交易日内，至少出现 {MIN_SURGE_TIMES} 次单日涨幅超过 {SURGE_THRESHOLD}%。

你必须严格按照下面格式输出：

**核心概念：**
用1-2句话说明这家公司主营业务、所处行业、市场概念，尽量大白话。

**资金逻辑：**
用1-2条 bullet 分析资金可能关注它的原因，比如题材催化、政策方向、业绩预期、行业情绪、资金偏好等。

**风险提示：**
用1-2句话提示追高、回撤、题材退潮、基本面不匹配等风险。
"""

    user_prompt = f"""请分析这只股票：

股票名称：{stock['name']}
股票代码：{stock['code']}
最近{LOOKBACK_TRADING_DAYS}个交易日内涨幅超过{SURGE_THRESHOLD}%的次数：{stock['times']}次
区间总涨幅：{stock['total_change']:.2f}%
最新收盘价：{stock['latest_close']:.2f}
异动日期：{detail_text}

请重点讲清楚：
1. 这家公司大概是做什么的。
2. 为什么最近会出现多次大涨，资金可能在炒什么。
3. 这种短线异动有什么风险。

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
            "lookback_trading_days": LOOKBACK_TRADING_DAYS,
            "surge_threshold": SURGE_THRESHOLD,
            "min_surge_times": MIN_SURGE_TIMES,
            "times": int(stock["times"]),
            "total_change": round(float(stock["total_change"]), 2),
            "latest_close": round(float(stock["latest_close"]), 2),
            "surge_days_detail": stock["surge_days_detail"],
            "text": text
        }

        save_ai_cache(ai_cache)

    return text


# ================= 核心3：生成 Hugo 博客文章 =================
def write_blog_post(stock_list):
    today_date = cn_now().strftime("%Y-%m-%d")
    post_time = cn_now().strftime("%Y-%m-%dT%H:%M:%S+08:00")

    os.makedirs(POST_FOLDER, exist_ok=True)

    # 删除旧的自动报告，只保留最新一篇
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
    - 网易兜底
    - DeepSeek
draft: false
---

# 🚀 全市场雷达：12日内3次暴涨异动股扫描

本报告由 **Python + 新浪/网易行情数据 + DeepSeek AI + 本地AI缓存** 自动生成。

> ⚠️ 风险提示：本文仅为基于公开行情数据的自动化整理与AI文本生成，不构成任何投资建议。股市有风险，交易需谨慎。

扫描条件：

- 股票范围：A股全市场，剔除 ST、退市、停牌无价格标的
- 时间窗口：最近 **{LOOKBACK_TRADING_DAYS}** 个交易日
- 异动标准：至少 **{MIN_SURGE_TIMES}** 次单日涨幅大于 **{SURGE_THRESHOLD}%**
- 排名方式：按最近区间总涨幅排序，截取 TOP {TOP_N}
- 数据来源：名单接口使用新浪行情接口为主、网易行情接口兜底；历史K线使用新浪历史K线接口
- AI模型：{DEEPSEEK_MODEL}
- AI缓存：已解析过且扫描数据一致的个股，会优先读取本地缓存

---

"""

    if stock_list is None:
        md_content += f"""
## 今日扫描结果

经过全市场扫描，最近 {LOOKBACK_TRADING_DAYS} 个交易日内，暂时没有股票满足：

> 至少 {MIN_SURGE_TIMES} 次单日涨幅大于 {SURGE_THRESHOLD}%

这通常说明短线极端活跃标的较少，市场资金可能处于分散或休整状态。

---

{get_random_philosophy()}

---

*本文由自动化程序于北京时间 {today_date} 自动发布。*
"""

    elif stock_list == "ERROR":
        md_content += f"""
## 今日扫描结果

今日新浪/网易数据抓取失败，未能完成全市场扫描。

可能原因包括：

- 新浪/网易接口临时不可用
- GitHub Actions 海外网络访问异常
- AkShare 接口返回字段变化
- 请求频率过高被临时限制

---

*本文由自动化程序于北京时间 {today_date} 自动发布。*
"""

    else:
        ai_cache = load_ai_cache()

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
        md_content += "## TOP 活跃股票逐只解读\n\n"

        for idx, s in enumerate(stock_list, start=1):
            detail_text = "；".join(s["surge_days_detail"])

            md_content += f"### {idx}. {s['name']}（{s['code']}）\n\n"

            md_content += (
                f"**异动数据**：最近 **{LOOKBACK_TRADING_DAYS}** 个交易日内，"
                f"出现 **{s['times']}** 次单日涨幅超过 **{SURGE_THRESHOLD}%**；"
                f"区间总涨幅 **{s['total_change']:.2f}%**；"
                f"最新收盘价 **{s['latest_close']:.2f}**。\n\n"
            )

            md_content += f"**异动日期**：{detail_text}\n\n"

            md_content += get_sina_chart_html(s["symbol"], s["name"])

            stock_brief = ask_deepseek_single_stock_brief(s, ai_cache=ai_cache)
            md_content += stock_brief + "\n\n"

            md_content += "---\n\n"

        md_content += """
## 总体观察

本轮筛选条件偏向捕捉短期内多次大幅上涨的高活跃标的。  
这类股票通常说明资金关注度较高、题材弹性较强，但也往往伴随较大的波动风险。  
连续异动之后，后续表现更容易受到市场情绪、题材持续性、成交量变化和监管环境影响。

## 风险声明

本文仅为基于公开行情数据的量化复盘和 AI 文本整理，不构成任何投资建议。  
文中提到的个股不代表推荐，不代表未来走势判断。短线高波动股票风险较高，交易需谨慎。

---

"""

        md_content += get_random_philosophy() + "\n\n"

        md_content += f"""
---

*本文由自动化程序于北京时间 {today_date} 自动发布。*
"""

    file_path = os.path.join(POST_FOLDER, f"{REPORT_PREFIX}-{today_date}.md")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"✅ 博客文章已成功生成：{file_path}")


# ================= 主程序执行 =================
if __name__ == "__main__":
    stock_list = get_surge_stocks()
    write_blog_post(stock_list)
