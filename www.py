import os
import requests
import datetime
import time
import random
import glob
import akshare as ak
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed


# ================= 参数区：以后主要改这里 =================
LOOKBACK_TRADING_DAYS = 12          # 最近12个交易日
SURGE_THRESHOLD = 7.0               # 单日涨幅大于7%
MIN_SURGE_TIMES = 3                 # 至少出现3次
TOP_N = 10                          # 最终给AI分析前10名

# 新浪接口别开太高，海外IP建议 4~6
MAX_WORKERS = 5

# 拉最近45个自然日，足够覆盖12个交易日
HIST_CALENDAR_DAYS = 45

# Hugo文章目录
POST_FOLDER = "content/post"

# Gemini模型
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")


# ================= 工具函数：获取日期区间 =================
def get_date_range():
    end_date = datetime.datetime.now().strftime("%Y%m%d")
    start_date = (datetime.datetime.now() - datetime.timedelta(days=HIST_CALENDAR_DAYS)).strftime("%Y%m%d")
    return start_date, end_date


# ================= 工具函数：识别新浪市场前缀 =================
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


# ================= 核心1-1：通过新浪获取全市场股票名单 =================
def get_all_a_stock_list_sina():
    print("📈 正在通过新浪接口获取A股全市场最新名单...")

    spot_df = None

    for attempt in range(3):
        try:
            spot_df = ak.stock_zh_a_spot()

            if spot_df is not None and not spot_df.empty:
                print("✅ 新浪全市场名单获取成功！")
                break

        except Exception as e:
            print(f"⚠️ 新浪全市场名单获取失败，第 {attempt + 1} 次：{str(e)}")
            time.sleep(3)

    if spot_df is None or spot_df.empty:
        print("❌ 无法通过新浪获取全市场股票列表。")
        return None

    try:
        code_col = [col for col in spot_df.columns if "代码" in col or "symbol" in col.lower()][0]
        name_col = [col for col in spot_df.columns if "名称" in col or "name" in col.lower()][0]

        spot_df[code_col] = spot_df[code_col].astype(str)
        spot_df[name_col] = spot_df[name_col].astype(str)

        spot_df["市场代码"] = spot_df[code_col].apply(get_market_prefix)
        spot_df["纯数字代码"] = spot_df["市场代码"].str.extract(r"(\d{6})")

        # 剔除 ST、*ST、退市股
        spot_df = spot_df[~spot_df[name_col].str.contains("ST|退", regex=True, na=False)].copy()

        # 如果有最新价字段，剔除停牌或无价格股票
        price_cols = [col for col in spot_df.columns if "最新" in col or "price" in col.lower()]
        if price_cols:
            price_col = price_cols[0]
            spot_df[price_col] = pd.to_numeric(spot_df[price_col], errors="coerce")
            spot_df = spot_df[spot_df[price_col] > 0].copy()

        all_symbols = spot_df["市场代码"].dropna().unique().tolist()

        name_dict = dict(zip(spot_df["市场代码"], spot_df[name_col]))
        pure_code_dict = dict(zip(spot_df["市场代码"], spot_df["纯数字代码"]))

        print(f"🚀 新浪成功返回全市场名单，剔除ST/退市/无价格后共计 {len(all_symbols)} 只股票。")

        return all_symbols, name_dict, pure_code_dict

    except Exception as e:
        print(f"❌ 新浪名单清洗失败：{str(e)}")
        print("当前字段：")
        print(spot_df.columns.tolist())
        return None


# ================= 核心1-2：扫描单只股票的最近12个交易日 =================
def scan_one_stock_sina(symbol, name_dict, pure_code_dict, start_date, end_date):
    try:
        # 随机延迟，降低被新浪限制的概率
        time.sleep(random.uniform(0.08, 0.25))

        hist_df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )

        if hist_df is None or hist_df.empty:
            return None

        if "date" not in hist_df.columns or "close" not in hist_df.columns:
            return None

        hist_df = hist_df.sort_values("date").copy()
        hist_df["close"] = pd.to_numeric(hist_df["close"], errors="coerce")
        hist_df = hist_df.dropna(subset=["close"])

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

        name = name_dict.get(symbol, "未知名称")
        pure_code = pure_code_dict.get(symbol, symbol)

        return {
            "name": name,
            "code": pure_code,
            "symbol": symbol,
            "times": count_surge_days,
            "total_change": total_change,
            "latest_close": close_latest,
            "surge_days_detail": surge_days_detail
        }

    except Exception:
        return None


# ================= 核心1-3：全市场量化扫描 新浪并发版 =================
def get_pattern_surge_stocks_all_market():
    stock_info = get_all_a_stock_list_sina()

    if stock_info is None:
        return "ERROR"

    all_symbols, name_dict, pure_code_dict = stock_info
    total_stocks = len(all_symbols)

    start_date, end_date = get_date_range()

    print(f"⏳ 开始通过新浪扫描最近 {HIST_CALENDAR_DAYS} 个自然日K线。")
    print(f"🎯 条件：最近 {LOOKBACK_TRADING_DAYS} 个交易日内，至少 {MIN_SURGE_TIMES} 次单日涨幅 > {SURGE_THRESHOLD}%。")
    print(f"🚀 当前并发线程数：{MAX_WORKERS}")
    print(f"📅 数据区间：{start_date} ~ {end_date}")

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
        f"已截取最强 TOP {TOP_N} 准备提交 Gemini 分析。"
    )

    return top_stocks


# 这个函数名保留给 GitHub Actions 自动识别脚本用
def get_surge_stocks():
    return get_pattern_surge_stocks_all_market()


# ================= Gemini API 通用请求函数 =================
def ask_gemini(prompt, system_prompt="", temperature=0.4, timeout=120):
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")

    if not api_key:
        return "❌ Gemini API Key 未配置。请在 GitHub Secrets 或部署平台环境变量中添加 GEMINI_API_KEY。"

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
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=timeout
            )

            if response.status_code != 200:
                print(f"❌ Gemini HTTP错误：{response.status_code}")
                print(response.text)
                time.sleep(2)
                continue

            data = response.json()
            candidates = data.get("candidates", [])

            if not candidates:
                print("❌ Gemini 没有返回 candidates。完整响应：")
                print(data)
                time.sleep(2)
                continue

            parts = candidates[0].get("content", {}).get("parts", [])

            if not parts:
                print("❌ Gemini 没有返回正文 parts。完整响应：")
                print(data)
                time.sleep(2)
                continue

            text = parts[0].get("text", "").strip()

            if text:
                return text

            print("❌ Gemini 返回内容为空。")
            time.sleep(2)

        except Exception as e:
            print(f"❌ Gemini 请求失败，第 {i + 1} 次：{str(e)}")
            time.sleep(2)

    return "❌ AI 分析生成失败。"


# ================= 核心2：让 Gemini 生成整篇博客分析 =================
def ask_gemini_to_analyze_for_blog(stock_list):
    if not stock_list:
        return "今日全市场未扫描到符合条件的高活跃股票。"

    stocks_str = ""

    for s in stock_list:
        detail_text = "；".join(s["surge_days_detail"])
        stocks_str += (
            f"【{s['name']}】(代码: {s['code']})："
            f"最近{LOOKBACK_TRADING_DAYS}个交易日内出现 {s['times']} 次涨幅超{SURGE_THRESHOLD}%；"
            f"区间总涨幅: {s['total_change']:.2f}%；"
            f"最新收盘价: {s['latest_close']:.2f}；"
            f"异动日期: {detail_text}\n"
        )

    system_prompt = f"""
你是一位严谨的A股量化研究员和市场策略分析师。

我会给你一个名单，这些股票是从A股全市场机器扫描出来的。

筛选条件是：
最近 {LOOKBACK_TRADING_DAYS} 个交易日内，至少出现 {MIN_SURGE_TIMES} 次单日涨幅超过 {SURGE_THRESHOLD}%。

这说明这些股票近期资金活跃度极高，但不代表一定还能继续上涨。

请生成一篇适合 Hugo 博客发布的 Markdown 正文内容。

严格要求：
1. 不要写 YAML front matter，我的程序会自动写。
2. 不要编造涨跌幅数字。
3. 不要承诺上涨。
4. 不要写“买入、推荐、目标价”等投资建议。
5. 每只股票必须原样复述我给你的股票名称、代码、异动次数、区间涨幅和异动日期。
6. 语言可以犀利，但必须客观。
7. 最后必须有风险提示。

文章结构：
## 一、全市场异动扫描结果

先用一段话解释这个量化条件的含义。

## 二、TOP活跃股票逐只拆解

每只股票用如下格式：

### 股票名称（代码）

**异动数据**：原样复述数据。

**核心概念**：用大白话说明主营业务和市场概念。

**资金逻辑**：解释近期资金可能为什么关注它，包括产业催化、政策方向、题材轮动等。

**风险提示**：明确指出追高、回撤、题材退潮、基本面不匹配等风险。

## 三、总体结论

总结这些股票共同反映了当前市场哪条主线最强。

## 四、风险声明

说明本文仅为量化数据复盘，不构成投资建议。
"""

    user_message = f"请基于以下真实扫描结果生成博客正文：\n\n{stocks_str}"

    print("🤖 Gemini 正在生成博客正文...")

    return ask_gemini(
        prompt=user_message,
        system_prompt=system_prompt,
        temperature=0.6,
        timeout=180
    )


# ================= 核心3：生成 Hugo 博客文章 =================
def write_blog_post(stock_list):
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")
    post_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S+08:00")

    os.makedirs(POST_FOLDER, exist_ok=True)

    # 删除旧的自动报告，只保留最新一篇
    for old_file in glob.glob(os.path.join(POST_FOLDER, "report-*.md")):
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

本报告由 **Python + 新浪行情接口 + Gemini AI** 自动生成。

扫描条件：

- 股票范围：A股全市场，剔除 ST、退市、停牌无价格标的
- 时间窗口：最近 **{LOOKBACK_TRADING_DAYS}** 个交易日
- 异动标准：至少 **{MIN_SURGE_TIMES}** 次单日涨幅大于 **{SURGE_THRESHOLD}%**
- 排名方式：按最近区间总涨幅排序，截取 TOP {TOP_N}
- 数据来源：新浪行情接口
- AI模型：{GEMINI_MODEL}

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

今日数据抓取失败，未能完成全市场扫描。

可能原因包括：

- 新浪行情接口临时不可用
- GitHub Actions 海外网络访问异常
- AkShare 接口返回字段变化
- 请求频率过高被临时限制

---

*本文由自动化程序于北京时间 {today_date} 自动发布。*
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

        ai_analysis = ask_gemini_to_analyze_for_blog(stock_list)
        md_content += ai_analysis + "\n\n"

        md_content += "---\n\n"
        md_content += get_random_philosophy() + "\n\n"

        md_content += f"""
---

*本文由自动化程序于北京时间 {today_date} 自动发布。*
"""

    file_path = os.path.join(POST_FOLDER, f"12天异动3次-{today_date}.md")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"✅ 博客文章已成功生成：{file_path}")


# ================= 主程序执行 =================
if __name__ == "__main__":
    stock_list = get_surge_stocks()
    write_blog_post(stock_list)
