import os
import requests
import datetime
import time
import akshare as ak
import pandas as pd
import glob
import random

# ================= 辅助函数：智能识别股票前缀（用于调用新浪实时图片） =================
def get_market_prefix(code):
    code_str = str(code)

    # 6开头是沪市，0和3开头是深市，4和8开头是北交所
    if code_str.startswith("6"):
        return f"sh{code_str}"
    elif code_str.startswith("0") or code_str.startswith("3"):
        return f"sz{code_str}"
    elif code_str.startswith("8") or code_str.startswith("4"):
        return f"bj{code_str}"

    return f"sh{code_str}"


# ================= 辅助函数：对接“一言” API，获取无限哲学盲盒 =================
def get_random_philosophy():
    # c=d代表文学，c=k代表哲学，c=i代表诗词。混合请求保证逼格
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


# ================= 核心1：Python 获取真实底层数据 双核引擎防封杀 =================
def get_surge_stocks():
    print("📈 正在潜入【新浪/网易】接口抓取A股真实数据...")

    # 你的核心 110 只股票池
    pool_str = "300308,300502,300394,002463,300476,601138,688012,002371,688072,600584,002156,688041,688256,688498,688630,300567,300456,603283,603893,000066,000034,002409,300666,603650,688268,688300,300054,600330,000962,002130,688234,605589,600183,003031,301377,688378,603773,300776,688716,603663,300905,688386,300174,688333,600363,688027,600580,688639,688065,001270,300045,002273,688496,600552,688150,301393,688076,000963,002422,300298,300430,300487,002385,301162,000821,688700,688102,600549,300339,300207,300285,688116,300133,603662,002353,600066,601058,300866,688169,688036,601689,002126,603298,603338,000157,300833,600933,603997,600309,002601,300396,603259,300529,002372,300415,603179,002028,603556,603129,002444,603596,603197,601100,002472,688187,600900,600938,601899,601225,601288,600941,600285,000423,600660,300821,000922,000629"
    my_pool_list = [code.strip() for code in pool_str.split(",")]

    df = None

    for attempt in range(3):
        try:
            df = ak.stock_zh_a_spot()
            if df is not None and not df.empty:
                print("✅ 成功连接新浪财经接口！")
                break
        except Exception as e:
            print(f"⚠️ 新浪接口第 {attempt + 1} 次失败：{str(e)}")

            try:
                df = ak.stock_zh_a_spot_netease()
                if df is not None and not df.empty:
                    print("✅ 成功连接网易财经接口！")
                    break
            except Exception as e2:
                print(f"⚠️ 网易接口第 {attempt + 1} 次失败：{str(e2)}")
                time.sleep(2)

    if df is None or df.empty:
        print("❌ 新浪/网易接口均未获取到有效数据。")
        return None

    try:
        code_col = [col for col in df.columns if "代码" in col or "symbol" in col.lower()][0]
        name_col = [col for col in df.columns if "名称" in col or "name" in col.lower()][0]

        possible_change_cols = [
            col for col in df.columns
            if "涨跌幅" in col or "涨幅" in col or "percent" in col.lower()
        ]

        if not possible_change_cols:
            print("❌ 没找到涨跌幅字段。当前字段为：")
            print(df.columns.tolist())
            return None

        change_col = possible_change_cols[0]

        df["纯数字代码"] = df[code_col].astype(str).str.extract(r"(\d{6})")
        my_df = df[df["纯数字代码"].isin(my_pool_list)].copy()

        if my_df.empty:
            print("❌ 股票池没有匹配到行情数据。")
            return None

        my_df[change_col] = pd.to_numeric(my_df[change_col], errors="coerce")

        # ⚠️ 注意：测试跑通后，可以改回 > 3.0 或 > 5.0
        # 这里设为 > -10.0 是为了保证周末或弱行情测试时也能抓到足够股票
        surge_df = my_df[my_df[change_col] > -10.0]

        if surge_df.empty:
            print("今日无符合涨幅条件的股票。")
            return None

        # 按涨跌幅降序排列，取前 10 只
        surge_df = surge_df.sort_values(by=change_col, ascending=False).head(10)

        stock_data_list = []

        for index, row in surge_df.iterrows():
            change_val = round(float(row[change_col]), 2)

            stock_data_list.append({
                "name": row[name_col],
                "code": row["纯数字代码"],
                "change": f"+{change_val}" if change_val > 0 else f"{change_val}"
            })

        return stock_data_list

    except Exception as e:
        print(f"❌ 数据清洗报错：{str(e)}")
        return None


# ================= Gemini API 通用请求函数 =================
def ask_gemini(prompt, system_prompt="", temperature=0.5, timeout=60):
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")

    if not api_key:
        return "❌ Gemini API Key 未配置。请在部署平台环境变量中添加 GEMINI_API_KEY。"

    # 默认使用 Gemini 2.5 Flash。以后想换模型，只需要在环境变量里加 GEMINI_MODEL。
    model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

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

            content = candidates[0].get("content", {})
            parts = content.get("parts", [])

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

    return "❌ AI分析生成失败。"


# ================= 核心2：单只股票深度分析引擎 Gemini 版 =================
def ask_gemini_single(stock_name):
    system_prompt = """
你是一位严谨的A股量化研究员。
请分析这只股票，必须严格按照以下三段格式输出（绝对不准写总标题，绝对不准捏造任何涨跌幅数字）：

【🏰 核心产业壁垒】：(一段话，写它的主营业务、护城河和行业地位)

【🔥 近期资金逻辑】：(一段话，写它近期受益于什么宏观政策或产业链爆发)

【🔍 核心驱动力解析】：
(用列表符号分点列出3条驱动它上涨的核心因素。每条因素必须包含一个小标题和简短的解释)

必须大白话客观分析，拒绝任何主观吹捧和废话。
"""

    prompt = f"请分析：{stock_name}"

    return ask_gemini(
        prompt=prompt,
        system_prompt=system_prompt,
        temperature=0.5,
        timeout=60
    )


# ================= 核心3：全局 TOP10 表格总结引擎 Gemini 版 =================
def ask_gemini_summary(stock_data_list):
    real_data_str = ""

    for s in stock_data_list:
        real_data_str += f"股票：{s['name']}，真实涨幅：{s['change']}%\n"

    system_prompt = f"""
你是一位顶级的A股策略分析师。
我会给你今天涨幅 TOP10 异动股票的【真实数据】。

【强制任务】：
1. 生成一个 Markdown 格式的总结表格。表头必须为：| 股票 | 涨幅 | 核心驱动力 | 风险提示 |
2. '股票'和'涨幅'这两列，【必须100%照抄】我提供给你的真实数据，绝对不准修改数字！
3. '核心驱动力'列：用极其精炼的几个字概括。
4. '风险提示'列：一针见血指出隐患。
5. 在表格的最后，写一段加粗的【一句话总结】，点评今天整体市场的主线方向。

我提供的真实数据如下：
{real_data_str}
"""

    print("🤖 正在使用 Gemini 生成文末 TOP10 总结表格...")

    return ask_gemini(
        prompt="请输出总结表格和一句话总结，不要任何多余废话。",
        system_prompt=system_prompt,
        temperature=0.3,
        timeout=60
    )


# ================= 核心4：强制大屏排版生成博客 =================
def write_blog_post(stock_data_list):
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")
    post_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S+08:00")

    folder_path = "content/post"
    os.makedirs(folder_path, exist_ok=True)

    # 删除旧的自动报告，只保留当天最新报告
    for old_file in glob.glob(os.path.join(folder_path, "report-*.md")):
        os.remove(old_file)

    md_content = f"""---
title: "🚀 【深度复盘】核心资产涨幅 TOP10 逻辑拆解 ({today_date})"
date: {post_time}
categories:
    - 量化研报
tags:
    - AI选股
draft: false
---

# 今日异动领涨 TOP 10

此报告由 **Python 抓取底层真实数据 + Gemini 深度逻辑分析** 组合生成。数据绝对真实，拒绝 AI 幻觉！

---

"""

    for stock in stock_data_list:
        print(f"🤖 正在呼叫 Gemini 单独分析：{stock['name']} ...")

        md_content += f"## 🏷️ 【{stock['name']}】({stock['code']}) 真实涨幅：<span style='color:red;'>**{stock['change']}%**</span>\n\n"

        ai_analysis = ask_gemini_single(stock["name"])
        md_content += ai_analysis + "\n\n"

        # 获取新浪分时图和K线图
        market_code = get_market_prefix(stock["code"])
        min_chart_url = f"https://image.sinajs.cn/newchart/min/n/{market_code}.gif"
        daily_chart_url = f"https://image.sinajs.cn/newchart/daily/n/{market_code}.gif"

        md_content += "**📊 行情走势图（左：今日分时，右：近期日K）：**\n\n"
        md_content += f"""<div style="display: flex; justify-content: space-between; gap: 20px; margin-bottom: 20px;">
  <div style="flex: 1; text-align: center;">
    <img src="{min_chart_url}" alt="分时图" style="width: 100%; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.15);">
  </div>
  <div style="flex: 1; text-align: center;">
    <img src="{daily_chart_url}" alt="日K线" style="width: 100%; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.15);">
  </div>
</div>

"""

        print("💡 正在从云端抽取哲学名言...")
        philosophy_quote = get_random_philosophy()
        md_content += philosophy_quote + "\n\n"

        md_content += "---\n\n"

    md_content += "## 📌 总结：今日 TOP10 领涨先锋的核心驱动力\n\n"

    summary_content = ask_gemini_summary(stock_data_list)
    md_content += summary_content + "\n\n"

    md_content += f"\n*本文由自动化程序于北京时间 {today_date} 自动发布。*"

    file_path = f"{folder_path}/report-{today_date}.md"

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"✅ 博客文章已成功生成：{file_path}")


if __name__ == "__main__":
    stock_data_list = get_surge_stocks()

    if stock_data_list:
        write_blog_post(stock_data_list)
    else:
        print("今日无符合条件的股票，停更。")
