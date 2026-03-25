import time
import schedule
from datetime import datetime
from hot_search_collector import HotSearchCollector
from keyword_weibo_spider import KeywordWeiboSpider
from user_profile_spider import UserProfileSpider

# ============================================================
#  配置区域：只需修改这里
# ============================================================

DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "",   # ← 改这里
    "database": "",                # ← 改成你的数据库名
    "charset":  "utf8mb4",
    "connect_timeout": 10,
}

# 登录微博后，F12 → Network → 任意请求 → Request Headers → Cookie
COOKIES=''

# 每次爬取热搜前 N 个关键词（最多50个，建议10~20）
TOP_N_KEYWORDS = 10

# 每个关键词爬几页搜索结果（每页约10条微博）
PAGES_PER_KEYWORD = 3

# 定时间隔（分钟），建议不低于30分钟，避免被封
INTERVAL_MINUTES = 60

# ============================================================

def run_once():
    print("\n" + "=" * 50)
    print(f"[开始] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    collector = None
    spider = None
    user_spider = None

    try:
        # ---------- 第一步：采集热搜榜 ----------
        print("\n【第一步】采集热搜榜...")
        collector = HotSearchCollector(DB_CONFIG)
        hot_items = collector.run_once()

        if not hot_items:
            print("[警告] 未获取到热搜数据，本次跳过")
            return

        keywords = [item["keyword"] for item in hot_items[:TOP_N_KEYWORDS]]
        print(f"\n本次将爬取以下 {len(keywords)} 个热搜词：")
        for i, kw in enumerate(keywords, 1):
            print(f"  {i:2d}. {kw}")

        # ---------- 第二步：爬取每个热搜词的微博 ----------
        print("\n【第二步】爬取热搜相关微博...")
        spider = KeywordWeiboSpider(COOKIE, DB_CONFIG)

        total_saved = 0
        for kw in keywords:
            posts = spider.crawl_keyword(kw, pages=PAGES_PER_KEYWORD)
            spider.save(posts)
            total_saved += len(posts)
            time.sleep(3)

        # ---------- 第三步：爬取发帖用户资料 ----------
        print("\n【第三步】采集话题参与用户资料...")
        user_spider = UserProfileSpider(COOKIE, DB_CONFIG)
        user_spider.crawl_from_topic_weibo()

        print("\n" + "-" * 50)
        print(f"[完成] 本轮共采集微博 {total_saved} 条")
        print(f"[下次] 将在 {INTERVAL_MINUTES} 分钟后执行")
        print("-" * 50)

    except KeyboardInterrupt:
        print("\n[中断] 用户手动停止")
        raise

    except Exception as e:
        print(f"\n[严重错误] {e}")
        import traceback
        traceback.print_exc()

    finally:
        if collector:
            collector.close()
        if spider:
            spider.close()
        if user_spider:
            user_spider.close()


def main():
    """
    主入口：立即执行一次，然后按设定间隔定时执行
    """
    print("=" * 50)
    print("  微博热搜话题实时采集系统")
    print(f"  采集间隔：每 {INTERVAL_MINUTES} 分钟")
    print(f"  热搜词数：前 {TOP_N_KEYWORDS} 个")
    print(f"  每词页数：{PAGES_PER_KEYWORD} 页")
    print("=" * 50)
    print("按 Ctrl+C 可随时停止\n")

    # 立即执行第一次
    run_once()

    # 设置定时任务
    schedule.every(INTERVAL_MINUTES).minutes.do(run_once)

    # 保持运行
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
