import requests
import pymysql
import time
from datetime import datetime


class HotSearchCollector:
    """
    模块一：微博热搜榜采集器
    功能：定时抓取热搜榜前50条，存入 hot_search 表
    """

    HOT_SEARCH_URL = "https://weibo.com/ajax/side/hotSearch"

    def __init__(self, db_config):
        self.db_config = db_config
        self.db = pymysql.connect(**db_config)
        self._init_table()

    def _init_table(self):
        """初始化热搜记录表"""
        sql = """
        CREATE TABLE IF NOT EXISTS hot_search (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            keyword     VARCHAR(100) NOT NULL COMMENT '热搜词',
            hot_value   BIGINT DEFAULT 0    COMMENT '热度值',
            label       VARCHAR(20)          COMMENT '标签：新/爆/沸',
            rank_num    INT                  COMMENT '排名',
            crawl_time  DATETIME NOT NULL    COMMENT '采集时间',
            INDEX idx_keyword (keyword),
            INDEX idx_time (crawl_time)
        ) DEFAULT CHARSET=utf8mb4 COMMENT='微博热搜记录表';
        """
        with self.db.cursor() as cur:
            cur.execute(sql)
        self.db.commit()
        print("[初始化] hot_search 表就绪")

    def fetch(self):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://weibo.com/",
            }
            resp = requests.get(self.HOT_SEARCH_URL, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json().get("data", {})
            results = []
            now = datetime.now()

            for rank, item in enumerate(data.get("realtime", []), 1):
                keyword = item.get("word", "").strip()
                if not keyword:
                    continue
                results.append({
                    "keyword": keyword,
                    "hot_value": item.get("num", 0),
                    "label": item.get("label_name", ""),
                    "rank_num": rank,
                    "crawl_time": now,
                })

            print(f"[{now.strftime('%H:%M:%S')}] 获取到 {len(results)} 条热搜")
            return results

        except requests.RequestException as e:
            print(f"[网络错误] 热搜接口请求失败: {e}")
            return []
        except Exception as e:
            print(f"[解析错误] {e}")
            return []
    def save(self, items):
        """将热搜列表存入数据库"""
        if not items:
            print("[跳过] 本次无热搜数据可存")
            return

        sql = """
        INSERT INTO hot_search (keyword, hot_value, label, rank_num, crawl_time)
        VALUES (%s, %s, %s, %s, %s)
        """
        try:
            # 断线重连
            self.db.ping(reconnect=True)
            with self.db.cursor() as cur:
                for item in items:
                    cur.execute(sql, (
                        item["keyword"],
                        item["hot_value"],
                        item["label"],
                        item["rank_num"],
                        item["crawl_time"],
                    ))
            self.db.commit()
            print(f"[存储] 成功写入 {len(items)} 条热搜记录")
        except pymysql.MySQLError as e:
            print(f"[数据库错误] {e}")
            self.db.rollback()

    def run_once(self):
        """
        执行一次完整的采集+存储流程
        返回热搜关键词列表，供 KeywordWeiboSpider 使用
        """
        items = self.fetch()
        self.save(items)
        return items

    def get_recent_keywords(self, limit=10):
        """
        从数据库读取最新一批热搜词（避免重复请求接口）
        返回关键词字符串列表
        """
        sql = """
        SELECT keyword FROM hot_search
        WHERE crawl_time = (SELECT MAX(crawl_time) FROM hot_search)
        ORDER BY rank_num ASC
        LIMIT %s
        """
        try:
            self.db.ping(reconnect=True)
            with self.db.cursor() as cur:
                cur.execute(sql, (limit,))
                rows = cur.fetchall()
            return [row[0] for row in rows]
        except pymysql.MySQLError as e:
            print(f"[数据库错误] 读取热搜词失败: {e}")
            return []

    def close(self):
        self.db.close()
