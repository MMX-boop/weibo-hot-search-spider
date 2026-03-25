import requests
import pymysql
import time
import re
from datetime import datetime
from urllib.parse import quote
from bs4 import BeautifulSoup


class KeywordWeiboSpider:
    """
    模块二：关键词微博爬取器
    功能：根据热搜词，通过微博搜索接口爬取相关微博内容，存入 topic_weibo 表
    """

    SEARCH_URL = "https://s.weibo.com/weibo"

    def __init__(self, cookie, db_config):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Cookie":    cookie,
            "Referer":   "https://s.weibo.com/",
            "Accept":    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        self.db_config = db_config
        self.db = pymysql.connect(**db_config)
        self._init_table()

    def _init_table(self):
        """初始化话题微博表"""
        sql = """
        CREATE TABLE IF NOT EXISTS topic_weibo (
            id              BIGINT AUTO_INCREMENT PRIMARY KEY,
            keyword         VARCHAR(100)   COMMENT '来源热搜词',
            weibo_id        VARCHAR(50)    COMMENT '微博ID',
            user_id         VARCHAR(50)    COMMENT '用户ID',
            screen_name     VARCHAR(100)   COMMENT '用户昵称',
            content         TEXT           COMMENT '微博正文',
            attitudes_count INT DEFAULT 0  COMMENT '点赞数',
            comments_count  INT DEFAULT 0  COMMENT '评论数',
            reposts_count   INT DEFAULT 0  COMMENT '转发数',
            created_at      VARCHAR(50)    COMMENT '发布时间',
            crawl_time      DATETIME       COMMENT '爬取时间',
            UNIQUE KEY uk_weibo_keyword (weibo_id, keyword),
            INDEX idx_keyword (keyword),
            INDEX idx_user (user_id),
            INDEX idx_crawl (crawl_time)
        ) DEFAULT CHARSET=utf8mb4 COMMENT='热搜话题相关微博表';
        """
        with self.db.cursor() as cur:
            cur.execute(sql)
        self.db.commit()
        print("[初始化] topic_weibo 表就绪")

    def crawl_keyword(self, keyword, pages=3):
        """
        爬取某个关键词的微博内容
        :param keyword: 热搜词
        :param pages:   爬取页数，默认3页（每页约10条）
        :return:        微博数据列表
        """
        print(f"  → 爬取关键词: 【{keyword}】共 {pages} 页")
        all_results = []

        for page in range(1, pages + 1):
            url = f"{self.SEARCH_URL}?q={quote(keyword)}&page={page}"
            try:
                resp = requests.get(url, headers=self.headers, timeout=15)
                resp.raise_for_status()

                # 检查是否被重定向到登录页
                if "passport.weibo.com" in resp.url:
                    print("  [警告] Cookie 已失效，请重新获取")
                    return all_results

                items = self._parse(resp.text, keyword)
                all_results.extend(items)
                print(f"    第 {page} 页解析到 {len(items)} 条")

                # 随机延迟，避免被封
                time.sleep(2)

            except requests.RequestException as e:
                print(f"  [网络错误] 第 {page} 页请求失败: {e}")
                time.sleep(5)
                continue

        return all_results

    def _parse(self, html, keyword):
        """
        解析微博搜索结果页 HTML
        """
        soup = BeautifulSoup(html, "html.parser")
        items = []
        now = datetime.now()

        # 微博搜索结果卡片
        cards = soup.select(".card-wrap")
        if not cards:
            # 尝试备用选择器
            cards = soup.select('[action-type="feed_list_item"]')

        for card in cards:
            try:
                item = {}
                item["keyword"] = keyword
                item["crawl_time"] = now

                # 微博ID（从卡片属性或链接中提取）
                mid = card.get("mid", "")
                if not mid:
                    link = card.select_one("a[href*='/detail/']")
                    if link:
                        match = re.search(r"/detail/(\w+)", link.get("href", ""))
                        mid = match.group(1) if match else ""
                item["weibo_id"] = mid

                # 用户信息
                user_el = card.select_one(".name")
                if user_el:
                    item["screen_name"] = user_el.get_text(strip=True)
                    uid_link = user_el.get("href", "")
                    uid_match = re.search(r"/u/(\d+)|weibo\.com/(\w+)", uid_link)
                    item["user_id"] = uid_match.group(1) or uid_match.group(2) if uid_match else ""
                else:
                    item["screen_name"] = ""
                    item["user_id"] = ""

                # 微博正文
                txt_el = card.select_one(".txt")
                if txt_el:
                    # 去掉"展开"按钮文字
                    for tag in txt_el.select(".expand, .hide"):
                        tag.decompose()
                    item["content"] = txt_el.get_text(strip=True)
                else:
                    item["content"] = ""

                # 互动数据（点赞/评论/转发）
                item["attitudes_count"] = self._get_count(card, "like")
                item["comments_count"]  = self._get_count(card, "comment")
                item["reposts_count"]   = self._get_count(card, "repost")

                # 发布时间
                time_el = card.select_one(".from a")
                item["created_at"] = time_el.get_text(strip=True) if time_el else ""

                # 过滤空内容
                if item["content"]:
                    items.append(item)

            except Exception as e:
                print(f"  [解析警告] 单条卡片解析失败: {e}")
                continue

        return items

    def _get_count(self, card, action_type):
        """从操作栏提取互动数字"""
        # 微博实际的 action-type 映射
        action_map = {
            "like": ["feed_list_like", "feed_list_attitude"],
            "comment": ["feed_list_comment"],
            "repost": ["feed_list_forward", "feed_list_repost"],
        }
        try:
            for at in action_map.get(action_type, []):
                el = card.select_one(f'[action-type="{at}"] em')
                if el:
                    text = el.get_text(strip=True).replace(",", "")
                    if text.isdigit():
                        return int(text)
            # 备用方案：找所有 em 标签按顺序取
            ems = card.select(".card-act li em")
            idx = {"like": -1, "comment": 1, "repost": 0}.get(action_type, -1)
            if ems and idx >= 0 and idx < len(ems):
                text = ems[idx].get_text(strip=True).replace(",", "")
                if text.isdigit():
                    return int(text)
        except:
            pass
        return 0

    def save(self, items):
        """将微博列表存入数据库，重复的微博自动跳过"""
        if not items:
            print("  [跳过] 本次无微博数据可存")
            return

        sql = """
        INSERT IGNORE INTO topic_weibo
            (keyword, weibo_id, user_id, screen_name, content,
             attitudes_count, comments_count, reposts_count, created_at, crawl_time)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            self.db.ping(reconnect=True)
            with self.db.cursor() as cur:
                for item in items:
                    cur.execute(sql, (
                        item["keyword"],
                        item.get("weibo_id", ""),
                        item.get("user_id", ""),
                        item.get("screen_name", ""),
                        item.get("content", ""),
                        item.get("attitudes_count", 0),
                        item.get("comments_count", 0),
                        item.get("reposts_count", 0),
                        item.get("created_at", ""),
                        item["crawl_time"],
                    ))
            self.db.commit()
            print(f"  [存储] 成功写入 {len(items)} 条微博")
        except pymysql.MySQLError as e:
            print(f"  [数据库错误] {e}")
            self.db.rollback()

    def close(self):
        self.db.close()
