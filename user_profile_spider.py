import requests
import pymysql
import time
from datetime import datetime


class UserProfileSpider:
    """
    模块四：话题参与用户资料爬取器
    功能：从 topic_weibo 表提取 user_id，
          逐个请求微博用户信息接口，存入 topic_user 表
    """

    USER_INFO_URL = "https://weibo.com/ajax/profile/info"
    USER_DETAIL_URL = "https://weibo.com/ajax/profile/detail"

    def __init__(self, cookie, db_config):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Cookie":     cookie,
            "Referer":    "https://weibo.com/",
            "Accept":     "application/json, text/plain, */*",
        }
        self.db = pymysql.connect(**db_config)
        self._init_table()

    def _init_table(self):
        """初始化用户资料表"""
        sql = """
        CREATE TABLE IF NOT EXISTS topic_user (
            id               BIGINT AUTO_INCREMENT PRIMARY KEY,
            user_id          VARCHAR(50) UNIQUE     COMMENT '用户ID',
            screen_name      VARCHAR(100)           COMMENT '昵称',
            gender           VARCHAR(5)             COMMENT '性别 m/f',
            followers_count  INT DEFAULT 0          COMMENT '粉丝数',
            follow_count     INT DEFAULT 0          COMMENT '关注数',
            statuses_count   INT DEFAULT 0          COMMENT '发帖总数',
            description      TEXT                   COMMENT '个人简介',
            verified         TINYINT DEFAULT 0      COMMENT '是否认证 0/1',
            verified_type    INT DEFAULT -1         COMMENT '认证类型 -1无/0个人/1企业',
            verified_reason  VARCHAR(255)           COMMENT '认证说明',
            profile_url      VARCHAR(255)           COMMENT '主页链接',
            crawl_time       DATETIME               COMMENT '爬取时间',
            INDEX idx_followers (followers_count),
            INDEX idx_verified (verified_type)
        ) DEFAULT CHARSET=utf8mb4 COMMENT='热搜话题参与用户资料表';
        """
        with self.db.cursor() as cur:
            cur.execute(sql)
        self.db.commit()
        print("[初始化] topic_user 表就绪")

    def get_pending_user_ids(self):
        sql = """
        SELECT DISTINCT tw.user_id
        FROM topic_weibo tw
        WHERE tw.user_id != ''
          AND tw.user_id IS NOT NULL
          AND tw.user_id NOT IN (
              SELECT user_id FROM topic_user 
              WHERE user_id IS NOT NULL
          )
        """
        try:
            self.db.ping(reconnect=True)
            with self.db.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            ids = [row[0] for row in rows]
            print(f"[待爬] 共 {len(ids)} 个新用户需要采集")
            return ids
        except pymysql.MySQLError as e:
            print(f"[数据库错误] 读取 user_id 失败: {e}")
            return []

    def fetch_user(self, user_id):
        """
        请求单个用户的资料
        返回格式化后的用户字典，失败返回 None
        """
        try:
            params = {"uid": user_id}
            resp = requests.get(
                self.USER_INFO_URL,
                params=params,
                headers=self.headers,
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()

            # 接口返回结构：data -> user
            user = data.get("data", {}).get("user", {})
            if not user:
                return None

            return {
                "user_id":         str(user.get("id", user_id)),
                "screen_name":     user.get("screen_name", ""),
                "gender":          user.get("gender", ""),
                "followers_count": user.get("followers_count", 0),
                "follow_count":    user.get("friends_count", 0),
                "statuses_count":  user.get("statuses_count", 0),
                "description":     user.get("description", ""),
                "verified":        1 if user.get("verified") else 0,
                "verified_type":   user.get("verified_type", -1),
                "verified_reason": user.get("verified_reason", ""),
                "profile_url":     f"https://weibo.com/u/{user_id}",
                "crawl_time":      datetime.now(),
            }

        except requests.RequestException as e:
            print(f"  [网络错误] user_id={user_id}: {e}")
            return None
        except Exception as e:
            print(f"  [解析错误] user_id={user_id}: {e}")
            return None

    def save_user(self, user):
        """存储单个用户资料，已存在则更新"""
        sql = """
        INSERT INTO topic_user
            (user_id, screen_name, gender, followers_count, follow_count,
             statuses_count, description, verified, verified_type,
             verified_reason, profile_url, crawl_time)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            screen_name     = VALUES(screen_name),
            followers_count = VALUES(followers_count),
            follow_count    = VALUES(follow_count),
            statuses_count  = VALUES(statuses_count),
            description     = VALUES(description),
            verified        = VALUES(verified),
            verified_type   = VALUES(verified_type),
            verified_reason = VALUES(verified_reason),
            crawl_time      = VALUES(crawl_time)
        """
        try:
            self.db.ping(reconnect=True)
            with self.db.cursor() as cur:
                cur.execute(sql, (
                    user["user_id"],
                    user["screen_name"],
                    user["gender"],
                    user["followers_count"],
                    user["follow_count"],
                    user["statuses_count"],
                    user["description"],
                    user["verified"],
                    user["verified_type"],
                    user["verified_reason"],
                    user["profile_url"],
                    user["crawl_time"],
                ))
            self.db.commit()
        except pymysql.MySQLError as e:
            print(f"  [数据库错误] 存储用户 {user.get('user_id')} 失败: {e}")
            self.db.rollback()

    def crawl_from_topic_weibo(self, delay=1.5):
        """
        主方法：自动读取待爬用户列表，逐个爬取并存储
        :param delay: 每次请求间隔秒数，默认1.5秒，不建议低于1秒
        """
        user_ids = self.get_pending_user_ids()
        if not user_ids:
            print("[跳过] 没有新用户需要采集")
            return

        success = 0
        fail = 0
        total = len(user_ids)

        for i, uid in enumerate(user_ids, 1):
            print(f"  [{i}/{total}] 爬取用户 {uid} ...", end=" ")
            user = self.fetch_user(uid)

            if user:
                self.save_user(user)
                fans = user["followers_count"]
                # 粉丝数格式化显示
                fans_str = f"{fans/10000:.1f}万" if fans >= 10000 else str(fans)
                vtype = {0: "个人认证", 1: "企业认证"}.get(user["verified_type"], "未认证")
                print(f"✓ {user['screen_name']} | 粉丝:{fans_str} | {vtype}")
                success += 1
            else:
                print("✗ 获取失败")
                fail += 1

            # 每50个用户暂停5秒，避免请求过密
            if i % 50 == 0:
                print(f"  [暂停] 已处理 {i} 个，休息5秒...")
                time.sleep(5)
            else:
                time.sleep(delay)

        print(f"\n[用户采集完成] 成功:{success} 失败:{fail} 共:{total}")

    def close(self):
        self.db.close()
