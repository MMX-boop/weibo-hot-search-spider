import requests
import pymysql
import time
from datetime import datetime

COOKIES='你的COOKIES'

DB = {
    "host": "localhost", "user": "root",
    "password": "", "database": "", "charset": "utf8mb4"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Cookie": COOKIES,
    "Referer": "https://weibo.com/",
    "Accept": "application/json, text/plain, */*",
}

def fix_counts():
    db = pymysql.connect(**DB)

    # 只取互动数全为0的记录
    with db.cursor() as cur:
        cur.execute("""
            SELECT id, weibo_id FROM topic_weibo
            WHERE attitudes_count = 0
              AND comments_count = 0
              AND reposts_count = 0
              AND weibo_id != ''
        """)
        rows = cur.fetchall()

    print(f"[待修复] 共 {len(rows)} 条互动数为0的微博")
    success = 0

    for i, (row_id, weibo_id) in enumerate(rows, 1):
        try:
            url = f"https://weibo.com/ajax/statuses/show?id={weibo_id}"
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.encoding = "utf-8"
            data = resp.json()

            attitudes = data.get("attitudes_count", 0)
            comments  = data.get("comments_count", 0)
            reposts   = data.get("reposts_count", 0)

            with db.cursor() as cur:
                cur.execute("""
                    UPDATE topic_weibo
                    SET attitudes_count=%s, comments_count=%s, reposts_count=%s
                    WHERE id=%s
                """, (attitudes, comments, reposts, row_id))
            db.commit()

            print(f"  [{i}/{len(rows)}] ✓ 点赞:{attitudes} 评论:{comments} 转发:{reposts}")
            success += 1

        except Exception as e:
            print(f"  [{i}/{len(rows)}] ✗ weibo_id={weibo_id} 失败: {e}")

        # 每50条暂停一下
        if i % 50 == 0:
            print("  [暂停] 休息5秒...")
            time.sleep(5)
        else:
            time.sleep(1.5)

    db.close()
    print(f"\n[完成] 成功修复 {success}/{len(rows)} 条")

if __name__ == "__main__":
    fix_counts()