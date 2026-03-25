[微博热搜话题实时采集系统.md]
#  微博热搜话题实时采集系统

> 自动抓取微博热搜榜单、相关微博内容及用户资料，存入 MySQL，支持定时循环采集。

------

## 📌 项目简介

本项目是一个面向微博平台的多模块数据采集系统，能够：

- 定时抓取**微博热搜榜**前 N 个热词及热度值
- 根据热搜词**批量爬取相关微博**（正文、点赞/评论/转发数、发布时间等）
- 自动采集发帖**用户资料**（昵称、粉丝数、认证信息等）
- 所有数据持久化存入 **MySQL**，便于后续分析与可视化

适用于舆情分析、社交媒体研究、课程实训等场景。

------

## 🗂️ 项目结构

```
weibo-hot-search/
├── main_hot_topic.py          # 主入口，调度三个模块顺序执行
├── hot_search_collector.py    # 模块一：热搜榜采集器
├── keyword_weibo_spider.py    # 模块二：关键词微博爬取器
├── user_profile_spider.py     # 模块三：用户资料爬取器
├── fix_counts.py              # 工具脚本：补全互动数为0的记录
└── README.md
```

------

## 🗄️ 数据库结构

程序会在首次运行时**自动建表**，无需手动执行 SQL。

| 表名          | 说明                                                   |
| ------------- | ------------------------------------------------------ |
| `hot_search`  | 热搜榜记录（关键词、热度值、标签、排名、采集时间）     |
| `topic_weibo` | 热搜相关微博（正文、点赞/评论/转发数、用户、发布时间） |
| `topic_user`  | 话题参与用户资料（昵称、粉丝数、认证信息等）           |

------

## ⚙️ 环境要求

- Python 3.8+
- MySQL 5.7+ / 8.0+

### 安装依赖

```
pip install requests pymysql beautifulsoup4 schedule
```

------

## 🚀 快速开始

### 第一步：创建数据库

登录 MySQL，执行：

```
CREATE DATABASE weibo DEFAULT CHARACTER SET utf8mb4;
```

### 第二步：获取微博 Cookie

1. 浏览器登录 [weibo.com](https://weibo.com/)
2. 按 `F12` 打开开发者工具 → Network 标签
3. 随意点击一个请求 → Request Headers → 找到 `Cookie` 字段
4. 复制完整的 Cookie 字符串
<img width="1266" height="299" alt="1f03dfc9213016027f4d9d1b7b85bca9" src="https://github.com/user-attachments/assets/b7ab359b-2dc5-4eea-af54-59853c564782" />
需要修改的东西很少 基本上可以直接拿来用，在这里填入你的cookie以及你的数据库 密码就好了


### 第三步：修改配置

打开 `main_hot_topic.py`，修改顶部配置区域：

```
DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "你的数据库密码",   # ← 修改这里
    "database": "weibo",
    "charset":  "utf8mb4",
}

COOKIE = "你的微博Cookie"           # ← 修改这里

TOP_N_KEYWORDS   = 10   # 每轮抓取热搜前 N 个关键词（建议 10~20）
PAGES_PER_KEYWORD = 3   # 每个关键词爬几页（每页约 10 条微博）
INTERVAL_MINUTES = 60   # 定时间隔（分钟），建议不低于 30
```

### 第四步：运行

```
# Windows（推荐，避免 GBK 编码问题）
python -X utf8 main_hot_topic.py

# macOS / Linux
python main_hot_topic.py
```

程序会立即执行第一轮采集，之后按设定间隔自动循环。按 `Ctrl+C` 停止。

------

## 🔧 工具脚本

### fix_counts.py — 补全互动数

若爬取时部分微博的点赞/评论/转发数均为 0（HTML 解析未取到），可运行此脚本，通过微博 API 逐条补全：

```
python -X utf8 fix_counts.py
```

运行前，同样需要在脚本顶部填入有效的 `COOKIE` 和数据库配置。

------

## 📊 采集流程

```
每轮定时任务
    │
    ├─【第一步】热搜榜采集
    │     └─ 请求热搜接口 → 写入 hot_search 表
    │
    ├─【第二步】关键词微博爬取
    │     └─ 对每个热搜词搜索 N 页 → 写入 topic_weibo 表
    │
    └─【第三步】用户资料采集
          └─ 提取新出现的 user_id → 请求用户接口 → 写入 topic_user 表
```

------

## ⚠️ 注意事项

- **Cookie 有效期**：微博 Cookie 通常有效期为数天至数周，失效后需重新获取。程序检测到重定向至登录页时会自动提示。
- **请求频率**：已内置随机延迟（关键词间隔 3 秒、用户请求间隔 1.5 秒、每 50 条暂停 5 秒），请勿擅自调低，以免触发封禁。
- **数据量**：以默认配置（10 词 × 3 页 × 每小时一次）运行，单日可累积数千条微博记录。
- **仅供学习**：本项目仅供学术研究与课程实训使用，请勿用于商业或违法用途，采集数据时请遵守微博[用户协议](https://weibo.com/signup/v5/protocol)及相关法律法规。

