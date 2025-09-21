# config.py
TELEGRAM_BOT_TOKEN = "токен бота"
ADMIN_USER_IDS = [830689820]  # список id админов (для тестирования)
RSS_FEEDS = [
    "https://habr.com/ru/rss/all/all/?fl=ru",
    "https://ria.ru/export/rss2/archive/index.xml"
]
FETCH_INTERVAL_SECONDS = 10
MAX_DAILY_ITEMS = 20  # сколько новостей в дайджесте
DATABASE_URL = "sqlite:///news.db"
