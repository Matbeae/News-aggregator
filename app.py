import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import List
import pytz
from sqlalchemy.orm import declarative_base
import feedparser
from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

try:
    from transformers import pipeline
    TRANSFORMERS_AVAILABLE = True
except Exception:
    TRANSFORMERS_AVAILABLE = False

try:
    import nltk
    nltk.download("punkt", quiet=True)
    from nltk.tokenize import sent_tokenize
    NLTK_AVAILABLE = True
except Exception:
    NLTK_AVAILABLE = False

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup

import config

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
Base = declarative_base()
engine = create_engine(config.DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class Article(Base):
    __tablename__ = "articles"
    id = Column(Integer, primary_key=True, index=True)
    guid = Column(String(500), unique=True, index=True)
    title = Column(String(1000))
    link = Column(String(2000))
    published = Column(DateTime, index=True)
    source = Column(String(300))
    summary = Column(Text)
    short = Column(Text)
    fetched_at = Column(DateTime, default=func.now())

Base.metadata.create_all(bind=engine)

# Summarizer
SUMMARIZER = None
if TRANSFORMERS_AVAILABLE:
    try:
        logger.info("Loading transformers summarization pipeline...")
        SUMMARIZER = pipeline("summarization", model="blanchefort/rubert-base-cased-sentiment")
        logger.info("Transformers summarizer ready.")
    except Exception as e:
        logger.warning("Transformers summarizer failed: %s", e)
        SUMMARIZER = None

def simple_extract_summary(text: str, max_sentences: int = 2) -> str:
    if not text:
        return ""
    if NLTK_AVAILABLE:
        sents = sent_tokenize(text)
        return " ".join(sents[:max_sentences])
    else:
        import re
        sents = re.split(r'(?<=[.!?]) +', text)
        return " ".join(sents[:max_sentences])

def summarise_text(text: str, max_words: int = 60) -> str:
    if not text or len(text.split()) < 5:  # Если текст слишком короткий, возвращаем его как есть
        return text
    if SUMMARIZER:
        try:
            summary = SUMMARIZER(text, max_length=max_words, min_length=20, do_sample=False)
            if isinstance(summary, list):
                return summary[0]["summary_text"].strip()
            return str(summary).strip()
        except Exception:
            return simple_extract_summary(text, max_sentences=2)
    else:
        return simple_extract_summary(text, max_sentences=2)


# Fetch RSS
def fetch_rss_feed(url: str) -> List[dict]:
    logger.info("Fetching RSS: %s", url)
    try:
        parsed = feedparser.parse(url)
        logger.debug(f"Parsed feed: {parsed}")  # Логирование всей RSS-ленты
    except Exception as e:
        logger.error("Failed to parse feed %s: %s", url, e)
        return []

    entries = []
    for e in parsed.entries:
        logger.debug(f"Parsed entry: {e}")  # Логирование каждого элемента в ленте

        guid = e.get("id") or e.get("guid") or e.get("link") or (e.get("title") + str(e.get("published", "")))
        title = e.get("title", "").strip()
        link = e.get("link", "")
        published = None
        if 'published_parsed' in e and e.published_parsed:
            published = datetime.fromtimestamp(time.mktime(e.published_parsed))
            published = published.astimezone(pytz.utc)
        elif 'updated_parsed' in e and e.updated_parsed:
            published = datetime.fromtimestamp(time.mktime(e.updated_parsed))
            published = published.astimezone(pytz.utc)
        else:
            published = datetime.utcnow()

        summary = e.get("summary") or e.get("content")[0].get("value") if e.get("content") else ""
        image = e.media_content[0]["url"] if "media_content" in e else None

        logger.debug(f"Article details: guid={guid}, title={title}, published={published}, summary={summary}")

        entries.append({
            "guid": str(guid),
            "title": title,
            "link": link,
            "published": published,
            "summary": summary,
            "source": url,
            "image": image
        })

    logger.info(f"Found {len(entries)} articles in {url}")
    return entries


# Storage
def save_articles(items: List[dict]):
    new_count = 0
    try:
        with SessionLocal() as db:
            for it in items:
                if not it.get("guid"):
                    logger.warning(f"Skipping article with missing GUID: {it}")
                    continue
                exists = db.query(Article).filter(Article.guid == it["guid"]).first()
                if exists:
                    logger.info(f"Article already exists in DB: {it['guid']}")
                    continue
                full_text = it.get("summary") or it.get("title") or ""
                short = summarise_text(full_text, max_words=60)
                article = Article(
                    guid=it["guid"],
                    title=it.get("title")[:1000],
                    link=it.get("link"),
                    published=it.get("published"),
                    source=it.get("source"),
                    summary=full_text,
                    short=short
                )
                db.add(article)
                new_count += 1
            db.commit()
    except Exception as e:
        logger.exception("Error saving articles: %s", e)
    logger.info(f"Saved {new_count} new articles.")
    return new_count


def get_latest_digest(limit: int = 20) -> List[Article]:
    db: Session = SessionLocal()
    try:
        articles = db.query(Article).order_by(Article.published.desc()).limit(limit).all()
        return articles
    finally:
        db.close()

# Telegram bot
bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

def format_article_for_telegram(a: Article) -> str:
    published = a.published.strftime("%Y-%m-%d %H:%M") if a.published else "?"
    summary = (a.short[:150] + "...") if a.short else (a.summary[:150] + "...")
    text = (
        f"📰 <b>{a.title}</b>\n"
        f"<i>{a.source}</i> — {published}\n"
        f"{summary}\n"
        f"<a href=\"{a.link}\">Читать полностью</a>"
    )
    return text

@dp.callback_query(F.data.startswith("filter:"))
async def process_filter(callback_query):
    _, filter_name = callback_query.data.split(":")
    db: Session = SessionLocal()
    try:
        query = db.query(Article)
        if filter_name == "habr":
            query = query.filter(Article.source.like("%habr%"))
        elif filter_name == "ria":
            query = query.filter(Article.source.like("%ria%"))
        articles = query.order_by(Article.published.desc()).limit(config.MAX_DAILY_ITEMS).all()
    finally:
        db.close()
    if not articles:
        await callback_query.message.edit_text("Нет новостей для выбранного фильтра.")
        return
    text = "\n\n".join([format_article_for_telegram(a) for a in articles[:10]])
    await callback_query.message.edit_text(text, parse_mode="HTML", disable_web_page_preview=False)

# /start
@dp.message(Command(commands=["start"]))
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я — агрегатор новостей.\n"
        "Отправь /digest чтобы получить свежие сводки.\n"
        "Настройки: добавлять/удалять источники пока нельзя в MVP.\n"
        f"По умолчанию бот проверяет RSS каждые {config.FETCH_INTERVAL_SECONDS // 60} минут."
    )

# /digest
@dp.message(Command(commands=["digest"]))
async def cmd_digest(message: Message):
    await message.chat.do("typing")
    articles = get_latest_digest(limit=config.MAX_DAILY_ITEMS)
    if not articles:
        await message.answer("Пока нет сохранённых новостей.")
        return
    # send compact digest
    for a in articles[:10]:
        await message.answer(format_article_for_telegram(a), parse_mode="HTML", disable_web_page_preview=False)
        await asyncio.sleep(0.2)

# /help
@dp.message(Command(commands=["help"]))
async def cmd_help(message: Message):
    await message.answer("/digest — получить свежие новости\n/start — приветствие")

# Admin fetch_now
@dp.message(Command(commands=["fetch_now"]))
async def cmd_fetch_now(message: Message):
    if message.from_user.id not in config.ADMIN_USER_IDS:
        await message.reply("Команда доступна только админам.")
        return
    count = 0
    for feed in config.RSS_FEEDS:
        items = fetch_rss_feed(feed)
        count += save_articles(items)
    await message.reply(f"Фиды обновлены. Новых статей: {count}")

# Background tasks
async def periodic_fetcher(interval_seconds: int = 1800):
    while True:
        logger.info("Aggregator tick: fetching feeds...")
        for feed in config.RSS_FEEDS:
            try:
                items = fetch_rss_feed(feed)
                save_articles(items)
            except Exception as e:
                logger.exception("Error fetching feed %s: %s", feed, e)
        await asyncio.sleep(interval_seconds)

async def scheduled_digest_sender(interval_seconds: int = 60*60):
    while True:
        try:
            logger.info("Scheduled digest: preparing to send to admins...")
            articles = get_latest_digest(limit=config.MAX_DAILY_ITEMS)
            if not articles:
                logger.info("No articles yet.")
            else:
                date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
                header = f"📰 Автоматический дайджест — {date_str} UTC\n\n"
                chunk = header + "\n\n".join([f"• <b>{a.title}</b>\n{a.link}" for a in articles[:10]])
                for admin in config.ADMIN_USER_IDS:
                    try:
                        await bot.send_message(admin, chunk, parse_mode="HTML", disable_web_page_preview=True)
                        await asyncio.sleep(0.2)
                    except Exception as e:
                        logger.exception("Failed to send scheduled digest to %s: %s", admin, e)
            await asyncio.sleep(interval_seconds)
        except Exception as e:
            logger.exception("Error in scheduled digest sender: %s", e)

# Entrypoint
async def main():
    fetch_task = asyncio.create_task(periodic_fetcher(config.FETCH_INTERVAL_SECONDS))
    digest_task = asyncio.create_task(scheduled_digest_sender(interval_seconds=60*60))
    try:
        logger.info("Starting bot polling...")
        await dp.start_polling(bot)
    finally:
        fetch_task.cancel()
        digest_task.cancel()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down.")
