import asyncpg
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
import os
from gigachat import GigaChat

BOT_TOKEN = os.environ.get("BOT_TOKEN")
GIGACHAT_CREDENTIALS = os.environ.get("GIGACHAT_CREDENTIALS")

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ.get("DB_NAME", "videos_db")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Serg77")

SYSTEM_PROMPT = """Ты — генератор SQL-запросов для PostgreSQL.

Тебе приходит вопрос на русском языке.
Ты должен вернуть ТОЛЬКО один SQL-запрос.
Без пояснений. Без комментариев. Без текста.
Только один SELECT.

База данных содержит только эти таблицы и поля:

TABLE videos (
    id UUID PRIMARY KEY,
    creator_id TEXT,
    video_created_at TIMESTAMPTZ,
    views_count INTEGER,
    likes_count INTEGER,
    comments_count INTEGER,
    reports_count INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

TABLE video_snapshots (
    id UUID PRIMARY KEY,
    video_id UUID REFERENCES videos(id),
    views_count INTEGER,
    likes_count INTEGER,
    comments_count INTEGER,
    reports_count INTEGER,
    delta_views_count INTEGER,
    delta_likes_count INTEGER,
    delta_comments_count INTEGER,
    delta_reports_count INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

Связь:
videos.id = video_snapshots.video_id

ВАЖНО: Используй только точные имена таблиц и полей, как указано выше. Никаких других таблиц и колонок не использовать.  

Правила генерации:

1. Разрешён только SELECT.
2. Запрос должен возвращать ОДНО число.
3. Никаких текстовых пояснений.
4. Если используется SUM — обязательно оборачивать в COALESCE(..., 0).
5. Для количества видео — COUNT(DISTINCT videos.id) И ВСЕГДА FROM videos.
6. Для прироста просмотров — SUM(delta_views_count) И ВСЕГДА FROM video_snapshots.
7. Для прироста лайков — SUM(delta_likes_count) И ВСЕГДА FROM video_snapshots.
8. Для прироста комментариев — SUM(delta_comments_count) И ВСЕГДА FROM video_snapshots.
9. Для прироста репортов — SUM(delta_reports_count) И ВСЕГДА FROM video_snapshots.
10. Общее количество лайков всех видео берётся из SUM(likes_count) И ВСЕГДА FROM videos.
11. Для фильтрации по конкретному дню используйте: created_at::date = 'YYYY-MM-DD'
12. Для диапазона дат используйте: created_at::date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
13. Если речь про дату публикации видео — использовать video_created_at FROM videos.
14. Если речь про приросты или события по дням — использовать video_snapshots.created_at FROM video_snapshots.
15. Нельзя использовать INSERT, UPDATE, DELETE, DROP.
16. ВСЕГДА включай таблицу в FROM.
17. Всегда завершай запрос точкой с запятой.

Примеры корректных запросов:

SELECT COUNT(*) FROM videos;

SELECT COUNT(*) FROM videos WHERE views_count > 100000;

SELECT COALESCE(SUM(delta_views_count), 0)
FROM video_snapshots
WHERE created_at::date = '2025-11-28';

SELECT COALESCE(SUM(delta_likes_count), 0)
FROM video_snapshots
WHERE created_at::date = '2025-11-28';

SELECT COALESCE(SUM(likes_count), 0)
FROM videos;

SELECT COUNT(DISTINCT video_id)
FROM video_snapshots
WHERE delta_views_count > 0
AND created_at::date = '2025-11-27';

Важно для приростов по времени после публикации видео:

- Если вопрос про прирост просмотров, лайков, комментариев или репортов в первые N часов после публикации каждого видео, используй video_snapshots и связывай с videos через video_id.
- Фильтруй записи так, чтобы snapshot.created_at был между video_created_at и video_created_at + INTERVAL 'N hours'.
- N часов берётся из текста вопроса (например: 'первые 3 часа' → INTERVAL '3 hours').
- Примеры:

  Для прироста комментариев за первые 3 часа после публикации каждого видео:

  SELECT COALESCE(SUM(s.delta_comments_count), 0)
  FROM video_snapshots s
  JOIN videos v ON s.video_id = v.id
  WHERE s.created_at BETWEEN v.video_created_at AND v.video_created_at + INTERVAL '3 hours';
"""


async def generate_sql(question: str, context) -> str:
    giga = context.application.bot_data["giga"]

    loop = asyncio.get_running_loop()

    response = await loop.run_in_executor(
        None,
        lambda: giga.chat(
            SYSTEM_PROMPT + "\n\nВопрос:\n" + question
        )
    )

    return response.choices[0].message.content.strip()


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    question = update.message.text
    sql = await generate_sql(question, context)

    if not sql.lower().startswith("select"):
        await update.message.reply_text("Ошибка запроса")
        return

    pool = context.application.bot_data["db_pool"]

    try:
        async with pool.acquire() as conn:
            result = await conn.fetchval(sql)
    except Exception as e:
        await update.message.reply_text(f"Ошибка в SQL: {e}")
        return

    await update.message.reply_text(str(result))


async def init_services(app):
    pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        min_size=1,
        max_size=10
    )
    app.bot_data["db_pool"] = pool

    giga = GigaChat(credentials=GIGACHAT_CREDENTIALS, verify_ssl_certs=False)
    app.bot_data["giga"] = giga


def main():
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(init_services)
        .build()
    )

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling()


if __name__ == "__main__":
    main()