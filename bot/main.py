import asyncpg
import httpx
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
import os

BOT_TOKEN = os.environ.get("BOT_TOKEN")
GIGACHAT_API_KEY = os.environ.get("GIGACHAT_API_KEY")

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

База данных содержит:

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

Правила генерации:

1. Разрешён только SELECT.
2. Запрос должен возвращать ОДНО число.
3. Никаких текстовых пояснений.
4. Если используется SUM — обязательно оборачивать в COALESCE(..., 0).
5. Если нужно количество видео — использовать COUNT(DISTINCT videos.id) И ВСЕГДА указывать FROM videos.
6. Для роста просмотров использовать SUM(delta_views_count) И ВСЕГДА FROM video_snapshots.
7. Для роста лайков использовать SUM(delta_likes_count) И ВСЕГДА FROM video_snapshots.
8. Для фильтрации по конкретному дню использовать:
   created_at::date = 'YYYY-MM-DD'
9. Для диапазона дат использовать:
   created_at::date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
10. Если речь про публикацию видео — использовать video_created_at FROM videos.
11. Если речь про приросты или события по дням — использовать video_snapshots.created_at FROM video_snapshots.
12. Нельзя использовать INSERT, UPDATE, DELETE, DROP.
13. ВСЕГДА включай таблицу в FROM. Нельзя пропускать FROM.
14. Всегда завершай запрос точкой с запятой.

Примеры корректных ответов:

SELECT COUNT(*) FROM videos;

SELECT COUNT(*) FROM videos WHERE views_count > 100000;

SELECT COALESCE(SUM(delta_views_count), 0)
FROM video_snapshots
WHERE created_at::date = '2025-11-28';

SELECT COUNT(DISTINCT video_id)
FROM video_snapshots
WHERE delta_views_count > 0
AND created_at::date = '2025-11-27';
"""


async def generate_sql(question: str) -> str:
    async with httpx.AsyncClient(verify=False, timeout=15.0) as client:
        resp = await client.post(
            "https://gigachat.devices.sberbank.ru/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {GIGACHAT_API_KEY}"},
            json={
                "model": "GigaChat",
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": question}
                ]
            },
        )
    data = resp.json()
    if "choices" not in data:
        return f"Ошибка от GigaChat: {data}"
    return data["choices"][0]["message"]["content"]


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    question = update.message.text
    sql = await generate_sql(question)

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


async def init_db(app):
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


def main():
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(init_db)
        .build()
    )

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    app.run_polling()


if __name__ == "__main__":
    main()
