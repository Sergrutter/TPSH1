import asyncpg
import asyncio
import json
import os
from datetime import datetime

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ.get("DB_NAME", "videos_db")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Serg77")


def parse_datetime(value: str):
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def create_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id UUID PRIMARY KEY,
            creator_id TEXT,
            video_created_at TIMESTAMPTZ,
            views_count INTEGER,
            likes_count INTEGER,
            comments_count INTEGER,
            reports_count INTEGER,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS video_snapshots (
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
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)


async def main():
    conn = await asyncpg.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    await create_tables(conn)

    with open("data/videos.json", "r", encoding="utf-8") as f:
        videos = json.load(f)["videos"]

    for video in videos:
        await conn.execute(
            """
            INSERT INTO videos(id, creator_id, video_created_at, views_count, likes_count, 
                               comments_count, reports_count, created_at, updated_at)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT(id) DO NOTHING
            """,
            video["id"],
            video["creator_id"],
            parse_datetime(video["video_created_at"]),
            video["views_count"],
            video["likes_count"],
            video["comments_count"],
            video["reports_count"],
            parse_datetime(video["created_at"]),
            parse_datetime(video["updated_at"]),
        )

        for snap in video.get("snapshots", []):
            await conn.execute(
                """
                INSERT INTO video_snapshots(id, video_id, views_count, likes_count, comments_count,
                                            reports_count, delta_views_count, delta_likes_count,
                                            delta_comments_count, delta_reports_count, created_at, updated_at)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT(id) DO NOTHING
                """,
                snap["id"],
                snap["video_id"],
                snap["views_count"],
                snap["likes_count"],
                snap["comments_count"],
                snap["reports_count"],
                snap["delta_views_count"],
                snap["delta_likes_count"],
                snap["delta_comments_count"],
                snap["delta_reports_count"],
                parse_datetime(snap["created_at"]),
                parse_datetime(snap["updated_at"]),
            )

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
