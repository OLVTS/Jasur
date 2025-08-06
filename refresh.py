# ─── weekly_refresh.py ─────────────────────────────────────────────
import asyncio, json, sqlite3, logging
from datetime import timedelta
from telegram import Bot
from telegram.ext import ContextTypes

DB_FILE = "realty.db"
TABLES  = ("old_fund", "new_fund", "land", "commerce")
CHUNK   = 25        # не более 25 запросов в секунду (лимит Bot API)

async def refresh_file_ids(context: ContextTypes.DEFAULT_TYPE) -> None:
    bot: Bot = context.bot
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()

    for tbl in TABLES:
        cur.execute(f"SELECT object_code, photos, videos FROM {tbl} "
                    "WHERE status='active'")
        for code, ph_json, vid_json in cur.fetchall():
            photos = json.loads(ph_json or "[]")
            videos = json.loads(vid_json or "[]")
            new_ph, new_vid = [], []
            updated = False

            # -- фото --
            for fid in photos:
                try:
                    f = await bot.get_file(fid)       # освежаем reference
                    new_ph.append(f.file_id)
                    updated |= (f.file_id != fid)
                except Exception as e:
                    logging.warning("↯ %s photo %s: %s", code, fid, e)

            # -- видео --
            for fid in videos:
                try:
                    f = await bot.get_file(fid)
                    new_vid.append(f.file_id)
                    updated |= (f.file_id != fid)
                except Exception as e:
                    logging.warning("↯ %s video %s: %s", code, fid, e)

            # -- обновляем БД, если что-то поменялось --
            if updated:
                cur.execute(f"UPDATE {tbl} SET photos=?, videos=? "
                            "WHERE object_code=?",
                            (json.dumps(new_ph), json.dumps(new_vid), code))
        await asyncio.sleep(1 / CHUNK)   # щадим лимиты

    conn.commit()
    conn.close()
    logging.info("Weekly refresh done")


