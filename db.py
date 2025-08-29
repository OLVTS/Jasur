# db_pg.py
# Полная версия под Postgres (psycopg2), без os.getenv.

import json
import re
import datetime
from typing import List, Dict, Any, Optional

import psycopg2
import psycopg2.extras


# ────────────────────── подключение ──────────────────────

_DATABASE_URL: Optional[str] = None

def set_dsn(dsn: str) -> None:
    """Задайте DSN перед любыми вызовами БД."""
    global _DATABASE_URL
    _DATABASE_URL = dsn

def _connect():
    if not _DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set. Call set_dsn(dsn) first.")
    conn = psycopg2.connect(_DATABASE_URL, connect_timeout=10)
    conn.autocommit = True
    return conn

def _cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def _ph(n: int) -> str:
    """n плейсхолдеров вида %s,%s,..."""
    return ",".join(["%s"] * n)

# ────────────────────── schema / init ─────────────────────

def init_db():
    """
    Создаём таблицы, добавляем недостающие колонки и функцию clean_num(text).
    """
    conn = _connect()
    cur = _cursor(conn)

    # таблицы
    cur.execute("""
    CREATE TABLE IF NOT EXISTS old_fund (
        object_code   TEXT PRIMARY KEY,
        realtor_code  TEXT,
        orientir      TEXT,
        district      TEXT,
        komnaty       INTEGER,
        ploshad       TEXT,
        etazh         INTEGER,
        etazhnost     INTEGER,
        sanuzly       INTEGER,
        sostoyanie    TEXT,
        material      TEXT,
        parkovka      TEXT,
        dop_info      TEXT,
        price         TEXT,
        photos        TEXT,
        videos        TEXT,
        status        TEXT DEFAULT 'active',
        created_at    TIMESTAMPTZ DEFAULT now(),
        updated_at    TIMESTAMPTZ
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS new_fund (
        object_code   TEXT PRIMARY KEY,
        realtor_code  TEXT,
        orientir      TEXT,
        district      TEXT,
        jk            TEXT,
        year          INTEGER,
        komnaty       INTEGER,
        ploshad       TEXT,
        etazh         INTEGER,
        etazhnost     INTEGER,
        sanuzly       INTEGER,
        sostoyanie    TEXT,
        material      TEXT,
        dop_info      TEXT,
        price         TEXT,
        photos        TEXT,
        videos        TEXT,
        status        TEXT DEFAULT 'active',
        created_at    TIMESTAMPTZ DEFAULT now(),
        updated_at    TIMESTAMPTZ
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS land (
        object_code       TEXT PRIMARY KEY,
        realtor_code      TEXT,
        orientir          TEXT,
        district          TEXT,
        type              TEXT,
        year              INTEGER,
        ploshad_uchastok  TEXT,
        ploshad_dom       TEXT,
        razmer            TEXT,
        etazhnost         INTEGER,
        sanuzly           INTEGER,
        sostoyanie        TEXT,
        material          TEXT,
        zaezd             TEXT,
        dop_info          TEXT,
        price             TEXT,
        photos            TEXT,
        videos            TEXT,
        status            TEXT DEFAULT 'active',
        created_at        TIMESTAMPTZ DEFAULT now(),
        updated_at        TIMESTAMPTZ
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS commerce (
        object_code      TEXT PRIMARY KEY,
        realtor_code     TEXT,
        orientir         TEXT,
        district         TEXT,
        nazna4enie       TEXT,
        raspolozhenie    TEXT,
        etazh            INTEGER,
        etazhnost        INTEGER,
        ploshad_pom      TEXT,
        ploshad_uchastok TEXT,
        nds              TEXT,
        owner            TEXT,
        dop_info         TEXT,
        price            TEXT,
        photos           TEXT,
        videos           TEXT,
        status           TEXT DEFAULT 'active',
        created_at       TIMESTAMPTZ DEFAULT now(),
        updated_at       TIMESTAMPTZ
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS client_secondary (
        id             BIGSERIAL PRIMARY KEY,
        date           TIMESTAMPTZ DEFAULT now(),
        user_id        BIGINT,
        phone          TEXT,
        username       TEXT,
        telegram_name  TEXT,
        client_name    TEXT,
        object_code    TEXT,
        realtor_code   TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS client_base (
        id            BIGSERIAL PRIMARY KEY,
        date          TIMESTAMPTZ NOT NULL,
        user_id       BIGINT UNIQUE NOT NULL,
        phone         TEXT,
        username      TEXT,
        telegram_name TEXT,
        client_name   TEXT
    );
    """)

    # добиваем дополнительные поля (если их нет)
    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS order_type   TEXT;")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS repost_date  TIMESTAMPTZ;")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS message_ids  TEXT;")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS old_price    TEXT;")
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS initial_price TEXT;")
    cur.execute("ALTER TABLE commerce ADD COLUMN IF NOT EXISTS owner TEXT;")
    cur.execute("ALTER TABLE commerce ADD COLUMN IF NOT EXISTS nds   TEXT;")

    # функция clean_num(text) → numeric (то же самое, что была в SQLite, только на SQL)
    cur.execute("""
    CREATE OR REPLACE FUNCTION clean_num(inp TEXT)
    RETURNS NUMERIC AS $$
      SELECT NULLIF(
               regexp_replace(replace(inp, ',', '.'), '[^0-9\.]', '', 'g'),
               ''
             )::numeric;
    $$ LANGUAGE SQL IMMUTABLE;
    """)

    cur.close()
    conn.close()


# ────────────────────── утилиты ─────────────────────

def _json_dump(value) -> str:
    return json.dumps(value or [], ensure_ascii=False)

def _parse_json(text: Optional[str]):
    return json.loads(text) if text else []

def _now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()

def _map_common_media(d: Dict[str, Any]) -> None:
    d["photos"] = _parse_json(d.get("photos"))
    d["videos"] = _parse_json(d.get("videos"))

# ────────────────────── AUTO-ID ─────────────────────

def next_object_code() -> str:
    """
    Следующий свободный код (максимум по всем таблицам + 1).
    Считаем только те object_code, которые состоят из цифр.
    """
    conn = _connect(); cur = _cursor(conn)
    max_code = 0
    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(
            f"SELECT MAX(object_code::int) AS mx "
            f"FROM {tbl} WHERE object_code ~ '^[0-9]+$';"
        )
        row = cur.fetchone()
        if row and row["mx"] is not None:
            max_code = max(max_code, int(row["mx"]))
    cur.close(); conn.close()
    return str(max_code + 1)

# ────────────────────── INSERT/UPSERT ─────────────────────

def insert_into_old_fund(data: dict):
    conn = _connect(); cur = _cursor(conn)
    cols = ("object_code","realtor_code","orientir","district","komnaty",
            "ploshad","etazh","etazhnost","sanuzly","sostoyanie",
            "material","parkovka","dop_info","price","initial_price",
            "photos","videos","message_ids","status","order_type")
    vals = (
        data.get("object_code"),
        data.get("realtor_code"),
        data.get("Ориентир"),
        data.get("district"),
        data.get("Комнаты"),
        data.get("Площадь"),
        data.get("Этаж"),
        data.get("Этажность"),
        data.get("Санузлы"),
        data.get("Состояние"),
        data.get("Материал строения"),
        data.get("Парковка"),
        data.get("Дополнительно"),
        data.get("Цена"),
        data.get("Цена"),
        _json_dump(data.get("photos")),
        _json_dump(data.get("videos")),
        _json_dump(data.get("message_ids")),
        "active",
        data.get("Тип заявки"),
    )
    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "object_code"])
    cur.execute(
        f"INSERT INTO old_fund ({','.join(cols)}) VALUES ({_ph(len(cols))}) "
        f"ON CONFLICT (object_code) DO UPDATE SET {updates};",
        vals
    )
    cur.close(); conn.close()

def insert_into_new_fund(data: dict):
    conn = _connect(); cur = _cursor(conn)
    cols = ("object_code","realtor_code","orientir","district","jk","year",
            "komnaty","ploshad","etazh","etazhnost","sanuzly",
            "sostoyanie","material","dop_info","price","initial_price",
            "photos","videos","message_ids","status","order_type")
    vals = (
        data.get("object_code"),
        data.get("realtor_code"),
        data.get("Ориентир"),
        data.get("district"),
        data.get("ЖК"),
        data.get("Год постройки"),
        data.get("Комнаты"),
        data.get("Площадь"),
        data.get("Этаж"),
        data.get("Этажность"),
        data.get("Санузлы"),
        data.get("Состояние"),
        data.get("Материал строения"),
        data.get("Дополнительно"),
        data.get("Цена"),
        data.get("Цена"),
        _json_dump(data.get("photos")),
        _json_dump(data.get("videos")),
        _json_dump(data.get("message_ids")),
        "active",
        data.get("Тип заявки"),
    )
    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "object_code"])
    cur.execute(
        f"INSERT INTO new_fund ({','.join(cols)}) VALUES ({_ph(len(cols))}) "
        f"ON CONFLICT (object_code) DO UPDATE SET {updates};",
        vals
    )
    cur.close(); conn.close()

def insert_into_land(data: dict):
    conn = _connect(); cur = _cursor(conn)
    cols = ("object_code","realtor_code","orientir","district","type","year",
            "ploshad_uchastok","ploshad_dom","razmer","etazhnost",
            "sanuzly","sostoyanie","material","zaezd","dop_info",
            "price","initial_price","photos","videos","message_ids","status","order_type")
    vals = (
        data.get("object_code"),
        data.get("realtor_code"),
        data.get("Ориентир"),
        data.get("district"),
        data.get("Тип недвижимости"),
        data.get("Год постройки"),
        data.get("Площадь участка"),
        data.get("Площадь дома"),
        data.get("Размер участка"),
        data.get("Этажность"),
        data.get("Санузлы"),
        data.get("Состояние"),
        data.get("Материал строения"),
        data.get("Заезд авто"),
        data.get("Дополнительно"),
        data.get("Цена"),
        data.get("Цена"),
        _json_dump(data.get("photos")),
        _json_dump(data.get("videos")),
        _json_dump(data.get("message_ids")),
        "active",
        data.get("Тип заявки"),
    )
    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "object_code"])
    cur.execute(
        f"INSERT INTO land ({','.join(cols)}) VALUES ({_ph(len(cols))}) "
        f"ON CONFLICT (object_code) DO UPDATE SET {updates};",
        vals
    )
    cur.close(); conn.close()

def insert_into_commerce(data: dict):
    conn = _connect(); cur = _cursor(conn)
    purpose = data.get("Целевое назначение")
    if isinstance(purpose, list):
        purpose = ", ".join(purpose)
    cols = ("object_code","realtor_code","orientir","district","nazna4enie",
            "raspolozhenie","etazh","etazhnost","ploshad_pom","ploshad_uchastok",
            "nds","owner","dop_info","price","initial_price",
            "photos","videos","message_ids","status","order_type")
    vals = (
        data.get("object_code"),
        data.get("realtor_code"),
        data.get("Ориентир"),
        data.get("district"),
        purpose or "",
        data.get("Расположение"),
        data.get("Этаж"),
        data.get("Этажность"),
        data.get("Площадь помещения"),
        data.get("Площадь участка"),
        data.get("Учёт НДС"),
        data.get("Собственник"),
        data.get("Дополнительно"),
        data.get("Цена"),
        data.get("Цена"),
        _json_dump(data.get("photos")),
        _json_dump(data.get("videos")),
        _json_dump(data.get("message_ids")),
        "active",
        data.get("Тип заявки"),
    )
    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "object_code"])
    cur.execute(
        f"INSERT INTO commerce ({','.join(cols)}) VALUES ({_ph(len(cols))}) "
        f"ON CONFLICT (object_code) DO UPDATE SET {updates};",
        vals
    )
    cur.close(); conn.close()

# ────────────────────── UPDATE helpers ─────────────────────

def update_price_old_fund(object_code: str, new_price: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE old_fund
           SET price=%s, updated_at=now()
         WHERE object_code=%s;
    """, (new_price, object_code))
    cur.close(); conn.close()

def update_price_new_fund(object_code: str, new_price: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE new_fund
           SET price=%s, updated_at=now()
         WHERE object_code=%s;
    """, (new_price, object_code))
    cur.close(); conn.close()

def update_price_land(object_code: str, new_price: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE land
           SET price=%s, updated_at=now()
         WHERE object_code=%s;
    """, (new_price, object_code))
    cur.close(); conn.close()

def update_price_commerce(object_code: str, new_price: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE commerce
           SET price=%s, updated_at=now()
         WHERE object_code=%s;
    """, (new_price, object_code))
    cur.close(); conn.close()

def mark_inactive_old_fund(object_code: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE old_fund
           SET status='inactive', updated_at=now()
         WHERE object_code=%s;
    """, (object_code,))
    cur.close(); conn.close()

def mark_inactive_new_fund(object_code: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE new_fund
           SET status='inactive', updated_at=now()
         WHERE object_code=%s;
    """, (object_code,))
    cur.close(); conn.close()

def mark_inactive_land(object_code: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE land
           SET status='inactive', updated_at=now()
         WHERE object_code=%s;
    """, (object_code,))
    cur.close(); conn.close()

def mark_inactive_commerce(object_code: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE commerce
           SET status='inactive', updated_at=now()
         WHERE object_code=%s;
    """, (object_code,))
    cur.close(); conn.close()

def drop_price_old_fund(object_code: str, new_price: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE old_fund
           SET old_price=price, price=%s, updated_at=now()
         WHERE object_code=%s;
    """, (new_price, object_code))
    cur.close(); conn.close()

def drop_price_new_fund(object_code: str, new_price: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE new_fund
           SET old_price=price, price=%s, updated_at=now()
         WHERE object_code=%s;
    """, (new_price, object_code))
    cur.close(); conn.close()

def drop_price_land(object_code: str, new_price: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE land
           SET old_price=price, price=%s, updated_at=now()
         WHERE object_code=%s;
    """, (new_price, object_code))
    cur.close(); conn.close()

def drop_price_commerce(object_code: str, new_price: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        UPDATE commerce
           SET old_price=price, price=%s, updated_at=now()
         WHERE object_code=%s;
    """, (new_price, object_code))
    cur.close(); conn.close()

# ────────────────────── SEARCH helpers ─────────────────────

def _apply_in(sql: str, params: list, column: str, values: Optional[list]) -> str:
    if values:
        sql += f" AND {column} IN ({_ph(len(values))})"
        params.extend(values)
    return sql

def _apply_range(sql: str, params: list, column: str, lo_key: str, hi_key: str, filters: dict) -> str:
    lo, hi = filters.get(lo_key), filters.get(hi_key)
    if lo is not None:
        sql += f" AND clean_num({column}) >= %s"
        params.append(lo)
    if hi is not None:
        sql += f" AND clean_num({column}) <= %s"
        params.append(hi)
    return sql

# ────────────────────── SEARCH: старый фонд ─────────────────

def search_old_fund(**filters) -> List[Dict[str, Any]]:
    conn = _connect(); cur = _cursor(conn)
    sql = """
    SELECT object_code, realtor_code, orientir, district, komnaty, ploshad,
           etazh, etazhnost, sostoyanie, price, dop_info, photos, videos
      FROM old_fund WHERE status='active'
    """
    params: list[Any] = []

    sql = _apply_in(sql, params, "district", filters.get("region"))
    sql = _apply_in(sql, params, "sostoyanie", filters.get("condition"))

    rooms = filters.get("rooms")
    if rooms:
        nums = [r for r in rooms if r != "4+"]
        four = "4+" in rooms
        subs, subp = [], []
        if nums:
            subs.append(f"komnaty IN ({_ph(len(nums))})")
            subp.extend(list(map(int, nums)))
        if four:
            subs.append("komnaty >= 4")
        if subs:
            sql += " AND (" + " OR ".join(subs) + ")"
            params.extend(subp)

    for col, lo, hi in (
        ("ploshad","area_min","area_max"),
        ("etazh","floor_min","floor_max"),
        ("etazhnost","floors_total_min","floors_total_max"),
        ("price","price_min","price_max"),
    ):
        sql = _apply_range(sql, params, col, lo, hi, filters)

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()

    res: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        mapping = {
            "orientir":"Ориентир",
            "komnaty":"Комнаты",
            "ploshad":"Площадь",
            "etazh":"Этаж",
            "etazhnost":"Этажность",
            "sostoyanie":"Состояние",
            "price":"Цена",
            "dop_info":"Дополнительно",
        }
        for db_key, ru_key in mapping.items():
            if db_key in d and d[db_key] is not None:
                d[ru_key] = d.pop(db_key)
        if "district" in d:
            d["Район"] = d.pop("district")
        _map_common_media(d)
        d["ptype"] = "Старыйфонд"
        res.append(d)
    return res

# ────────────────────── SEARCH: новый фонд ─────────────────

def search_new_fund(**filters) -> List[Dict[str, Any]]:
    conn = _connect(); cur = _cursor(conn)
    sql = """
    SELECT object_code, realtor_code, orientir, district, jk, year,
           komnaty, ploshad, etazh, etazhnost, sostoyanie,
           price, dop_info, photos, videos
      FROM new_fund WHERE status='active'
    """
    params: list[Any] = []

    sql = _apply_in(sql, params, "district", filters.get("region"))
    sql = _apply_in(sql, params, "sostoyanie", filters.get("condition"))

    rooms = filters.get("rooms")
    if rooms:
        nums = [r for r in rooms if r != "4+"]
        four = "4+" in rooms
        subs, subp = [], []
        if nums:
            subs.append(f"komnaty IN ({_ph(len(nums))})")
            subp.extend(list(map(int, nums)))
        if four:
            subs.append("komnaty >= 4")
        if subs:
            sql += " AND (" + " OR ".join(subs) + ")"
            params.extend(subp)

    for col, lo, hi in (
        ("ploshad","area_min","area_max"),
        ("etazh","floor_min","floor_max"),
        ("etazhnost","floors_total_min","floors_total_max"),
        ("price","price_min","price_max"),
    ):
        sql = _apply_range(sql, params, col, lo, hi, filters)

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()

    res: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        mapping = {
            "orientir":"Ориентир",
            "komnaty":"Комнаты",
            "ploshad":"Площадь",
            "etazh":"Этаж",
            "etazhnost":"Этажность",
            "sostoyanie":"Состояние",
            "price":"Цена",
            "dop_info":"Дополнительно",
        }
        for db_key, ru_key in mapping.items():
            if db_key in d and d[db_key] is not None:
                d[ru_key] = d.pop(db_key)
        if "district" in d:
            d["Район"] = d.pop("district")
        _map_common_media(d)
        d["ptype"] = "Новыйфонд"
        res.append(d)
    return res

# ────────────────────── SEARCH: участок ─────────────────

def search_land(**filters) -> List[Dict[str, Any]]:
    conn = _connect(); cur = _cursor(conn)
    sql = """
    SELECT object_code, realtor_code, orientir, district, type, year,
           ploshad_dom, ploshad_uchastok, razmer, etazhnost, sanuzly,
           sostoyanie, material, zaezd, price, dop_info, photos, videos
      FROM land WHERE status='active'
    """
    params: list[Any] = []

    sql = _apply_in(sql, params, "district",  filters.get("region"))
    sql = _apply_in(sql, params, "type",      filters.get("landtype"))
    sql = _apply_in(sql, params, "sostoyanie",filters.get("condition"))

    sql = _apply_range(sql, params, "ploshad_uchastok","area_min","area_max",filters)
    sql = _apply_range(sql, params, "price","price_min","price_max",filters)

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()

    res: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        mapping = {
            "orientir":"Ориентир",
            "type":"Тип недвижимости",
            "ploshad_dom":"Площадь дома",
            "ploshad_uchastok":"Площадь участка",
            "razmer":"Размер участка",
            "etazhnost":"Этажность",
            "sanuzly":"Санузлы",
            "sostoyanie":"Состояние",
            "material":"Материал строения",
            "zaezd":"Заезд авто",
            "price":"Цена",
            "dop_info":"Дополнительно",
        }
        for db_key, ru_key in mapping.items():
            if db_key in d and d[db_key] is not None:
                d[ru_key] = d.pop(db_key)
        if "district" in d:
            d["Район"] = d.pop("district")
        _map_common_media(d)
        d["ptype"] = "Участок"
        res.append(d)
    return res

# ────────────────────── SEARCH: коммерция ────────────────

def search_commerce(**filters) -> List[Dict[str, Any]]:
    conn = _connect(); cur = _cursor(conn)
    sql = """
    SELECT object_code, realtor_code, orientir, district, nazna4enie, raspolozhenie,
           etazh, etazhnost, ploshad_pom, ploshad_uchastok, nds,
           price, dop_info, photos, videos
      FROM commerce WHERE status='active'
    """
    params: list[Any] = []

    sql = _apply_in(sql, params, "district",  filters.get("region"))
    sql = _apply_in(sql, params, "nazna4enie",filters.get("purpose"))
    sql = _apply_range(sql, params, "price","price_min","price_max",filters)

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()

    res: List[Dict[str, Any]] = []
    mapping = {
        "orientir":"Ориентир",
        "nazna4enie":"Целевое назначение",
        "raspolozhenie":"Расположение",
        "etazh":"Этаж",
        "etazhnost":"Этажность",
        "ploshad_pom":"Площадь помещения",
        "ploshad_uchastok":"Площадь участка",
        "nds":"Учёт НДС",
        "price":"Цена",
        "dop_info":"Дополнительно",
    }
    for r in rows:
        d = dict(r)
        for db_key, ru_key in mapping.items():
            if db_key in d and d[db_key] is not None:
                d[ru_key] = d.pop(db_key)
        if "district" in d:
            d["Район"] = d.pop("district")
        _map_common_media(d)
        d["ptype"] = "Коммерция"
        res.append(d)
    return res

# ────────────────────── clients ───────────────────────────

def insert_client_secondary(
    user_id: int,
    phone: str,
    username: str,
    telegram_name: str,
    client_name: str,
    object_code: str,
    realtor_code: str
):
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        INSERT INTO client_secondary (
            user_id, phone, username, telegram_name,
            client_name, object_code, realtor_code
        ) VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (user_id, phone, username, telegram_name, client_name, object_code, realtor_code))
    cur.close(); conn.close()

def get_last_client_secondary(user_id: int) -> Optional[tuple[str, str]]:
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        SELECT client_name, phone
          FROM client_secondary
         WHERE user_id=%s
         ORDER BY date DESC
         LIMIT 1;
    """, (user_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return (row["client_name"], row["phone"]) if row else None

def get_client_base(user_id: int) -> Optional[Dict[str, Any]]:
    conn = _connect(); cur = _cursor(conn)
    cur.execute("SELECT * FROM client_base WHERE user_id=%s;", (user_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return dict(row) if row else None

def upsert_client_base(
    user_id: int,
    phone: str,
    username: str,
    telegram_name: str,
    client_name: str
):
    now = _now_iso()
    conn = _connect(); cur = _cursor(conn)
    cur.execute("""
        INSERT INTO client_base (date, user_id, phone, username, telegram_name, client_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
            phone=EXCLUDED.phone,
            username=EXCLUDED.username,
            telegram_name=EXCLUDED.telegram_name,
            client_name=EXCLUDED.client_name,
            date=EXCLUDED.date;
    """, (now, user_id, phone, username, telegram_name, client_name))
    cur.close(); conn.close()


# ────────────────────── misc ──────────────────────────────

def _clean_number(expr: str) -> str:
    """Удаляет всё, кроме цифр. (Может пригодиться снаружи.)"""
    if not expr:
        return ""
    return re.sub(r"[^\d]", "", expr)
