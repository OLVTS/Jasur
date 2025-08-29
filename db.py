# db_pg.py
# Полная версия под Postgres (psycopg2), без os.getenv.

import json
import re
import datetime
from typing import List, Dict, Any, Optional, Tuple, Union

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

def _execute(sql: str, params: tuple | list | None = None) -> None:
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(sql, params or ())
        cur.close()
    finally:
        conn.close()

def _query(sql: str, params: tuple | list | None = None):
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


def _ph(n: int) -> str:
    """n плейсхолдеров вида %s,%s,..."""
    return ",".join(["%s"] * n)

def list_employees() -> dict[int, str]:
    rows = _query("SELECT user_id, name FROM employees ORDER BY user_id")
    return {int(r["user_id"]): (r["name"] or "") for r in rows}

def get_employee_name(user_id: int) -> Optional[str]:
    rows = _query("SELECT name FROM employees WHERE user_id = %s", (user_id,))
    return (rows[0]["name"] if rows else None)

def upsert_employee(user_id: int, name: str) -> None:
    _execute("""
        INSERT INTO employees (user_id, name)
        VALUES (%s, %s)
        ON CONFLICT (user_id) DO UPDATE SET name = EXCLUDED.name
    """, (user_id, name or ""))

def delete_employee(user_id: int) -> None:
    _execute("DELETE FROM employees WHERE user_id = %s", (user_id,))

# ────────────────────── schema / init ─────────────────────

def init_db():
    """
    Создаём таблицы, добавляем недостающие колонки и функцию clean_num(text).
    """
    conn = _connect()
    cur = _cursor(conn)

# --- ACL: сотрудники с доступом ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employees (
      user_id    BIGINT PRIMARY KEY,
      name       TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)
    
    # триггер для updated_at
    cur.execute("""
    CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = now();
      RETURN NEW;
    END; $$ LANGUAGE plpgsql;
    """)
    cur.execute("DROP TRIGGER IF EXISTS trg_employees_updated_at ON employees;")
    cur.execute("""
    CREATE TRIGGER trg_employees_updated_at
    BEFORE UPDATE ON employees
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    """)


    # таблицы
    cur.execute("""
    CREATE TABLE IF NOT EXISTS old_fund (
        object_code        TEXT PRIMARY KEY,
        realtor_code       TEXT,
        orientir           TEXT,
        district           TEXT,
        komnaty            INTEGER,
        ploshad            TEXT,
        etazh              INTEGER,
        etazhnost          INTEGER,
        sanuzly            INTEGER,
        sostoyanie         TEXT,
        material           TEXT,
        parkovka           TEXT,
        dop_info           TEXT,
        price              TEXT,
        photos             TEXT,
        videos             TEXT,
        status             TEXT DEFAULT 'active',
        created_at         TIMESTAMPTZ DEFAULT now(),
        updated_at         TIMESTAMPTZ,
        order_type         TEXT,
        repost_date        TIMESTAMPTZ,
        message_ids        TEXT,
        old_price          TEXT,
        initial_price      TEXT,
        channel_message_id BIGINT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS new_fund (
        object_code        TEXT PRIMARY KEY,
        realtor_code       TEXT,
        orientir           TEXT,
        district           TEXT,
        jk                 TEXT,
        year               INTEGER,
        komnaty            INTEGER,
        ploshad            TEXT,
        etazh              INTEGER,
        etazhnost          INTEGER,
        sanuzly            INTEGER,
        sostoyanie         TEXT,
        material           TEXT,
        dop_info           TEXT,
        price              TEXT,
        photos             TEXT,
        videos             TEXT,
        status             TEXT DEFAULT 'active',
        created_at         TIMESTAMPTZ DEFAULT now(),
        updated_at         TIMESTAMPTZ,
        order_type         TEXT,
        repost_date        TIMESTAMPTZ,
        message_ids        TEXT,
        old_price          TEXT,
        initial_price      TEXT,
        channel_message_id BIGINT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS land (
        object_code        TEXT PRIMARY KEY,
        realtor_code       TEXT,
        orientir           TEXT,
        district           TEXT,
        type               TEXT,
        year               INTEGER,
        ploshad_uchastok   TEXT,
        ploshad_dom        TEXT,
        razmer             TEXT,
        etazhnost          INTEGER,
        sanuzly            INTEGER,
        sostoyanie         TEXT,
        material           TEXT,
        zaezd              TEXT,
        dop_info           TEXT,
        price              TEXT,
        photos             TEXT,
        videos             TEXT,
        status             TEXT DEFAULT 'active',
        created_at         TIMESTAMPTZ DEFAULT now(),
        updated_at         TIMESTAMPTZ,
        order_type         TEXT,
        repost_date        TIMESTAMPTZ,
        message_ids        TEXT,
        old_price          TEXT,
        initial_price      TEXT,
        channel_message_id BIGINT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS commerce (
        object_code        TEXT PRIMARY KEY,
        realtor_code       TEXT,
        orientir           TEXT,
        district           TEXT,
        nazna4enie         TEXT,
        raspolozhenie      TEXT,
        etazh              INTEGER,
        etazhnost          INTEGER,
        ploshad_pom        TEXT,
        ploshad_uchastok   TEXT,
        nds                TEXT,
        owner              TEXT,
        dop_info           TEXT,
        price              TEXT,
        photos             TEXT,
        videos             TEXT,
        status             TEXT DEFAULT 'active',
        created_at         TIMESTAMPTZ DEFAULT now(),
        updated_at         TIMESTAMPTZ,
        order_type         TEXT,
        repost_date        TIMESTAMPTZ,
        message_ids        TEXT,
        old_price          TEXT,
        initial_price      TEXT,
        channel_message_id BIGINT
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

    # функция clean_num(text) → numeric
    cur.execute("""
    CREATE OR REPLACE FUNCTION clean_num(inp TEXT)
    RETURNS NUMERIC AS $$
      SELECT NULLIF(
               regexp_replace(replace(inp, ',', '.'), '[^0-9\\.]', '', 'g'),
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
    if not text:
        return []
    try:
        return json.loads(text)
    except Exception:
        return []

def _now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()

def _map_common_media(d: Dict[str, Any]) -> None:
    d["photos"] = _parse_json(d.get("photos"))
    d["videos"] = _parse_json(d.get("videos"))
    ids = _parse_json(d.get("message_ids"))
    d["message_ids"] = ids
    if not d.get("channel_message_id"):
        # подложим первый message_id, если он есть
        d["channel_message_id"] = ids[0] if ids else None

def _ptype_by_table(table: str) -> str:
    return {
        "old_fund": "Старыйфонд",
        "new_fund": "Новыйфонд",
        "land":     "Участок",
        "commerce": "Коммерция",
    }[table]

def _add_public_fields(table: str, d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Преобразует БД-поля → публичные (русские) ключи + ptype, Тип заявки,
    не удаляя «английские» ключи (они остаются для внутреннего кода).
    """
    res = dict(d)  # копия
    res["ptype"] = _ptype_by_table(table)
    if "order_type" in res and res["order_type"] is not None:
        res["Тип заявки"] = res["order_type"]

    if table == "old_fund":
        mapping = {
            "orientir": "Ориентир",
            "district": "Район",
            "komnaty": "Комнаты",
            "ploshad": "Площадь",
            "etazh": "Этаж",
            "etazhnost": "Этажность",
            "sanuzly": "Санузлы",
            "sostoyanie": "Состояние",
            "material": "Материал строения",
            "parkovka": "Парковка",
            "dop_info": "Дополнительно",
            "price": "Цена",
        }
    elif table == "new_fund":
        mapping = {
            "orientir": "Ориентир",
            "district": "Район",
            "jk": "ЖК",
            "year": "Год постройки",
            "komnaty": "Комнаты",
            "ploshad": "Площадь",
            "etazh": "Этаж",
            "etazhnost": "Этажность",
            "sanuzly": "Санузлы",
            "sostoyanie": "Состояние",
            "material": "Материал строения",
            "dop_info": "Дополнительно",
            "price": "Цена",
        }
    elif table == "land":
        mapping = {
            "orientir": "Ориентир",
            "district": "Район",
            "type": "Тип недвижимости",
            "year": "Год постройки",
            "ploshad_uchastok": "Площадь участка",
            "ploshad_dom": "Площадь дома",
            "razmer": "Размер участка",
            "etazhnost": "Этажность",
            "sanuzly": "Санузлы",
            "sostoyanie": "Состояние",
            "material": "Материал строения",
            "zaezd": "Заезд авто",
            "dop_info": "Дополнительно",
            "price": "Цена",
        }
    else:  # commerce
        mapping = {
            "orientir": "Ориентир",
            "district": "Район",
            "nazna4enie": "Целевое назначение",
            "raspolozhenie": "Расположение",
            "etazh": "Этаж",
            "etazhnost": "Этажность",
            "ploshad_pom": "Площадь помещения",
            "ploshad_uchastok": "Площадь участка",
            "nds": "Учёт НДС",
            "owner": "Собственник",
            "dop_info": "Дополнительно",
            "price": "Цена",
        }

    for src, dst in mapping.items():
        if src in res and res[src] is not None:
            res[dst] = res[src]

    _map_common_media(res)
    return res


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
    msg_ids = data.get("message_ids") or []
    ch_id = (msg_ids[0] if isinstance(msg_ids, list) and msg_ids else None)

    cols = ("object_code","realtor_code","orientir","district","komnaty",
            "ploshad","etazh","etazhnost","sanuzly","sostoyanie",
            "material","parkovka","dop_info","price","initial_price",
            "photos","videos","message_ids","status","order_type","channel_message_id")
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
        _json_dump(msg_ids),
        "active",
        data.get("Тип заявки"),
        ch_id,
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
    msg_ids = data.get("message_ids") or []
    ch_id = (msg_ids[0] if isinstance(msg_ids, list) and msg_ids else None)

    cols = ("object_code","realtor_code","orientir","district","jk","year",
            "komnaty","ploshad","etazh","etazhnost","sanuzly",
            "sostoyanie","material","dop_info","price","initial_price",
            "photos","videos","message_ids","status","order_type","channel_message_id")
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
        _json_dump(msg_ids),
        "active",
        data.get("Тип заявки"),
        ch_id,
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
    msg_ids = data.get("message_ids") or []
    ch_id = (msg_ids[0] if isinstance(msg_ids, list) and msg_ids else None)

    cols = ("object_code","realtor_code","orientir","district","type","year",
            "ploshad_uchastok","ploshad_dom","razmer","etazhnost",
            "sanuzly","sostoyanie","material","zaezd","dop_info",
            "price","initial_price","photos","videos","message_ids","status","order_type","channel_message_id")
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
        _json_dump(msg_ids),
        "active",
        data.get("Тип заявки"),
        ch_id,
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
    msg_ids = data.get("message_ids") or []
    ch_id = (msg_ids[0] if isinstance(msg_ids, list) and msg_ids else None)

    purpose = data.get("Целевое назначение")
    if isinstance(purpose, list):
        purpose = ", ".join(purpose)

    cols = ("object_code","realtor_code","orientir","district","nazna4enie",
            "raspolozhenie","etazh","etazhnost","ploshad_pom","ploshad_uchastok",
            "nds","owner","dop_info","price","initial_price",
            "photos","videos","message_ids","status","order_type","channel_message_id")
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
        _json_dump(msg_ids),
        "active",
        data.get("Тип заявки"),
        ch_id,
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

def clear_old_price(table: str, object_code: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute(f"UPDATE {table} SET old_price=NULL, updated_at=now() WHERE object_code=%s;", (object_code,))
    cur.close(); conn.close()

def update_message_ids_and_repost_date(table: str, object_code: str, message_ids: List[int]):
    conn = _connect(); cur = _cursor(conn)
    mid_json = _json_dump(message_ids)
    ch_id = message_ids[0] if message_ids else None
    cur.execute(
        f"UPDATE {table} SET message_ids=%s, channel_message_id=%s, repost_date=now(), updated_at=now() "
        f"WHERE object_code=%s;",
        (mid_json, ch_id, object_code)
    )
    cur.close(); conn.close()

def update_channel_message_and_repost_date(table: str, object_code: str, message_id: int):
    conn = _connect(); cur = _cursor(conn)
    cur.execute(
        f"UPDATE {table} SET channel_message_id=%s, repost_date=now(), updated_at=now() "
        f"WHERE object_code=%s;",
        (message_id, object_code)
    )
    cur.close(); conn.close()

def touch_repost_now(table: str, object_code: str):
    conn = _connect(); cur = _cursor(conn)
    cur.execute(f"UPDATE {table} SET repost_date=now(), updated_at=now() WHERE object_code=%s;", (object_code,))
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

def _select_by_code(table: str, object_code: str) -> Optional[Dict[str, Any]]:
    conn = _connect(); cur = _cursor(conn)
    cur.execute(f"SELECT * FROM {table} WHERE object_code=%s LIMIT 1;", (object_code,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        return None
    d = dict(row)
    _map_common_media(d)
    return d

def _list_active_by_filters(table: str, select_cols: str, **filters) -> List[Dict[str, Any]]:
    """
    Универсальная заготовка: собирает SELECT ... FROM table WHERE status='active' + фильтры.
    Возвращает список словарей (сырые БД ключи).
    """
    conn = _connect(); cur = _cursor(conn)
    sql = f"SELECT {select_cols} FROM {table} WHERE status='active'"
    params: list[Any] = []

    # Применяем общие фильтры, если переданы (детали — в конкретных функциях)
    if table in ("old_fund", "new_fund", "land", "commerce"):
        if filters.get("districts"):
            sql = _apply_in(sql, params, "district", filters["districts"])
        if filters.get("order_type"):
            sql = _apply_in(sql, params, "order_type", [filters["order_type"]])

    # Частные диапазоны и IN — добавляются в конкретных функциях до вызова cur.execute
    # Здесь сразу исполнять нельзя — дочерние функции расширяют sql/params.
    return sql, params, conn, cur  # вернём заготовку (см. ниже)

# ────────────────────── SEARCH: старый фонд ─────────────────

def search_old_fund(object_code: Optional[str] = None, **filters) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Два режима:
      • search_old_fund('123') → dict одного объекта (по коду) или None.
      • search_old_fund(rooms=[...], area_min=..., ...) → список активных.
    """
    table = "old_fund"
    if object_code is not None:
        raw = _select_by_code(table, object_code)
        if not raw:
            return None  # type: ignore[return-value]
        return _add_public_fields(table, raw)  # type: ignore[return-value]

    # режим списка
    select_cols = "object_code, realtor_code, orientir, district, komnaty, ploshad, etazh, etazhnost, sanuzly, sostoyanie, material, parkovka, price, dop_info, photos, videos, order_type"
    sql, params, conn, cur = _list_active_by_filters(table, select_cols, **filters)

    # свои фильтры
    if filters.get("condition"):
        sql = _apply_in(sql, params, "sostoyanie", filters["condition"])

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
        res.append(_add_public_fields(table, d))
    return res

# ────────────────────── SEARCH: новый фонд ─────────────────

def search_new_fund(object_code: Optional[str] = None, **filters) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    table = "new_fund"
    if object_code is not None:
        raw = _select_by_code(table, object_code)
        if not raw:
            return None  # type: ignore[return-value]
        return _add_public_fields(table, raw)  # type: ignore[return-value]

    select_cols = "object_code, realtor_code, orientir, district, jk, year, komnaty, ploshad, etazh, etazhnost, sanuzly, sostoyanie, material, price, dop_info, photos, videos, order_type"
    sql, params, conn, cur = _list_active_by_filters(table, select_cols, **filters)

    if filters.get("condition"):
        sql = _apply_in(sql, params, "sostoyanie", filters["condition"])

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
        res.append(_add_public_fields(table, d))
    return res

# ────────────────────── SEARCH: участок ─────────────────

def search_land(object_code: Optional[str] = None, **filters) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    table = "land"
    if object_code is not None:
        raw = _select_by_code(table, object_code)
        if not raw:
            return None  # type: ignore[return-value]
        return _add_public_fields(table, raw)  # type: ignore[return-value]

    select_cols = "object_code, realtor_code, orientir, district, type, year, ploshad_dom, ploshad_uchastok, razmer, etazhnost, sanuzly, sostoyanie, material, zaezd, price, dop_info, photos, videos, order_type"
    sql, params, conn, cur = _list_active_by_filters(table, select_cols, **filters)

    if filters.get("landtype"):
        sql = _apply_in(sql, params, "type", filters["landtype"])
    if filters.get("condition"):
        sql = _apply_in(sql, params, "sostoyanie", filters["condition"])

    sql = _apply_range(sql, params, "ploshad_uchastok", "area_min", "area_max", filters)
    sql = _apply_range(sql, params, "price", "price_min", "price_max", filters)

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()

    res: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        res.append(_add_public_fields(table, d))
    return res

# ────────────────────── SEARCH: коммерция ────────────────

def search_commerce(object_code: Optional[str] = None, **filters) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    table = "commerce"
    if object_code is not None:
        raw = _select_by_code(table, object_code)
        if not raw:
            return None  # type: ignore[return-value]
        return _add_public_fields(table, raw)  # type: ignore[return-value]

    select_cols = "object_code, realtor_code, orientir, district, nazna4enie, raspolozhenie, etazh, etazhnost, ploshad_pom, ploshad_uchastok, nds, owner, price, dop_info, photos, videos, order_type"
    sql, params, conn, cur = _list_active_by_filters(table, select_cols, **filters)

    if filters.get("purpose"):
        sql = _apply_in(sql, params, "nazna4enie", filters["purpose"])
    sql = _apply_range(sql, params, "price", "price_min", "price_max", filters)

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()

    res: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        res.append(_add_public_fields(table, d))
    return res


# ────────────────────── Служебные выборки для основного бота ─────────────────────

def exists_active_object_code(object_code: str) -> bool:
    conn = _connect(); cur = _cursor(conn)
    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(f"SELECT 1 FROM {tbl} WHERE object_code=%s AND status='active' LIMIT 1;", (object_code,))
        if cur.fetchone():
            cur.close(); conn.close()
            return True
    cur.close(); conn.close()
    return False

def list_active_by_realtor(realtor_code: str) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Список активных объектов по риелтору:
    возвращает [('old_fund', rec), ...], где rec — row из БД с распарсенными media/message_ids.
    """
    conn = _connect(); cur = _cursor(conn)
    out: List[Tuple[str, Dict[str, Any]]] = []
    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(
            f"SELECT * FROM {tbl} WHERE realtor_code=%s AND status='active' ORDER BY created_at DESC;",
            (realtor_code,)
        )
        rows = cur.fetchall()
        for r in rows:
            d = dict(r)
            _map_common_media(d)
            out.append((tbl, d))
    cur.close(); conn.close()
    return out

def list_active_objects_for_repost(cutoff_iso: str) -> List[Tuple[str, str, Optional[str]]]:
    """
    Возвращает [('old_fund', object_code, repost_date_iso_or_None), ...]
    для тех, у кого repost_date пуст или < cutoff_iso.
    """
    cutoff = datetime.datetime.fromisoformat(cutoff_iso)
    conn = _connect(); cur = _cursor(conn)
    out: List[Tuple[str, str, Optional[str]]] = []
    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(
            f"SELECT object_code, repost_date FROM {tbl} WHERE status='active';"
        )
        for row in cur.fetchall():
            rdate = row["repost_date"]
            if (rdate is None) or (rdate < cutoff):
                out.append((tbl, row["object_code"], rdate.isoformat() if rdate else None))
    cur.close(); conn.close()
    return out

def client_secondary_exists(user_id: int, object_code: str) -> bool:
    conn = _connect(); cur = _cursor(conn)
    cur.execute(
        "SELECT 1 FROM client_secondary WHERE user_id=%s AND object_code=%s LIMIT 1;",
        (user_id, object_code)
    )
    ok = cur.fetchone() is not None
    cur.close(); conn.close()
    return ok


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



