import sqlite3
import json
import re
from typing import List, Dict, Any
import datetime

DB_FILE = "/data/realty.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()

    # ── создаём таблицы (теперь уже с updated_at) ─────────────
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
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at    TIMESTAMP                       -- ★ добавили
    )""")

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
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at    TIMESTAMP                       -- ★
    )""")

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
        zaezd            TEXT,
        dop_info          TEXT,
        price             TEXT,
        photos            TEXT,
        videos            TEXT,
        status            TEXT DEFAULT 'active',
        created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at        TIMESTAMP                    -- ★
    )""")

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
        dop_info         TEXT,
        price            TEXT,
        photos           TEXT,
        videos           TEXT,
        status           TEXT DEFAULT 'active',
        created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at       TIMESTAMP                     -- ★
    )""")

    # Новая таблица для заявок по вторичке
    cur.execute("""
    CREATE TABLE IF NOT EXISTS client_secondary (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        user_id        INTEGER,
        phone          TEXT,
        username       TEXT,
        telegram_name  TEXT,
        client_name    TEXT,
        object_code    TEXT,
        realtor_code   TEXT
    )""")


    cur.execute("""
    CREATE TABLE IF NOT EXISTS client_base (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        date          TEXT    NOT NULL,
        user_id       INTEGER UNIQUE NOT NULL,
        phone         TEXT,
        username      TEXT,
        telegram_name TEXT,
        client_name   TEXT
    )
    """)

    # ── для старых БД: пытаемся «добавить» колонку, если её нет ──
    def _ensure_updated_at(table: str):
        cur.execute(f"PRAGMA table_info({table})")
        if "updated_at" not in [row[1] for row in cur.fetchall()]:
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN updated_at TIMESTAMP")
            except sqlite3.OperationalError:
                pass  # колонку мог добавить другой поток


        # дополнительные поля…
        for tbl in ("old_fund", "new_fund", "land", "commerce"):
            _ensure_column(tbl, "order_type",   "TEXT")
            _ensure_column(tbl, "repost_date",  "TIMESTAMP")
            _ensure_column(tbl, "message_ids",  "TEXT")
            _ensure_column(tbl, "old_price",    "TEXT")
            _ensure_column(tbl, "initial_price", "TEXT")
            if tbl == "commerce":
                _ensure_column(tbl, "nds", "TEXT")
                _ensure_column(tbl, "owner", "TEXT")


    def _ensure_column(table: str, col: str, ddl: str):
        cur.execute(f"PRAGMA table_info({table})")
        if col not in [r[1] for r in cur.fetchall()]:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")

    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        _ensure_updated_at(tbl)

    _ensure_column("new_fund", "district", "TEXT")


    conn.commit()
    conn.close()


def _clean_num_py(val: str | None):

    try:
        if val is None:
            return None
        s = str(val).replace(",", ".")           # 73,5 → 73.5

        cleaned = re.sub(r"[^\d.]", "", s)

        if not re.search(r"\d", cleaned):
                return None

        if cleaned.count(".") > 1:
            parts = cleaned.split(".")
            cleaned = parts[0] + "." + "".join(parts[1:])

        if cleaned in ("", "."):
            return None
        return float(cleaned) if "." in cleaned else int(cleaned)
    except Exception:
        return None

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.create_function("clean_num", 1, _clean_num_py)
    return conn

# ─────────────────── AUTO-ID ────────────────────
def next_object_code() -> str:
    """
    Возвращает следующий свободный номер объявления
    (максимум по всем 4 таблицам + 1).
    """
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()

    max_code = 0
    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(f"SELECT MAX(CAST(object_code AS INTEGER)) FROM {tbl}")
        row = cur.fetchone()
        if row and row[0]:
            max_code = max(max_code, int(row[0]))

    conn.close()
    return str(max_code + 1)

# ─────── ФУНКЦИИ ДОБАВЛЕНИЯ ─────────────────────────
def insert_into_old_fund(data: dict):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO old_fund (
            object_code, realtor_code, orientir, district, komnaty,
            ploshad, etazh, etazhnost, sanuzly, sostoyanie,
            material, parkovka, dop_info, price, initial_price, photos, videos,
            message_ids, status, order_type
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
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
        json.dumps(data.get("photos", [])),
        json.dumps(data.get("videos", [])),
        json.dumps(data.get("message_ids", [])),
        "active",
        data.get("Тип заявки"),
    ])
    conn.commit()
    conn.close()

def insert_into_new_fund(data: dict):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO new_fund (
            object_code, realtor_code, orientir, district, jk, year,
            komnaty, ploshad, etazh, etazhnost, sanuzly,
            sostoyanie, material, dop_info, price, initial_price, photos, videos,
            message_ids, status, order_type
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
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
        json.dumps(data.get("photos", [])),
        json.dumps(data.get("videos", [])),
        json.dumps(data.get("message_ids", [])),
        "active",
        data.get("Тип заявки"),
    ])
    conn.commit()
    conn.close()

def insert_into_land(data: dict):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO land (
            object_code, realtor_code, orientir, district, type,
            year, ploshad_uchastok, ploshad_dom, razmer, etazhnost,
            sanuzly, sostoyanie, material, zaezd, dop_info,
            price, initial_price, photos, videos, message_ids, status, order_type
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
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
        json.dumps(data.get("photos", [])),
        json.dumps(data.get("videos", [])),
        json.dumps(data.get("message_ids", [])),
        "active",
        data.get("Тип заявки"),
    ])
    conn.commit()
    conn.close()

def insert_into_commerce(data: dict):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO commerce (
            object_code, realtor_code, orientir, district, nazna4enie,
            raspolozhenie, etazh, etazhnost, ploshad_pom, ploshad_uchastok, nds, owner,
            dop_info, price, initial_price, photos, videos, message_ids, status, order_type
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
        data.get("object_code"),
        data.get("realtor_code"),
        data.get("Ориентир"),
        data.get("district"),
        ", ".join(data.get("Целевое назначение", [])
                  if isinstance(data.get("Целевое назначение"), list)
                  else [data.get("Целевое назначение") or ""]),
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
        json.dumps(data.get("photos", [])),
        json.dumps(data.get("videos", [])),
        json.dumps(data.get("message_ids", [])),
        "active",
        data.get("Тип заявки"),
    ])
    conn.commit()
    conn.close()

def update_price_old_fund(object_code: str, new_price: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        UPDATE old_fund
           SET price = ?, updated_at = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (new_price, object_code))
    conn.commit()
    conn.close()

def update_price_new_fund(object_code: str, new_price: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        UPDATE new_fund
           SET price = ?, updated_at = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (new_price, object_code))
    conn.commit()
    conn.close()

def update_price_land(object_code: str, new_price: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        UPDATE land
           SET price = ?, updated_at = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (new_price, object_code))
    conn.commit()
    conn.close()

def update_price_commerce(object_code: str, new_price: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        UPDATE commerce
           SET price = ?, updated_at = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (new_price, object_code))
    conn.commit()
    conn.close()

def mark_inactive_old_fund(object_code: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        UPDATE old_fund
           SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (object_code,))
    conn.commit()
    conn.close()

def mark_inactive_new_fund(object_code: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        UPDATE new_fund
           SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (object_code,))
    conn.commit()
    conn.close()

def mark_inactive_land(object_code: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        UPDATE land
           SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (object_code,))
    conn.commit()
    conn.close()

def mark_inactive_commerce(object_code: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        UPDATE commerce
           SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (object_code,))
    conn.commit()
    conn.close()

def _clean_number(expr: str) -> str:
    """Удаляет пробелы и валютные суффиксы."""
    if not expr:
        return ""
    return re.sub(r"[^\d]", "", expr)

# ─────────────────── СТАРЫЙ ФОНД ───────────────────
def search_old_fund(**filters) -> List[Dict[str, Any]]:
    conn = _connect(); cur = conn.cursor()
    sql = """
    SELECT object_code, realtor_code, orientir, district, komnaty, ploshad,
           etazh, etazhnost, sostoyanie, price, dop_info, photos, videos
      FROM old_fund WHERE status='active'
    """
    params: list[Any] = []

    # — фильтры (как раньше) —
    if (regs := filters.get("region")):
        sql += f" AND district IN ({','.join('?'*len(regs))})"; params += regs
    if (conds := filters.get("condition")):
        sql += f" AND sostoyanie IN ({','.join('?'*len(conds))})"; params += conds
    if (rooms := filters.get("rooms")):
        nums, four = [r for r in rooms if r != "4+"], "4+" in rooms
        sub, subp = [], []
        if nums:
            sub.append(f"komnaty IN ({','.join('?'*len(nums))})")
            subp += list(map(int, nums))
        if four: sub.append("komnaty >= 4")
        sql += " AND (" + " OR ".join(sub) + ")"; params += subp
    for col, lo, hi in (
        ("ploshad","area_min","area_max"),
        ("etazh","floor_min","floor_max"),
        ("etazhnost","floors_total_min","floors_total_max"),
        ("price","price_min","price_max"),
    ):
        a,b = filters.get(lo), filters.get(hi)
        if a is not None: sql += f" AND clean_num({col})>=?"; params.append(a)
        if b is not None: sql += f" AND clean_num({col})<=?"; params.append(b)

    cur.execute(sql, params); rows = cur.fetchall(); conn.close()

    res = []
    for r in rows:
        d = dict(r)
        # ► маппинг ключей
        map_ru = {
            "orientir":"Ориентир", "komnaty":"Комнаты", "ploshad":"Площадь",
            "etazh":"Этаж", "etazhnost":"Этажность",
            "sostoyanie":"Состояние", "price":"Цена", "dop_info":"Дополнительно",
        }
        for db_key, ru_key in map_ru.items():
            if d.get(db_key) is not None:
                d[ru_key] = d.pop(db_key)
        if "district" in d:
            d["Район"] = d["district"]
        d["photos"] = json.loads(d["photos"]) if d["photos"] else []
        d["videos"] = json.loads(d["videos"]) if d["videos"] else []
        d["ptype"]  = "Старыйфонд"
        res.append(d)
    return res


# ─────────────────── НОВЫЙ ФОНД ───────────────────
def search_new_fund(**filters) -> List[Dict[str, Any]]:
    conn = _connect(); cur = conn.cursor()
    sql = """
    SELECT object_code, realtor_code, orientir, district, jk, year,
           komnaty, ploshad, etazh, etazhnost, sostoyanie,
           price, dop_info, photos, videos
      FROM new_fund WHERE status='active'
    """
    params: list[Any] = []

    if (regs := filters.get("region")):
        sql += f" AND district IN ({','.join('?'*len(regs))})"; params += regs
    if (conds := filters.get("condition")):
        sql += f" AND sostoyanie IN ({','.join('?'*len(conds))})"; params += conds
    if (rooms := filters.get("rooms")):
        nums, four = [r for r in rooms if r != "4+"], "4+" in rooms
        sub, subp = [], []
        if nums:
            sub.append(f"komnaty IN ({','.join('?'*len(nums))})")
            subp += list(map(int, nums))
        if four: sub.append("komnaty >= 4")
        sql += " AND (" + " OR ".join(sub) + ")"; params += subp
    for col, lo, hi in (
        ("ploshad","area_min","area_max"),
        ("etazh","floor_min","floor_max"),
        ("etazhnost","floors_total_min","floors_total_max"),
        ("price","price_min","price_max"),
    ):
        a,b = filters.get(lo), filters.get(hi)
        if a is not None: sql += f" AND clean_num({col})>=?"; params.append(a)
        if b is not None: sql += f" AND clean_num({col})<=?"; params.append(b)

    cur.execute(sql, params); rows = cur.fetchall(); conn.close()

    res = []
    for r in rows:
        d = dict(r)
        map_ru = {
            "orientir":"Ориентир", "komnaty":"Комнаты", "ploshad":"Площадь",
            "etazh":"Этаж", "etazhnost":"Этажность",
            "sostoyanie":"Состояние", "price":"Цена", "dop_info":"Дополнительно",
        }
        for k_ru,k_db in map_ru.items(): pass
        for db_key, ru_key in map_ru.items():
            if d.get(db_key) is not None:
                d[ru_key] = d.pop(db_key)
        if "district" in d:
            d["Район"] = d["district"]
        d["photos"] = json.loads(d["photos"]) if d["photos"] else []
        d["videos"] = json.loads(d["videos"]) if d["videos"] else []
        d["ptype"]  = "Новыйфонд"
        res.append(d)
    return res


# ─────────────────── УЧАСТОК ───────────────────
def search_land(**filters) -> List[Dict[str, Any]]:
    conn = _connect(); cur = conn.cursor()
    sql = """
    SELECT object_code, realtor_code, orientir, district, type, year,
           ploshad_dom, ploshad_uchastok, razmer, etazhnost, sanuzly,
           sostoyanie, material, zaezd, price, dop_info, photos, videos
      FROM land WHERE status='active'
    """
    params: list[Any] = []

    if (regs := filters.get("region")):
        sql += f" AND district IN ({','.join('?'*len(regs))})"; params += regs
    if (types := filters.get("landtype")):
        sql += f" AND type IN ({','.join('?'*len(types))})"; params += types
    if (conds := filters.get("condition")):
        sql += f" AND sostoyanie IN ({','.join('?'*len(conds))})"; params += conds
    for col, lo, hi in (
        ("ploshad_uchastok","area_min","area_max"),
        ("price","price_min","price_max"),
    ):
        a,b = filters.get(lo), filters.get(hi)
        if a is not None: sql += f" AND clean_num({col})>=?"; params.append(a)
        if b is not None: sql += f" AND clean_num({col})<=?"; params.append(b)

    cur.execute(sql, params); rows = cur.fetchall(); conn.close()

    res = []
    for r in rows:
        d = dict(r)
        map_ru = {
            "orientir":"Ориентир", "type":"Тип недвижимости",
            "ploshad_dom":"Площадь дома", "ploshad_uchastok":"Площадь участка",
            "razmer":"Размер участка", "etazhnost":"Этажность",
            "sanuzly":"Санузлы", "sostoyanie":"Состояние",
            "material":"Материал строения", "zaezd":"Заезд авто",
            "price":"Цена", "dop_info":"Дополнительно",
        }
        for db_key, ru_key in map_ru.items():
            if d.get(db_key) is not None:
                d[ru_key] = d.pop(db_key)
        if "district" in d:
            d["Район"] = d["district"]
        d["photos"] = json.loads(d["photos"]) if d["photos"] else []
        d["videos"] = json.loads(d["videos"]) if d["videos"] else []
        d["ptype"]  = "Участок"
        res.append(d)
    return res


# ─────────────────── КОММЕРЦИЯ ───────────────────
def search_commerce(**filters) -> List[Dict[str, Any]]:
    conn = _connect(); cur = conn.cursor()
    sql = """
    SELECT object_code, realtor_code, orientir, district, nazna4enie, raspolozhenie,
           etazh, etazhnost, ploshad_pom, ploshad_uchastok, nds,
           price, dop_info, photos, videos
      FROM commerce WHERE status='active'
    """
    params: list[Any] = []

    if (regs := filters.get("region")):
        sql += f" AND district IN ({','.join('?'*len(regs))})"; params += regs
    if (purp := filters.get("purpose")):
        sql += f" AND nazna4enie IN ({','.join('?'*len(purp))})"; params += purp
    lo, hi = filters.get("price_min"), filters.get("price_max")
    if lo is not None: sql += " AND clean_num(price)>=?"; params.append(lo)
    if hi is not None: sql += " AND clean_num(price)<=?"; params.append(hi)

    cur.execute(sql, params); rows = cur.fetchall(); conn.close()

    res = []
    map_ru = {
        "orientir":"Ориентир", "nazna4enie":"Целевое назначение",
        "raspolozhenie":"Расположение", "etazh":"Этаж", "etazhnost":"Этажность",
        "ploshad_pom":"Площадь помещения", "ploshad_uchastok":"Площадь участка", "nds":"Учёт НДС",
        "price":"Цена", "dop_info":"Дополнительно",
    }
    for r in rows:
        d = dict(r)
        for db_key, ru_key in map_ru.items():
            if d.get(db_key) is not None:
                d[ru_key] = d.pop(db_key)
        if "district" in d:
            d["Район"] = d["district"]
        d["photos"] = json.loads(d["photos"]) if d["photos"] else []
        d["videos"] = json.loads(d["videos"]) if d["videos"] else []
        d["ptype"]  = "Коммерция"
        res.append(d)
    return res

def drop_price_old_fund(object_code: str, new_price: str):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        UPDATE old_fund
           SET old_price    = price,
               price        = ?,
               updated_at   = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (new_price, object_code))
    conn.commit()
    conn.close()

def drop_price_new_fund(object_code: str, new_price: str):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        UPDATE new_fund
           SET old_price    = price,
               price        = ?,
               updated_at   = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (new_price, object_code))
    conn.commit()
    conn.close()

def drop_price_land(object_code: str, new_price: str):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        UPDATE land
           SET old_price    = price,
               price        = ?,
               updated_at   = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (new_price, object_code))
    conn.commit()
    conn.close()

def drop_price_commerce(object_code: str, new_price: str):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        UPDATE commerce
           SET old_price    = price,
               price        = ?,
               updated_at   = CURRENT_TIMESTAMP
         WHERE object_code = ?
    """, (new_price, object_code))
    conn.commit()
    conn.close()

def insert_client_secondary(
    user_id: int,
    phone: str,
    username: str,
    telegram_name: str,
    client_name: str,
    object_code: str,
    realtor_code: str
):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO client_secondary (
            user_id, phone, username, telegram_name,
            client_name, object_code, realtor_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id, phone, username, telegram_name,
        client_name, object_code, realtor_code
    ))
    conn.commit()
    conn.close()


def get_last_client_secondary(user_id: int) -> tuple[str, str] | None:
    """
    Возвращает (client_name, phone) последней заявки secondary для данного user_id
    или None, если нет.
    """
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute(
        "SELECT client_name, phone, object_code FROM client_secondary "
        "WHERE user_id = ? ORDER BY date DESC LIMIT 1",
        (user_id,)
    )
    row = cur.fetchone()
    conn.close()
    return (row[0], row[1]) if row else None


def get_client_base(user_id: int) -> sqlite3.Row | None:
    """
    Возвращает запись из client_base по user_id или None.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur  = conn.cursor()
    cur.execute("SELECT * FROM client_base WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row

def upsert_client_base(
    user_id: int,
    phone: str,
    username: str,
    telegram_name: str,
    client_name: str
):
    """
    Если пользователя ещё нет — вставляем, иначе обновляем phone+date.
    """
    now = datetime.datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    # проверяем, есть ли уже запись
    cur.execute("SELECT id FROM client_base WHERE user_id = ?", (user_id,))
    if cur.fetchone():
        cur.execute("""
            UPDATE client_base
               SET phone         = ?,
                   username      = ?,
                   telegram_name = ?,
                   client_name   = ?,
                   date          = ?
             WHERE user_id = ?
        """, (phone, username, telegram_name, client_name, now, user_id))
    else:
        cur.execute("""
            INSERT INTO client_base
                (date, user_id, phone, username, telegram_name, client_name)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (now, user_id, phone, username, telegram_name, client_name))
    conn.commit()

    conn.close()
