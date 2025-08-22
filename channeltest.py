#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import math
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any
import html
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.request import HTTPXRequest
from telegram import (
    Update, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton,
    InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo, Bot,
    ReplyKeyboardRemove, InputMedia, KeyboardButtonRequestUsers
)
import telegram
from telegram.ext import (
    Application, CommandHandler, ConversationHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters, JobQueue
)
from telegram.ext.filters import StatusUpdate
from db import (
    DB_FILE,
    init_db,
    insert_into_old_fund,
    insert_into_new_fund,
    insert_into_land,
    insert_into_commerce,
    update_price_old_fund,
    update_price_new_fund,
    update_price_land,
    update_price_commerce,
    mark_inactive_old_fund,
    mark_inactive_new_fund,
    mark_inactive_land,
    mark_inactive_commerce,
    search_old_fund,
    search_new_fund,
    search_land,
    search_commerce,
    drop_price_old_fund, drop_price_new_fund, drop_price_land, drop_price_commerce,
    insert_client_secondary, get_last_client_secondary, get_client_base, upsert_client_base, next_object_code
)
import asyncio, json, logging, re, sqlite3

from refresh import refresh_file_ids

MYADS_SHOW, MYADS_PRICE = range(4, 6)

BOT_TOKEN  = "7246794083:AAFTSak3gBA6iv1K-jNNxmdmMGGpi0AYt6Y"

CHANNEL_ID = -1002567680498
NOTIFY_ID  = -4862692379

SHEET_ID        = "1zSwr45ZRY_hHR-Z7DPhj3M3d31YcWzBH9p7FeXp6WR0"
WORKSHEET_NAME_NEWBUILD = "Новостройки"
WORKSHEET_NAME  = "Телеграмм"
STATUS_SHEET    = "Статус сотрудника BI"
CRED_FILE       = "credentials.json"

USERS_FILE = "users_id.txt"

ADMIN_IDS  = {6864823290, 5498741148}

ACCESS_MENU, ACCESS_OPEN_WAIT, ACCESS_CLOSE_PICK = range(3)
def load_allowed() -> dict[int,str]:
    """
    Читает users_id.txt, где каждая строка: user_id:user_name
    Возвращает словарь {id: name}.
    """
    users: dict[int,str] = {}
    try:
        with open(USERS_FILE, encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(":", 1)
                if not parts[0].isdigit():
                    continue
                uid = int(parts[0])
                name = parts[1] if len(parts)>1 else ""
                users[uid] = name
    except FileNotFoundError:
        pass
    return users

def save_allowed(users: dict[int,str]) -> None:
    """
    Записывает в users_id.txt строки вида:
        12345678:Иван Иванов
    """
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        for uid, name in users.items():
            f.write(f"{uid}:{name}\n")

# ─── Новые состояния для ConversationHandler ────────────────────────
NEWB_ASK_NAME, NEWB_ASK_PHONE = range(6, 8)

# ─── СОСТОЯНИЯ ДИАЛОГА (/ad) ───────────────────────────────────────
WAITING_MEDIA, EDITING, ASK_NAME, ASK_PHONE = range(4)

#  ————————————————————————————————————————————————————————————————————
# Новый список для выбора «Тип заявки»
DEAL_TYPES = ["Продажа", "Аренда"]
#  ————————————————————————————————————————————————————————————————————


FIELD_KEYS = {
    "district": "d",
    "Состояние": "st",
    "Материал строения": "mt",
    # "Парковка": "pk",
    "Тип недвижимости": "fm",
    "Целевое назначение": "dn",
    "Расположение": "rl",
    "Учёт НДС": "vat",
    "Собственник": "own",
    "Заезд авто": "za",
    "Тип заявки": "tz",
    "Санузлы": "wc",
}
# обратная мапа
KEY_FIELDS = {v: k for k, v in FIELD_KEYS.items()}


# ─── ЛОГИ ───────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s :: %(levelname)s :: %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── Inline-справочники ────────────────────────────────────────────
PROPERTY_TYPES = ["Старыйфонд", "Новыйфонд", "Участок", "Коммерция"]
DISTRICTS = [
    "Мирабадский", "Мирзо-Улугбекский", "Юнусабадский", "Яшнабадский",
    "Яккасарайский", "Сергелийский", "Алмазарский", "Чиланзарский",
    "Бектемирский", "Шайхантаурский", "Учтепинский", "Ташкентская область",
]
STATE_OLD  = ["Старый ремонт", "Косметический ремонт", "Новый ремонт", "Требует ремонта", "Коробка"]
STATE_NEW  = [
    "Коробка", "Чистовая отделка", "Предчистовая отделка",
    "Черновая отделка", "Косметический ремонт", "Новый ремонт", "Требует ремонта"
]

# ─── Обновлённый словарь шаблонов (8 шт.: 4 типа × 2 варианта) ────
TEMPLATES: dict[str, dict[str, dict[str, Any]]] = {
    "Старыйфонд": {
        "Продажа": {
            "menu": {
                "Состояние": STATE_OLD,
                "Материал строения": ["Монолит", "Кирпич", "Газоблок", "Панель"],
                #"Парковка": ["Есть", "Нет"],
                "Санузлы": ["1 санузел", "2 санузла", "3 санузла",
                        "4 санузла", "5 санузлов", "раздельный санузел", "совмещенный санузел"],
            },
            "manual": [
                "Ориентир", "Комнаты", "Площадь",
                "Этаж", "Этажность",
                #"Санузлы",
                "Цена",
                "Дополнительно",
            ],
            "required": [
                "ptype", "Тип заявки", "district",
                "Комнаты", "Этаж", "Этажность",
                "Площадь", "Состояние", "Цена", "Ориентир",
            ],
        },
        "Аренда": {
            "menu": {
                "Состояние": STATE_OLD,
                "Материал строения": ["Монолит", "Кирпич", "Газоблок", "Панель"],
                #"Парковка": ["Есть", "Нет"],
            },
            "manual": [
                "Ориентир", "Комнаты", "Площадь",
                "Этаж", "Этажность",
                "Санузлы", "Цена", "Дополнительно",
            ],
            "required": [
                "ptype", "Тип заявки", "district",
                "Комнаты", "Этаж", "Этажность",
                "Площадь", "Состояние", "Цена", "Ориентир",
            ],
        },
    },
    "Новыйфонд": {
        "Продажа": {
            "menu": {
                "Состояние": STATE_NEW,
                "Материал строения": ["Монолит", "Кирпич", "Газоблок", "Панель"],
                "Санузлы": ["1 санузел", "2 санузла", "3 санузла",
                        "4 санузла", "5 санузлов", "раздельный санузел", "совмещенный санузел"],
            },
            "manual": [
                "Ориентир", "ЖК", "Год постройки",
                "Комнаты", "Площадь",
                "Этаж", "Этажность",
                #"Санузлы",
                "Цена",
                "Дополнительно",
            ],
            "required": [
                "ptype", "Тип заявки", "district",
                "Комнаты", "Этаж", "Этажность",
                "Площадь", "Состояние", "Цена", "Ориентир",
            ],
        },
        "Аренда": {
            "menu": {
                "Состояние": STATE_NEW,
                "Материал строения": ["Монолит", "Кирпич", "Газоблок", "Панель"],
            },
            "manual": [
                "Ориентир", "ЖК", "Год постройки",
                "Комнаты", "Площадь",
                "Этаж", "Этажность",
                "Санузлы", "Цена", "Дополнительно",
            ],
            "required": [
                "ptype", "Тип заявки", "district",
                "Комнаты", "Этаж", "Этажность",
                "Площадь", "Состояние", "Цена", "Ориентир",
            ],
        },
    },
    "Участок": {
        "Продажа": {
            "menu": {
                "Тип недвижимости": ["Дом", "Земельный участок"],
                "Материал строения": ["Кирпич","Газоблок","Керамоблок","Дерево","Монолит","Камень"],
                "Состояние": STATE_OLD,
                "Заезд авто": ["Есть","Отсутствует"],
            },
            "manual": [
                "Ориентир", "Размер участка", "Этажность",
                "Площадь участка", "Площадь дома",
                #"Санузлы", "Год постройки",
                "Цена", "Дополнительно",
            ],
            "required": [
                "ptype", "Тип заявки", "district", "Тип недвижимости",
                "Площадь участка", "Состояние",
                "Цена", "Ориентир",
            ],
        },
        "Аренда": {
            "menu": {
                "Тип недвижимости": ["Дом", "Земельный участок"],
                "Материал строения": ["Кирпич","Газоблок","Керамоблок","Дерево","Монолит","Камень"],
                "Состояние": STATE_OLD,
                "Заезд авто": ["Есть","Отсутствует"],
            },
            "manual": [
                "Ориентир", "Размер участка", "Этажность",
                "Площадь участка", "Площадь дома",
                "Санузлы", "Год постройки",
                "Цена", "Дополнительно",
            ],
            "required": [
                "ptype", "Тип заявки", "district", "Тип недвижимости",
                "Площадь участка", "Состояние",
                "Цена", "Ориентир",
            ],
        },
    },
    "Коммерция": {
        "Продажа": {
            "menu": {
                "Тип заявки": DEAL_TYPES,
                "Целевое назначение": ["Офис","Торговое помещение","Склад","Производственное помещение","Свободного назначения","Отдельно стоящее здание","Готовый бизнес"],
                "Расположение": ["1 - линия","2 - линия","БЦ","в промзоне", "махалля"],
            },
            "manual": [
                "Ориентир", "Этаж", "Этажность",
                "Площадь помещения", "Площадь участка",
                "Цена", "Дополнительно",
            ],
            "required": [
                "ptype", "Тип заявки", "district",
                "Целевое назначение", "Площадь помещения",
                "Цена", "Ориентир",
            ],
        },
        "Аренда": {
            "menu": {
                "Тип заявки": DEAL_TYPES,
                "Целевое назначение": ["Офис","Торговое помещение","Склад","Производственное помещение","Свободного назначения","Отдельно стоящее здание","Готовый бизнес"],
                "Расположение": ["1 - линия","2 - линия","БЦ","в промзоне", "махалля"],
                "Учёт НДС": ["с учетом НДС", "без учета НДС"],
                "Собственник": ["физ. лицо", "юр. лицо"],
            },
            "manual": [
                "Ориентир", "Этаж", "Этажность",
                "Площадь помещения", "Площадь участка",
                "Цена", "Дополнительно",
            ],
            "required": [
                "ptype", "Тип заявки", "district",
                "Целевое назначение", "Площадь помещения",
                "Цена", "Ориентир", "Собственник",
            ],
        },
    },
}

# ─── Категории для быстрой валидации ──────────────────────────────
INT_FIELDS   = {
    "Комнаты", "Этаж", "Этажность", "Санузлы", "Год постройки"
}
AREA_FIELDS  = {
    "Площадь", "Площадь участка", "Площадь дома", "Площадь помещения"
}
PRICE_FIELD  = "Цена"

async def delete_object_in_channel(
    bot: Bot,
    code: str,
    *,
    deactivate: bool = True,
    use_saved_ids: bool = False,
) -> bool:
    """
    Удаляет пост-альбом по object_code:
      1) по сохранённым message_ids (если use_saved_ids=True),
      2) иначе — ищет «Код объекта: <code>» и удаляет весь альбом,
      3) если что-то удалено и deactivate=True — помечает запись inactive.
    """
    # 1) читаем message_ids и функцию mark_inactive
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    rec_data = None
    inactive_fn = None
    for tbl, mark_fn in (
        ("old_fund",   mark_inactive_old_fund),
        ("new_fund",   mark_inactive_new_fund),
        ("land",       mark_inactive_land),
        ("commerce",   mark_inactive_commerce),
    ):
        cur.execute(f"SELECT message_ids FROM {tbl} WHERE object_code = ?", (code,))
        row = cur.fetchone()
        if row and row[0]:
            rec_data, inactive_fn = row[0], mark_fn
            break
    conn.close()

    deleted_any = False

    # 2) удаляем по сохранённым ID, если запрошено
    if use_saved_ids and rec_data:
        try:
            msg_ids = json.loads(rec_data)
            for mid in msg_ids:
                try:
                    await bot.delete_message(CHANNEL_ID, mid)
                    deleted_any = True
                except Exception as e:
                    logger.warning("Не смог удалить %s: %s", mid, e)
        except Exception as e:
            logger.error("Плохой JSON message_ids для %s: %s", code, e)


    # 4) если что-то удалилось и нужно — помечаем запись inactive
    if deleted_any and deactivate and inactive_fn:
        # чтобы снять потенциальный read-lock SQLite
        await asyncio.sleep(0)
        inactive_fn(code)

    return deleted_any


# ── helpers: price utils ───────────────────────────────────────────
def _bold(val) -> str:
    """
    Возвращает строку жирным шрифтом.
    Безопасно обрабатывает None и не-строковые значения.
    """
    if not val:                     # None, "" или 0
        return ""
    return f"<b>{html.escape(str(val))}</b>"
# ── helpers: price utils ────────────────────────────────────────────
_PRICE_RE = re.compile(r"\d[\d\s]*")

def _price_to_int(price: str) -> int:
    """«12 500 у.е.» → 12500 (без разделителей)."""
    return int(re.sub(r"\D", "", price))

async def _current_price(code: str) -> tuple[int, str] | None:
    """
    Возвращает (int_value, pretty) актуальной цены
    из realty.db, игнорируя зачёркнутые старые.
    """
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()

    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(
            f"SELECT price, initial_price FROM {tbl} WHERE object_code = ? AND status = 'active'",
            (code,)
        )
        row = cur.fetchone()
        if row and row[0]:
            price_str = row[0]  # например "123 000 у.е."
            digits    = re.sub(r"\D", "", price_str)
            try:
                val = int(digits)
            except ValueError:
                conn.close()
                return None
            conn.close()
            return val, price_str

    conn.close()
    return None

MYADS_FIELDS = [
    ("district",   "Район"),
    ("Ориентир",   "Ориентир"),

    # вот эти три — для Участка
    ("Тип недвижимости",  "Тип недвижимости"),
    ("Площадь участка",   "Площадь участка"),
    ("Площадь дома",      "Площадь дома"),
    ("Год постройки",     "Год постройки"),

    ("Комнаты",    "Комнаты"),
    ("Площадь",    "Площадь"),
    ("Этаж",       "Этаж"),
    ("Этажность",  "Этажность"),
    ("Санузлы", "Санузлы"),
    ("Состояние",  "Состояние"),
    ("Материал строения", "Материал строения"),
    ("Парковка",   "Парковка"),

    ("Цена",       "Цена"),
    ("Заезд авто", "Заезд авто"),
    ("Целевое назначение", "Целевое назначение"),
    ("Расположение", "Расположение"),
    ("Дополнительно", "Дополнительно"),
]

def build_myads_caption(data: dict, bot_username: str) -> str:
    """
    Делегируем всю разметку единой функции build_caption(), а затем
    убираем две «длинные» HTML-ссылки, чтобы выдача в /myads была короче.

    • Шаблон (emoji, порядок строк, формат цены) остаётся точно таким же,
      как в канале — нет риска расхождений.
    • Если ссылки в личке всё-таки нужны, удалите фильтр `<a href`.
    """
    # Полный капшен (со ссылками «Оставить заявку / Больше объектов»)
    full_caption = build_caption(data, bot_username)

    # Отсекаем строки, содержащие HTML-ссылки
    compact_lines = [
        ln for ln in full_caption.splitlines()
        if "<a href" not in ln
    ]

    # Дополнительно убираем возможные дублирующиеся пустые строки
    cleaned = []
    for ln in compact_lines:
        if ln or (cleaned and cleaned[-1]):  # не допускаем двойных blank-line
            cleaned.append(ln)

    return "\n".join(cleaned)

# utils ────────────────────────────────────────────────────────────
def _chunk(lst, size):
    """Разбивает список lst на части не длиннее size."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

async def safe_preview(bot, chat_id, photos, videos, caption, logger):
    """
    Пытается отправить 1-е фото, затем 1-е видео.
    Если оба file_id битые → падаем на текстовое сообщение.
    """
    for idx, pid in enumerate(photos[:1] + videos[:1]):          # максимум 1 медиа
        try:
            if idx == 0 and photos:
                return await bot.send_photo(chat_id, pid,
                                             caption=caption, parse_mode=ParseMode.HTML)
            else:
                return await bot.send_video(chat_id, pid,
                                             caption=caption, parse_mode=ParseMode.HTML)
        except telegram.error.BadRequest as e:
            logger.warning("myads preview failed %s : %s", pid, e.message)

    # fallback → обычный текст
    return await bot.send_message(chat_id, caption, parse_mode=ParseMode.HTML)

async def send_myads_page(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    page: int,
) -> None:
    """
    Показывает пользователю по 3 его объявления на страницу.
    • Подпись формируется единой функцией build_myads_caption() → одинаково
      выглядит и в личке, и в канале.
    • Капшены HTML; лишние ссылки отфильтрованы в build_myads_caption().
    """

    records     = context.user_data.get("myads_records", [])
    page_size   = 3
    total_pages = max(1, math.ceil(len(records) / page_size))
    page        = max(0, min(page, total_pages - 1))

    # ⚠️ Сохраняем текущую страницу
    context.user_data["myads_page"] = page

    # 1. удаляем все старые сообщения-контролы
    old_ctrls = context.user_data.get("myads_controls", [])
    del_tasks = [
        context.bot.delete_message(chat_id, c["mid"])
        for c in old_ctrls
    ]
    if del_tasks:
        await asyncio.gather(*del_tasks, return_exceptions=True)
    context.user_data["myads_controls"] = []

    start = page * page_size
    end   = start + page_size

    bot_username = context.bot.username  # нужен для ссылки «Оставить заявку»

    # ── 2. выводим объекты текущей страницы ─────────────────────
    for table, rec in records[start:end]:

        # 2.1 формируем словарь «универсального» шаблона
        data = {
            "ptype":        rec.get("ptype"),
            "Тип заявки":   rec.get("order_type"),          # «Продажа» / «Аренда»
            "object_code":  rec.get("object_code"),
            "district":     rec.get("district"),
        }

        if table == "old_fund":
            data.update({
                "Ориентир":           rec.get("orientir"),
                "Комнаты":            rec.get("komnaty"),
                "Площадь":            rec.get("ploshad"),
                "Этаж":               rec.get("etazh"),
                "Этажность":          rec.get("etazhnost"),
                "Санузлы":    rec.get("sanuzly"),
                "Состояние":          rec.get("sostoyanie"),
                "Материал строения":  rec.get("material"),
                "Дополнительно":      rec.get("dop_info"),
                "Цена":               rec.get("price"),
            })

        elif table == "new_fund":
            data.update({
                "Ориентир":           rec.get("orientir"),
                "ЖК":                 rec.get("jk"),
                "Год постройки":      rec.get("year"),
                "Комнаты":            rec.get("komnaty"),
                "Площадь":            rec.get("ploshad"),
                "Этаж":               rec.get("etazh"),
                "Этажность":          rec.get("etazhnost"),
                "Санузлы":    rec.get("sanuzly"),
                "Состояние":          rec.get("sostoyanie"),
                "Материал строения":  rec.get("material"),
                "Дополнительно":      rec.get("dop_info"),
                "Цена":               rec.get("price"),
            })

        elif table == "land":
            data.update({
                "Ориентир":           rec.get("orientir"),
                "Площадь участка":    rec.get("ploshad_uchastok"),
                "Площадь дома":       rec.get("ploshad_dom"),
                "Размер участка":     rec.get("razmer"),
                "Этажность":          rec.get("etazhnost"),
                "Санузлы":    rec.get("sanuzly"),
                "Состояние":          rec.get("sostoyanie"),
                "Материал строения":  rec.get("material"),
                "Заезд авто":         rec.get("zaezd"),
                "Дополнительно":      rec.get("dop_info"),
                "Цена":               rec.get("price"),
            })

        elif table == "commerce":
            data.update({
                "Ориентир":           rec.get("orientir"),
                "Целевое назначение": ", ".join(rec.get("nazna4enie"))
                                    if isinstance(rec.get("nazna4enie"), list)
                                    else rec.get("nazna4enie"),
                "Расположение":       rec.get("raspolozhenie"),
                "Этаж":               rec.get("etazh"),
                "Этажность":          rec.get("etazhnost"),
                "Площадь помещения":  rec.get("ploshad_pom"),
                "Площадь участка":    rec.get("ploshad_uchastok"),
                "Учёт НДС":           rec.get("nds"),
                "Собственник":        rec.get("owner"),
                "Дополнительно":      rec.get("dop_info"),
                "Цена":               rec.get("price"),
            })

        # если в записи есть старая цена – передаём её и ставим флаг
        if rec.get("old_price"):
            data["old_price"] = rec["old_price"]
            data["_price_drop_flag"] = True
        # 2.2 генерируем подпись
        caption = build_myads_caption(data, bot_username)
        if len(caption) > 1024:                # Telegram-лимит
            caption = caption[:1000].rstrip() + "…"

        m = await safe_preview(
            context.bot,
            chat_id,
            rec["photos"],
            rec["videos"],
            caption,
            logger,
        )
        context.user_data["myads_controls"].append({"mid": m.message_id, "type": "prev"})


        # 2.5 inline-кнопки управления под объектом
        code = data["object_code"]
        kb   = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🗑 Удалить",          callback_data=f"del:{code}"),
                InlineKeyboardButton("🔁 Репостнуть",       callback_data=f"repost:{code}"),
            ],
            [
                InlineKeyboardButton("⬇ Снизить + репост",  callback_data=f"dec:{code}"),
                InlineKeyboardButton("⬆ Повысить цену",     callback_data=f"rise:{code}"),
            ],
        ])
        ctrl_msg = await context.bot.send_message(chat_id, text=f"Объект {code}", reply_markup=kb)
        context.user_data["myads_controls"].append({"mid": ctrl_msg.message_id, "type": "ctrl"})

    # 3. строим навигацию: кнопок ≤ 8 в каждой строке
    nav_btns = []
    if page > 0:
        nav_btns.append(InlineKeyboardButton("«", callback_data=f"nav:{page-1}"))
    for i in range(total_pages):
        txt = f"[{i+1}]" if i == page else str(i+1)
        nav_btns.append(InlineKeyboardButton(txt, callback_data=f"nav:{i}"))
    if page < total_pages - 1:
        nav_btns.append(InlineKeyboardButton("»", callback_data=f"nav:{page+1}"))

    rows = [list(chunk) for chunk in _chunk(nav_btns, 8)]
    nav_msg = await context.bot.send_message(
        chat_id,
        "Страницы:",
        reply_markup=InlineKeyboardMarkup(rows),
    )
    context.user_data["myads_controls"].append(
    {"mid": nav_msg.message_id, "type": "nav"}
    )

async def myads_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Показывает пользователю его активные объявления из базы данных постранично.
    Теперь realtor_code = Telegram-ID пользователя → никаких users_data.json не нужно.
    """
    # 1) Только в личке
    if update.effective_chat.type != "private":
        await update.message.reply_text(
            "❗ Команда /myads работает только в личных сообщениях."
        )
        return ConversationHandler.END

    # 2) Telegram-ID текущего пользователя = realtor_code в БД
    realtor = str(update.effective_user.id)

    # 3) Берём все активные объекты этого риелтора из четырёх таблиц
    conn    = sqlite3.connect(DB_FILE)
    cur     = conn.cursor()
    records = []

    for table in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(
            f"SELECT * FROM {table} WHERE realtor_code = ? AND status = 'active'",
            (realtor,)
        )
        cols = [c[0] for c in cur.description]

        for row in cur.fetchall():
            rec = dict(zip(cols, row))

            # ---- десериализация фото / видео ------------------------
            for key in ("photos", "videos"):
                raw = rec.get(key)
                if isinstance(raw, str):
                    try:
                        rec[key] = json.loads(raw)
                    except json.JSONDecodeError:
                        rec[key] = []
                elif not isinstance(raw, list):
                    rec[key] = []

            # ---- ptype + order_type для build_caption ---------------
            rec["ptype"] = {
                "old_fund": "Старыйфонд",
                "new_fund": "Новыйфонд",
                "land":     "Участок",
                "commerce": "Коммерция",
            }[table]
            rec["order_type"] = str(rec.get("order_type", ""))

            records.append((table, rec))

    conn.close()

    if not records:
        await update.message.reply_text("У вас нет активных объявлений.")
        return ConversationHandler.END

    # 4) Убираем старые элементы управления
    chat_id = update.effective_chat.id
    del_tasks = [
        context.bot.delete_message(chat_id, c["mid"])
        for c in context.user_data.get("myads_controls", [])
    ]
    if del_tasks:
        await asyncio.gather(*del_tasks, return_exceptions=True)
    context.user_data["myads_controls"] = []

    # 5) Сохраняем список объектов в user_data и показываем первую страницу
    context.user_data["myads_records"] = records
    context.user_data["myads_page"]    = 0
    await send_myads_page(context, chat_id, 0)

    return MYADS_SHOW



async def myads_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка inline-кнопок в /myads (del / repost / dec / rise / navigation)."""
    q     = update.callback_query
    await q.answer()
    data  = context.user_data
    cd    = q.data

    # защита от странных callback-ов
    if ":" in cd:
        act, payload = cd.split(":", 1)
    else:
        act, payload = cd, None

    uid        = update.effective_user.id
    keep_mid   = q.message.message_id

    # 0. чистим «старые» контролы, кроме keep_mid
    chat_id = uid
    del_tasks = [                                   # удаляем ТОЛЬКО ctrl-сообщения
        context.bot.delete_message(chat_id, c["mid"])
        for c in data.get("myads_controls", [])
        if c.get("type") == "ctrl" and c["mid"] != keep_mid
    ]
    if del_tasks:
        await asyncio.gather(*del_tasks, return_exceptions=True)
    # оставляем только то сообщение, на котором нажата кнопка
    data["myads_controls"] = [
        c for c in data.get("myads_controls", [])
        if not (c["type"] == "ctrl" and c["mid"] != keep_mid)
    ]

    if act == "nav":
        page = int(payload or 0)
        await send_myads_page(context, uid, page)
        return MYADS_SHOW

    # 1) Удаление объявления
    if act == "del":
        code = payload
        # Показываем статус удаления
        try:
            await q.edit_message_text("⏳ Удаляю объявление…")
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise

        # сначала удаляем в канале
        ok = await delete_object_in_channel(context.bot, code, deactivate=True, use_saved_ids=True)

        if ok:
            # затем помечаем запись в БД как inactive
            conn = sqlite3.connect(DB_FILE)
            cur  = conn.cursor()
            table = next(
                (tbl for tbl, rec in data.get("myads_records", [])
                 if str(rec.get("object_code")) == code),
                None
            )
            if table:
                cur.execute(
                    f"UPDATE {table} SET status = 'inactive' WHERE object_code = ?",
                    (code,)
                )
                conn.commit()
            conn.close()

        # сообщаем пользователю об результате
        try:
            await context.bot.edit_message_text(
                chat_id=uid,
                message_id=keep_mid,
                text="✅ Объявление удалено и деактивировано." if ok
                     else "❌ Не удалось удалить объявление."
            )
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise

        return ConversationHandler.END

    # 2) Репост объявления
    if act == "repost":
        code = payload
        try:
            await q.edit_message_text("⏳ Репостую…")
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise

        ok = await repost_object_in_channel(context.bot, code, None, uid)

        try:
            await context.bot.edit_message_text(
                chat_id=uid,
                message_id=keep_mid,
                text="✅ Репост выполнен." if ok else "❌ Не удалось репостнуть."
            )
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise

        return ConversationHandler.END

    # 4) Снижение или повышение цены
    if act in ("dec", "rise"):
        data["pending_code"]   = payload
        data["pending_action"] = act
        await context.bot.send_message(uid, "Введите новую цену (только цифры):")
        return MYADS_PRICE

    return MYADS_SHOW

async def handle_myads_price_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Цикл ввода цены для действий dec / rise, с проверкой и репостом."""
    data = context.user_data
    uid  = update.effective_user.id

    # 1) удаляем прошлое предупреждение
    if (mid := data.pop("ask_price_mid", None)):
        try:
            await context.bot.delete_message(uid, mid)
        except:
            pass

    code   = data.get("pending_code")
    action = data.get("pending_action")
    if not code or action not in ("dec", "rise"):
        return MYADS_SHOW

    # 2) вынимаем только цифры и форматируем
    digits = re.sub(r"\D", "", update.message.text)

    # 3) получаем запись из БД (SELECT * WHERE object_code)
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    rec   = None
    table = None
    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(f"SELECT * FROM {tbl} WHERE object_code = ?", (code,))
        row = cur.fetchone()
        if row:
            cols = [c[0] for c in cur.description]
            rec   = dict(zip(cols, row))
            table = tbl
            break
    conn.close()

    if not rec:
        await update.message.reply_text("❌ Объявление не найдено. Попробуйте /myads.")
        return ConversationHandler.END

    # 4) проверяем: если это аренда — снижаем требование по длине
    is_rent = (rec.get("order_type") or "").lower() == "аренда"
    if len(digits) < 5 and not is_rent:
        msg = await update.message.reply_text(
            "Цена должна быть минимум 5-значной. Попробуйте ещё раз:"
        )
        data["ask_price_mid"] = msg.message_id
        return MYADS_PRICE

    new_price = _format_price(digits)
    # 5) текущее значение из БД
    cur_price = await _current_price(code)
    if not cur_price:
        await update.message.reply_text("❌ Не удалось получить текущую цену.")
        return ConversationHandler.END
    cur_val, cur_pretty = cur_price

    # 6) Снижение + репост
    if action == "dec":
        if int(digits) >= cur_val:
            msg = await update.message.reply_text(
                f"Новая цена ({new_price}) не ниже текущей ({cur_pretty}). Введите другую:"
            )
            data["ask_price_mid"] = msg.message_id
            return MYADS_PRICE

        await update.message.reply_text("⏳ Снижаю цену и репощу…")
        ok = await repost_object_in_channel(context.bot, code, new_price, uid)
        if ok:
            data.clear()
            await update.message.reply_text("✅ Цена снижена и объявление репощено.")
            return ConversationHandler.END
        else:
            msg = await update.message.reply_text("❌ Не удалось репостить. Попробуйте позже:")
            data["ask_price_mid"] = msg.message_id
            return MYADS_PRICE

    # 7) Повышение цены
    if action == "rise":
        if int(digits) <= cur_val:
            msg = await update.message.reply_text(
                f"Новая цена ({new_price}) не выше текущей ({cur_pretty}). Введите другую:"
            )
            data["ask_price_mid"] = msg.message_id
            return MYADS_PRICE

        await update.message.reply_text("⏳ Повышаю цену и репощу…")
        ok = await repost_object_in_channel(context.bot, code, new_price, uid)
        if ok:
            data.clear()
            await update.message.reply_text("✅ Цена повышена и объявление репощено.")
            return ConversationHandler.END
        else:
            msg = await update.message.reply_text("❌ Не удалось репостнуть новую цену. Попробуйте позже:")
            data["ask_price_mid"] = msg.message_id
            return MYADS_PRICE

    return MYADS_SHOW

async def update_price_in_channel(bot: Bot, code: str, new_price: str) -> bool:
    """
    Снижает цену, обновляет подпись (с зачёркнутой старой) и БД.
    """
    # 1. поиск записи (без изменений) …
    for tbl, search_fn, drop_fn, upd_fn in (
        ("old_fund", search_old_fund, drop_price_old_fund, update_price_old_fund),
        ("new_fund", search_new_fund, drop_price_new_fund, update_price_new_fund),
        ("land",     search_land,     drop_price_land,     update_price_land),
        ("commerce", search_commerce, drop_price_commerce, update_price_commerce),
    ):
        rec = search_fn(code)
        if rec:
            break
    else:
        logger.error(f"Объявление {code} не найдено при снижении цены")
        return False

    # 2. вычисления (без изменений) …
    msg_id   = rec.get("channel_message_id")
    raw_price = rec.get("Цена", "")
    cur_val   = int(re.sub(r"\D", "", raw_price))
    new_val   = int(re.sub(r"\D", "", new_price))
    if new_val >= cur_val:
        return False

    # 3. подготавливаем данные для build_caption  ̶(̶т̶у̶т̶ ̶и̶д̶ё̶т̶ ̶ф̶л̶а̶г̶)̶
    rec["old_price"]        = raw_price          # обязательно сохраняем старую
    rec["Цена"]             = new_price
    rec["_price_drop_flag"] = True               # ← ключевой флаг для зачёркивания

    caption = build_caption(rec, bot.username)

    # 4. ме-дия-редакт (без изменений) …
    try:
        await bot.edit_message_caption(
            chat_id   = CHANNEL_ID,
            message_id= msg_id,
            caption   = caption,
            parse_mode= "HTML",
        )
    except Exception as e:
        logger.error(f"Failed to edit caption for {code}: {e}")
        return False

    # 5. обновление базы (без изменений) …
    drop_fn(code, new_price)
    return True

async def update_price_in_channel_raise(
    bot: Bot,
    code: str,
    new_price: str
) -> bool:
    """
    Повышает цену в канале по кнопке «Повысить»:
    - Если new_price ≤ old_price: старое old_price не меняется,
      price = new_price, остаётся 🔥Цена снижена и зачёркнутая old_price.
    - Если new_price > old_price: переносит текущую price → old_price,
      ставит price = new_price, убирает 🔥 и зачёркивание.
    Затем удаляет старый пост, репостит новый и обновляет repost_date.
    """
    # 1) Найти запись и таблицу
    for tbl, search_fn, drop_fn, upd_fn in (
        ("old_fund",   search_old_fund,   drop_price_old_fund,   update_price_old_fund),
        ("new_fund",   search_new_fund,   drop_price_new_fund,   update_price_new_fund),
        ("land",       search_land,       drop_price_land,       update_price_land),
        ("commerce",   search_commerce,   drop_price_commerce,   update_price_commerce),
    ):
        rec = search_fn(code)
        if rec:
            break
    else:
        logger.error(f"Объявление {code} не найдено при повышении цены")
        return False

    msg_id = rec.get("channel_message_id")
    if not msg_id:
        logger.error(f"No channel_message_id for {code}")
        return False

    # 2) Получить числовые значения
    baseline_str = rec.get("old_price") or rec.get("price", "")
    current_str  = rec.get("price", "")

    base_val = int(re.sub(r"\D", "", baseline_str))
    new_val  = int(re.sub(r"\D", "", new_price))

    # 3) Две логики в зависимости от сравнения
    if new_val <= base_val:
        # Всё ещё «горячий» коридор: old_price не меняется
        rec["old_price"]        = baseline_str
        rec["Цена"]             = new_price
        rec["_price_drop_flag"] = True
        # Обновляем только price
        upd_fn(code, new_price)
    else:
        rec["old_price"] = None
        rec["Цена"] = new_price
        rec["_price_drop_flag"] = False
        upd_fn(code, new_price)  # обновляем только price
        conn = sqlite3.connect(DB_FILE)  # + обнуляем колонку в БД
        conn.execute(f"UPDATE {tbl} SET old_price = NULL WHERE object_code = ?", (code,))
        conn.commit(); conn.close()

    # 4) Репост: удалить старое сообщение
    await delete_object_in_channel(bot, code, deactivate=False, use_saved_ids=True)

    # 5) Постим заново
    caption = build_caption(rec, bot.username)
    photos = json.loads(rec.get("photos", "[]") or "[]")
    videos = json.loads(rec.get("videos", "[]") or "[]")
    media  = _pack_media(photos, videos, caption)


    if not media:
        logger.error(f"No media for repost {code}")
        return False

    try:
        sent_msgs = await bot.send_media_group(CHANNEL_ID, media)
    except Exception as e:
        logger.error(f"Failed to repost media for {code}: {e}")
        return False

    # 6) Обновить message_id и repost_date в БД
    new_msg_id = sent_msgs[0].message_id
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute(
        f"UPDATE {tbl} "
        "   SET channel_message_id = ?, repost_date = CURRENT_TIMESTAMP "
        " WHERE object_code = ?",
        (new_msg_id, code)
    )
    conn.commit()
    conn.close()

    return True


MAX_ALBUM = 10          # глобальная константа

def _pack_media(photos: list[str],
                videos: list[str],
                caption: str) -> list[InputMedia]:
    """
    Формирует список Media, гарантируя лимит в 10 штук и
    ставя подпись (caption) только на первом элементе.
    """
    media: list[InputMedia] = []

    # сначала фото
    for fid in photos:
        if len(media) == MAX_ALBUM:
            break
        if not media:                       # первое фото → с подписью
            media.append(InputMediaPhoto(fid, caption=caption,
                                          parse_mode="HTML"))
        else:
            media.append(InputMediaPhoto(fid))

    # затем видео
    for fid in videos:
        if len(media) == MAX_ALBUM:
            break
        if not media:                       # альбом может начаться с видео
            media.append(InputMediaVideo(fid, caption=caption,
                                          parse_mode="HTML"))
        else:
            media.append(InputMediaVideo(fid))

    return media


# ─── служебное: экранирование MarkdownV2 ───────────────────────────
_MD2_SPECIAL = r'[_*\[\]()~`>#+\-=|{}.!\\]'
def md2_escape(text: str) -> str:
    """Экранирует спец-символы MarkdownV2."""
    return re.sub(_MD2_SPECIAL, r'\\\g<0>', text)

async def repost_object_in_channel(
    bot,
    code: str,
    new_price: Optional[str],
    user_id: int
) -> bool:
    """
    Репостит объявление:
      - new_price=None  → обычный репост не чаще 3 дней.
      - new_price задана → «горячее» обновление цены:
         • new_val < cur_val:
             – drop_price: old_price = cur_price, price = new_price, 🔥.
         • new_val >= cur_val:
             – если есть old_price и new_val < old_price:
                 * price = new_price, old_price не меняется, 🔥.
             – иначе:
                 * drop_price: old_price = cur_price, price = new_price, без 🔥.
    Затем всегда пытается удалить старый пост (но не зависит от успеха),
    публикует новый, обновляет message_ids и repost_date.
    """
    # 1) Извлечь запись и таблицу
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    rec, table = None, None

    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(f"SELECT * FROM {tbl} WHERE object_code = ?", (code,))
        row = cur.fetchone()
        if row:
            cols = [c[0] for c in cur.description]
            rec   = dict(zip(cols, row))
            table = tbl
            break

    if not rec:
        conn.close()
        return False

    # 2) Обычный репост: не чаще 3 дней
    if new_price is None and rec.get("repost_date"):
        last  = datetime.fromisoformat(rec["repost_date"])
        delta = datetime.now() - last
        limit = timedelta(days=1) if rec.get("old_price") else timedelta(days=3)
        if delta < limit:
            if user_id:
                rem = limit - delta
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"❗️ Объект {code} можно репостнуть через "
                        f"{rem.days} д {rem.seconds // 3600} ч."
                    )
                )
            conn.close()
            return False

    # 3) «Горячее» обновление цены, если задано new_price
    if new_price:
        cur_str      = rec.get("price", "") or ""
        baseline_str = rec.get("old_price") or cur_str

        cur_val  = int(re.sub(r"\D", "", cur_str))
        base_val = int(re.sub(r"\D", "", baseline_str))
        new_val  = int(re.sub(r"\D", "", new_price))

        # 3a) Снижение цены
        if new_val < cur_val:
            drop_funcs = {
                "old_fund":   drop_price_old_fund,
                "new_fund":   drop_price_new_fund,
                "land":       drop_price_land,
                "commerce":   drop_price_commerce,
            }
            drop_funcs[table](code, new_price)
            rec["old_price"]        = cur_str
            rec["price"]            = new_price
            rec["_price_drop_flag"] = True

        # 3b) Повышение или равенство цены
        else:
            up_funcs = {
                "old_fund":   update_price_old_fund,
                "new_fund":   update_price_new_fund,
                "land":       update_price_land,
                "commerce":   update_price_commerce,
            }
            if rec.get("old_price"):
                if new_val < base_val:
                    up_funcs[table](code, new_price)
                    rec["old_price"]        = baseline_str
                    rec["price"]            = new_price
                    rec["_price_drop_flag"] = True
                else:
                    up_funcs[table](code, new_price)
                    rec["old_price"]        = None
                    rec["price"]            = new_price
                    rec["_price_drop_flag"] = False
                    conn.execute(
                        f"UPDATE {table} SET old_price = NULL WHERE object_code = ?",
                        (code,)
                    )
                    conn.commit()
            else:
                up_funcs[table](code, new_price)
                rec["old_price"]        = None
                rec["price"]            = new_price
                rec["_price_drop_flag"] = False

    # 3c) Обычный репост: считаем флаг 🔥 по старой цене, если new_price не задан
    else:
        rec["_price_drop_flag"] = False
        if rec.get("old_price") and rec.get("updated_at"):
            last = datetime.fromisoformat(rec["updated_at"])
            if datetime.now() - last <= timedelta(weeks=5):
                old_val = int(re.sub(r"\D", "", rec["old_price"]))
                cur_val = int(re.sub(r"\D", "", rec.get("price", "") or "0"))
                if old_val > cur_val:
                    rec["_price_drop_flag"] = True
            else:
                rec["old_price"]        = None
                rec["_price_drop_flag"] = False
                conn.execute(
                    f"UPDATE {table} SET old_price = NULL WHERE object_code = ?",
                    (code,)
                )
                conn.commit()

    # 4) Пробуем удалить старый пост, но игнорируем ошибки
    await delete_object_in_channel(bot, code, deactivate=False, use_saved_ids=True)

    # 5) Переименовываем поля для build_caption
    ptype_map = {
        "old_fund": "Старыйфонд",
        "new_fund": "Новыйфонд",
        "land":     "Участок",
        "commerce": "Коммерция",
    }
    rec["ptype"]      = ptype_map[table]
    rec["Тип заявки"] = rec.get("order_type", "Продажа")

    if table in ("old_fund", "new_fund"):
        rec["Ориентир"]          = rec.pop("orientir", None)
        rec["Район"]             = rec.pop("district", None)
        rec["Комнаты"]           = rec.pop("komnaty", None)
        rec["Площадь"]           = rec.pop("ploshad", None)
        rec["Этаж"]              = rec.pop("etazh", None)
        rec["Этажность"]         = rec.pop("etazhnost", None)
        rec["Санузлы"]           = rec.pop("sanuzly", None)
        rec["Состояние"]         = rec.pop("sostoyanie", None)
        rec["Материал строения"] = rec.pop("material", None)
        rec["Дополнительно"]     = rec.pop("dop_info", None)
        rec["Цена"]              = rec.pop("price", None)
        if table == "new_fund":
            rec["ЖК"]            = rec.pop("jk", None)
            rec["Год постройки"] = rec.pop("year", None)
        else:
            rec["Парковка"]      = rec.pop("parkovka", None)

    elif table == "land":
        rec["Ориентир"]           = rec.pop("orientir", None)
        rec["Район"]              = rec.pop("district", None)
        rec["Тип недвижимости"]   = rec.pop("type", None)
        rec["Год постройки"]      = rec.pop("year", None)
        rec["Площадь участка"]    = rec.pop("ploshad_uchastok", None)
        rec["Площадь дома"]       = rec.pop("ploshad_dom", None)
        rec["Размер участка"]     = rec.pop("razmer", None)
        rec["Этажность"]          = rec.pop("etazhnost", None)
        rec["Санузлы"]            = rec.pop("sanuzly", None)
        rec["Состояние"]          = rec.pop("sostoyanie", None)
        rec["Материал строения"]  = rec.pop("material", None)
        rec["Заезд авто"]         = rec.pop("zaezd", None)
        rec["Дополнительно"]      = rec.pop("dop_info", None)
        rec["Цена"]               = rec.pop("price", None)

    else:  # commerce
        rec["Ориентир"]            = rec.pop("orientir", None)
        rec["Район"]               = rec.pop("district", None)
        rec["Целевое назначение"]  = rec.pop("nazna4enie", None)
        rec["Расположение"]        = rec.pop("raspolozhenie", None)
        rec["Этаж"]                = rec.pop("etazh", None)
        rec["Этажность"]           = rec.pop("etazhnost", None)
        rec["Площадь помещения"]   = rec.pop("ploshad_pom", None)
        rec["Площадь участка"]     = rec.pop("ploshad_uchastok", None)
        rec["Учёт НДС"]            = rec.pop("nds", None)
        rec["Собственник"]         = rec.pop("owner", None)
        rec["Дополнительно"]       = rec.pop("dop_info", None)
        rec["Цена"]                = rec.pop("price", None)

    conn.close()

    # 6) Собираем подпись и медиагруппу
    caption = build_caption(rec, bot.username)

    # локальный помощник: безопасно превращает JSON-строку или list → list[str]
    def _as_list(raw) -> list[str]:
        if isinstance(raw, list):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                logger.warning("Bad JSON in photos/videos: %s", raw)
        return []

    photos = _as_list(rec.get("photos"))
    videos = _as_list(rec.get("videos"))
    media  = _pack_media(photos, videos, caption)

    if not media:
        return False

    # 7) Отправляем и сохраняем новые message_ids и repost_date
    try:
        sent = await bot.send_media_group(CHANNEL_ID, media)
    except Exception as e:
        logger.error(f"Не удалось репостнуть объект {code}: {e}")
        return False

    new_ids = [m.message_id for m in sent]
    conn    = sqlite3.connect(DB_FILE)
    cur     = conn.cursor()
    cur.execute(
        f"UPDATE {table} "
        "   SET message_ids = ?, repost_date = CURRENT_TIMESTAMP "
        " WHERE object_code = ?",
        (json.dumps(new_ids), code)
    )
    conn.commit()
    conn.close()

    return True

# ─── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ───────────────────────────────────────
def load_allowed_ids() -> set[int]:
    try:
        with open(USERS_FILE, encoding="utf-8") as f:
            return {
                int(l.split(":",1)[0])
                for l in f
                if (s := l.strip()) and s.split(":",1)[0].isdigit()
            }
    except FileNotFoundError:
        return set()


def _format_price(digits: str) -> str:
    return f"{int(digits):,}".replace(",", " ") + " у.е."

# ─── Обновлённые функции-скаффолды ────────────────────────────
def get_template(data: dict) -> dict[str, Any]:
    ptype = data.get("ptype", "Старыйфонд")
    deal  = data.get("Тип заявки", "Продажа")          # ← оставляем то, что выбрал юзер

    return (
        TEMPLATES.get(ptype, {})
                 .get(deal,
                      TEMPLATES.get(ptype, {})
                               .get("Продажа",
                                    TEMPLATES["Старыйфонд"]["Продажа"]))
    )

def get_menu_fields(data: dict) -> dict[str, List[str]]:
    return get_template(data)["menu"]

def get_manual_fields(data: dict) -> List[str]:
    return get_template(data)["manual"]

def required_fields(data: dict) -> List[str]:
    return get_template(data)["required"]

def all_option_fields() -> set[str]:
    res = set()
    for t in TEMPLATES.values():
        res.update(t["menu"].keys())
    return res

# ─── CSV-УТИЛИТЫ для заявок (без изменений) ────────────────────────
CSV_HEADER = [
    "DateTime", "UserID", "Username", "TelegramName",
    "ClientName", "Phone", "ObjectCode", "RealtorCode",
]

def has_client_secondary_request(user_id: int, object_code: str) -> bool:
    """
    Проверяет, оставлял ли пользователь user_id уже заявку
    на объект object_code в таблице client_secondary.
    """
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute(
        "SELECT 1 FROM client_secondary WHERE user_id = ? AND object_code = ? LIMIT 1",
        (user_id, object_code)
    )
    found = cur.fetchone() is not None
    conn.close()
    return found

def _join_l(*parts) -> str:
    """Соединяет непустые куски через ' l '."""
    return " l ".join([p for p in parts if p])

def _line_summary(ptype, rooms, area, type_ned: str | None = None) -> str:
    """
    • Квартиры / коммерция: прежняя логика.
    • Участок:
        — иконка «🏡»
        — тип недвижимости («Дом» / «Земельный участок»)
        — через « | » площадь участка, если она задана.
    """
    if ptype == "Участок":
        icon      = "🏡"
        type_name = type_ned or "Земельный участок"
        return (f"{icon} {type_name}" + (f" | {area}" if area else ""))

    icon = {"Старыйфонд": "🏠", "Новыйфонд": "🏢",
            "Коммерция": "🏬"}[ptype]
    if ptype == "Коммерция":
        kind = rooms or "Свободного назначения"
        return f"{icon} {_join_l(kind, area)}"

    # квартиры
    kind = "квартира"
    rooms_txt = f"{rooms} - комнатная {kind}" if rooms else kind
    return f"{icon} {_join_l(rooms_txt, area)}"

def _line_location(district, orientir):
    district = f"{district} район"
    parts = [p for p in (district, orientir) if p]
    return "📍 " + ", ".join(parts) if parts else ""

def _join_comma(*parts: str) -> str:
    """Соединяет непустые элементы через ', '."""
    return ", ".join(p for p in parts if p)

def _line_floor(ptype, floor, floors, material):
    mat = material.capitalize() if material else None

    # «этаж …» – с прописной, если материала нет; иначе – со строчной
    if floor and floors:
        fl = f"{'этаж' if mat else 'Этаж'} {floor} из {floors}"
    elif floor:
        fl = f"{'этаж' if mat else 'Этаж'} {floor}"
    else:
        fl = None

    # собираем в нужном порядке: материал, затем этажи
    if mat and fl:
        joined = f"{mat}, {fl}"
    else:
        joined = mat or fl

    return f"🏗 {joined}" if joined else ""

def _line_key(jk: str | None, year: str | None) -> str:
    """
    • оба поля → «🔑 <ЖК> l год постройки: 2025»
    • только ЖК  → «🔑 Sky City»
    • только год → «🔑 Год постройки: 2025»   ← заглавная «Г»
    """
    if jk and year:
        return f"🔑 {_join_l(jk, f'год постройки: {year}')}"
    if jk:
        return f"🔑 {jk}"
    if year:
        return f"🔑 Год постройки: {year}"
    return ""

def _fmt_baths(val: str | int | None) -> str:
    """
    Приводит поле «Кол-во санузлов» к правильной форме:
        • 1  → 1 санузел
        • 2–4→ N санузла
        • 5+ → N санузлов
        • строка с буквами возвращается как есть
    """
    if not val:
        return ""
    s = str(val).strip()
    if s.isdigit():
        n = int(s)
        if n == 1:        return "1 санузел"
        elif 2 <= n <= 4: return f"{n} санузла"
        else:             return f"{n} санузлов"
    return s          # уже готовая строка из меню («раздельный санузел»)

def decline_ru(number: int, one: str, two: str, five: str) -> str:
    """
    Возвращает слово в правильной форме: 1 этаж, 2 этажа, 5 этажей.
    """
    n = abs(int(number))
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return two
    return five

def format_floors(floors: int | str | None) -> str | None:
    if not floors:
        return None
    word = decline_ru(int(floors), "этаж", "этажа", "этажей")
    return f"{floors} {word}"

def build_caption(rec: dict, bot_username: str) -> str:
    """
    Полностью учитывает:
      • 🔑-строку с заглавным «Год постройки» при отсутствии ЖК;
      • материал в старом/новом фонде с маленькой буквы;
      • корректную шапку «Дом / Земельный участок | 4 сот».
    """
    ptype = rec.get("ptype") or "Старыйфонд"
    deal  = rec.get("Тип заявки")
    title = f"#{ptype}" + (f" #{deal}" if deal else "")

    # --- 🔥 Цена снижена -------------------------------------------------
    old_price = rec.get("old_price") if rec.get("_price_drop_flag") else None
    header = [title]
    if old_price:
        header.append("🔥Цена снижена")
    header.append("")             # пустая строка-разделитель

    lines = header                # сюда будем добавлять остальное

    # --- общие переменные -----------------------------------------------
    rooms    = rec.get("Комнаты")
    area     = (rec.get("Площадь") or
                rec.get("Площадь помещения") or
                rec.get("Площадь участка"))
    district = rec.get("district") or rec.get("Район")
    orientir = rec.get("Ориентир")

    floor    = rec.get("Этаж")
    floors   = rec.get("Этажность")
    material = rec.get("Материал строения") or rec.get("Материал")

    cond   = rec.get("Состояние")
    baths  = rec.get("Санузлы")
    jk     = rec.get("ЖК")
    year   = rec.get("Год постройки")

    house_area = rec.get("Площадь дома")
    lot_size   = rec.get("Размер участка")
    drive_in   = rec.get("Заезд авто")

    position = rec.get("Расположение")
    purpose = rec.get("Целевое назначение")
    if isinstance(purpose, list):  # мультивыбор
        purpose = ", ".join(purpose)
    owner = rec.get("Собственник") if deal == "Аренда" else None
    price = rec.get("Цена")

    # --- блоки по типу объекта ------------------------------------------
    if ptype in ("Старыйфонд", "Новыйфонд"):
        lines += [
            _line_summary(ptype, rooms, area),
            _line_location(district, orientir),
        ]
        if ptype == "Новыйфонд":
            key_line = _line_key(jk, year)
            if key_line:
                lines.append(key_line)
        lines += [
            _line_floor(ptype, floor, floors, material),
            f"🔧 {_join_l(cond, _fmt_baths(baths))}",
        ]

    elif ptype == "Участок":
        type_ned = rec.get("Тип недвижимости") or "Земельный участок"
        lines += [
            _line_summary(ptype, None, rec.get("Площадь участка"), type_ned),
            _line_location(district, orientir),
        ]
                # 🏗 — материал/площадь дома (показываем, только если есть данные)
        house_line = _join_l(
            material and material.capitalize(),
            house_area and f"Площадь дома: {house_area}"
        )
        if house_line:
            lines.append(f"🏗 {house_line}")

        # 🔧 — состояние / этажность (как было)
        lines.append(
            f"🔧 {_join_l(cond, format_floors(floors))}"
        )

        # 🚗 — заезд авто / размер участка (только при наличии)
        drive_line = _join_l(
            drive_in and ("Заезд авто" if drive_in == "Есть" else "Заезд авто отсутствует"),
            lot_size and (f'Размер участка: {lot_size}' if not drive_in else f'размер участка: {lot_size}')
        )
        if drive_line:
            lines.append(f"🚗 {drive_line}")

    else:  # Коммерция
        # Учёт НДС выводим только при аренде
        vat = rec.get("Учёт НДС") if deal == "Аренда" else None

        first = f"🏬 {purpose}" if purpose else "🏬"
        lines += [
            first,
            _line_location(district, orientir),
        ]

        # ─ «📪 Расположение | этаж …» ─────────────────────────────
        if position or floor:
            fl = (
                floor and floors and f'этаж {floor} из {floors}'
                or  floor and f'этаж {floor}'
            )
            # если Расположение пусто, пишем «Этаж …» с заглавной
            if not position and fl:
                fl = fl.capitalize()          # «Этаж …»
            lines.append(f"📪 {_join_l(position, fl)}")

        # ─ площади помещения/участка ─────────────────────────────
        space_line = _join_l(
            (rec.get('Площадь помещения') and f"Помещение: {rec['Площадь помещения']}"),
            (rec.get('Площадь участка')   and f"участок: {rec['Площадь участка']}")
        )
        if space_line:
            lines.append(f"🏗 {space_line}")

        # ─ цена (+ НДС) ──────────────────────────────────────────
        if owner:
            lines.append(f"👨‍💼 Собственник: {owner}")
        # ─ цена (+ НДС) ──────────────────────────────────────────
        vat_str   = _bold(vat) if vat else ''
        price_core = f"{_bold(price)}{(' ' + vat_str) if vat else ''}"
        if old_price:
            lines.append(f"💵 <s>{old_price}</s> {price_core}")
        else:
            lines.append(f"💵 {price_core}")

    # --- цена ------------------------------------------------------------
    if ptype != "Коммерция":
        if old_price:
            lines.append(f"💵 <s>{old_price}</s> <b>{price}</b>")
        else:
            lines.append(f"💵 <b>{price}</b>")

    # --- Дополнительно ---------------------------------------------------
    extra = rec.get("Дополнительно")
    if extra:
        lines += ["", "Дополнительно:", extra]

    # --- код + ссылки ----------------------------------------------------
    code     = rec.get("object_code") or rec.get("code") or ""
    realtor  = (rec.get("Риэлтор") or
                rec.get("realtor")  or
                rec.get("realtor_code", ""))
    link = f"https://t.me/{bot_username}?start=object={code}_realtor={realtor}"


    lines += [
        "", f"Код объекта: {code}", "",
        f'<a href="{link}">Оставить заявку</a>',
    ]

    # финальная зачистка двойных пустых строк
    caption = "\n".join(
        ln.rstrip(" l")
        for i, ln in enumerate(lines)
        if ln or (i and lines[i - 1])
    )
    return caption


EMOJI_BULLET = "•"

def build_keyboard(data: dict) -> InlineKeyboardMarkup:

    # текущий тип
    ptype = data.get("ptype", PROPERTY_TYPES[0])

    # 1) строка типа недвижимости
    type_row: List[InlineKeyboardButton] = [
        InlineKeyboardButton(
            f"{EMOJI_BULLET}{t}" if t == ptype else t,
            callback_data=f"ptype:{t}"
        )
        for t in PROPERTY_TYPES
    ]
    rows: List[List[InlineKeyboardButton]] = [type_row]

    # локальная функция для метки кнопки
    def label(fld: str) -> str:
        # внутренний ключ для «Района»
        key = "district" if fld == "Район" else fld
        val = data.get(key)
        txt = f"{fld}: {val}" if val else fld
        if key in required_fields(data):
            txt += "*"
        return txt

    # 2) всегда «Тип заявки»
    rows.append([
        InlineKeyboardButton(label("Тип заявки"), callback_data="menu:Тип заявки")
    ])

    # 3) всегда «Район»
    rows.append([
        InlineKeyboardButton(label("Район"), callback_data="menu:district")
    ])

    # 4) остальные поля из меню
    for fld in get_menu_fields(data):
        if fld == "Тип заявки":
            continue  # дубли не нужны
        rows.append([
            InlineKeyboardButton(label(fld), callback_data=f"menu:{fld}")
        ])

    # 5) ручные поля двумя в ряд
    btns = [
        InlineKeyboardButton(label(f), callback_data=f"ask:{f}")
        for f in get_manual_fields(data)
    ]
    for i in range(0, len(btns), 2):
        rows.append(btns[i : i + 2])

    # 6) публиковать / отмена
    rows.append([
        InlineKeyboardButton("✅ Опубликовать", callback_data="publish"),
        InlineKeyboardButton("❌ Отмена",      callback_data="cancel"),
    ])

    return InlineKeyboardMarkup(rows)


# ─────────── команда /ad ───────────────────────────────────────────
async def ad_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in load_allowed_ids():
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data["ptype"] = PROPERTY_TYPES[0]
    context.user_data["realtor_code"] = str(update.effective_user.id)
    context.user_data["realtor_uname"]  = (
            "@" + update.effective_user.username
            if update.effective_user.username else ""
    )
    await update.message.reply_text(
        "Отправьте *одним* сообщением медиагруппу (до 10 фото/видео).\n"
        "Бот подставит ваш аккаунт автоматически.\n"
        "Отменить: /cancel",
        parse_mode="Markdown"
    )

    return WAITING_MEDIA

# ─────────── HANDLE_MEDIA (safe) ──────────────────────────────────
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return WAITING_MEDIA
    if not (update.message.photo or update.message.video):
        return await ad_error_and_cancel(update, "Разрешены только фото или видео.")
    ud = context.user_data
    grp_id = update.message.media_group_id or update.message.message_id
    ud.setdefault("group_id", grp_id)
    if grp_id != ud["group_id"]:
        return await ad_error_and_cancel(update, "Вся медиагруппа должна прийти одним сообщением.")
    ud.setdefault("album_msgs", []).append(update.message)
    if update.message.caption:
        ud["caption"] = update.message.caption
    # (re)start таймер
    if task := ud.get("_publish_task"):
        if not task.done():
            task.cancel()
    ud["_publish_task"] = asyncio.create_task(_delayed_prepare(update, context))
    return WAITING_MEDIA

async def _delayed_prepare(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await asyncio.sleep(0.3)
        await start_editing(update, context)
    except asyncio.CancelledError:
        pass

async def start_editing(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Принимает медиагруппу от риелтора, присваивает автоматический
    object_code, связывает объявление с Telegram-ID автора и
    создаёт превью-альбом + сообщение «Описание» с интерактивной
    клавиатурой.
    """
    ud      = context.user_data
    caption = ud.get("caption", "")           # caption пригодится для build_caption()

    # 1) формируем новый object_code
    obj_code = next_object_code()

    # 2) проверяем, что такого object_code ещё нет в базе (status = active)
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    for table in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(
            f"SELECT 1 FROM {table} "
            "WHERE object_code = ? AND status = 'active' LIMIT 1",
            (obj_code,)
        )
        if cur.fetchone():
            await update.message.reply_text(
                f"❗️ Объект {obj_code} уже опубликован.\n"
                f"Перезапустите /ad, чтобы сгенерировать новый код."
            )
            conn.close()
            return ConversationHandler.END
    conn.close()

    # 3) сохраняем базовые поля объявления
    ud["object_code"]   = obj_code
    ud["realtor_code"]  = str(update.effective_user.id)          # ID риелтора
    ud["realtor_uname"] = (
        "@" + update.effective_user.username
        if update.effective_user.username else ""
    )

    # 4) собираем медиагруппу с «правильной» подписью
    initial_caption = build_caption(ud, context.bot.username)
    media_album: list[InputMedia] = []
    for i, m in enumerate(ud.get("album_msgs", [])):
        fid = m.photo[-1].file_id if m.photo else m.video.file_id
        if i == 0:                                              # первая — с подписью
            media_album.append(
                InputMediaPhoto(fid, caption=initial_caption, parse_mode="HTML")
                if m.photo else
                InputMediaVideo(fid, caption=initial_caption, parse_mode="HTML")
            )
        else:                                                   # остальные — без подписи
            media_album.append(
                InputMediaPhoto(fid) if m.photo else InputMediaVideo(fid)
            )

    sent = await context.bot.send_media_group(update.effective_chat.id, media_album)
    ud["preview_first_id"] = sent[0].message_id   # ID «обложки» для последующих правок

    # 5) текст «Описание» + клавиатура
    desc_msg = await update.message.reply_text(
        "Описание:\n" + initial_caption,
        parse_mode  = "HTML",
        reply_markup=build_keyboard(ud),
    )
    ud["desc_mid"] = desc_msg.message_id

    return EDITING



async def safe_edit_reply_markup(msg, **kwargs):
    """
    q  – объект CallbackQuery или Message
    Остальные kwargs – как в edit_message_reply_markup
    Проглатывает 'Message is not modified'.
    """
    try:
        await msg.edit_message_reply_markup(**kwargs)
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            raise  # все остальные ошибки пробрасываем дальше

async def edit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает ВСЕ inline-кнопки мастера /ad.
    После любого изменения вызывает refresh_description,
    а при открытии под-меню меняет только reply_markup.
    """
    q    = update.callback_query
    data = context.user_data
    act  = q.data
    await q.answer()

    # ── 1. Cancel / Publish ────────────────────────────────────
    if act == "cancel":
        await q.edit_message_text("Отменено.")
        data.clear()
        return ConversationHandler.END
    if act == "publish":
        return await finalize_publish(update, context)

    # ── 2. смена типа недвижимости ─────────────────────────────
    if act.startswith("ptype:"):
        new_ptype = act.split(":", 1)[1]
        if new_ptype != data.get("ptype"):
            data["ptype"] = new_ptype
            # сбрасываем неуниверсальные поля
            keep = {"ptype", "object_code", "realtor_code",
                    "album_msgs", "group_id", "caption",
                    "preview_first_id", "desc_mid"}
            for k in list(data):
                if k not in keep:
                    data.pop(k)
        await refresh_description(update, context)
        return EDITING

    if act == "menu:Тип заявки":
        rows = [
            [InlineKeyboardButton(txt, callback_data=f"m:tz:{i}")]
            for i, txt in enumerate(DEAL_TYPES)  # DEAL_TYPES = ["Продажа", "Аренда"]
        ]
        rows.append([InlineKeyboardButton("← Назад", callback_data="back")])

        await safe_edit_reply_markup(q, reply_markup=InlineKeyboardMarkup(rows))
        return EDITING
    elif act == "menu:district":
        rows = [[InlineKeyboardButton(d, callback_data=f"m:d:{i}")]
                for i, d in enumerate(DISTRICTS)]
    elif act.startswith("menu:"):
        fld = act.split(":", 1)[1]
        opts = get_menu_fields(data).get(fld, [])
        key = FIELD_KEYS[fld]

        # --- мультивыбор только для Целевого назначения ---------------
        if fld == "Целевое назначение":
            chosen = set(data.get(fld, []))  # список уже выбранных
            rows = []
            for i, txt in enumerate(opts):
                mark = "✅ " if txt in chosen else ""
                rows.append([InlineKeyboardButton(
                    mark + txt, callback_data=f"t:{key}:{i}"
                )])
        else:
            rows = [[InlineKeyboardButton(o, callback_data=f"m:{key}:{i}")]
                    for i, o in enumerate(opts)]
    else:
        rows = None   # не «menu:…»

    if rows is not None:
        rows.append([InlineKeyboardButton("← Назад", callback_data="back")])
        await safe_edit_reply_markup(q, reply_markup=InlineKeyboardMarkup(rows))
        return EDITING

    # ── 4. Выбор из выпадающего списка (m:…) ───────────────────
    if act.startswith("t:"):
        _, key, idx = act.split(":", 3)
        idx = int(idx)
        fld = KEY_FIELDS[key]  # == 'Целевое назначение'
        value = get_menu_fields(data)[fld][idx]

        chosen: list = data.get(fld, [])
        if value in chosen:
            chosen.remove(value)
        else:
            if len(chosen) >= 3:
                await q.answer("Можно выбрать не более трёх.", show_alert=True)
                return EDITING
            chosen.append(value)
        data[fld] = chosen

        # если выбрано < 2 пунктов — остаёмся в том же меню
        if len(chosen) < 3:
            rows = [
                [InlineKeyboardButton(("✅ " if opt in chosen else "") + opt,
                                      callback_data=f"t:{key}:{i}")]
                for i, opt in enumerate(get_menu_fields(data)[fld])
            ]
            rows.append([InlineKeyboardButton("← Назад", callback_data="back")])
            await safe_edit_reply_markup(q, reply_markup=InlineKeyboardMarkup(rows))
            return EDITING

        # выбрали второй пункт → возвращаемся в главное меню
        await refresh_description(update, context)
        return EDITING

    if act.startswith("m:"):
        _, key, idx = act.split(":", 2)
        idx = int(idx)
        if key == "d":
            data["district"]   = DISTRICTS[idx]
        elif key == "tz":
            choices = DEAL_TYPES  # ["Продажа", "Аренда"] всегда
            if idx >= len(choices):  # защита от случайных значений
                await q.answer("Некорректный выбор.", show_alert=True)
                return EDITING

            data["Тип заявки"] = choices[idx]
        else:
            fld   = KEY_FIELDS[key]
            data[fld] = get_menu_fields(data)[fld][idx]

        await refresh_description(update, context)
        return EDITING

    # ── 5. Поля ручного ввода (ask:…) ──────────────────────────
    if act.startswith("ask:"):
        fld = act.split(":", 1)[1]
        prompt = "Введите фасад Размер участка (м):" if fld == "Размер участка" else f"Введите значение поля «{fld}»:"
        data["await_field"] = "Размер участка_len" if fld == "Размер участка" else fld

        if old := data.pop("ask_mid", None):
            try: await context.bot.delete_message(update.effective_chat.id, old)
            except: pass
        msg = await context.bot.send_message(update.effective_chat.id, prompt)
        data["ask_mid"] = msg.message_id

        await safe_edit_reply_markup(
            q,
            reply_markup=build_keyboard(data)
        )
        return EDITING

    # ── 6. «← Назад» ───────────────────────────────────────────
    await refresh_description(update, context)
    return EDITING



# ─────────── HANDLE_MANUAL_INPUT (v4) ─────────────────────────────
async def handle_manual_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Ручной ввод. Удаляем все подсказки бота и сообщения пользователя.
    Размер участка обрабатываем в два шага, сохраняя длину × ширину.
    """
    data = context.user_data
    fld = data.get("await_field")
    if not fld:
        return EDITING

    txt = update.message.text.strip()

    # --- двухшаговый «Размер участка» — сначала длина -------------------
    if fld == "Размер участка_len":
        if not txt.isdigit():
            await update.message.reply_text("Введите целое число для фасада.")
            return EDITING

        # удаляем старую подсказку и сообщение юзера
        if mid := data.pop("ask_mid", None):
            await context.bot.delete_message(update.effective_chat.id, mid)
        await update.message.delete()

        data["_fasad_len"] = txt
        data["await_field"] = "Размер участка_wid"

        # новая подсказка — сохраняем ask_mid для её удаления
        m = await context.bot.send_message(update.effective_chat.id, "Введите глубину (м):")
        data["ask_mid"] = m.message_id
        return EDITING

    # --- второй шаг «Размер участка»: ширина ---------------------------
    if fld == "Размер участка_wid":
        if not txt.isdigit():
            await update.message.reply_text("Введите целое число для глубины.")
            return EDITING

        # удаляем подсказку и сообщение юзера
        if mid := data.pop("ask_mid", None):
            await context.bot.delete_message(update.effective_chat.id, mid)
        await update.message.delete()

        length = data.pop("_fasad_len")
        data["Размер участка"] = f"{length} × {txt}"
        data.pop("await_field", None)

        await refresh_description(update, context)
        return EDITING

    # --- остальные поля: сначала удаляем подсказку, потом сообщение юзера ---
    # подсказка лежит в data["ask_mid"]
    if mid := data.pop("ask_mid", None):
        await context.bot.delete_message(update.effective_chat.id, mid)

    # --- площади ---
    if fld == "Площадь участка":
        norm = txt.replace(",", ".")
        if not re.fullmatch(r"\d+(\.\d{1,2})?", norm):
            await update.message.reply_text("Неверный формат. Пример: 12.5")
            return EDITING
        val = norm.replace(".", ",") + " сот"
    elif fld in AREA_FIELDS:
        norm = txt.replace(",", ".")
        if not re.fullmatch(r"\d+(\.\d{1,2})?", norm):
            await update.message.reply_text("Неверный формат. Пример: 45.6")
            return EDITING
        val = norm.replace(".", ",") + " м²"

    # --- цена ---
    elif fld == PRICE_FIELD:
        digits = re.sub(r"\D", "", txt)

        # минимальная длина нужна, только если это не аренда
        if len(digits) < 5 and data.get("Тип заявки") != "Аренда":
            await update.message.reply_text("Цена должна быть минимум пятизначной.")
            return EDITING

        val = _format_price(digits)

    # --- целые поля ---
    elif fld in INT_FIELDS:
        if not txt.isdigit() or (fld == "Год постройки" and len(txt) != 4):
            msg = "Год постройки — четыре цифры." if fld == "Год постройки" else "Введите целое число."
            await update.message.reply_text(msg)
            return EDITING
        val = txt

    elif fld == "Дополнительно":
        # Удаляем emoji / «спец» символы (Sм. Unicode So & Sk и доп. диапазоны)
        clean = re.sub(
            r"["
            r"\U0001F300-\U0001FAFF"  # emoji (символы, флаги, объекты…)
            r"\u2600-\u27BF"          # дополнительные пиктограммы
            r"\uFE0F"                 # variation selectors
            r"]+",
            "",
            txt,
            flags=re.UNICODE,
        ).strip()

        if len(clean) > 100:
            await update.message.reply_text("«Дополнительно» не должно превышать 100 символов.")
            return EDITING

        val = clean

    elif fld == "ЖК":
        if not re.fullmatch(r"[A-Za-z0-9\s\-]+", txt):
            await update.message.reply_text("Название ЖК пишите латиницей.")
            return EDITING
        val = txt

    else:
        val = txt

    # сохраняем значение и удаляем сообщение пользователя
    data[fld] = val
    await update.message.delete()
    data.pop("await_field", None)

    await refresh_description(update, context)
    return EDITING


# ─────────── REFRESH_DESCRIPTION (safe) ───────────────────────────

async def refresh_description(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Перерисовывает и подпись под обложкой, и текст «Описание»
    после любого изменения полей в мастере /ad.
    """
    ud = context.user_data
    if not ud:
        return

    caption = build_caption(ud, context.bot.username)
    if len(caption) > 1024:
        caption = caption[:1000].rstrip() + "…"

    chat_id = update.effective_chat.id

    # ── 1. редактируем подпись у первой фотографии альбома ───────
    msg_id = ud.get("preview_first_id")
    if msg_id:
        first = ud["album_msgs"][0]
        fid   = first.photo[-1].file_id if first.photo else first.video.file_id
        media_obj = (
            InputMediaPhoto(fid, caption=caption, parse_mode=ParseMode.HTML)
            if first.photo else
            InputMediaVideo(fid, caption=caption, parse_mode=ParseMode.HTML)
        )
        try:
            await context.bot.edit_message_media(chat_id=chat_id, message_id=msg_id, media=media_obj)
        except telegram.error.BadRequest:
            pass

    # ── 2. обновляем текст + клавиатуру «Описание» ───────────────
    if ud.get("desc_mid"):
        try:
            await context.bot.edit_message_text(
                chat_id    = chat_id,
                message_id = ud["desc_mid"],
                text       = "Описание:\n" + caption,
                parse_mode = ParseMode.HTML,
                reply_markup = build_keyboard(ud),
            )
        except telegram.error.BadRequest:
            pass


async def finalize_publish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Проверяет обязательные поля, отсутствие дубля по object_code
    и публикует альбом в канал.
    """
    ud      = context.user_data
    ptype   = ud.get("ptype", PROPERTY_TYPES[0])
    obj_id  = ud.get("object_code")

    # 1) обязательные поля
    missing = [f for f in required_fields(ud) if not ud.get(f)]
    if missing:
        await context.bot.send_message(
            update.effective_chat.id,
            "Заполните все поля перед публикацией: " + ", ".join(missing)
        )
        await refresh_description(update.effective_chat.id, context)
        return EDITING

    # 2) защита от дублей: смотрим БД, статус = active
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    duplicate = False
    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(
            f"SELECT 1 FROM {tbl} WHERE object_code = ? AND status = 'active' LIMIT 1",
            (obj_id,)
        )
        if cur.fetchone():
            duplicate = True
            break
    conn.close()

    if duplicate:
        # две лишние answer() нужны, чтобы убрать «часики» у callback-кнопки
        await update.callback_query.answer()
        await update.callback_query.answer()
        await context.bot.send_message(
            update.effective_chat.id,
            f"❗️ Объект с кодом {obj_id} уже опубликован в канале. "
            f"Измените код или используйте /myads."
        )
        await refresh_description(update.effective_chat.id, context)
        return EDITING

    # 3) строим caption + публикуем
    caption_html = build_caption(ud, context.bot.username)

    media_album = []
    for i, m in enumerate(ud["album_msgs"]):
        fid = m.photo[-1].file_id if m.photo else m.video.file_id
        if i == 0:
            media_album.append(
                InputMediaPhoto(fid, caption=caption_html, parse_mode="HTML")
                if m.photo else
                InputMediaVideo(fid, caption=caption_html, parse_mode="HTML")
            )
        else:
            media_album.append(InputMediaPhoto(fid) if m.photo else InputMediaVideo(fid))

    try:
        sent = await context.bot.send_media_group(CHANNEL_ID, media_album)
        await update.callback_query.edit_message_text("✅ Опубликовано в канал.")
        message_ids = [m.message_id for m in sent]
        first_msg = sent[0]
        link = (
            f"https://t.me/{first_msg.chat.username}/{first_msg.message_id}"
            if first_msg.chat.username else
            f"https://t.me/c/{str(CHANNEL_ID)[4:]}/{first_msg.message_id}"
        )
    except Exception as e:
        await update.callback_query.edit_message_text(f"Ошибка публикации: {e}")
        return EDITING

    # ─── НОВЫЙ БЛОК: сохраняем в БД ──────────────────────────────────

    # 1) Собираем file_id медиа, которые вы уже достали в media_album или сохранили в ud
    photos = [m.photo[-1].file_id for m in ud["album_msgs"] if m.photo]
    videos = [m.video.file_id for m in ud["album_msgs"] if m.video]

    # 2) Подготавливаем общий словарь
    db_data = {
        **ud,               # все поля, которые накопились (Ориентир, Район, Цена и т.п.)
        "photos": photos,
        "videos": videos,
        "message_ids": message_ids,
    }

    # 3) Вставляем в нужную таблицу
    if ptype == "Старыйфонд":
        insert_into_old_fund(db_data)
    elif ptype == "Новыйфонд":
        insert_into_new_fund(db_data)
    elif ptype == "Участок":
        insert_into_land(db_data)
    elif ptype == "Коммерция":
        insert_into_commerce(db_data)

    # ────────────────────────────────────────────────────────────────

    # 3-бис) репост-дата = время публикации
    table_map = {
        "Старыйфонд": "old_fund",
        "Новыйфонд":  "new_fund",
        "Участок":    "land",
        "Коммерция":  "commerce",
    }
    tbl = table_map[ptype]

    conn = sqlite3.connect(DB_FILE)
    with conn:
        conn.execute(
            f"UPDATE {tbl} "
            "SET repost_date = CURRENT_TIMESTAMP "
            "WHERE object_code = ?",
            (obj_id,)
        )
    conn.close()

    # 4) Очищаем пользовательские данные и выходим
    ud.clear()
    return ConversationHandler.END


async def ad_error_and_cancel(update: Update, text: str):
    await update.message.reply_text(f"{text}\nПопробуйте /ad заново.")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Диалог отменён.")
    context.user_data.clear()
    return ConversationHandler.END


# ─────────── кеширование ссылок из канала (без изменений) ──────────
async def channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post
    if msg and msg.caption:
        m = re.search(r"Код объекта:\s*(\d+)", msg.caption)
        if m:
            code = m.group(1)
            link = (f"https://t.me/{msg.chat.username}/{msg.message_id}"
                    if msg.chat.username else
                    f"https://t.me/c/{str(msg.chat.id)[4:]}/{msg.message_id}")

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    uid  = update.effective_user.id


    # 2) Просто /start без параметров
    if not args:
        await update.message.reply_text(
            "Вас приветствует бот 👋\n\n"
            "Чтобы оставить заявку — перейдите по ссылке из объявления в [канале](https://t.me/pravdainedvijimost)\n"
            "или звоните напрямую: +998938013204",
            parse_mode="Markdown"
        )
        return ConversationHandler.END

    # 3) Заявка по вторичке: /start object=…_realtor=…
    m = re.match(r"object=(\d+)_realtor=(\d+)", " ".join(args))
    if not m:
        await update.message.reply_text("Неверная ссылка.")
        return ConversationHandler.END

    obj, realtor = m.groups()

    # 3a) Проверяем статус объявления — должно быть active
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    status = None
    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(f"SELECT status FROM {tbl} WHERE object_code = ? LIMIT 1", (obj,))
        row = cur.fetchone()
        if row:
            status = row[0]
            break
    conn.close()
    if status != "active":
        await update.message.reply_text(
            "❗️ К сожалению, на этот объект заявки закрыты."
        )
        return ConversationHandler.END

    # 3c) проверяем в БД exact user_id + object_code
    if has_client_secondary_request(uid, obj):
        await update.message.reply_text(
            "По этому объекту вы уже оставляли заявку.\n"
            "Вернитесь в (канал)[https://t.me/pravdainedvijimost] и выберите другой объект.\n\n"
            "Если с вами не связались, обратитесь по номеру: +998938013204",
            parse_mode="Markdown"
        )
        return ConversationHandler.END


    # b) сохраняем в контекст основные параметры
    context.user_data.update({
        "object_code":   obj,
        "realtor_code":  realtor,
        "username":      update.effective_user.username or "",
        "telegram_name": update.effective_user.full_name or "",
    })

    # c) если пользователь уже в БД (secondary) — сразу сохраняем заявку
    prev_sec = get_last_client_secondary(uid)
    if prev_sec:
        name, phone = prev_sec
        context.user_data["client_name"]   = name
        context.user_data["phone_number"]  = phone
        return await finish_request(update, context)

    # d) новый клиент — спрашиваем имя
    await update.message.reply_text(f"Объект №{obj}. Введите, пожалуйста, ваше имя:")
    return ASK_NAME


async def ask_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    client = update.message.text.strip()
    context.user_data["client_name"] = client

    # 30-днейний чек
    uid = update.effective_user.id
    rec = get_client_base(uid)
    if rec:
        last = datetime.fromisoformat(rec["date"])
        if (datetime.utcnow() - last).days < 30:
            # телефон ещё актуален — сразу в финал
            context.user_data["phone_number"] = rec["phone"]
            return await finish_request(update, context)

    kb = ReplyKeyboardMarkup([[KeyboardButton("Отправить телефон", request_contact=True)]],
                             resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text("Отправьте контакт:", reply_markup=kb)
    return ASK_PHONE

async def ask_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Проверяем, что это контакт от кнопки
    if not update.message.contact:
        return await update.message.reply_text("Нажмите кнопку «Отправить телефон».")

    # Получаем «сырое» значение и оставляем только цифры
    raw = update.message.contact.phone_number  # например "+998901234567" или "998901234567"
    digits = re.sub(r"\D+", "", raw)           # "998901234567"
    phone = f"+{digits}"                       # "+998901234567"

    # Сохраняем нормализованный номер
    context.user_data["phone_number"] = phone

    # После ввода телефона — создаём заявку
    creating_msg = await update.message.reply_text("Создаётся заявка...")
    context.user_data["creating_msg_id"] = creating_msg.message_id

    # Переходим к финальной функции обработки заявки
    return await finish_request(update, context)


async def get_object_link(bot: Bot, code: str) -> Optional[str]:
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    for tbl in ("old_fund","new_fund","land","commerce"):
        cur.execute(
            f"SELECT message_ids FROM {tbl} WHERE object_code = ? AND status = 'active'",
            (code,)
        )
        row = cur.fetchone()
        if row and row[0]:
            ids = json.loads(row[0])
            first = ids[0]
            # тут можно получить username один раз через bot.get_chat или хранить его константой
            chat = await bot.get_chat(CHANNEL_ID)
            if chat.username:
                link = f"https://t.me/{chat.username}/{first}"
            else:
                link = f"https://t.me/c/{str(CHANNEL_ID)[4:]}/{first}"
            conn.close()
            return link
    conn.close()
    return None


TABLE_PTYPE = {
    "old_fund":    "Старыйфонд",
    "new_fund":    "Новыйфонд",
    "land":        "Участок",
    "commerce":    "Коммерция",
}


async def forward_object_post(bot, chat_id: int, code: str) -> bool:
    """
    Берёт из realty.db запись по object_code, собирает media_group
    и отправляет пользователю только фото/видео + описание + код объекта,
    без ссылок «Оставить заявку» и «Больше объектов…».
    """
    # 1) достаём запись из любой таблицы
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    data = None
    table = None
    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(f"SELECT * FROM {tbl} WHERE object_code = ?", (code,))
        row = cur.fetchone()
        if row:
            cols = [c[0] for c in cur.description]
            data = dict(zip(cols, row))
            data["ptype"] = TABLE_PTYPE[tbl]
            data["Тип заявки"] = data.get("order_type", "Продажа")
            table = tbl
            break
    conn.close()

    if not data:
        logger.warning(f"forward_object_post: object {code} not found in DB")
        return False

    # 2) маппим колонки SQL → русские поля, ожидаемые build_caption()
    if table == "old_fund":
        data["Ориентир"]          = data.pop("orientir", None)
        data["Комнаты"]           = data.pop("komnaty", None)
        data["Площадь"]           = data.pop("ploshad", None)
        data["Этаж"]              = data.pop("etazh", None)
        data["Этажность"]         = data.pop("etazhnost", None)
        data["Санузлы"]   = data.pop("sanuzly", None)
        data["Состояние"]         = data.pop("sostoyanie", None)
        data["Материал строения"] = data.pop("material", None)
        data["Парковка"]          = data.pop("parkovka", None)
        data["Дополнительно"]     = data.pop("dop_info", None)
        data["Цена"]              = data.pop("price", None)
    elif table == "new_fund":
        data["Ориентир"]          = data.pop("orientir", None)
        data["ЖК"]                = data.pop("jk", None)
        data["Год постройки"]     = data.pop("year", None)
        data["Комнаты"]           = data.pop("komnaty", None)
        data["Площадь"]           = data.pop("ploshad", None)
        data["Этаж"]              = data.pop("etazh", None)
        data["Этажность"]         = data.pop("etazhnost", None)
        data["Санузлы"]   = data.pop("sanuzly", None)
        data["Состояние"]         = data.pop("sostoyanie", None)
        data["Материал строения"] = data.pop("material", None)
        data["Дополнительно"]     = data.pop("dop_info", None)
        data["Цена"]              = data.pop("price", None)
    elif table == "land":
        data["Ориентир"]          = data.pop("orientir", None)
        data["Тип недвижимости"]  = data.pop("type", None)
        data["Год постройки"]     = data.pop("year", None)
        data["Площадь участка"]   = data.pop("ploshad_uchastok", None)
        data["Площадь дома"]      = data.pop("ploshad_dom", None)
        data["Размер участка"]    = data.pop("razmer", None)
        data["Этажность"]         = data.pop("etazhnost", None)
        data["Санузлы"]   = data.pop("sanuzly", None)
        data["Состояние"]         = data.pop("sostoyanie", None)
        data["Материал строения"] = data.pop("material", None)
        data["Заезд авто"]        = data.pop("zaezd", None)
        data["Дополнительно"]     = data.pop("dop_info", None)
        data["Цена"]              = data.pop("price", None)
    else:  # commerce
        data["Ориентир"]           = data.pop("orientir", None)
        data["Целевое назначение"] = data.pop("nazna4enie", None)
        data["Расположение"]       = data.pop("raspolozhenie", None)
        data["Этаж"]               = data.pop("etazh", None)
        data["Этажность"]          = data.pop("etazhnost", None)
        data["Площадь помещения"]  = data.pop("ploshad_pom", None)
        data["Площадь участка"]    = data.pop("ploshad_uchastok", None)
        data["Учёт НДС"] = data.pop("nds", None)
        data["Дополнительно"]      = data.pop("dop_info", None)
        data["Цена"]               = data.pop("price", None)

    # 3) строим подпись и отфильтровываем лишние строки
    bot_user    = await bot.get_me()
    full_caption = build_caption(data, bot_user.username)
    caption_lines = [
        ln for ln in full_caption.splitlines()
        if "Оставить заявку" not in ln and "Больше объектов" not in ln
    ]
    caption = "\n".join(caption_lines)

    # 4) безопасно собираем media_group (максимум 10 элементов)
    photos = json.loads(data.get("photos", "[]") or "[]")
    videos = json.loads(data.get("videos", "[]") or "[]")

    media = _pack_media(photos, videos, caption)   # гарантирует лимит 10
    if not media:                                  # вдруг нет ни одного file_id
        logger.warning(f"forward_object_post: no media for {code}")
        return False

    # 5) шлём
    try:
        await bot.send_media_group(chat_id, media)
        return True
    except Exception as e:
        logger.error(f"forward_object_post send error: {e}")
        return False

async def finish_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:

    uid = update.effective_user.id
    data = context.user_data

    # 0) Если пользователь уже был — подгружаем имя и телефон
    if not data.get("client_name") or not data.get("phone_number"):
        prev = get_last_client_secondary(uid)
        if prev:
            data["client_name"], data["phone_number"] = prev

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 1) Сохраняем в БД
    insert_client_secondary(
        user_id       = uid,
        phone         = data["phone_number"],
        username      = data.get("username", ""),
        telegram_name = data.get("telegram_name", ""),
        client_name   = data["client_name"],
        object_code   = data["object_code"],
        realtor_code  = data["realtor_code"],
    )

    upsert_client_base(
        user_id       = uid,
        phone         = data["phone_number"],
        username      = data.get("username", ""),
        telegram_name = data.get("telegram_name", ""),
        client_name   = data["client_name"],
    )

    # 3) Пересылаем объявление клиенту
    await forward_object_post(context.bot, uid, data["object_code"])

    # 4) Благодарим клиента
    link_url = await get_object_link(context.bot, data["object_code"])
    obj_hyper = f"[{data['object_code']}]({link_url})" if link_url else data["object_code"]
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Продолжить поиск", url=link_url or "")
    ]])
    await context.bot.send_message(
        chat_id=uid,
        text=(
            f"Спасибо! Заявка по {obj_hyper} принята.\n"
            "📞 Наш специалист свяжется с вами в ближайшее время."
        ),
        parse_mode="Markdown",
        disable_web_page_preview=True,
        reply_markup=keyboard,
    )

    realtor_id_str = str(data.get("realtor_code", "")).strip()
    realtor_display = ""

    try:
        realtor_display = load_allowed().get(int(realtor_id_str), "")
    except ValueError:
        pass                                    # realtor_id не является числом

    if realtor_display:
        # если найдено «… @username» ─ берём @username, иначе оставляем ФИО
        m = re.search(r"@[\w\d_]+", realtor_display)
        if m:
            realtor_display = m.group(0)
    else:
        # fallback: берём то, что сохранили при публикации, или сам ID
        realtor_display = data.get("realtor_uname", "") or realtor_id_str

    notify_text = (
        f"Новая заявка\n"
        f"Дата: {now}\n"
        f"Клиент: {data['client_name']}\n"
        f"Телефон: {data['phone_number']}\n"
        f"Объект: {data['object_code']}\n"
        f"Риелтор: {realtor_display}\n"
    )
    await context.bot.send_message(NOTIFY_ID, notify_text)

    context.user_data.clear()
    return ConversationHandler.END


# Открыть доступ — показываем request_users-кнопку
async def access_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pick_btn = KeyboardButton(
        text="Выбрать пользователя",
        request_users=KeyboardButtonRequestUsers(
            request_id=1,
            user_is_bot=False,
            request_name=True,
            request_username=True
        )
    )
    await update.effective_message.reply_text(
        "Нажмите кнопку и выберите пользователя:",
        reply_markup=ReplyKeyboardMarkup(
            [[pick_btn]], one_time_keyboard=True, resize_keyboard=True
        )
    )
    return ACCESS_OPEN_WAIT

# Закрыть доступ — показываем inline-кнопки с текущими ID

async def access_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "🚫 Закрываем доступ. Клавиатура снята.",
        reply_markup=ReplyKeyboardRemove()
    )

    users = load_allowed()         # dict[id,name]
    # убираем админов
    for aid in ADMIN_IDS:
        users.pop(aid, None)

    if not users:
        await update.message.reply_text("Список пуст.")
        return ConversationHandler.END

    rows = []
    for uid, name in sorted(users.items()):
        text = f"{name or uid} ({uid}) ✔️"
        rows.append([InlineKeyboardButton(text, callback_data=f"acc:del:{uid}")])

    rows.append([InlineKeyboardButton("Готово", callback_data="acc:done")])

    await update.message.reply_text(
        "Нажмите на пользователя, чтобы отозвать доступ:",
        reply_markup=InlineKeyboardMarkup(rows)
    )
    return ACCESS_CLOSE_PICK

async def access_open_wait(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    uid  = None
    name = None

    # 1) Выбор через picker
    if msg.users_shared and getattr(msg.users_shared, "users", None):
        shared = msg.users_shared.users[0]  # SharedUser
        uid    = shared.user_id

        # пытаемся собрать имя из picker
        parts = []
        if getattr(shared, "first_name", None):
            parts.append(shared.first_name)
        if getattr(shared, "last_name", None):
            parts.append(shared.last_name)
        if getattr(shared, "username", None):
            parts.append("@" + shared.username)
        name = " ".join(parts).strip()

        # ─── если имени нет → просим переслать контакт ─────────────
        if not name:
            await msg.reply_text(
                "Не удалось получить имя из списка. "
                "Пожалуйста, перешлите КОНТАКТ этого же пользователя."
            )
            return ACCESS_OPEN_WAIT
        # ────────────────────────────────────────────────────────────

    # 2) Кнопка «поделиться контактом»
    elif msg.contact and msg.contact.user_id:
        uid  = msg.contact.user_id
        name = " ".join(filter(None, [msg.contact.first_name, msg.contact.last_name]))
        if msg.contact.username:
            name += f" (@{msg.contact.username})"

    # 3) Пересланное сообщение
    elif getattr(msg, "forward_from", None):
        frm  = msg.forward_from
        uid  = frm.id
        name = " ".join(filter(None, [frm.first_name, frm.last_name]))
        if frm.username:
            name += f" (@{frm.username})"

    # 4) Ответ на сообщение
    elif msg.reply_to_message:
        frm  = msg.reply_to_message.from_user
        uid  = frm.id
        name = " ".join(filter(None, [frm.first_name, frm.last_name]))
        if frm.username:
            name += f" (@{frm.username})"

    if uid is None:
        return await msg.reply_text(
            "Не удалось определить пользователя. Попробуйте ещё раз."
        )

    # ─── Сохраняем ID + имя в файл ─────────────────────────────────
    users = load_allowed()      # теперь {id: name}
    users[uid] = name or ""
    save_allowed(users)
    # ────────────────────────────────────────────────────────────────

    await msg.reply_text(
        f"✅ Доступ открыт для {name or uid}.",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END


async def access_close_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "acc:done":
        await q.edit_message_text("✅ Готово.")
        return ConversationHandler.END

    # acc:del:<uid>
    _, _, uid_str = q.data.split(":")
    uid = int(uid_str)

    users = load_allowed()
    users.pop(uid, None)           # убираем этого пользователя
    save_allowed(users)

    # фильтруем админов
    for aid in ADMIN_IDS:
        users.pop(aid, None)

    if not users:
        return await q.edit_message_text("Список пуст.")

    rows = []
    for u, name in sorted(users.items()):
        text = f"{name or u} ({u}) ✔️"
        rows.append([InlineKeyboardButton(text, callback_data=f"acc:del:{u}")])
    rows.append([InlineKeyboardButton("Готово", callback_data="acc:done")])

    await q.edit_message_text(
        "Нажмите на пользователя, чтобы отозвать доступ:",
        reply_markup=InlineKeyboardMarkup(rows)
    )
    return ACCESS_CLOSE_PICK


async def post_init(application: Application) -> None:
    """Вызывается сразу после initialize(); job_queue уже существует."""
    application.job_queue.run_repeating(
        refresh_file_ids,
        interval=timedelta(days=7),      # каждые 7 дней
        first=timedelta(seconds=30),     # первый запуск через 30 с
        name="refresh_file_ids",
    )

# ─── AUTO-REPOST JOB ──────────────────────────────────────────────
async def auto_repost_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Каждые 3 суток перепостит все active-объявления, если с момента
    последнего репоста прошло ≥ 3 дней.
    • object_code сохраняется тем же.
    • new_price=None → обычный «холодный» репост.
    """
    bot     = context.bot
    cutoff  = datetime.utcnow() - timedelta(days=3)
    conn    = sqlite3.connect(DB_FILE)
    cur     = conn.cursor()

    for tbl in ("old_fund", "new_fund", "land", "commerce"):
        cur.execute(
            f"SELECT object_code, COALESCE(repost_date, '') FROM {tbl} "
            "WHERE status='active'"
        )
        for code, rdate in cur.fetchall():
            try:
                if not rdate or datetime.fromisoformat(rdate) < cutoff:
                    # user_id = 0 → никому не шлём уведомлений
                    await repost_object_in_channel(bot, str(code), None, user_id=0)
            except Exception as e:
                logger.warning("auto_repost %s failed: %s", code, e)

    conn.close()

def main():
    init_db()
    req = HTTPXRequest(
        connect_timeout=30,
        write_timeout=180,
        pool_timeout=60,
        read_timeout=180,
    )
    app = (
        Application.builder()
            .token(BOT_TOKEN)
            .request(req)
            .post_init(post_init)
            .build()
    )

    access_open_conv = ConversationHandler(
        entry_points=[CommandHandler("access_open", access_open)],
        states={
            ACCESS_OPEN_WAIT: [
                MessageHandler(
                    StatusUpdate.USERS_SHARED | filters.CONTACT | filters.FORWARDED,
                    access_open_wait
                ),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
        per_user=True,
        name="access_open_flow",
    )

    access_close_conv = ConversationHandler(
        entry_points=[CommandHandler("access_close", access_close)],
        states={
            ACCESS_CLOSE_PICK: [
                CallbackQueryHandler(
                    access_close_cb,
                    pattern=r"^acc:(del:\d+|done)$"
                ),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
        per_user=True,
        name="access_close_flow",
    )

    app.add_handler(access_open_conv)
    app.add_handler(access_close_conv)

    app.add_handler(CommandHandler("cancel", cancel))


    app.add_handler(MessageHandler(filters.Chat(CHANNEL_ID) & filters.ALL, channel_post))

    start_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start_cmd)],
        states={

            ASK_NAME:  [MessageHandler(filters.TEXT   & ~filters.COMMAND, ask_name)],
            ASK_PHONE: [MessageHandler((filters.CONTACT | (filters.TEXT & ~filters.COMMAND)),
                                       ask_phone)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
        per_user=True,
        name="start_flow",
    )
    app.add_handler(start_conv)

    ad_conv = ConversationHandler(
        entry_points=[CommandHandler("ad", ad_cmd)],
        states={
            WAITING_MEDIA: [
                MessageHandler(
                    filters.ChatType.PRIVATE
                    & (filters.PHOTO | filters.VIDEO)
                    & ~filters.COMMAND,
                    handle_media,
                ),
                CallbackQueryHandler(
                    edit_callback,
                    pattern=r"^(?:ptype:|menu:|m:|t:|ask:|publish|cancel|back)"
                ),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_manual_input),
            ],
            EDITING: [
                CallbackQueryHandler(
                    edit_callback,
                    pattern=r"^(?:ptype:|menu:|m:|t:|ask:|publish|cancel|back)"
                ),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_manual_input),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
        per_chat=True,
        name="ad",
    )
    app.add_handler(ad_conv)

    # ─── /myads ────────────────────────────────────────────────────
    myads_conv = ConversationHandler(
        entry_points=[CommandHandler("myads", myads_cmd)],
        states={

            MYADS_SHOW: [
                CallbackQueryHandler(
                    myads_callback,
                    pattern=r"^(nav|del|repost|dec|rise):"
                ),
            ],

            MYADS_PRICE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND,
                               handle_myads_price_input),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
        per_chat=True,
        name="myads",
    )
    app.add_handler(myads_conv)

    # app.job_queue.run_repeating(
    #     auto_repost_job,
    #     interval=timedelta(days=1),
    #     first=timedelta(minutes=30),
    #     name="auto_repost",
    # )

    app.run_polling()

# ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()


