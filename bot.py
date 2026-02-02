import os
import random
import re
import aiosqlite
import uuid
import secrets
import logging
import asyncio
from datetime import datetime
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile,
    CallbackQuery
)
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv
from yookassa import Configuration, Payment

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
load_dotenv()

# =============== НАСТРОЙКИ ===============
CURRENT_YEAR = 2026
PRICE = 999
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
BOT_TOKEN = os.getenv("BOT_TOKEN")

# YooKassa
Configuration.account_id = os.getenv("YOOKASSA_SHOP_ID")
Configuration.secret_key = os.getenv("YOOKASSA_SECRET_KEY")

# =============== БАЗА ДАННЫХ ===============
DB_PATH = "users.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            birth_date TEXT,
            status TEXT DEFAULT 'free',
            archetype TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS premium_codes (
            code TEXT PRIMARY KEY,
            used_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            used_at TIMESTAMP
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            user_id INTEGER,
            session_count INTEGER DEFAULT 1,
            last_active TIMESTAMP,
            PRIMARY KEY (user_id)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_achievements (
            user_id INTEGER,
            achievement_id TEXT,
            unlocked_at TIMESTAMP,
            PRIMARY KEY (user_id, achievement_id)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_progress (
            user_id INTEGER PRIMARY KEY,
            total_sessions INTEGER DEFAULT 0,
            insights_received INTEGER DEFAULT 0
        )
        """)
        await db.commit()

async def save_user(user_id: int, username: str, full_name: str, status: str = "free", birth_date: str = None, archetype: str = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT OR REPLACE INTO users
            (user_id, username, full_name, status, birth_date, archetype)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, username, full_name, status, birth_date, archetype)
        )
        await db.commit()

async def update_user_session(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT session_count FROM user_sessions WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if row:
            await db.execute(
                "UPDATE user_sessions SET session_count = session_count + 1, last_active = datetime('now') WHERE user_id = ?",
                (user_id,)
            )
        else:
            await db.execute(
                "INSERT INTO user_sessions (user_id, last_active) VALUES (?, datetime('now'))",
                (user_id,)
            )
        await db.execute("""
        INSERT INTO user_progress (user_id, total_sessions)
        VALUES (?, 1)
        ON CONFLICT(user_id) DO UPDATE SET
        total_sessions = total_sessions + 1
        """, (user_id,))
        await db.commit()

# =============== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===============
async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users")
        return [row[0] for row in await cursor.fetchall()]

async def get_users_by_status(status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE status = ?", (status,))
        return [row[0] for row in await cursor.fetchall()]

async def generate_premium_code():
    code = "MATRIX-" + "-".join([
        secrets.token_urlsafe(3)[:3].upper(),
        secrets.token_urlsafe(3)[:3].upper(),
        secrets.token_urlsafe(3)[:3].upper()
    ])
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM premium_codes WHERE code = ?", (code,))
        if await cursor.fetchone():
            return await generate_premium_code()
    return code

async def save_premium_code(code: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO premium_codes (code) VALUES (?)", (code,))
        await db.commit()

async def use_premium_code(code: str, user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT used_by FROM premium_codes WHERE code = ?", (code,))
        result = await cursor.fetchone()
        if not result or result[0] is not None:
            return False
        await db.execute(
            "UPDATE premium_codes SET used_by = ?, used_at = datetime('now') WHERE code = ?",
            (user_id, code)
        )
        await db.commit()
        return True

async def get_user_status(user_id: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT status FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return row[0] if row else "free"

async def get_user_data(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT username, full_name, birth_date, status, archetype FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cursor.fetchone()
        if row:
            return {
                "username": row[0],
                "full_name": row[1],
                "birth_date": row[2],
                "status": row[3],
                "archetype": row[4]
            }
        return None

async def user_has_data(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT birth_date, full_name FROM users WHERE user_id = ? AND birth_date IS NOT NULL AND full_name IS NOT NULL",
            (user_id,)
        )
        row = await cursor.fetchone()
        return row is not None

# =============== МЕДИА-ФУНКЦИИ ===============
def get_random_file(folder, extensions):
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
        return None
    files = []
    for ext in extensions:
        files.extend([f for f in os.listdir(folder) if f.lower().endswith(ext)])
    if not files:
        return None
    return os.path.join(folder, random.choice(files))

def get_karmic_files(karmic_debts):
    paths = []
    for debt in karmic_debts:
        path = f"media/karmic/{debt}.jpg"
        if os.path.exists(path):
            paths.append(path)
    return paths

def get_random_daily_energy_image(energy: int) -> str:
    folder = f"media/daily_energy/{energy}"
    if not os.path.exists(folder):
        return None
    extensions = ('.jpg', '.jpeg', '.png', '.gif')
    files = [f for f in os.listdir(folder) if f.lower().endswith(extensions)]
    if not files:
        return None
    filename = random.choice(files)
    return os.path.join(folder, filename)

# =============== ПОМОЩНИКИ ===============
def reduce_number(n: int) -> int:
    while n >= 10:
        n = sum(int(d) for d in str(n))
    return n if n != 0 else 9

LETTER_VALUES = {
    'А': 1, 'Б': 2, 'В': 3, 'Г': 4, 'Д': 5, 'Е': 6, 'Ё': 7, 'Ж': 8, 'З': 9,
    'И': 1, 'Й': 2, 'К': 3, 'Л': 4, 'М': 5, 'Н': 6, 'О': 7, 'П': 8, 'Р': 9,
    'С': 1, 'Т': 2, 'У': 3, 'Ф': 4, 'Х': 5, 'Ц': 6, 'Ч': 7, 'Ш': 8, 'Щ': 9,
    'Ъ': 1, 'Ы': 2, 'Ь': 3, 'Э': 4, 'Ю': 5, 'Я': 6
}

VOWELS = set("АЕЁИОУЫЭЮЯ")

def name_to_number(name: str, use_vowels: bool = None) -> int:
    name = name.upper().replace(" ", "")
    total = 0
    for char in name:
        if char in LETTER_VALUES:
            is_vowel = char in VOWELS
            if use_vowels is None or is_vowel == use_vowels:
                total += LETTER_VALUES[char]
    return reduce_number(total)

# =============== РАСЧЁТ ЭНЕРГИИ ДНЯ ===============
def calculate_daily_energy(birth_day: str, today_date: str) -> int:
    bd = birth_day.lstrip("0") or "1"
    td = today_date.lstrip("0") or "1"
    total_digits = []

    if len(bd) == 1 and len(td) == 1:
        total = int(bd) + int(td)
        total_digits = [int(d) for d in str(total)]
    elif len(bd) == 1 and len(td) == 2:
        a = int(bd) + int(td[0])
        b = int(bd) + int(td[1])
        total_digits = [int(d) for d in str(a) + str(b)]
    elif len(bd) == 2 and len(td) == 2:
        a = int(bd[0]) + int(td[0])
        b = int(bd[1]) + int(td[1])
        total_digits = [int(d) for d in str(a) + str(b)]
    elif len(bd) == 2 and len(td) == 1:
        a = int(bd[0]) + int(td)
        b = int(bd[1]) + int(td)
        total_digits = [int(d) for d in str(a) + str(b)]

    total_sum = sum(total_digits)
    while total_sum > 9:
        total_sum = sum(int(d) for d in str(total_sum))
    return total_sum if total_sum != 0 else 9

def calculate_universal_day_energy(day: int, month: int, year: int) -> int:
    year_sum = reduce_number(year)
    total = day + month + year_sum
    return reduce_number(total)

METHOD_NAMES = {
    1: "инициативу и лидерство",
    2: "сотрудничество и дипломатию",
    3: "творчество и самовыражение",
    4: "порядок и системный труд",
    5: "гибкость и адаптацию",
    6: "заботу и гармонизацию",
    7: "анализ и интуицию",
    8: "фокус на результате",
    9: "завершение и отпускание",
    11: "интуитивные озарения",
    22: "практическое воплощение идей",
    33: "служение через мудрость"
}

GOAL_NAMES = {
    1: "новых начинаний",
    2: "гармонии в отношениях",
    3: "творческой реализации",
    4: "стабильности и порядка",
    5: "свободы и перемен",
    6: "семейного благополучия",
    7: "глубокого понимания",
    8: "материальных достижений",
    9: "завершения циклов",
    11: "духовного прорыва",
    22: "грандиозных проектов",
    33: "высшего служения"
}

def analyze_mission_task(personal_sum: int) -> dict:
    if personal_sum in (11, 22, 33):
        return {
            "method": personal_sum,
            "goal": personal_sum,
            "method_str": METHOD_NAMES[personal_sum],
            "goal_str": GOAL_NAMES[personal_sum]
        }
    s = str(personal_sum)
    method_num = int(s[0])
    goal_num = int(s[-1]) if len(s) > 1 else method_num
    return {
        "method": method_num,
        "goal": goal_num,
        "method_str": METHOD_NAMES.get(method_num, "осознанные действия"),
        "goal_str": GOAL_NAMES.get(goal_num, "достижения целей")
    }

def generate_dual_axis_recommendations(birth_day_num: int, universal_energy: int) -> dict:
    birth_do = {
        1: ["брать инициативу", "начинать новые дела"],
        2: ["слушать других", "искать компромиссы"],
        3: ["выражать эмоции", "делиться идеями"],
        4: ["следовать плану", "работать системно"],
        5: ["оставлять пространство для спонтанности"],
        6: ["проявлять заботу", "гармонизировать отношения"],
        7: ["доверять интуиции", "выделять время на размышления"],
        8: ["ставить чёткие цели", "фокусироваться на главном"],
        9: ["завершать старое", "прощать и отпускать"]
    }.get(birth_day_num, ["доверять себе", "действовать осознанно"])

    day_do = {
        1: ["принимать решения", "действовать первым"],
        2: ["договариваться", "строить мосты"],
        3: ["творить", "общаться легко"],
        4: ["упорядочивать", "работать усердно"],
        5: ["быть гибким", "принимать перемены"],
        6: ["заботиться о близких", "создавать уют"],
        7: ["молчать и слушать", "анализировать"],
        8: ["управлять ресурсами", "быть стратегом"],
        9: ["подводить итоги", "делиться щедро"]
    }.get(universal_energy, ["следовать потоку"])

    return {
        "birth_do": list(set(birth_do))[:3],
        "day_do": list(set(day_do))[:3]
    }

# =============== ЗАГРУЗКА ТЕКСТОВ ===============
def read_narrative(path: str) -> str:
    """Читает текстовый файл с проверкой размера"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read().strip()
            
        # Проверяем размер файла
        if len(text) > 10000:  # Если файл больше 10к символов
            logger.warning(f"File {path} is too large: {len(text)} characters")
            # Берем только начало
            text = text[:1000] + "... [текст обрезан из-за большого размера]"
            
        return text
    except FileNotFoundError:
        logger.warning(f"File not found: {path}")
        return "[Текст пока не готов. Скоро будет!]"
    except Exception as e:
        logger.error(f"Error reading file {path}: {e}")
        return "[Ошибка при загрузке текста]"

def calculate_object_number(text: str) -> int:
    total = 0
    for char in text.upper():
        if char.isdigit():
            total += int(char)
        elif char in LETTER_VALUES:
            total += LETTER_VALUES[char]
    return reduce_number(total)

def read_compatibility_narrative(person_num: int, obj_num: int, obj_type: str) -> str:
    path = f"narratives/full/compatibility/{obj_type}/{person_num}_{obj_num}.txt"
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return (
            f"[Текст для {obj_type} {person_num}/{obj_num} ещё не готов.]\n"
            "Но вот краткий анализ:\n"
            f"• Твоя энергия: {person_num}\n"
            f"• Энергия объекта: {obj_num}\n"
            "Совместимость будет рассчитана в ближайшем обновлении."
        )

# =============== РАСЧЁТ ПРОФИЛЯ ===============
def calculate_numerology_profile(birth_date: str, full_name: str, current_year: int = CURRENT_YEAR):
    day, month, year = map(int, birth_date.split('.'))
    mind = reduce_number(day)
    all_digits = [int(d) for d in f"{day:02d}{month:02d}{year}"]
    action_raw = sum(all_digits)
    action = reduce_number(action_raw)
    realization = reduce_number(mind + action)
    destiny_lesson = reduce_number(mind + action + realization)
    personal_year = reduce_number(day + month + current_year)
    soul_urge = name_to_number(full_name, use_vowels=True)
    personality = name_to_number(full_name, use_vowels=False)
    karmic_debts = set()
    for num in [action_raw, mind + action, mind + action + realization]:
        temp = num
        while temp >= 10:
            if temp in (13, 14, 16, 19):
                karmic_debts.add(temp)
            temp = sum(int(d) for d in str(temp))
    return {
        "mind": mind,
        "action": action,
        "realization": realization,
        "destiny_lesson": destiny_lesson,
        "personal_year": personal_year,
        "soul_urge": soul_urge,
        "personality": personality,
        "karmic_debts": sorted(karmic_debts),
        "birth_date": birth_date,
        "full_name": full_name
    }

# =============== МАТРИЦА ПИФАГОРА ===============
def calculate_pythagoras_matrix(birth_date: str):
    day, month, year = map(int, birth_date.split('.'))
    digits = []
    for num in [day, month, year]:
        digits.extend([int(d) for d in str(num)])
    first_work = sum(digits)
    second_work = sum(int(d) for d in str(first_work))
    third_work = first_work - 2 * (int(str(day)[0]) if day >= 10 else day)
    if third_work < 0:
        third_work = abs(third_work)
    fourth_work = sum(int(d) for d in str(third_work))
    all_numbers = digits + [first_work, second_work, third_work, fourth_work]
    digit_counts = {str(i): 0 for i in range(1, 10)}
    for num in all_numbers:
        for char in str(num):
            if char in digit_counts:
                digit_counts[char] += 1
    matrix = [
        [digit_counts["1"], digit_counts["2"], digit_counts["3"]],
        [digit_counts["4"], digit_counts["5"], digit_counts["6"]],
        [digit_counts["7"], digit_counts["8"], digit_counts["9"]]
    ]
    return matrix, digit_counts

def analyze_pythagoras_lines(matrix: dict) -> list:
    line_configs = [
        {"name": "самореализации", "digits": ["3", "6", "9"], "purpose": "отвечает за твои таланты, способность доводить дела до конца и видеть смысл в том, что ты создаёшь"},
        {"name": "семьи и денег", "digits": ["2", "5", "8"], "purpose": "отвечает за стабильные отношения, финансовую грамотность и умение чувствовать поддержку"},
        {"name": "здоровья и труда", "digits": ["1", "4", "7"], "purpose": "отвечает за твою физическую энергию, здоровье, трудоспособность и способность зарабатывать"},
        {"name": "целеустремлённости", "digits": ["1", "2", "3"], "purpose": "отвечает за умение ставить цели, сохранять энергию на пути к ним и фокусироваться"},
        {"name": "благополучия", "digits": ["4", "5", "6"], "purpose": "отвечает за уют, порядок, материальную базу и ощущение безопасности в жизни"},
        {"name": "духовности", "digits": ["7", "8", "9"], "purpose": "отвечает за связь с высшим, удачу, чувство долга и понимание смысла жизни"},
        {"name": "темперамента", "digits": ["3", "5", "7"], "purpose": "отвечает за чувственность, способность наслаждаться жизнью и доверять интуиции"},
        {"name": "миссии", "digits": ["1", "5", "9"], "purpose": "отвечает за осознанность, способность видеть свой вклад в мир и жить в согласии с собой"}
    ]
    energy_advice = {
        "1": "наработать через ежедневные решения: каждое утро задавай себе — «Что я выбираю сегодня?». Начни с малого — даже выбор одежды укрепляет волю.",
        "2": "наработать через заботу о теле: прогулки на природе, контрастный душ, йога. Энергия растёт, когда ты уважаешь своё физическое «я».",
        "3": "наработать через обучение: читай 10 страниц в день, записывай мысли, задавай «почему?». Логика — это мышца, её нужно тренировать.",
        "4": "наработать через физический труд: уборка, садоводство, спорт. Тело — твой фундамент. Даже 15 минут активности в день укрепят здоровье.",
        "5": "наработать через тишину: медитация, прогулки без телефона, дневник интуиции. Задавай себе: «Что я чувствую?» — и доверяй ответу.",
        "6": "наработать через регулярный труд: выбери дело, которое приносит доход, и делай его каждый день, даже по 20 минут. Деньги любят системность.",
        "7": "наработать через благодарность: каждый вечер пиши 3 вещи, за которые ты благодарен. Удача приходит к тем, кто видит добро в жизни.",
        "8": "наработать через выполнение обещаний: начни с обещаний себе. Если сказал «я сделаю», — сделай. Ответственность — это тренировка характера.",
        "9": "наработать через служение: помогай другим без ожидания награды. Интеллект раскрывается, когда ты делишься знаниями."
    }
    results = []
    for config in line_configs:
        missing_digits = [d for d in config["digits"] if matrix[d] == 0]
        if missing_digits:
            advice_parts = [f"энергию {d} — {energy_advice[d]}" for d in missing_digits]
            full_advice = "Нужно наработать " + " и ".join(advice_parts)
            results.append({
                "title": f"Линия {config['name']} ({'-'.join(config['digits'])})",
                "message": f"отвечает за {config['purpose']}. Но у тебя отсутствует(ют) цифра(ы): {', '.join(missing_digits)}. Поэтому эта сфера даётся с трудом. {full_advice}"
            })
    return results

def generate_matrix_visual(matrix):
    symbols = []
    for row in matrix:
        symbols_row = []
        for cell in row:
            if cell == 0:
                symbols_row.append("⚫")
            elif cell == 1:
                symbols_row.append("🔵")
            elif cell == 2:
                symbols_row.append("🟢")
            elif cell == 3:
                symbols_row.append("🟡")
            else:
                symbols_row.append("🔴")
        symbols.append(symbols_row)
    visual = (
        f"🌀 <b>МАТРИЦА 3×3</b>\n"
        f"    {symbols[0][0]}  {symbols[0][1]}  {symbols[0][2]}\n"
        f"    {symbols[1][0]}  {symbols[1][1]}  {symbols[1][2]}\n"
        f"    {symbols[2][0]}  {symbols[2][1]}  {symbols[2][2]}\n"
        f"⚫ Нет | 🔵 Слабо | 🟢 Средне | 🟡 Сильно | 🔴 Очень сильно"
    )
    return visual

def determine_archetype(digit_counts):
    strong_digits = [d for d, count in digit_counts.items() if count >= 2]
    if "1" in strong_digits or "4" in strong_digits or "7" in strong_digits:
        return "⚔️ Воин Духа"
    elif "3" in strong_digits or "6" in strong_digits or "9" in strong_digits:
        return "📚 Хранитель Знаний"
    elif "2" in strong_digits or "5" in strong_digits or "8" in strong_digits:
        return "🎨 Создатель"
    else:
        return "💚 Целитель"

# =============== ГЕНЕРАЦИЯ ОТЧЁТОВ ===============
def generate_free_report(profile: dict) -> str:
    """Генерирует бесплатный отчет с проверкой длины"""
    try:
        free_folder = "narratives/free"
        
        try:
            mind_text = read_narrative(f"{free_folder}/mind/{profile['mind']}.txt")
            if len(mind_text) > 300:
                mind_text = mind_text[:297] + "..."
        except:
            mind_text = f"особую энергию числа {profile['mind']}."
        
        try:
            action_text = read_narrative(f"{free_folder}/action/{profile['action']}.txt")
            if len(action_text) > 300:
                action_text = action_text[:297] + "..."
        except:
            action_text = f"раскрыть потенциал числа {profile['action']}."
        
        try:
            py_text = read_narrative(f"{free_folder}/personal_year/{profile['personal_year']}.txt")
            if len(py_text) > 300:
                py_text = py_text[:297] + "..."
        except:
            py_text = f"пройти через опыт числа {profile['personal_year']}."
        
        report = (
            f"✨ <b>Твоё число Ума — {profile['mind']}.</b>\n"
            f"Ты пришёл в этот мир, уже неся в себе {mind_text}\n\n"
            f"🌍 <b>Число Миссии — {profile['action']}.</b>\n"
            f"Поэтому твоя душа выбрала эту жизнь, чтобы научиться {action_text}\n\n"
            f"📅 <b>Прямо сейчас у тебя Личный год — {profile['personal_year']}.</b>\n"
            f"Вселенная даёт тебе особый шанс: {py_text}\n\n"
            "💎 <b>Хочешь узнать полную историю своей души?</b>\n"
            "— Число Сердца и Личности\n"
            "— Кармические долги и пути их преодоления\n"
            "— Глубокий анализ реализации и итога\n"
            "— Матрица Судьбы с архетипом и визуализацией\n\n"
            "👉 Нажми кнопку ниже, чтобы получить полный разбор!"
        )
        
        # Проверяем длину
        if len(report) > 4000:
            report = report[:3997] + "..."
        
        return report
        
    except Exception as e:
        logger.error(f"Error in generate_free_report: {e}")
        return "✨ <b>БЕСПЛАТНЫЙ ОТЧЁТ</b>\n\nК сожалению, произошла ошибка. Пожалуйста, попробуйте позже."

def generate_full_report(profile: dict, matrix_data: dict) -> str:
    """Генерирует полный отчет с проверкой длины сообщения"""
    try:
        full_folder = "narratives/full"
        
        n = {}
        for key in ["mind", "action", "realization", "destiny_lesson", "soul_urge", "personality", "personal_year"]:
            try:
                n[key] = read_narrative(f"{full_folder}/{key}/{profile[key]}.txt")
                # Обрезаем слишком длинные тексты
                if len(n[key]) > 500:
                    n[key] = n[key][:497] + "..."
            except:
                n[key] = f"Энергия числа {profile[key]}."
        
        # Основной текст
        narrative = (
            f"🌟 <b>ПОЛНЫЙ ЭНЕРГЕТИЧЕСКИЙ ОТЧЁТ</b>\n"
            f"🎭 <b>ТВОЙ АРХЕТИП: {matrix_data['archetype']}</b>\n\n"
            f"✨ <b>Твоё число Ума — {profile['mind']}.</b>\n"
            f"Ты пришёл в этот мир, уже неся в себе {n['mind']}\n\n"
            f"🌍 <b>Число Миссии — {profile['action']}.</b>\n"
            f"Но твоя душа выбрала эту жизнь, чтобы научиться {n['action']}\n\n"
            f"🌀 <b>Число Реализации — {profile['realization']}.</b>\n"
            f"И чтобы освоить этот урок, тебе дан особый путь — через {n['realization']}\n\n"
            f"🏁 <b>Итог жизни — {profile['destiny_lesson']}.</b>\n"
            f"Если ты пройдёшь его честно, в зрелости ты обретёшь {n['destiny_lesson']}\n\n"
            f"❤️ <b>Число Сердца — {profile['soul_urge']}.</b>\n"
            f"В глубине сердца ты жаждешь {n['soul_urge']}\n\n"
            f"🎭 <b>Число Личности — {profile['personality']}.</b>\n"
            f"Мир видит тебя как человека, который {n['personality']}\n\n"
            f"📅 <b>Личный год — {profile['personal_year']}.</b>\n"
            f"Прямо сейчас Вселенная даёт тебе особый шанс: {n['personal_year']}\n"
        )
        
        # Кармические долги
        if profile['karmic_debts']:
            debt_texts = []
            for debt in profile['karmic_debts']:
                try:
                    debt_text = read_narrative(f"{full_folder}/karmic_debts/{debt}.txt")
                    if len(debt_text) > 200:
                        debt_text = debt_text[:197] + "..."
                    debt_texts.append(debt_text)
                except:
                    debt_texts.append(f"Кармический урок числа {debt}")
            
            debts = " ".join(debt_texts)
            narrative += f"\n\n⚠️ <b>На этом пути есть особые испытания:</b>\n{debts}"
        else:
            narrative += "\n\n✅ <b>У тебя нет кармических долгов</b> — твоя душа пришла с чистого листа."
        
        # Матрица
        matrix_text = matrix_data.get("matrix_visual", "🌀 <b>МАТРИЦА 3×3</b>\n[Матрица временно недоступна]")
        narrative += f"\n\n{matrix_text}"
        
        # Линии матрицы
        line_results = matrix_data.get("line_analysis", [])
        if line_results:
            narrative += "\n\n🧩 <b>ЛИНИИ МАТРИЦЫ</b>"
            for line in line_results[:3]:  # Ограничиваем 3 линиями
                line_text = f"\n\n<b>{line.get('title', 'Линия')}</b>\n{line.get('message', '')}"
                # Проверяем, не превысим ли лимит
                if len(narrative + line_text) > 3800:
                    narrative += "\n\n... [ещё линии скрыты из-за ограничения длины]"
                    break
                narrative += line_text
        
        # Добавляем финальную часть
        narrative += "\n\n<i>Этот рассказ — отражение твоей нумерологической карты.</i>"
        
        # Проверяем общую длину
        if len(narrative) > 4000:
            # Если слишком длинный, обрезаем
            narrative = narrative[:3997] + "..."
        
        return narrative
        
    except Exception as e:
        logger.error(f"Error in generate_full_report: {e}")
        return "🌟 <b>ПОЛНЫЙ ЭНЕРГЕТИЧЕСКИЙ ОТЧЁТ</b>\n\nК сожалению, произошла ошибка при генерации отчёта. Пожалуйста, попробуйте позже."

# =============== ВАЛИДАЦИЯ ===============
def validate_date(date_str: str) -> bool:
    try:
        parts = date_str.split('.')
        if len(parts) != 3: return False
        day, month, year = map(int, parts)
        return 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2025
    except:
        return False

def validate_name(name: str) -> bool:
    cleaned = name.strip().replace(" ", "")
    allowed = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя"
    return len(cleaned) >= 3 and all(c in allowed for c in cleaned)

# =============== FSM ===============
class Form(StatesGroup):
    waiting_for_birth_date = State()
    waiting_for_full_name = State()
    waiting_for_payment = State()
    waiting_for_broadcast_target = State()
    waiting_for_broadcast_message = State()
    waiting_for_premium_user_id = State()
    waiting_for_home_input = State()
    waiting_for_car_input = State()
    waiting_for_promo_code = State()

# =============== КЛАВИАТУРЫ ===============
def get_main_keyboard(user_id: int, has_data: bool = False):
    keyboard = [
        [KeyboardButton(text="🔄 Новый расчёт")],
        [KeyboardButton(text="📊 Моя статистика")],
        [KeyboardButton(text="🎁 Бонусы")],
        [KeyboardButton(text="🌞 Энергия дня")],
    ]
    if has_
        keyboard.insert(0, [KeyboardButton(text="📈 Мой отчёт")])
    keyboard.append([KeyboardButton(text="🏠 Анализ квартиры")])
    keyboard.append([KeyboardButton(text="🚗 Анализ машины")])
    if user_id == ADMIN_USER_ID:
        keyboard.append([KeyboardButton(text="⚙️ Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_report_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔄 Новый расчёт")],
            [KeyboardButton(text="📈 Мой отчёт")],
            [KeyboardButton(text="🔙 На главную")]
        ],
        resize_keyboard=True
    )

def get_admin_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👑 Выдать премиум"), KeyboardButton(text="🎫 Создать промокод")],
            [KeyboardButton(text="📋 Список промокодов"), KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="📢 Рассылка"), KeyboardButton(text="🔙 На главную")]
        ],
        resize_keyboard=True
    )

# =============== ПЛАТЁЖ ===============
async def create_payment(user_id: int, description: str):
    payment = Payment.create({
        "amount": {"value": str(PRICE), "currency": "RUB"},
        "confirmation": {"type": "redirect", "return_url": "https://t.me/your_bot_username"},
        "capture": True,
        "description": description,
        "metadata": {"user_id": str(user_id)}
    }, uuid.uuid4())
    return payment

# =============== AIOGRAM БОТ ===============
router = Router()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# =============== ОСНОВНЫЕ КОМАНДЫ ===============
@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await update_user_session(message.from_user.id)
    await save_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name or "Unknown"
    )
    has_data = await user_has_data(message.from_user.id)
    welcome_img = get_random_file("media/welcome", ('.jpg', '.png', '.gif'))
    caption = (
        "🌌 Ты не случайно оказался здесь.\n"
        "Этот бот — не просто расчёт чисел.\n"
        "Это <b>карта твоя души</b>, составленная из даты рождения и имени.\n"
    )
    if has_
        caption += (
            "✅ <b>У тебя уже есть сохраненные данные!</b>\n"
            "Ты можешь:\n"
            "• 📈 Посмотреть свой отчёт\n"
            "• 🔄 Сделать новый расчёт\n"
            "• 🌞 Узнать энергию дня\n"
        )
    else:
        caption += (
            "✨ <b>Бесплатно</b> — общий прогноз:\n"
            "— С какой энергией ты пришёл в этот мир\n"
            "— Какую миссию выбрала твоя душа\n"
            "— Что ждёт тебя в 2026 году\n\n"
            "💎 <b>В премиум-версии</b> — глубокое понимание себя:\n"
            "— Число Сердца и Личности\n"
            "— Кармические долги и пути их преодоления\n"
            "— Полный нарратив твоего пути от начала до итога\n"
            "— Матрица Судьбы с архетипом и визуализацией\n"
            "— Анализ совместимости с жильём и автомобилем\n"
            "— Энергия дня на каждый день\n\n"
            "Готов взглянуть на свою жизнь глазами Вселенной?\n"
            "Нажми «🔄 Новый расчёт» или «📈 Мой отчёт», если данные уже есть!"
        )
    if welcome_img:
        if welcome_img.endswith('.gif'):
            await message.answer_animation(animation=FSInputFile(welcome_img), caption=caption, parse_mode="HTML")
        else:
            await message.answer_photo(photo=FSInputFile(welcome_img), caption=caption, parse_mode="HTML")
    else:
        await message.answer(caption, parse_mode="HTML", reply_markup=get_main_keyboard(message.from_user.id, has_data))

@router.message(F.text == "🔄 Новый расчёт")
async def start_new_calculation(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "📝 <b>НАЧНЁМ НОВЫЙ РАСЧЁТ</b>\n"
        "Пришли свою <b>дату рождения</b> в формате:\n"
        "<code>ДД.ММ.ГГГГ</code>\n"
        "Пример: <code>14.05.1990</code>",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_birth_date)

@router.message(F.text == "📈 Мой отчёт")
async def show_my_report(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT birth_date, full_name, status, archetype FROM users WHERE user_id = ?",
                (user_id,)
            )
            row = await cursor.fetchone()
            
            if not row or not row[0] or not row[1]:
                await message.answer(
                    "📝 <b>У ВАС ЕЩЁ НЕТ СОХРАНЕННОГО ОТЧЁТА</b>\n\n"
                    "Сначала создайте отчёт, нажав «🔄 Новый расчёт» и введя свои данные.",
                    parse_mode="HTML",
                    reply_markup=get_main_keyboard(user_id, False)
                )
                return
            
            birth_date, full_name, current_status, archetype = row
            
        profile = calculate_numerology_profile(birth_date, full_name, CURRENT_YEAR)
        
        matrix, digit_counts = calculate_pythagoras_matrix(birth_date)
        matrix_visual = generate_matrix_visual(matrix)
        line_analysis = analyze_pythagoras_lines(digit_counts)
        
        if not archetype:
            archetype = determine_archetype(digit_counts)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE users SET archetype = ? WHERE user_id = ?",
                    (archetype, user_id)
                )
                await db.commit()
        
        matrix_data = {
            "matrix_visual": matrix_visual,
            "line_analysis": line_analysis,
            "archetype": archetype
        }
        
        if current_status == "paid":
            # Генерируем и отправляем отчет частями
            full_report = generate_full_report(profile, matrix_data)
            
            # Разбиваем на части если нужно
            if len(full_report) > 4000:
                parts = []
                current_part = ""
                
                # Делим по абзацам
                paragraphs = full_report.split('\n\n')
                for para in paragraphs:
                    if len(current_part) + len(para) + 2 > 4000:
                        parts.append(current_part)
                        current_part = para
                    else:
                        if current_part:
                            current_part += "\n\n" + para
                        else:
                            current_part = para
                
                if current_part:
                    parts.append(current_part)
                
                # Отправляем все части
                for i, part in enumerate(parts):
                    try:
                        await message.answer(part, parse_mode="HTML")
                        if i < len(parts) - 1:  # Небольшая задержка между сообщениями
                            await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Error sending part {i}: {e}")
            else:
                await message.answer(full_report, parse_mode="HTML")
            
            # Отправляем медиа
            try:
                premium_media = get_random_file("media/premium", ('.mp4', '.jpg', '.png', '.gif'))
                if premium_media:
                    if premium_media.endswith('.mp4'):
                        await message.answer_video(video=FSInputFile(premium_media))
                    elif premium_media.endswith('.gif'):
                        await message.answer_animation(animation=FSInputFile(premium_media))
                    else:
                        await message.answer_photo(photo=FSInputFile(premium_media))
            except Exception as e:
                logger.error(f"Error sending premium media: {e}")
            
            # Кармические изображения
            for img_path in get_karmic_files(profile['karmic_debts']):
                if os.path.exists(img_path):
                    try:
                        await message.answer_photo(photo=FSInputFile(img_path))
                    except Exception as e:
                        logger.error(f"Error sending karmic image {img_path}: {e}")
            
            await message.answer(
                "✨ <b>ВАШ ПРЕМИУМ-ОТЧЁТ ЗАГРУЖЕН!</b>",
                parse_mode="HTML",
                reply_markup=get_report_keyboard()
            )
            
        else:
            # Бесплатный отчет
            free_report = generate_free_report(profile)
            await state.update_data(profile=profile, matrix_data=matrix_data)
            
            if len(free_report) > 4000:
                free_report = free_report[:3997] + "..."
            
            await message.answer(free_report, parse_mode="HTML")
            
            free_img = get_random_file("media/free", ('.jpg', '.png', '.gif'))
            if free_img:
                if free_img.endswith('.gif'):
                    await message.answer_animation(animation=FSInputFile(free_img))
                else:
                    await message.answer_photo(photo=FSInputFile(free_img))
            
            buy_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"💎 Полный разбор — {PRICE} ₽", callback_data="buy_full")],
                [InlineKeyboardButton(text="🎁 Ввести промокод", callback_data="enter_promo")],
                [InlineKeyboardButton(text="🌞 Энергия дня (премиум)", callback_data="daily_energy")]
            ])
            
            await message.answer(
                "🚀 <b>ГОТОВЫ РАСКРЫТЬ ВСЮ ПРАВДУ?</b>\n"
                "Полный доступ даст вам:\n"
                "• Анализ всех линий матрицы\n"
                "• Конкретные рекомендации\n"
                "• Индивидуальные практики",
                parse_mode="HTML",
                reply_markup=buy_kb
            )
            await state.set_state(Form.waiting_for_payment)
            
    except Exception as e:
        logger.error(f"Error in show_my_report for user {user_id}: {e}", exc_info=True)
        
        error_msg = str(e)
        if "Message is too long" in error_msg or "message is too long" in error_msg:
            await message.answer(
                "⚠️ <b>ОШИБКА: ОТЧЁТ СЛИШКОМ ДЛИННЫЙ</b>\n\n"
                "Пожалуйста, обратитесь к администратору для настройки текстовых файлов.",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(user_id, False)
            )
        else:
            await message.answer(
                "❌ <b>ОШИБКА ПРИ ЗАГРУЗКЕ ОТЧЁТА</b>\n\n"
                "Пожалуйста, попробуйте:\n"
                "1. Нажать «🔄 Новый расчёт» и ввести данные заново\n"
                "2. Проверить, что дата рождения введена в формате ДД.ММ.ГГГГ\n"
                "3. Обратиться к администратору, если проблема повторяется",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(user_id, False)
            )

@router.message(F.text == "📊 Моя статистика")
async def show_stats(message: Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
        SELECT us.session_count, us.last_active, u.status, u.archetype, u.birth_date, u.full_name
        FROM user_sessions us
        LEFT JOIN users u ON us.user_id = u.user_id
        WHERE us.user_id = ?
        """, (user_id,))
        row = await cursor.fetchone()
        if row:
            sessions, last_active, status, archetype, birth_date, full_name = row
            stats_text = (
                f"📊 <b>ВАША СТАТИСТИКА</b>\n"
                f"• Сессий: {sessions}\n"
                f"• Последняя активность: {last_active[:16] if last_active else 'Нет'}\n"
                f"• Статус: {'💎 ПРЕМИУМ' if status == 'paid' else '🆓 БЕСПЛАТНЫЙ'}\n"
            )
            if archetype:
                stats_text += f"• Архетип: {archetype}\n"
            if birth_date and full_name:
                stats_text += f"• Данные: сохранены ✅\n"
                stats_text += f"• Дата рождения: {birth_date}\n"
                stats_text += f"• Имя: {full_name}\n"
            else:
                stats_text += f"• Данные: не сохранены ❌\n"
            stats_text += "\n🎯 <b>Чем больше сессий — тем точнее анализ!</b>"
            await message.answer(stats_text, parse_mode="HTML")
        else:
            await message.answer("Сделайте первый расчёт!")

@router.message(F.text == "🎁 Бонусы")
async def show_bonuses(message: Message):
    await message.answer(
        "🎁 <b>БОНУСНАЯ СИСТЕМА</b>\n"
        "💎 <b>ЗА КАЖДЫЙ РАСЧЁТ:</b>\n"
        "• +1 к точности анализа\n"
        "• Новые инсайты\n"
        "• Углубление в архетип\n\n"
        "🌟 <b>ПРЕМИУМ-БОНУСЫ:</b>\n"
        "• Персональные рекомендации\n"
        "• Еженедельные отчеты\n"
        "• Доступ ко всем обновлениям\n"
        "• Энергия дня каждый день\n\n"
        "<i>Чем больше исследуешь — тем больше открываешь!</i>",
        parse_mode="HTML"
    )

# =============== ЭНЕРГИЯ ДНЯ ===============
@router.message(F.text == "🌞 Энергия дня")
async def daily_energy_handler(message: Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT birth_date, status FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if not row or not row[0]:
            await message.answer(
                "📅 <b>СНАЧАЛА ЗАПОЛНИТЕ ДАННЫЕ</b>\n"
                "Для расчета энергии дня мне нужна ваша дата рождения.\n"
                "Нажмите «🔄 Новый расчёт» и введите данные.",
                parse_mode="HTML"
            )
            return
        birth_date, current_status = row
        if current_status != "paid":
            buy_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"💎 Получить премиум — {PRICE} ₽", callback_data="buy_full")],
                [InlineKeyboardButton(text="🎁 Ввести промокод", callback_data="enter_promo")]
            ])
            await message.answer(
                "🔒 <b>ЭНЕРГИЯ ДНЯ — ПРЕМИУМ-ФУНКЦИЯ</b>\n"
                "Расчет персональной энергии дня доступен только в премиум-версии.\n\n"
                "💎 <b>Что дает премиум:</b>\n"
                "• Персональная энергия на каждый день\n"
                "• Рекомендации по активности\n"
                "• Лучшее время для принятия решений\n"
                "• Анализ совместимости с жильем и авто\n"
                "• Полный нумерологический разбор\n"
                "• Энергия дня каждый день",
                parse_mode="HTML",
                reply_markup=buy_kb
            )
            return
    day_part = birth_date.split(".")[0]
    today_day = datetime.now().strftime("%d")
    energy = calculate_daily_energy(day_part, today_day)
    energy_descriptions = {
        1: "День новых начинаний и лидерства. Идеальное время для старта проектов, принятия решений. Ваша энергия на максимуме - действуйте смело!",
        2: "День гармонии и сотрудничества. Сосредоточьтесь на отношениях, переговорах. Избегайте конфликтов, ищите компромиссы.",
        3: "День творчества и общения. Проявляйте креативность, делитесь идеями. Отличное время для презентаций и встреч.",
        4: "День порядка и системности. Займитесь планированием, организацией. Работайте над стабильностью и структурой.",
        5: "День перемен и свободы. Будьте гибкими, открытыми новому. Идеальное время для обучения и путешествий.",
        6: "День семьи и заботы. Уделите время близким, создавайте уют. Проявляйте заботу и внимание.",
        7: "День интуиции и анализа. Прислушивайтесь к внутреннему голосу. Займитесь саморазвитием, медитацией.",
        8: "День достижений и финансов. Фокусируйтесь на целях, управляйте ресурсами. Хорошее время для бизнес-решений.",
        9: "День завершения и отпускания. Завершайте старые дела, прощайте обиды. Готовьтесь к новому циклу."
    }
    energy_text = energy_descriptions.get(energy,
        "Сегодня важный день для вашего развития. Доверяйте интуиции и действуйте осознанно.")
    recommendations = {
        1: ["Начните новое дело", "Проявите инициативу", "Примите важное решение"],
        2: ["Проведите переговоры", "Укрепите отношения", "Будьте дипломатичны"],
        3: ["Запишите идеи", "Поделитесь творчеством", "Пообщайтесь с интересными людьми"],
        4: ["Составьте план", "Наведите порядок", "Работайте системно"],
        5: ["Попробуйте что-то новое", "Будьте гибкими", "Учитесь"],
        6: ["Проведите время с семьей", "Позаботьтесь о близких", "Создайте уют"],
        7: ["Послушайте интуицию", "Поразмышляйте", "Запишите сны"],
        8: ["Поставьте финансовые цели", "Сфокусируйтесь на результате", "Инвестируйте в себя"],
        9: ["Завершите старые дела", "Простите обиды", "Поблагодарите за опыт"]
    }
    daily_recommendations = recommendations.get(energy, ["Доверяйте себе", "Действуйте осознанно", "Следуйте интуиции"])
    today_date = datetime.now().strftime("%d.%m.%Y")
    message_text = (
        f"🌞 <b>ВАША ЭНЕРГИЯ НА {today_date}</b>\n"
        f"🌀 <b>Число энергии: {energy}</b>\n"
        f"{energy_text}\n"
        f"💡 <b>РЕКОМЕНДАЦИИ НА СЕГОДНЯ:</b>\n"
    )
    for i, rec in enumerate(daily_recommendations, 1):
        message_text += f"{i}. {rec}\n"
    message_text += "\n✨ <i>Используйте эту энергию максимально эффективно!</i>"
    await message.answer(message_text, parse_mode="HTML")
    energy_image = get_random_daily_energy_image(energy)
    if energy_image:
        try:
            await message.answer_photo(photo=FSInputFile(energy_image))
        except:
            pass

# =============== АДМИН-ПАНЕЛЬ ===============
@router.message(F.text == "⚙️ Админ-панель")
async def admin_panel(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total_users = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE status = 'paid'")
        paid_users = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM premium_codes WHERE used_by IS NULL")
        available_codes = (await cursor.fetchone())[0]
        admin_text = (
            f"⚙️ <b>АДМИН-ПАНЕЛЬ</b>\n"
            f"• Всего пользователей: {total_users}\n"
            f"• Премиум: {paid_users}\n"
            f"• Доступно промокодов: {available_codes}\n"
            f"• Доход: {paid_users * PRICE} ₽\n\n"
            "<b>Доступные действия:</b>"
        )
        await message.answer(admin_text, parse_mode="HTML", reply_markup=get_admin_keyboard())

@router.message(F.text == "👑 Выдать премиум")
async def grant_premium_menu(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        return
    await message.answer(
        "👑 <b>ВЫДАЧА ПРЕМИУМ-ДОСТУПА</b>\n"
        "Отправь <b>ID пользователя</b> для выдачи премиума:\n"
        "<code>Пример: 123456789</code>",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_premium_user_id)

@router.message(Form.waiting_for_premium_user_id)
async def grant_premium_by_id(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        return
    try:
        user_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT username, full_name, status FROM users WHERE user_id = ?", (user_id,))
            user_data = await cursor.fetchone()
            if not user_data:
                await message.answer("❌ Пользователь не найден")
                return
            username, full_name, current_status = user_data
            if current_status == "paid":
                await message.answer(f"ℹ️ Пользователь {user_id} уже имеет премиум")
                return
            await db.execute("UPDATE users SET status = 'paid' WHERE user_id = ?", (user_id,))
            await db.execute("""
            INSERT OR IGNORE INTO user_achievements (user_id, achievement_id, unlocked_at)
            VALUES (?, 'premium_seeker', datetime('now'))
            """, (user_id,))
            await db.commit()
            try:
                await bot.send_message(
                    user_id,
                    "🎉 <b>ПОЗДРАВЛЯЕМ!</b>\n"
                    "Администратор выдал вам <b>ПРЕМИУМ-ДОСТУП</b>!\n"
                    "Нажмите «📈 Мой отчёт» для просмотра полного отчёта!\n"
                    "Также теперь вам доступна функция «🌞 Энергия дня»!",
                    parse_mode="HTML"
                )
            except:
                pass
            await message.answer(
                f"✅ <b>ПРЕМИУМ ВЫДАН!</b>\n"
                f"ID: {user_id}\n"
                f"Имя: {full_name or 'Не указано'}\n"
                f"Username: @{username or 'Не указан'}",
                parse_mode="HTML"
            )
    except ValueError:
        await message.answer("❌ ID должен быть числом")
    await state.clear()
    await admin_panel(message, state)

@router.message(F.text == "🎫 Создать промокод")
async def create_promo_code(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        return
    code = await generate_premium_code()
    await save_premium_code(code)
    await message.answer(
        f"🎫 <b>НОВЫЙ ПРОМОКОД</b>\n"
        f"<code>{code}</code>",
        parse_mode="HTML"
    )

@router.message(F.text == "📋 Список промокодов")
async def list_promo_codes(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT code FROM premium_codes WHERE used_by IS NULL ORDER BY created_at DESC LIMIT 10")
        available = await cursor.fetchall()
        cursor = await db.execute("""
        SELECT pc.code, u.username
        FROM premium_codes pc
        LEFT JOIN users u ON pc.used_by = u.user_id
        WHERE pc.used_by IS NOT NULL
        ORDER BY pc.used_at DESC LIMIT 10
        """)
        used = await cursor.fetchall()
        response = "🎫 <b>ПРОМОКОДЫ</b>\n"
        if available:
            response += "<b>Доступные:</b>\n"
            for code, in available:
                response += f"• <code>{code}</code>\n"
        else:
            response += "⚠️ Нет доступных промокодов\n"
        if used:
            response += "\n<b>Использованные:</b>\n"
            for code, username in used:
                user = f"@{username}" if username else "Неизвестно"
                response += f"• <code>{code}</code> ({user})\n"
        await message.answer(response, parse_mode="HTML")

@router.message(F.text == "📊 Статистика")
async def admin_stats(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE status = 'paid'")
        paid = (await cursor.fetchone())[0]
        stats_text = (
            f"📈 <b>СТАТИСТИКА БОТА</b>\n"
            f"👥 <b>ПОЛЬЗОВАТЕЛИ:</b>\n"
            f"• Всего: {total}\n"
            f"• Премиум: {paid} ({paid/total*100:.1f}%)\n\n"
            f"💰 <b>ФИНАНСЫ:</b>\n"
            f"• Доход: {paid * PRICE} ₽"
        )
        await message.answer(stats_text, parse_mode="HTML")

# =============== РАССЫЛКА ===============
@router.message(F.text == "📢 Рассылка")
async def admin_broadcast(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        return
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👥 Всем")],
            [KeyboardButton(text="🆓 Бесплатным")],
            [KeyboardButton(text="💎 Премиум")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )
    await message.answer("Выберите аудиторию:", reply_markup=kb)
    await state.set_state(Form.waiting_for_broadcast_target)

@router.message(Form.waiting_for_broadcast_target)
async def handle_broadcast_target(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        return
    if message.text == "🔙 Назад":
        await admin_panel(message, state)
        await state.clear()
        return
    target = {
        "👥 Всем": "all",
        "🆓 Бесплатным": "free",
        "💎 Премиум": "paid"
    }.get(message.text)
    if not target:
        await message.answer("Пожалуйста, выберите аудиторию из кнопок.")
        return
    await state.update_data(broadcast_target=target)
    await message.answer(
        "📝 <b>ВВЕДИТЕ СООБЩЕНИЕ ДЛЯ РАССЫЛКИ:</b>\n"
        "Можно использовать HTML-разметку\n"
        "<i>Или нажмите ❌ Отменить рассылку</i>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отменить рассылку")]],
            resize_keyboard=True
        )
    )
    await state.set_state(Form.waiting_for_broadcast_message)

@router.message(Form.waiting_for_broadcast_message)
async def handle_broadcast_message(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        return
    if message.text == "❌ Отменить рассылку":
        await message.answer("Рассылка отменена.", reply_markup=get_admin_keyboard())
        await state.clear()
        return
    data = await state.get_data()
    target = data.get("broadcast_target", "all")
    await message.answer("⏳ Начинаю рассылку...")
    users_to_send = []
    if target == "all":
        users_to_send = await get_all_users()
    else:
        users_to_send = await get_users_by_status(target)
    success = 0
    failed = 0
    for user_id in users_to_send:
        try:
            await bot.send_message(
                user_id,
                f"📢 <b>РАССЫЛКА ОТ АДМИНИСТРАЦИИ:</b>\n{message.text}",
                parse_mode="HTML"
            )
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Failed to send to {user_id}: {e}")
    await message.answer(
        f"✅ <b>РАССЫЛКА ЗАВЕРШЕНА</b>\n"
        f"• Отправлено: {success}\n"
        f"• Не удалось: {failed}\n"
        f"• Всего получателей: {len(users_to_send)}",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )
    await state.clear()

# =============== КНОПКА НАЗАД ===============
@router.message(F.text == "🔙 Назад")
async def back_to_admin(message: Message):
    if message.from_user.id != ADMIN_USER_ID:
        return
    await admin_panel(message, None)

@router.message(F.text == "🔙 На главную")
async def back_to_main(message: Message, state: FSMContext):
    await cmd_start(message, state)

# =============== ОБРАБОТКА ДАННЫХ ===============
@router.message(Form.waiting_for_birth_date)
async def process_birth_date(message: Message, state: FSMContext):
    if message.text in ["⚙️ Админ-панель", "📊 Моя статистика", "🎁 Бонусы", "🌞 Энергия дня", "📈 Мой отчёт"]:
        return
    if not validate_date(message.text):
        await message.answer(
            "❌ <b>НЕВЕРНЫЙ ФОРМАТ ИЛИ ДАТА</b>\n"
            "Пожалуйста, введите дату в формате <b>ДД.ММ.ГГГГ</b>\n"
            "Пример: <code>14.05.1990</code>\n"
            "Убедитесь, что:\n"
            "• День от 1 до 31\n"
            "• Месяц от 1 до 12\n"
            "• Год от 1900 до 2025\n"
            "• Дата существует (например, 30.02 - неверно)",
            parse_mode="HTML"
        )
        return
    await state.update_data(birth_date=message.text.strip())
    await message.answer(
        "✅ <b>ДАТА ПРИНЯТА!</b>\n"
        "🔤 Теперь пришли своё <b>полное имя</b> (имя, отчество, фамилия):\n"
        "Пример: <i>Алексей Сергеевич Петров</i>\n"
        "<i>Используйте только русские буквы и пробелы</i>",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_full_name)

@router.message(Form.waiting_for_full_name)
async def process_full_name(message: Message, state: FSMContext):
    full_name = message.text.strip()
    user_id = message.from_user.id
    
    if not validate_name(full_name):
        await message.answer(
            "❌ <b>НЕВЕРНЫЙ ФОРМАТ ИМЕНИ</b>\n\n"
            "Имя должно:\n"
            "• Содержать только русские буквы, пробелы и дефисы\n"
            "• Быть полным (например: Имя Отчество Фамилия)\n"
            "• Содержать минимум 2 символа\n\n"
            "Примеры:\n"
            "• <i>Иван Иванович Иванов</i>\n"
            "• <i>Анна-Мария Петрова</i>\n"
            "• <i>Сергей Сидоров</i>\n\n"
            "Пожалуйста, введите имя заново:",
            parse_mode="HTML"
        )
        return
    
    data = await state.get_data()
    birth_date = data.get('birth_date')
    if not birth_date:
        await message.answer("❌ Дата рождения не найдена. Пожалуйста, начните заново с /start")
        await state.clear()
        return
    
    try:
        logger.info(f"Calculating for user {user_id}: {birth_date}, {full_name}")
        
        # Рассчитываем профиль
        profile = calculate_numerology_profile(birth_date, full_name, CURRENT_YEAR)
        
        # Рассчитываем матрицу
        matrix, digit_counts = calculate_pythagoras_matrix(birth_date)
        matrix_visual = generate_matrix_visual(matrix)
        line_analysis = analyze_pythagoras_lines(digit_counts)
        archetype = determine_archetype(digit_counts)
        
        matrix_data = {
            "matrix_visual": matrix_visual,
            "line_analysis": line_analysis,
            "archetype": archetype
        }
        
        # Получаем статус
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT status FROM users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            current_status = row[0] if row else "free"
        
        # Сохраняем пользователя
        await save_user(
            user_id,
            message.from_user.username,
            full_name,
            current_status,
            birth_date,
            archetype
        )
        
        if current_status == "paid":
            # Отправляем отчет частями
            full_report = generate_full_report(profile, matrix_data)
            
            # Разбиваем отчет на части если он слишком длинный
            if len(full_report) > 4000:
                # Делим по абзацам
                parts = full_report.split('\n\n')
                current_part = ""
                
                for part in parts:
                    if len(current_part) + len(part) + 2 > 4000:
                        # Отправляем текущую часть
                        await message.answer(current_part, parse_mode="HTML")
                        current_part = part
                    else:
                        if current_part:
                            current_part += "\n\n" + part
                        else:
                            current_part = part
                
                # Отправляем последнюю часть
                if current_part:
                    await message.answer(current_part, parse_mode="HTML")
            else:
                await message.answer(full_report, parse_mode="HTML")
            
            # Отправляем медиа
            try:
                premium_media = get_random_file("media/premium", ('.mp4', '.jpg', '.png', '.gif'))
                if premium_media:
                    if premium_media.endswith('.mp4'):
                        await message.answer_video(video=FSInputFile(premium_media))
                    elif premium_media.endswith('.gif'):
                        await message.answer_animation(animation=FSInputFile(premium_media))
                    else:
                        await message.answer_photo(photo=FSInputFile(premium_media))
            except Exception as e:
                logger.error(f"Error sending premium media: {e}")
            
            await message.answer(
                "✨ <b>ВАШ ПРЕМИУМ-ОТЧЁТ СОХРАНЁН!</b>\n\n"
                "Теперь вы можете в любой момент посмотреть его, нажав «📈 Мой отчёт».",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(user_id, True)
            )
            
        else:
            # Бесплатный отчет
            free_report = generate_free_report(profile)
            await state.update_data(profile=profile, matrix_data=matrix_data)
            
            if len(free_report) > 4000:
                # Обрезаем если слишком длинный
                free_report = free_report[:3997] + "..."
            
            await message.answer(free_report, parse_mode="HTML")
            
            # Медиа
            try:
                free_img = get_random_file("media/free", ('.jpg', '.png', '.gif'))
                if free_img:
                    if free_img.endswith('.gif'):
                        await message.answer_animation(animation=FSInputFile(free_img))
                    else:
                        await message.answer_photo(photo=FSInputFile(free_img))
            except Exception as e:
                logger.error(f"Error sending free image: {e}")
            
            # Кнопки оплаты
            buy_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"💎 Полный разбор — {PRICE} ₽", callback_data="buy_full")],
                [InlineKeyboardButton(text="🎁 Ввести промокод", callback_data="enter_promo")]
            ])
            
            await message.answer(
                "🚀 <b>ГОТОВЫ РАСКРЫТЬ ВСЮ ПРАВДУ?</b>\n"
                "Полный доступ даст вам:\n"
                "• Анализ всех линий матрицы\n"
                "• Конкретные рекомендации\n"
                "• Индивидуальные практики",
                parse_mode="HTML",
                reply_markup=buy_kb
            )
            await state.set_state(Form.waiting_for_payment)
            
    except Exception as e:
        logger.error(f"Error in process_full_name for user {user_id}: {e}", exc_info=True)
        
        # Более информативное сообщение об ошибке
        error_msg = str(e)
        if "Message is too long" in error_msg or "message is too long" in error_msg:
            await message.answer(
                "⚠️ <b>ОШИБКА: СООБЩЕНИЕ СЛИШКОМ ДЛИННОЕ</b>\n\n"
                "Попробуйте:\n"
                "1. Проверить текстовые файлы в папке narratives\n"
                "2. Убедиться, что они не слишком большие\n"
                "3. Обратиться к администратору",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(user_id, False)
            )
        else:
            await message.answer(
                f"❌ <b>ОШИБКА ПРИ РАСЧЁТЕ</b>\n\n"
                f"Тип ошибки: {type(e).__name__}\n\n"
                "Пожалуйста:\n"
                "1. Проверьте формат даты (ДД.ММ.ГГГГ)\n"
                "2. Убедитесь, что имя содержит только русские буквы\n"
                "3. Попробуйте снова, нажав «🔄 Новый расчёт»\n\n"
                "Если ошибка повторяется, обратитесь к администратору.",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(user_id, False)
            )

# =============== ОПЛАТА И ПРОМОКОДЫ ===============
@router.callback_query(F.data == "buy_full")
async def process_buy(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    profile = data.get("profile")
    if not profile:
        await callback.message.answer("Сначала введи дату и имя.")
        return
    payment = await create_payment(callback.from_user.id, "Полный нумерологический разбор")
    pay_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Оплатить", url=payment.confirmation.confirmation_url)],
        [InlineKeyboardButton(text="Я оплатил", callback_data="check_payment")]
    ])
    await callback.message.answer(
        f"💳 Перейди по ссылке, чтобы оплатить <b>{PRICE} ₽</b>:\n"
        "После оплаты нажми «Я оплатил».",
        parse_mode="HTML",
        reply_markup=pay_kb
    )
    await callback.answer()

@router.callback_query(F.data == "check_payment")
async def check_payment(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = await state.get_data()
    profile = data.get("profile")
    matrix_data = data.get("matrix_data")
    if profile and matrix_data:
        await save_user(
            user_id,
            callback.from_user.username,
            callback.from_user.full_name or "Unknown",
            "paid",
            profile["birth_date"],
            matrix_data["archetype"]
        )
        full_report = generate_full_report(profile, matrix_data)
        await callback.message.answer(full_report, parse_mode="HTML")
        premium_media = get_random_file("media/premium", ('.mp4', '.jpg', '.png', '.gif'))
        if premium_media:
            if premium_media.endswith('.mp4'):
                await callback.message.answer_video(video=FSInputFile(premium_media))
            elif premium_media.endswith('.gif'):
                await callback.message.answer_animation(animation=FSInputFile(premium_media))
            else:
                await callback.message.answer_photo(photo=FSInputFile(premium_media))
        for img_path in get_karmic_files(profile['karmic_debts']):
            if os.path.exists(img_path):
                await callback.message.answer_photo(photo=FSInputFile(img_path))
        await callback.answer("✅ Премиум-доступ активирован!")
        await callback.message.answer(
            "✨ <b>ВАШ ПРЕМИУМ-ДОСТУП АКТИВИРОВАН!</b>\n"
            "Теперь вам доступны все функции бота:\n"
            "• 📈 Мой отчёт (ваши данные сохранены)\n"
            "• 🏠 Анализ квартиры\n"
            "• 🚗 Анализ машины\n"
            "• 🌞 Энергия дня\n"
            "• 📊 Полная статистика\n"
            "Используйте меню для навигации!",
            parse_mode="HTML",
            reply_markup=get_main_keyboard(user_id, True)
        )
    else:
        await callback.answer("❌ Ошибка активации. Пожалуйста, начните заново с команды /start")

@router.callback_query(F.data == "enter_promo")
async def enter_promo(callback: CallbackQuery):
    await callback.message.answer(
        "🔑 <b>АКТИВАЦИЯ ПРОМОКОДА</b>\n"
        "Отправьте промокод в формате:\n"
        "<code>MATRIX-XXX-YYY-ZZZ</code>",
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(F.data == "daily_energy")
async def show_daily_energy_callback(callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    user_status = await get_user_status(user_id)
    if user_status != "paid":
        await callback_query.answer("Энергия дня доступна только в премиум-версии", show_alert=True)
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT birth_date FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if not row or not row[0]:
            await callback_query.answer("Сначала укажите дату рождения через «🔄 Новый расчёт»", show_alert=True)
            return
        birth_date = row[0]
        day_part = birth_date.split(".")[0]
        today_day = datetime.now().strftime("%d")
        energy = calculate_daily_energy(day_part, today_day)
        energy_text = read_narrative(f"narratives/full/daily_energy/{energy}.txt")
        if not energy_text or "не готов" in energy_text:
            energy_text = (
                f"Ваша энергия сегодня: {energy}\n"
                "Доверяйте интуиции и действуйте осознанно. "
                "Это день важных insights и внутренних открытий."
            )
        full_message = f"✨ <b>Твоя энергия на {datetime.now().strftime('%d.%m.%Y')}:</b>\n{energy_text}"
        await callback_query.message.answer(full_message, parse_mode="HTML")
        await callback_query.answer()

# =============== ОБРАБОТКА ПРОМОКОДОВ ===============
@router.message(F.text.regexp(r'^MATRIX-[A-Z0-9]{3}-[A-Z0-9]{3}-[A-Z0-9]{3}$'))
async def process_promo_code(message: Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip().upper()
    logger.info(f"Пользователь {user_id} пытается активировать промокод: {code}")
    try:
        current_status = await get_user_status(user_id)
        if current_status == "paid":
            await message.answer(
                "✅ <b>У вас уже есть премиум-доступ!</b>\n"
                "Нажмите «📈 Мой отчёт» для получения полного отчета.",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(user_id, await user_has_data(user_id))
            )
            return
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT code, used_by FROM premium_codes WHERE code = ?",
                (code,)
            )
            code_row = await cursor.fetchone()
            if not code_row:
                await message.answer(
                    "❌ <b>ПРОМОКОД НЕ НАЙДЕН</b>\n"
                    "Такого промокода не существует. Проверьте правильность ввода.",
                    parse_mode="HTML",
                    reply_markup=get_main_keyboard(user_id, await user_has_data(user_id))
                )
                return
            if code_row[1] is not None:
                await message.answer(
                    "❌ <b>ПРОМОКОД УЖЕ ИСПОЛЬЗОВАН</b>\n"
                    "Этот промокод уже был активирован другим пользователем.",
                    parse_mode="HTML",
                    reply_markup=get_main_keyboard(user_id, await user_has_data(user_id))
                )
                return
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE premium_codes SET used_by = ?, used_at = datetime('now') WHERE code = ?",
                (user_id, code)
            )
            await db.execute(
                "UPDATE users SET status = 'paid' WHERE user_id = ?",
                (user_id,)
            )
            await db.execute("""
            INSERT OR IGNORE INTO user_achievements (user_id, achievement_id, unlocked_at)
            VALUES (?, 'premium_seeker', datetime('now'))
            """, (user_id,))
            await db.commit()
        logger.info(f"Промокод {code} успешно активирован для пользователя {user_id}")
        has_data = await user_has_data(user_id)
        if has_
            await message.answer(
                "🎉 <b>ПРОМОКОД УСПЕШНО АКТИВИРОВАН!</b>\n"
                "✅ <b>Ваш премиум-доступ активирован!</b>\n"
                "✨ <b>Теперь вам доступны:</b>\n"
                "• Полный нумерологический отчет\n"
                "• Анализ совместимости с жильем и авто\n"
                "• Энергия дня каждый день\n"
                "• Все премиум-функции\n"
                "📈 <b>Для просмотра полного отчета нажмите «Мой отчёт»!</b>",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(user_id, True)
            )
        else:
            await message.answer(
                "🎉 <b>ПРОМОКОД УСПЕШНО АКТИВИРОВАН!</b>\n"
                "✅ <b>Ваш премиум-доступ активирован!</b>\n"
                "✨ <b>Теперь вам доступны:</b>\n"
                "• Полный нумерологический отчет\n"
                "• Анализ совместимости с жильем и авто\n"
                "• Энергия дня каждый день\n"
                "• Все премиум-функции\n"
                "🚀 <b>Для получения полного отчета:</b>\n"
                "1. Нажмите кнопку «🔄 Новый расчёт»\n"
                "2. Введите дату рождения и имя\n"
                "3. Получите полный премиум-отчет!",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(user_id, False)
            )
    except Exception as e:
        logger.error(f"Критическая ошибка при активации промокода: {e}")
        await message.answer(
            "⚠️ <b>ПРОИЗОШЛА ОШИБКА ПРИ АКТИВАЦИИ</b>\n"
            "Пожалуйста, попробуйте позже или обратитесь к администратору.\n"
            "Если ошибка повторяется, напишите в поддержку.",
            parse_mode="HTML",
            reply_markup=get_main_keyboard(user_id, await user_has_data(user_id))
        )

# =============== АНАЛИЗ СОВМЕСТИМОСТИ ===============
@router.message(F.text == "🏠 Анализ квартиры")
async def handle_home_analysis(message: Message, state: FSMContext):
    user_status = await get_user_status(message.from_user.id)
    if user_status != "paid":
        await message.answer("🔒 Эта функция доступна только в премиум-версии.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT birth_date FROM users WHERE user_id = ?", (message.from_user.id,))
        row = await cursor.fetchone()
        if not row or not row[0]:
            await message.answer("Сначала отправь дату рождения.")
            return
    await message.answer("🏠 Пришли номер своей квартиры, дома или этажа (например: 72, 15А, 3)")
    await state.set_state(Form.waiting_for_home_input)

@router.message(F.text == "🚗 Анализ машины")
async def handle_car_analysis(message: Message, state: FSMContext):
    user_status = await get_user_status(message.from_user.id)
    if user_status != "paid":
        await message.answer("🔒 Эта функция доступна только в премиум-версии.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT birth_date FROM users WHERE user_id = ?", (message.from_user.id,))
        row = await cursor.fetchone()
        if not row or not row[0]:
            await message.answer("Сначала отправь дату рождения.")
            return
    await message.answer("🚗 Пришли свой автомобильный номер (например: А123БВ)")
    await state.set_state(Form.waiting_for_car_input)

@router.message(Form.waiting_for_home_input)
async def process_home_input(message: Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT birth_date FROM users WHERE user_id = ?", (message.from_user.id,))
        row = await cursor.fetchone()
        if not row or not row[0]:
            await message.answer("Ошибка: дата рождения не найдена.")
            return
    birth_date = row[0]
    person_num = reduce_number(sum(int(d) for d in birth_date.replace(".", "")))
    obj_num = calculate_object_number(message.text.strip())
    report = read_compatibility_narrative(person_num, obj_num, "home")
    await message.answer(f"🏠 <b>ГЛУБОКИЙ АНАЛИЗ КВАРТИРЫ</b>\n\n{report}", parse_mode="HTML")
    await state.clear()

@router.message(Form.waiting_for_car_input)
async def process_car_input(message: Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT birth_date FROM users WHERE user_id = ?", (message.from_user.id,))
        row = await cursor.fetchone()
        if not row or not row[0]:
            await message.answer("Ошибка: дата рождения не найдена.")
            return
    birth_date = row[0]
    person_num = reduce_number(sum(int(d) for d in birth_date.replace(".", "")))
    obj_num = calculate_object_number(message.text.strip())
    report = read_compatibility_narrative(person_num, obj_num, "car")
    await message.answer(f"🚗 <b>ГЛУБОКИЙ АНАЛИЗ АВТОМОБИЛЯ</b>\n\n{report}", parse_mode="HTML")
    await state.clear()

# =============== ЗАПУСК ===============
async def main():
    await init_db()
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())