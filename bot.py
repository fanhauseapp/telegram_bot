import os
import logging
import sqlite3
import asyncio
import random
import json
import pathlib
import traceback
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# ================= НАСТРОЙКИ =================
TOKEN = os.getenv("TELEGRAM_TOKEN", "8455824950:AAFjowp9RInYwWpnN2fs8556d8TO57niadE")
DB_PATH = pathlib.Path(__file__).parent / "similarity_bot.db"
DB_NAME = str(DB_PATH)
ADMIN_ID = int(os.getenv("ADMIN_ID", "769173453"))
# ===========================================

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

print("=" * 50)
print("🤖 БОТ ЗАПУСКАЕТСЯ НА RAILWAY С GOOGLE SHEETS")
print("=" * 50)

# Инициализация бота
bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ================= GOOGLE SHEETS ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ =================
sheet = None
GOOGLE_SHEETS_ENABLED = False

# ================= ИНИЦИАЛИЗАЦИЯ GOOGLE SHEETS =================
def init_google_sheets():
    global sheet, GOOGLE_SHEETS_ENABLED
    
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        
        print("🔄 Инициализация Google Sheets...")
        
        SPREADSHEET_ID = "1sCbHGFMy8crwUWgUcwarxQ_2W1opX8ol_ONKcItW86U"
        google_credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        
        if not google_credentials_json:
            print("❌ Переменная GOOGLE_CREDENTIALS_JSON не найдена в окружении")
            GOOGLE_SHEETS_ENABLED = False
            return
        
        try:
            credentials_info = json.loads(google_credentials_json)
            print("✅ Загружен GOOGLE_CREDENTIALS_JSON из окружения")
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка парсинга JSON: {e}")
            GOOGLE_SHEETS_ENABLED = False
            return
        
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        credentials = Credentials.from_service_account_info(
            credentials_info, 
            scopes=scope
        )
        
        client = gspread.authorize(credentials)
        
        try:
            spreadsheet = client.open_by_key(SPREADSHEET_ID)
            sheet = spreadsheet.sheet1
            
            test_value = sheet.acell('A1').value
            print(f"✅ Google Sheets подключен. Ячейка A1: {test_value}")
            
            if not sheet.get_all_values():
                sheet.append_row([
                    "User ID", "Username", "Fandom", "Subcategory", 
                    "First Seen", "Last Updated", "Timestamp"
                ])
                print("✅ Созданы заголовки в Google Sheets")
            
            GOOGLE_SHEETS_ENABLED = True
            print("✅ Google Sheets готов к работе")
            
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"❌ Таблица с ID {SPREADSHEET_ID} не найдена")
            GOOGLE_SHEETS_ENABLED = False
        except gspread.exceptions.APIError as e:
            print(f"❌ Ошибка API Google: {e}")
            GOOGLE_SHEETS_ENABLED = False
            
    except ImportError as e:
        print(f"⚠️ Не установлены библиотеки для Google Sheets: {e}")
        GOOGLE_SHEETS_ENABLED = False
    except Exception as e:
        print(f"❌ Критическая ошибка подключения Google Sheets: {e}")
        traceback.print_exc()
        GOOGLE_SHEETS_ENABLED = False

# ================= БАЗА ДАННЫХ SQLite =================
def get_db_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        tg_id INTEGER PRIMARY KEY,
        username TEXT NOT NULL,
        fandom TEXT NOT NULL,
        subcategory TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()
    print(f"✅ База данных SQLite создана: {DB_NAME}")

init_db()
init_google_sheets()

# ================= ФУНКЦИИ ДЛЯ GOOGLE SHEETS =================
async def update_google_sheets(user_id, username, fandom, subcategory):
    global sheet, GOOGLE_SHEETS_ENABLED
    
    print(f"📤 update_google_sheets вызвана для user_id {user_id}, GOOGLE_SHEETS_ENABLED={GOOGLE_SHEETS_ENABLED}")
    
    if not GOOGLE_SHEETS_ENABLED or sheet is None:
        print(f"⚠️ Google Sheets недоступен, пропускаем user_id {user_id}")
        return
    
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"🔍 Ищем user_id {user_id} в Google Sheets...")
        
        try:
            cell = sheet.find(str(user_id))
            print(f"🔍 Результат поиска: {cell}")  # если не найдено, будет None
        except gspread.exceptions.CellNotFound:
            cell = None
            print("🔍 Ячейка не найдена, будет добавлена новая строка")
        except Exception as e:
            print(f"⚠️ Ошибка при поиске: {e}")
            return
        
        if cell:
            print(f"🔄 Обновление строки {cell.row} для user_id {user_id}")
            row = cell.row
            sheet.update_cell(row, 1, str(user_id))
            sheet.update_cell(row, 2, username)
            sheet.update_cell(row, 3, fandom)
            sheet.update_cell(row, 4, subcategory)
            sheet.update_cell(row, 6, current_time)
            sheet.update_cell(row, 7, current_time)
            print(f"✅ Google Sheets: обновлена строка {row} для user_id {user_id}")
        else:
            print(f"🔄 Добавление новой строки для user_id {user_id}")
            new_row = [
                str(user_id), 
                username, 
                fandom, 
                subcategory,
                current_time,
                current_time,
                current_time
            ]
            sheet.append_row(new_row)
            print(f"✅ Google Sheets: добавлен новый user_id {user_id}")
            
    except Exception as e:
        print(f"❌ Ошибка при работе с Google Sheets: {e}")
        traceback.print_exc()

async def delete_from_google_sheets(user_id):
    global sheet, GOOGLE_SHEETS_ENABLED
    if not GOOGLE_SHEETS_ENABLED or sheet is None:
        return
    try:
        cell = sheet.find(str(user_id))
        if cell:
            sheet.delete_row(cell.row)
            print(f"🗑️ Удален user_id {user_id} из Google Sheets")
    except Exception as e:
        print(f"⚠️ Не удалось удалить из Google Sheets: {e}")

# ================= СОСТОЯНИЯ =================
class UserState(StatesGroup):
    waiting_for_start = State()
    waiting_for_fandom = State()
    waiting_for_subcategory = State()

# ================= СПИСКИ ФАНДОМОВ =================
FANDOMS = {
    "Гарри Поттер": ["книги", "фанфики", "фильмы"],
    "Очень Странные дела": ["теории", "пейринги", "сюжет"],
    "Всё ради игры": ["арты", "эндрилы", "сюжетные дыры"],
    "Аниме": ["атака титанов", "всё подряд", "хочу рекомендаций"],
    "BTS": ["айдолы", "мерч", "концерты"]
}

# ================= КОМАНДА /START =================
@dp.message(Command("start"))
async def start_command(message: Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or "без username"
    print(f"👤 @{username} ({user_id}) написал /start")
    if not message.from_user.username:
        await message.answer("❗ Для работы бота нужен username.")
        return
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="выбрать свой фандом", callback_data="start_survey")]
        ]
    )
    await message.answer(
        "Привет, это бот, который поможет найти вам собеседника по вашим любимым фандомам. "
        "Пройди небольшую анкету и мы подберем тебе собеседника. "
        "Нажимай на кнопку ниже 👇",
        reply_markup=keyboard
    )
    await state.set_state(UserState.waiting_for_start)

@dp.callback_query(lambda c: c.data == "start_survey")
async def start_survey(callback_query: CallbackQuery, state: FSMContext):
    await callback_query.answer()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Гарри Поттер", callback_data="fandom_Гарри Поттер")],
            [InlineKeyboardButton(text="Очень Странные дела", callback_data="fandom_Очень Странные дела")],
            [InlineKeyboardButton(text="Всё ради игры", callback_data="fandom_Всё ради игры")],
            [InlineKeyboardButton(text="Аниме", callback_data="fandom_Аниме")],
            [InlineKeyboardButton(text="BTS", callback_data="fandom_BTS")]
        ]
    )
    await callback_query.message.edit_text(
        "Начнем с базы - выбери ОДИН фандом, по которому ты бы хотел найти собеседника. "
        "Сейчас список состоит из 5 тем, но не переживай, в будущем он будет расширяться.",
        reply_markup=keyboard
    )
    await state.set_state(UserState.waiting_for_fandom)

@dp.callback_query(lambda c: c.data.startswith("fandom_"))
async def choose_fandom(callback_query: CallbackQuery, state: FSMContext):
    await callback_query.answer()
    fandom = callback_query.data.replace("fandom_", "")
    await state.update_data(fandom=fandom)
    subcategories = FANDOMS.get(fandom, [])
    buttons = []
    for sub in subcategories:
        buttons.append([InlineKeyboardButton(text=sub, callback_data=f"sub_{sub}")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback_query.message.edit_text(
        f"Ты выбрал(а): <b>{fandom}</b>\n\n"
        "Чтобы найти подходящего собеседника давай решим что тебе ближе?\n"
        "Выбери один вариант:",
        reply_markup=keyboard
    )
    await state.set_state(UserState.waiting_for_subcategory)

@dp.callback_query(lambda c: c.data.startswith("sub_"))
async def choose_subcategory(callback_query: CallbackQuery, state: FSMContext):
    await callback_query.answer()
    subcategory = callback_query.data.replace("sub_", "")
    data = await state.get_data()
    fandom = data.get("fandom", "")
    user_id = callback_query.from_user.id
    username = callback_query.from_user.username or "без username"
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT 1 FROM users WHERE tg_id=?", (user_id,))
        exists = cursor.fetchone()
        
        if exists:
            cursor.execute("""
            UPDATE users SET username=?, fandom=?, subcategory=?, updated_at=CURRENT_TIMESTAMP
            WHERE tg_id=?
            """, (username, fandom, subcategory, user_id))
            action = "обновлена"
        else:
            cursor.execute("""
            INSERT INTO users (tg_id, username, fandom, subcategory)
            VALUES (?, ?, ?, ?)
            """, (user_id, username, fandom, subcategory))
            action = "завершена"
        
        conn.commit()
        
        # Сохраняем в Google Sheets
        if GOOGLE_SHEETS_ENABLED:
            print(f"📌 Попытка записи в Google Sheets для user_id {user_id}")
            await update_google_sheets(user_id, username, fandom, subcategory)
            status_text = f"<i>🤖 Бот работает на Railway + Google Sheets</i>"
        else:
            status_text = f"<i>🤖 Бот работает на Railway (только локальная база)</i>"
        
        await callback_query.message.edit_text(
            f"🎉 <b>Анкета {action}!</b>\n\n"
            f"Твои предпочтения:\n"
            f"• Фандом: <b>{fandom}</b>\n"
            f"• Категория: <b>{subcategory}</b>\n\n"
            f"Теперь напиши <b>/find</b> — я найду тебе собеседника! 👀\n\n"
            f"{status_text}"
        )
        
        print(f"💾 Анкета {action} для @{username}: {fandom} - {subcategory}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        await callback_query.message.answer("❌ Ошибка при сохранении")
    finally:
        conn.close()
        await state.clear()

@dp.message(Command("find"))
async def find_matches(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or "без username"
    print(f"🔍 @{username} ({user_id}) ищет совпадения")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT fandom, subcategory FROM users WHERE tg_id=?", (user_id,))
        user_data = cursor.fetchone()
        if not user_data:
            await message.answer("❌ Сначала пройди анкету — /start")
            return
        my_fandom, my_subcategory = user_data
        cursor.execute("""
        SELECT username, fandom, subcategory FROM users 
        WHERE tg_id != ? AND username IS NOT NULL
        """, (user_id,))
        users = cursor.fetchall()
        if not users:
            await message.answer("😔 Пока нет других пользователей")
            return
        full_matches = []
        partial_matches = []
        for user in users:
            username, fandom, subcategory = user
            if fandom == my_fandom and subcategory == my_subcategory:
                full_matches.append(f"@{username}")
            elif fandom == my_fandom:
                partial_matches.append(f"@{username}")
        random.shuffle(full_matches)
        random.shuffle(partial_matches)
        selected_full = full_matches[:2]
        selected_partial = partial_matches[:2]
        if not selected_full and not selected_partial:
            await message.answer("😔 Пока нет совпадений")
            return
        text = "🔍 <b>Найдены собеседники:</b>\n\n"
        if selected_full:
            text += f"🔥 <b>Идеальное совпадение ({my_fandom} - {my_subcategory}):</b>\n"
            for username in selected_full:
                text += f"• {username}\n"
            text += "\n"
        if selected_partial:
            text += f"✨ <b>Совпадение по фандому ({my_fandom}):</b>\n"
            for username in selected_partial:
                text += f"• {username}\n"
            text += "\n"
        if len(full_matches) > 2 or len(partial_matches) > 2:
            text += f"ℹ️ Всего совпадений: {len(full_matches)} полных, {len(partial_matches)} частичных\n"
            text += "🎲 Показаны случайные 1-2 из каждой категории\n\n"
        text += "💬 <b>Напиши любому из них первым!</b>"
        await message.answer(text)
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        await message.answer("❌ Ошибка поиска")
    finally:
        conn.close()

@dp.message(Command("stats"))
async def bot_stats(message: Message):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM users")
        total = cursor.fetchone()[0]
        cursor.execute("SELECT fandom, COUNT(*) FROM users GROUP BY fandom ORDER BY COUNT(*) DESC")
        fandoms = cursor.fetchall()
        text = "📊 <b>Статистика бота:</b>\n\n"
        text += f"👥 Всего пользователей: <b>{total}</b>\n"
        text += f"🖥️ Сервер: <b>Railway</b>\n"
        if GOOGLE_SHEETS_ENABLED:
            text += f"📊 Хранилище: <b>SQLite + Google Sheets</b>\n"
        else:
            text += f"📊 Хранилище: <b>Только SQLite (данные в таблицу не сохраняются)</b>\n"
        text += f"⏰ Время: <b>{datetime.now().strftime('%H:%M')}</b>\n\n"
        if fandoms:
            text += "<b>По фандомам:</b>\n"
            for fandom, count in fandoms:
                percentage = (count / total * 100) if total > 0 else 0
                text += f"• {fandom}: {count} ({percentage:.1f}%)\n"
        await message.answer(text)
    except Exception as e:
        await message.answer("❌ Ошибка получения статистики")
    finally:
        conn.close()

@dp.message(Command("status"))
async def bot_status(message: Message):
    text = "🟢 <b>Бот работает нормально</b>\n\n"
    text += f"📡 Статус: <b>Активен 24/7</b>\n"
    text += f"⏱️ Время: <b>{datetime.now().strftime('%H:%M')}</b>\n"
    text += f"🌐 Хостинг: <b>Railway</b>\n"
    if GOOGLE_SHEETS_ENABLED:
        text += f"📊 Хранилище: <b>SQLite + Google Sheets</b>\n"
        text += f"📈 Данные сохраняются в таблицу\n"
    else:
        text += f"📊 Хранилище: <b>Только SQLite (данные НЕ сохраняются в таблицу!)</b>\n"
        text += f"⚠️ Данные потеряются при перезапуске бота\n"
    text += f"⚡ Режим: <b>Постоянная работа</b>\n\n"
    text += "<i>🤖 Бот работает в облаке</i>"
    await message.answer(text)

@dp.message(Command("restart"))
async def restart_command(message: Message, state: FSMContext):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="выбрать свой фандом", callback_data="start_survey")]
        ]
    )
    await message.answer(
        "🔄 <b>Начинаем анкету заново!</b>\n\n"
        "Выбери свой фандом 👇",
        reply_markup=keyboard
    )
    await state.set_state(UserState.waiting_for_start)

@dp.message(Command("delete"))
async def delete_data(message: Message):
    user_id = message.from_user.id
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE tg_id=?", (user_id,))
    conn.commit()
    deleted = cursor.rowcount
    conn.close()
    if GOOGLE_SHEETS_ENABLED:
        await delete_from_google_sheets(user_id)
    if deleted > 0:
        await message.answer("✅ <b>Ваши данные удалены</b>\n\n/start - начать заново")
    else:
        await message.answer("ℹ️ <b>Ваши данные не найдены</b>\n\n/start - пройти анкету")

@dp.message()
async def handle_text_messages(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state in [UserState.waiting_for_start, 
                         UserState.waiting_for_fandom, 
                         UserState.waiting_for_subcategory]:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="выбрать свой фандом", callback_data="start_survey")]
            ]
        )
        await message.answer(
            "⚠️ <b>Пожалуйста, используйте кнопки для продолжения опроса!</b>\n\n"
            "Нажмите на кнопку ниже, чтобы выбрать фандом 👇",
            reply_markup=keyboard
        )
        await state.set_state(UserState.waiting_for_start)
    else:
        await message.answer(
            "🤖 Я бот для поиска собеседников по фандомам!\n\n"
            "📡 <b>Работаю на Railway</b>\n\n"
            "Команды:\n"
            "• /start - начать анкету\n"
            "• /find - найти собеседника\n"
            "• /stats - статистика\n"
            "• /status - статус бота\n"
            "• /restart - начать заново\n"
            "• /delete - удалить данные"
        )

async def main():
    print("\n" + "=" * 50)
    print("🤖 БОТ ЗАПУЩЕН НА RAILWAY!")
    print("=" * 50)
    print(f"👑 Админ ID: {ADMIN_ID}")
    print(f"📊 Google Sheets: {'ВКЛЮЧЕН' if GOOGLE_SHEETS_ENABLED else 'ОТКЛЮЧЕН'}")
    if GOOGLE_SHEETS_ENABLED:
        print("✅ Данные сохраняются в Google Sheets")
    else:
        print("⚠️ Данные НЕ сохраняются в Google Sheets!")
        print("ℹ️ Проверьте переменную GOOGLE_CREDENTIALS_JSON в Railway")
    print("📡 Режим: Постоянная работа")
    print("📱 Напишите боту /start")
    print("=" * 50)
    bot_info = await bot.get_me()
    print(f"🤖 Бот: @{bot_info.username}")
    print(f"🆔 ID: {bot_info.id}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())