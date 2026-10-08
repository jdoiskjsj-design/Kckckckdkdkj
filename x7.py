# -*- coding: utf-8 -*-
"""
بوت تحميل الفيديوهات - Instagram / Facebook / Snapchat / YouTube / مواقع أخرى
aiogram 3 + yt-dlp + SQLite
"""
import asyncio
import html
import logging
import os
import random
import re
import shutil
import tempfile
import time
import uuid
from functools import partial
from pathlib import Path
from urllib.parse import urlparse

import aiosqlite
import yt_dlp
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import (TelegramBadRequest, TelegramForbiddenError,
                                TelegramRetryAfter)
from aiogram.filters import BaseFilter, Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (BotCommand, CallbackQuery, FSInputFile,
                           InlineKeyboardButton, InlineKeyboardMarkup,
                           InputMediaPhoto, InputMediaVideo, Message)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("dlbot")

# ───────────────────────────── الإعدادات ─────────────────────────────
TOKEN = "8934416577:AAGxYiqGlt8s1hTlIZxXO-Av7NQOYnh-ivY"
ADMINS = 8706836075
DEV_USERNAME =" x_7_v3"
POT_URL = "http://t.me/x_7_asbot"
PROXIES = [p.strip() for p in os.getenv("PROXIES", "").split(",") if p.strip()]
COOKIES_DIR = Path(os.getenv("COOKIES_DIR", "./cookies"))
FORCE_IPV4 = os.getenv("FORCE_IPV4", "1") == "1"
YT_CLIENTS = [[c.strip() for c in grp.split(",") if c.strip()]
              for grp in os.getenv("YT_CLIENTS", "tv,web_safari;mweb;android_vr;web_embedded;ios").split(";")
              if grp.strip()]
MAX_MB = int(os.getenv("MAX_MB", "50"))
API_BASE = os.getenv("API_BASE", "").strip()
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))
COOLDOWN = float(os.getenv("COOLDOWN_SEC", "4"))
DB_PATH = os.getenv("DB_PATH", "data/bot.db")
MAX_BYTES = MAX_MB * 1024 * 1024

DL_SEM = asyncio.Semaphore(MAX_CONCURRENT)
ACTIVE: set[int] = set()
LAST_REQ: dict[int, float] = {}
PENDING: dict[str, dict] = {}          # token -> {url, platform, ts}
db: aiosqlite.Connection

# ───────────────────────────── المنصات ─────────────────────────────
PLATFORMS = {
    "youtube": ("youtube.com", "youtu.be", "youtube-nocookie.com"),
    "instagram": ("instagram.com", "instagr.am"),
    "facebook": ("facebook.com", "fb.watch", "fb.com", "fb.me"),
    "snapchat": ("snapchat.com",),
    "tiktok": ("tiktok.com",),
    "twitter": ("twitter.com", "x.com"),
}
PLATFORM_LABEL = {
    "youtube": "▶️ يوتيوب", "instagram": "📸 انستغرام", "facebook": "📘 فيسبوك",
    "snapchat": "👻 سناب شات", "tiktok": "🎵 تيك توك", "twitter": "🐦 تويتر/X",
    "other": "🌐 موقع آخر",
}
URL_RE = re.compile(r"(?:https?://|www\.)[^\s<>\"']+", re.I)
BARE_RE = re.compile(
    r"\b(?:instagram\.com|instagr\.am|facebook\.com|fb\.watch|snapchat\.com|youtu\.be|"
    r"youtube\.com|tiktok\.com|x\.com|twitter\.com)/[^\s<>\"']+", re.I)


def extract_url(text: str) -> str | None:
    if not text:
        return None
    m = URL_RE.search(text)
    if m:
        u = m.group(0).rstrip(").,،!؟")
        return u if u.startswith("http") else "https://" + u
    m = BARE_RE.search(text)
    return "https://" + m.group(0).rstrip(").,،!؟") if m else None


def detect_platform(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    for name, doms in PLATFORMS.items():
        if any(host == d or host.endswith("." + d) for d in doms):
            return name
    return "other"


# ───────────────────────────── قاعدة البيانات ─────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
  id INTEGER PRIMARY KEY, name TEXT, username TEXT,
  joined INTEGER, last_seen INTEGER, banned INTEGER DEFAULT 0, dead INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS channels(chat_id INTEGER PRIMARY KEY, title TEXT, link TEXT);
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS downloads(id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER, platform TEXT, kind TEXT, ts INTEGER);
CREATE TABLE IF NOT EXISTS cache(key TEXT PRIMARY KEY, file_id TEXT, ftype TEXT, caption TEXT);
CREATE TABLE IF NOT EXISTS contact_map(admin_msg_id INTEGER PRIMARY KEY, user_id INTEGER);
"""


async def init_db():
    global db
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.executescript(SCHEMA)
    await db.commit()


async def q_one(sql, *a):
    cur = await db.execute(sql, a)
    r = await cur.fetchone()
    await cur.close()
    return r


async def q_all(sql, *a):
    cur = await db.execute(sql, a)
    r = await cur.fetchall()
    await cur.close()
    return r


async def q_exec(sql, *a):
    await db.execute(sql, a)
    await db.commit()


async def get_set(k, default=None):
    r = await q_one("SELECT v FROM settings WHERE k=?", k)
    return r["v"] if r else default


async def put_set(k, v):
    await q_exec("INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", k, v)


async def del_set(k):
    await q_exec("DELETE FROM settings WHERE k=?", k)


# ───────────────────────────── أدوات واجهة ─────────────────────────────
def esc(s) -> str:
    return html.escape(str(s or ""))


def user_link(u) -> str:
    name = esc(u.full_name)
    return f'<a href="tg://user?id={u.id}">{name}</a>'


LINE = "━━━━━━━━━━━━━━━━━━"


def main_kb(is_admin: bool) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📸 تحميل انستغرام", callback_data="m:instagram"),
          InlineKeyboardButton(text="📘 تحميل فيسبوك", callback_data="m:facebook"))
    b.row(InlineKeyboardButton(text="👻 تحميل سناب شات", callback_data="m:snapchat"),
          InlineKeyboardButton(text="▶️ تحميل يوتيوب", callback_data="m:youtube"))
    b.row(InlineKeyboardButton(text="🌐 مواقع أخرى", callback_data="m:other"))
    b.row(InlineKeyboardButton(text="📩 مراسلة المطور", callback_data="contact"))
    if is_admin:
        b.row(InlineKeyboardButton(text="🛠 لوحة المطور", callback_data="adm:home"))
    return b.as_markup()


MENU_TEXT = {
    "instagram": "📸 <b>انستغرام</b>\nأرسل رابط (ريلز • بوست • ستوري • صور متعددة) وسأحمّله لك فوراً.",
    "facebook": "📘 <b>فيسبوك</b>\nأرسل رابط الفيديو أو الريلز وسأحمّله بأعلى جودة.",
    "snapchat": "👻 <b>سناب شات</b>\nأرسل رابط السبوتلايت أو القصة المشتركة وسأحمّلها لك.",
    "youtube": "▶️ <b>يوتيوب</b>\nأرسل الرابط (فيديو • شورتس) وستختار الجودة أو MP3.",
    "other": "🌐 <b>مواقع أخرى</b>\nتيك توك • تويتر/X • ريديت • فيميو • ساوند كلاود وأكثر من 1000 موقع.\nأرسل الرابط فقط.",
}


async def start_text(u) -> str:
    footer = ""
    if await get_set("log_links", "1") == "1":
        footer = "\n\n<i>🔒 يتم تسجيل الروابط المرسلة للبوت لأغراض الجودة ومنع الإساءة.</i>"
    return (f"✨ <b>أهلاً {esc(u.first_name)}</b> ✨\n{LINE}\n"
            "🚀 <b>أسرع بوت تحميل في تيليجرام</b>\n\n"
            "📥 أرسل أي رابط وسأتعرف عليه تلقائياً:\n"
            "• 📸 انستغرام  • 📘 فيسبوك  • 👻 سناب\n"
            "• ▶️ يوتيوب  • 🎵 تيك توك  • 🐦 X  • وأكثر\n\n"
            "⚡ جودة عالية  •  🎧 تحويل MP3  •  🗂 حفظ ذكي للتكرار\n"
            f"{LINE}\n👇 اختر المنصة أو أرسل الرابط مباشرة{footer}")


async def send_home(bot: Bot, chat_id: int, u):
    kb = main_kb(u.id in ADMINS)
    text = await start_text(u)
    photo = await get_set("welcome_photo")
    if photo:
        try:
            await bot.send_photo(chat_id, photo, caption=text, reply_markup=kb)
            return
        except TelegramBadRequest:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb)


# ───────────────────────────── الاشتراك الإجباري ─────────────────────────────
async def not_joined(bot: Bot, uid: int) -> list[aiosqlite.Row]:
    missing = []
    for ch in await q_all("SELECT * FROM channels"):
        try:
            m = await bot.get_chat_member(ch["chat_id"], uid)
            if m.status in ("left", "kicked"):
                missing.append(ch)
        except Exception as e:  # البوت ليس مشرفاً أو القناة محذوفة
            log.warning("sub check failed for %s: %s", ch["chat_id"], e)
    return missing


def sub_kb(missing) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for ch in missing:
        b.row(InlineKeyboardButton(text=f"📢 {ch['title']}", url=ch["link"]))
    b.row(InlineKeyboardButton(text="✅ تحققت من الاشتراك", callback_data="check_sub"))
    return b.as_markup()


class Gate(BaseMiddleware):
    """تسجيل المستخدم + الحظر + الاشتراك الإجباري"""

    async def __call__(self, handler, event, data):
        u = data.get("event_from_user")
        if not u or u.is_bot:
            return await handler(event, data)
        now = int(time.time())
        await q_exec(
            "INSERT INTO users(id,name,username,joined,last_seen) VALUES(?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET name=excluded.name, username=excluded.username, "
            "last_seen=excluded.last_seen, dead=0",
            u.id, u.full_name, u.username, now, now)
        if u.id in ADMINS:
            return await handler(event, data)
        row = await q_one("SELECT banned FROM users WHERE id=?", u.id)
        if row and row["banned"]:
            return
        if isinstance(event, CallbackQuery) and event.data == "check_sub":
            return await handler(event, data)
        bot: Bot = data["bot"]
        missing = await not_joined(bot, u.id)
        if missing:
            text = "🔒 <b>للاستخدام يجب الاشتراك أولاً</b>\n\nاشترك بالقنوات التالية ثم اضغط «تحققت»:"
            if isinstance(event, CallbackQuery):
                await event.answer("اشترك بالقنوات أولاً 🔒", show_alert=True)
                await event.message.answer(text, reply_markup=sub_kb(missing))
            else:
                await event.answer(text, reply_markup=sub_kb(missing))
            return
        return await handler(event, data)


# ───────────────────────────── التحميل (yt-dlp) ─────────────────────────────
class DlError(Exception):
    pass


MEDIA_EXT = {".mp4", ".mkv", ".webm", ".mov", ".m4v", ".mp3", ".m4a", ".opus",
             ".jpg", ".jpeg", ".png", ".webp"}
IMG_EXT = {".jpg", ".jpeg", ".png", ".webp"}
AUD_EXT = {".mp3", ".m4a", ".opus"}
RETRY_HINTS = ("sign in to confirm", "not a bot", "429", "too many requests", "http error 403",
               "po token", "unable to download", "timed out", "connection", "unavailable")
FATAL_HINTS = ("private video", "this video is private", "has been removed", "video unavailable",
               "unsupported url", "age-restricted", "members-only", "copyright", "not available in your country")


def cookie_for(platform: str, workdir: Path) -> str | None:
    src = COOKIES_DIR / f"{platform}.txt"
    if not src.exists():
        return None
    dst = workdir / "cookies.txt"  # نسخة لأن yt-dlp يعدّل الملف
    shutil.copy(src, dst)
    return str(dst)


def build_opts(workdir: Path, kind: str, height: int, platform: str, attempt: int,
               proxy: str | None, hook) -> dict:
    o = {
        "outtmpl": str(workdir / "%(id)s.%(ext)s"),
        "quiet": True, "no_warnings": True, "noprogress": True,
        "retries": 3, "fragment_retries": 3, "socket_timeout": 30,
        "concurrent_fragment_downloads": 4, "restrictfilenames": True,
        "merge_output_format": "mp4", "progress_hooks": [hook],
        "noplaylist": platform == "youtube",
        "playlist_items": "1-10",
        "geo_bypass": True,
    }
    if kind == "audio":
        o["format"] = "bestaudio/best"
        o["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3",
                                "preferredquality": "192"}]
    else:
        o["format"] = "bv*+ba/b"
        # تفضيل H.264/AAC ليعمل مباشرة في تيليجرام وبالدقة المطلوبة
        o["format_sort"] = [f"res:{height}", "vcodec:h264", "acodec:aac", "ext:mp4:m4a"]
    if proxy:
        o["proxy"] = proxy
    if platform == "youtube":
        ea = {"youtube": {"player_client": YT_CLIENTS[attempt % len(YT_CLIENTS)]}}
        if POT_URL:
            ea["youtubepot-bgutilhttp"] = {"base_url": [POT_URL]}
        o["extractor_args"] = ea
        if FORCE_IPV4:
            o["source_address"] = "0.0.0.0"
        o["sleep_interval_requests"] = 0.5
    ck = cookie_for(platform, workdir)
    if ck:
        o["cookiefile"] = ck
    return o


def collect_files(workdir: Path) -> list[Path]:
    return sorted(p for p in workdir.iterdir() if p.is_file() and p.suffix.lower() in MEDIA_EXT)


def download_sync(url, kind, height, platform, workdir: Path, attempt, proxy, hook):
    ladder = [height] + [h for h in (480, 360, 240) if h < height] if kind == "video" else [0]
    for hh in ladder:
        for f in workdir.iterdir():
            if f.is_file() and f.name != "cookies.txt":
                f.unlink()
        opts = build_opts(workdir, kind, hh or 1080, platform, attempt, proxy, hook)
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
        files = collect_files(workdir)
        if not files:
            raise DlError("لم أجد وسائط قابلة للتحميل في هذا الرابط.")
        if all(f.stat().st_size <= MAX_BYTES for f in files):
            return files, info
    raise DlError(f"حجم الملف أكبر من الحد المسموح ({MAX_MB}MB). جرّب جودة أقل أو MP3.")


def friendly_error(e: Exception, platform: str) -> str:
    m = str(e).lower()
    if isinstance(e, DlError):
        return str(e)
    if "sign in to confirm" in m or "not a bot" in m:
        return "يوتيوب يحظر السيرفر مؤقتاً 🛡 جرّب بعد قليل (المطور: فعّل البروكسي/الكوكيز/PO Token)."
    if "private" in m or "login" in m or "log in" in m or "cookies" in m:
        return "المحتوى خاص أو يتطلب تسجيل دخول 🔐"
    if "unsupported url" in m:
        return "هذا الرابط غير مدعوم ❌"
    if "removed" in m or "unavailable" in m or "not available" in m:
        return "المحتوى محذوف أو غير متاح 🚫"
    if "age" in m and "restrict" in m:
        return "المحتوى مقيّد بالعمر 🔞"
    return "تعذّر التحميل حالياً، تأكد من الرابط وحاول لاحقاً ⚠️"


def pick_proxy(attempt: int, start: int) -> str | None:
    return PROXIES[(start + attempt) % len(PROXIES)] if PROXIES else None


async def fetch(url, kind, height, platform, workdir: Path, on_progress):
    loop = asyncio.get_running_loop()
    last_t = [0.0]

    def hook(d):
        if d.get("status") != "downloading":
            return
        now = time.time()
        if now - last_t[0] < 2.5:
            return
        last_t[0] = now
        tot = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
        if tot:
            asyncio.run_coroutine_threadsafe(on_progress(d.get("downloaded_bytes", 0) / tot), loop)

    attempts = max(len(YT_CLIENTS), 3) if platform == "youtube" else 2
    start = random.randrange(len(PROXIES)) if PROXIES else 0
    last = None
    for i in range(attempts):
        try:
            return await loop.run_in_executor(
                None, partial(download_sync, url, kind, height, platform, workdir, i,
                              pick_proxy(i, start), hook))
        except DlError:
            raise
        except Exception as e:
            last = e
            m = str(e).lower()
            log.warning("attempt %s failed (%s): %s", i + 1, platform, str(e)[:200])
            if any(k in m for k in FATAL_HINTS) and not any(k in m for k in ("sign in to confirm", "not a bot")):
                break
            await asyncio.sleep(1 + i)
    raise DlError(friendly_error(last, platform))


def bar(p: float) -> str:
    n = int(p * 10)
    return "▰" * n + "▱" * (10 - n) + f" {int(p * 100)}%"


async def safe_edit(msg: Message, text: str, **kw):
    try:
        await msg.edit_text(text, **kw)
    except (TelegramBadRequest, TelegramRetryAfter):
        pass


def result_kb(token: str, bot_username: str, show_mp3=True) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    if show_mp3:
        b.row(InlineKeyboardButton(text="🎧 تحويل إلى MP3", callback_data=f"dl:{token}:audio:0"))
    b.row(InlineKeyboardButton(
        text="📤 شارك البوت",
        url=f"https://t.me/share/url?url=https://t.me/{bot_username}&text=أفضل بوت تحميل 🔥"))
    return b.as_markup()


async def process_download(bot: Bot, chat_id: int, user, url: str, platform: str,
                           kind: str, height: int, reply_to: Message | None = None):
    uid = user.id
    token = uuid.uuid4().hex[:10]
    PENDING[token] = {"url": url, "platform": platform, "ts": time.time()}
    me = await bot.get_me()
    status = await bot.send_message(chat_id, "🔎 <b>جاري فحص الرابط...</b>")
    cache_key = f"{url}|{kind}|{height}"
    workdir = Path(tempfile.mkdtemp(prefix="dl_"))
    try:
        # ذاكرة ذكية: نفس الرابط أُرسل سابقاً → إرسال فوري
        c = await q_one("SELECT * FROM cache WHERE key=?", cache_key)
        if c:
            try:
                sender = {"video": bot.send_video, "audio": bot.send_audio, "photo": bot.send_photo}[c["ftype"]]
                await sender(chat_id, c["file_id"], caption=c["caption"],
                             reply_markup=result_kb(token, me.username, kind == "video"))
                await status.delete()
                await q_exec("INSERT INTO downloads(user_id,platform,kind,ts) VALUES(?,?,?,?)",
                             uid, platform, kind, int(time.time()))
                return
            except Exception:
                await q_exec("DELETE FROM cache WHERE key=?", cache_key)

        async with DL_SEM:
            await safe_edit(status, f"⏳ <b>جاري التحميل من {PLATFORM_LABEL[platform]}</b>\n{bar(0)}")

            async def on_prog(p):
                await safe_edit(status, f"⏳ <b>جاري التحميل من {PLATFORM_LABEL[platform]}</b>\n{bar(p)}")

            files, info = await fetch(url, kind, height, platform, workdir, on_prog)

        await safe_edit(status, "📤 <b>جاري الرفع إلى تيليجرام...</b>")
        await bot.send_chat_action(chat_id, ChatAction.UPLOAD_VIDEO)
        title = esc((info.get("title") or "")[:180])
        caption = (f"🎬 <b>{title}</b>\n\n" if title else "") + f"📥 بواسطة @{me.username}"
        await send_files(bot, chat_id, files, info, caption, cache_key,
                         result_kb(token, me.username, kind == "video"))
        await q_exec("INSERT INTO downloads(user_id,platform,kind,ts) VALUES(?,?,?,?)",
                     uid, platform, kind, int(time.time()))
        await status.delete()
    except DlError as e:
        await safe_edit(status, f"❌ {esc(e)}")
    except Exception as e:
        log.exception("download failed")
        await safe_edit(status, "❌ حدث خطأ غير متوقع، حاول مرة أخرى.")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        ACTIVE.discard(uid)


async def send_files(bot: Bot, chat_id, files: list[Path], info, caption, cache_key, kb):
    if len(files) == 1:
        f = files[0]
        ext = f.suffix.lower()
        inp = FSInputFile(f)
        sent = None
        try:
            if ext in IMG_EXT:
                sent = await bot.send_photo(chat_id, inp, caption=caption, reply_markup=kb)
                ft, fid = "photo", sent.photo[-1].file_id
            elif ext in AUD_EXT:
                sent = await bot.send_audio(chat_id, inp, caption=caption, reply_markup=kb,
                                            title=(info.get("title") or "")[:60],
                                            performer=info.get("uploader"),
                                            duration=int(info.get("duration") or 0) or None)
                ft, fid = "audio", sent.audio.file_id
            else:
                sent = await bot.send_video(
                    chat_id, inp, caption=caption, reply_markup=kb, supports_streaming=True,
                    duration=int(info.get("duration") or 0) or None,
                    width=info.get("width"), height=info.get("height"))
                ft, fid = "video", sent.video.file_id
        except TelegramBadRequest:
            sent = await bot.send_document(chat_id, inp, caption=caption, reply_markup=kb)
            return
        await q_exec("INSERT OR REPLACE INTO cache(key,file_id,ftype,caption) VALUES(?,?,?,?)",
                     cache_key, fid, ft, caption)
        return
    # ألبوم (كاروسيل انستغرام ...)
    for i in range(0, len(files), 10):
        group = []
        for j, f in enumerate(files[i:i + 10]):
            cap = caption if (i == 0 and j == 0) else None
            if f.suffix.lower() in IMG_EXT:
                group.append(InputMediaPhoto(media=FSInputFile(f), caption=cap))
            else:
                group.append(InputMediaVideo(media=FSInputFile(f), caption=cap, supports_streaming=True))
        await bot.send_media_group(chat_id, group)
    await bot.send_message(chat_id, "✅ تم التحميل", reply_markup=kb)


# ───────────────────────────── راوتر المستخدمين ─────────────────────────────
user_router = Router()


class ContactSt(StatesGroup):
    wait = State()


async def log_link(bot: Bot, user, url: str):
    if await get_set("log_links", "1") != "1" or not ADMINS:
        return
    try:
        await bot.send_message(ADMINS[0], f"🔗 {user_link(user)} (<code>{user.id}</code>)\n<code>{esc(url)}</code>",
                               disable_web_page_preview=True, disable_notification=True)
    except Exception:
        pass


@user_router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await send_home(m.bot, m.chat.id, m.from_user)


@user_router.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer("📖 <b>طريقة الاستخدام</b>\n\n1️⃣ انسخ رابط الفيديو\n2️⃣ أرسله هنا\n"
                   "3️⃣ استلم الفيديو أو حوّله إلى MP3 🎧")


@user_router.callback_query(F.data == "check_sub")
async def cb_check(c: CallbackQuery):
    missing = await not_joined(c.bot, c.from_user.id)
    if missing:
        await c.answer("لم تشترك بعد ❌", show_alert=True)
        return
    await c.answer("شكراً لاشتراكك ✅")
    try:
        await c.message.delete()
    except Exception:
        pass
    await send_home(c.bot, c.message.chat.id, c.from_user)


@user_router.callback_query(F.data.startswith("m:"))
async def cb_menu(c: CallbackQuery):
    await c.answer()
    await c.message.answer(MENU_TEXT.get(c.data[2:], "أرسل الرابط 👇"))


@user_router.callback_query(F.data == "contact")
async def cb_contact(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(ContactSt.wait)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="❌ إلغاء", callback_data="contact_cancel"))
    if DEV_USERNAME:
        b.row(InlineKeyboardButton(text="💬 حساب المطور", url=f"https://t.me/{DEV_USERNAME}"))
    await c.message.answer("📩 <b>اكتب رسالتك الآن</b> (نص/صورة/صوت) وسأوصلها للمطور.",
                           reply_markup=b.as_markup())


@user_router.callback_query(F.data == "contact_cancel")
async def cb_contact_cancel(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.answer("تم الإلغاء")
    await c.message.delete()


@user_router.message(ContactSt.wait)
async def on_contact(m: Message, state: FSMContext):
    await state.clear()
    for aid in ADMINS:
        try:
            await m.bot.send_message(
                aid, f"📩 <b>رسالة جديدة</b> من {user_link(m.from_user)} "
                     f"(<code>{m.from_user.id}</code>)\n↩️ <i>اعمل Reply على الرسالة التالية للرد</i>")
            mid = await m.bot.copy_message(aid, m.chat.id, m.message_id)
            await q_exec("INSERT OR REPLACE INTO contact_map VALUES(?,?)", mid.message_id, m.from_user.id)
        except Exception:
            pass
    await m.answer("✅ وصلت رسالتك للمطور، سيتم الرد قريباً.")


@user_router.callback_query(F.data.startswith("dl:"))
async def cb_download(c: CallbackQuery):
    _, token, kind, h = c.data.split(":")
    p = PENDING.get(token)
    if not p:
        await c.answer("انتهت صلاحية الطلب، أعد إرسال الرابط", show_alert=True)
        return
    uid = c.from_user.id
    if uid in ACTIVE:
        await c.answer("⏳ لديك عملية قيد التنفيذ", show_alert=True)
        return
    ACTIVE.add(uid)
    await c.answer("🚀 بدأ التحميل")
    try:
        await c.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    asyncio.create_task(process_download(c.bot, c.message.chat.id, c.from_user, p["url"],
                                         p["platform"], kind, int(h) or 1080))


@user_router.message(F.text | F.caption)
async def on_text(m: Message):
    url = extract_url(m.text or m.caption or "")
    if not url:
        await m.answer("🤔 لم أجد رابطاً في رسالتك.\nأرسل رابط الفيديو مباشرة أو اختر المنصة من /start")
        return
    uid = m.from_user.id
    now = time.time()
    if uid in ACTIVE:
        await m.answer("⏳ انتظر انتهاء التحميل الحالي أولاً.")
        return
    if now - LAST_REQ.get(uid, 0) < COOLDOWN and uid not in ADMINS:
        await m.answer("🐢 رويداً، أرسل الطلب التالي بعد ثوانٍ.")
        return
    LAST_REQ[uid] = now
    for k in [k for k, v in PENDING.items() if now - v["ts"] > 3600]:
        PENDING.pop(k, None)
    platform = detect_platform(url)
    await log_link(m.bot, m.from_user, url)

    if platform == "youtube":
        token = uuid.uuid4().hex[:10]
        PENDING[token] = {"url": url, "platform": platform, "ts": now}
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="🎬 1080p", callback_data=f"dl:{token}:video:1080"),
              InlineKeyboardButton(text="🎬 720p", callback_data=f"dl:{token}:video:720"))
        b.row(InlineKeyboardButton(text="🎬 480p", callback_data=f"dl:{token}:video:480"),
              InlineKeyboardButton(text="🎬 360p", callback_data=f"dl:{token}:video:360"))
        b.row(InlineKeyboardButton(text="🎧 صوت MP3", callback_data=f"dl:{token}:audio:0"))
        await m.answer(f"▶️ <b>تم التعرف على الرابط</b>\n{LINE}\n👇 اختر الصيغة / الجودة:",
                       reply_markup=b.as_markup())
        return
    ACTIVE.add(uid)
    asyncio.create_task(process_download(m.bot, m.chat.id, m.from_user, url, platform, "video", 1080))


# ───────────────────────────── لوحة المطور ─────────────────────────────
admin_router = Router()


class IsAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        u = getattr(event, "from_user", None)
        return bool(u and u.id in ADMINS)


class IsContactReply(BaseFilter):
    async def __call__(self, m: Message):
        if not m.reply_to_message:
            return False
        r = await q_one("SELECT user_id FROM contact_map WHERE admin_msg_id=?", m.reply_to_message.message_id)
        return {"uid": r["user_id"]} if r else False


admin_router.message.filter(IsAdmin())
admin_router.callback_query.filter(IsAdmin())


class AdmSt(StatesGroup):
    add_channel = State()
    set_photo = State()
    broadcast = State()
    ban = State()
    unban = State()


def adm_kb(log_on: bool) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📊 الإحصائيات", callback_data="adm:stats"),
          InlineKeyboardButton(text="📣 إذاعة", callback_data="adm:bc"))
    b.row(InlineKeyboardButton(text="📢 الاشتراك الإجباري", callback_data="adm:subs"))
    b.row(InlineKeyboardButton(text="🖼 صورة الترحيب", callback_data="adm:photo"))
    b.row(InlineKeyboardButton(text="🚫 حظر", callback_data="adm:ban"),
          InlineKeyboardButton(text="♻️ فك حظر", callback_data="adm:unban"))
    b.row(InlineKeyboardButton(text=f"🔗 سجل الروابط: {'✅ مفعّل' if log_on else '⛔ متوقف'}",
                               callback_data="adm:log"))
    b.row(InlineKeyboardButton(text="🏠 القائمة الرئيسية", callback_data="adm:close"))
    return b.as_markup()


def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 رجوع", callback_data="adm:home")]])


async def adm_home_text() -> str:
    return f"🛠 <b>لوحة المطور</b>\n{LINE}\nاختر ما تريد إدارته:"


@admin_router.message(Command("admin"))
async def cmd_admin(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(await adm_home_text(), reply_markup=adm_kb(await get_set("log_links", "1") == "1"))


@admin_router.callback_query(F.data == "adm:home")
async def adm_home(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.answer()
    await safe_edit(c.message, await adm_home_text(),
                    reply_markup=adm_kb(await get_set("log_links", "1") == "1"))


@admin_router.callback_query(F.data == "adm:close")
async def adm_close(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.answer()
    await c.message.delete()
    await send_home(c.bot, c.message.chat.id, c.from_user)


@admin_router.callback_query(F.data == "adm:stats")
async def adm_stats(c: CallbackQuery):
    await c.answer()
    now = int(time.time())
    day = now - 86400
    total = (await q_one("SELECT COUNT(*) n FROM users"))["n"]
    new24 = (await q_one("SELECT COUNT(*) n FROM users WHERE joined>?", day))["n"]
    act24 = (await q_one("SELECT COUNT(*) n FROM users WHERE last_seen>?", day))["n"]
    banned = (await q_one("SELECT COUNT(*) n FROM users WHERE banned=1"))["n"]
    dead = (await q_one("SELECT COUNT(*) n FROM users WHERE dead=1"))["n"]
    dl = (await q_one("SELECT COUNT(*) n FROM downloads"))["n"]
    dl24 = (await q_one("SELECT COUNT(*) n FROM downloads WHERE ts>?", day))["n"]
    chs = (await q_one("SELECT COUNT(*) n FROM channels"))["n"]
    per = await q_all("SELECT platform, COUNT(*) n FROM downloads GROUP BY platform ORDER BY n DESC")
    per_txt = "\n".join(f"   {PLATFORM_LABEL.get(r['platform'], r['platform'])}: <b>{r['n']}</b>" for r in per) or "   —"
    text = (f"📊 <b>الإحصائيات</b>\n{LINE}\n"
            f"👥 المستخدمون: <b>{total}</b>\n🆕 جدد (24س): <b>{new24}</b>\n"
            f"🔥 نشطون (24س): <b>{act24}</b>\n🚫 محظورون: <b>{banned}</b>\n"
            f"💤 حظروا البوت: <b>{dead}</b>\n{LINE}\n"
            f"📥 إجمالي التحميلات: <b>{dl}</b>\n⚡ تحميلات (24س): <b>{dl24}</b>\n"
            f"📌 حسب المنصة:\n{per_txt}\n{LINE}\n📢 قنوات الاشتراك: <b>{chs}</b>")
    await safe_edit(c.message, text, reply_markup=back_kb())


@admin_router.callback_query(F.data == "adm:log")
async def adm_log(c: CallbackQuery):
    cur = await get_set("log_links", "1")
    await put_set("log_links", "0" if cur == "1" else "1")
    await c.answer("تم التبديل")
    await safe_edit(c.message, await adm_home_text(),
                    reply_markup=adm_kb(await get_set("log_links", "1") == "1"))


# ── الاشتراك الإجباري
@admin_router.callback_query(F.data == "adm:subs")
async def adm_subs(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.answer()
    chs = await q_all("SELECT * FROM channels")
    b = InlineKeyboardBuilder()
    for ch in chs:
        b.row(InlineKeyboardButton(text=f"🗑 {ch['title']}", callback_data=f"adm:delch:{ch['chat_id']}"))
    b.row(InlineKeyboardButton(text="➕ إضافة قناة/مجموعة", callback_data="adm:addch"))
    b.row(InlineKeyboardButton(text="🔙 رجوع", callback_data="adm:home"))
    await safe_edit(c.message, f"📢 <b>الاشتراك الإجباري</b>\n{LINE}\nالقنوات الحالية: <b>{len(chs)}</b>\n"
                               "اضغط على قناة لحذفها.\n<i>يجب أن يكون البوت مشرفاً فيها.</i>",
                    reply_markup=b.as_markup())


@admin_router.callback_query(F.data.startswith("adm:delch:"))
async def adm_delch(c: CallbackQuery, state: FSMContext):
    await q_exec("DELETE FROM channels WHERE chat_id=?", int(c.data.split(":")[2]))
    await c.answer("تم الحذف 🗑")
    await adm_subs(c, state)


@admin_router.callback_query(F.data == "adm:addch")
async def adm_addch(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(AdmSt.add_channel)
    await safe_edit(c.message, "➕ أرسل <b>يوزر القناة</b> (@channel) أو <b>آيديها</b> أو حوّل لي رسالة منها.\n"
                               "⚠️ تأكد أن البوت مشرف فيها.", reply_markup=back_kb())


@admin_router.message(AdmSt.add_channel)
async def on_add_channel(m: Message, state: FSMContext):
    ref = None
    fo = getattr(m, "forward_origin", None)
    if fo is not None and getattr(fo, "chat", None):
        ref = fo.chat.id
    else:
        t = (m.text or "").strip()
        t = re.sub(r"^https?://t\.me/", "@", t)
        if t.lstrip("-").isdigit():
            ref = int(t)
        elif t:
            ref = t if t.startswith("@") else "@" + t
    if ref is None:
        await m.answer("❌ مدخل غير صالح، حاول مجدداً.")
        return
    try:
        chat = await m.bot.get_chat(ref)
        me = await m.bot.get_chat_member(chat.id, (await m.bot.get_me()).id)
        if me.status not in ("administrator", "creator"):
            await m.answer("❌ البوت ليس مشرفاً في هذه القناة. ارفعه مشرفاً ثم أعد المحاولة.")
            return
        link = f"https://t.me/{chat.username}" if chat.username else await m.bot.export_chat_invite_link(chat.id)
    except Exception as e:
        await m.answer(f"❌ تعذّر الوصول للقناة: <code>{esc(e)}</code>")
        return
    await q_exec("INSERT OR REPLACE INTO channels VALUES(?,?,?)", chat.id, chat.title or str(chat.id), link)
    await state.clear()
    await m.answer(f"✅ تمت إضافة <b>{esc(chat.title)}</b> للاشتراك الإجباري.", reply_markup=back_kb())


# ── صورة الترحيب
@admin_router.callback_query(F.data == "adm:photo")
async def adm_photo(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(AdmSt.set_photo)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🗑 حذف الصورة الحالية", callback_data="adm:delphoto"))
    b.row(InlineKeyboardButton(text="🔙 رجوع", callback_data="adm:home"))
    cur = "✅ موجودة" if await get_set("welcome_photo") else "⛔ لا يوجد"
    await safe_edit(c.message, f"🖼 <b>صورة الترحيب</b> ({cur})\nأرسل الصورة الجديدة الآن:",
                    reply_markup=b.as_markup())


@admin_router.callback_query(F.data == "adm:delphoto")
async def adm_delphoto(c: CallbackQuery, state: FSMContext):
    await del_set("welcome_photo")
    await state.clear()
    await c.answer("تم الحذف 🗑")
    await adm_home(c, state)


@admin_router.message(AdmSt.set_photo, F.photo)
async def on_photo(m: Message, state: FSMContext):
    await put_set("welcome_photo", m.photo[-1].file_id)
    await state.clear()
    await m.answer("✅ تم حفظ صورة الترحيب.", reply_markup=back_kb())


@admin_router.message(AdmSt.set_photo)
async def on_photo_bad(m: Message):
    await m.answer("❌ أرسل صورة (Photo) فقط.")


# ── حظر / فك حظر
@admin_router.callback_query(F.data.in_({"adm:ban", "adm:unban"}))
async def adm_ban(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(AdmSt.ban if c.data == "adm:ban" else AdmSt.unban)
    await safe_edit(c.message, "أرسل <b>آيدي</b> المستخدم:", reply_markup=back_kb())


@admin_router.message(AdmSt.ban)
@admin_router.message(AdmSt.unban)
async def on_ban(m: Message, state: FSMContext):
    st = await state.get_state()
    t = (m.text or "").strip()
    if not t.isdigit():
        await m.answer("❌ آيدي غير صالح.")
        return
    await q_exec("UPDATE users SET banned=? WHERE id=?", 1 if st == AdmSt.ban.state else 0, int(t))
    await state.clear()
    await m.answer("✅ تم." , reply_markup=back_kb())


# ── الإذاعة
@admin_router.callback_query(F.data == "adm:bc")
async def adm_bc(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(AdmSt.broadcast)
    await safe_edit(c.message, "📣 <b>الإذاعة</b>\nأرسل الآن ما تريد إذاعته: نص • صورة • فيديو • رابط • ملصق • "
                               "رسالة صوتية • ملف...\n<i>سيُرسل كما هو لكل المستخدمين.</i>",
                    reply_markup=back_kb())


@admin_router.message(AdmSt.broadcast)
async def on_bc(m: Message, state: FSMContext):
    n = (await q_one("SELECT COUNT(*) n FROM users WHERE banned=0 AND dead=0"))["n"]
    await state.update_data(mid=m.message_id, cid=m.chat.id)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text=f"✅ إرسال إلى {n} مستخدم", callback_data="adm:bcgo"),
          InlineKeyboardButton(text="❌ إلغاء", callback_data="adm:home"))
    await m.reply("هل تريد إرسال هذه الرسالة؟", reply_markup=b.as_markup())


@admin_router.callback_query(F.data == "adm:bcgo")
async def on_bc_go(c: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.clear()
    if not d:
        await c.answer("انتهت الجلسة", show_alert=True)
        return
    await c.answer("🚀 بدأت الإذاعة")
    await safe_edit(c.message, "📣 جاري الإرسال...")
    asyncio.create_task(run_broadcast(c.bot, c.message, d["cid"], d["mid"]))


async def run_broadcast(bot: Bot, status: Message, cid: int, mid: int):
    ids = [r["id"] for r in await q_all("SELECT id FROM users WHERE banned=0 AND dead=0")]
    ok = fail = 0
    for i, uid in enumerate(ids, 1):
        try:
            await bot.copy_message(uid, cid, mid)
            ok += 1
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            try:
                await bot.copy_message(uid, cid, mid)
                ok += 1
            except Exception:
                fail += 1
        except TelegramForbiddenError:
            fail += 1
            await q_exec("UPDATE users SET dead=1 WHERE id=?", uid)
        except Exception:
            fail += 1
        if i % 50 == 0:
            await safe_edit(status, f"📣 جاري الإرسال... {i}/{len(ids)}")
        await asyncio.sleep(0.04)
    await safe_edit(status, f"✅ <b>انتهت الإذاعة</b>\n{LINE}\n📬 نجح: <b>{ok}</b>\n❌ فشل: <b>{fail}</b>",
                    reply_markup=back_kb())


# ── رد المطور على رسائل المستخدمين
@admin_router.message(IsContactReply())
async def admin_reply(m: Message, uid: int):
    try:
        await m.bot.send_message(uid, "📬 <b>رد من المطور:</b>")
        await m.bot.copy_message(uid, m.chat.id, m.message_id)
        await m.reply("✅ تم إرسال الرد.")
    except Exception as e:
        await m.reply(f"❌ تعذّر الإرسال: {esc(e)}")


# ───────────────────────────── التشغيل ─────────────────────────────
async def main():
    if not TOKEN:
        raise SystemExit("ضع BOT_TOKEN في ملف .env")
    await init_db()
    session = AiohttpSession(
        api=TelegramAPIServer.from_base(API_BASE, is_local=True) if API_BASE else TelegramAPIServer.from_base("https://api.telegram.org"),
        timeout=900)
    bot = Bot(TOKEN, session=session, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.message.outer_middleware(Gate())
    dp.callback_query.outer_middleware(Gate())
    dp.include_router(admin_router)   # أولاً ليأخذ حالات FSM الخاصة بالمطور
    dp.include_router(user_router)
    await bot.set_my_commands([BotCommand(command="start", description="القائمة الرئيسية"),
                               BotCommand(command="help", description="طريقة الاستخدام")])
    me = await bot.get_me()
    log.info("Bot @%s started | yt-dlp %s | proxies=%d | pot=%s",
             me.username, yt_dlp.version.__version__, len(PROXIES), bool(POT_URL))
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())