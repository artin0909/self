#!/usr/bin/env python3
"""
Telegram Banner Userbot (Telethon) - Railway Ready
Single-file version

قابلیت‌ها:
- ذخیره بنر با ریپلای روی پیام
- زمان‌بندی ارسال بنر بعد از N دقیقه
- ارسال تکراری
- مدیریت بنرها و زمان‌بندی‌ها
- ذخیره‌سازی پایدار با SQLite (روی Volume)
- هندل FloodWait و خطاها
- پشتیبانی Railway (Volume + Env Vars + Proxy اختیاری)
"""

import os
import re
import json
import time
import asyncio
import urllib.request
import urllib.parse
import urllib.error
import aiosqlite
from datetime import datetime
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError,
    ChatWriteForbiddenError,
    ChannelPrivateError,
    UserBannedInChannelError,
)
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

# ==================== بارگذاری تنظیمات ====================
load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PHONE = os.getenv("PHONE", "")
SESSION_NAME = os.getenv("SESSION_NAME", "banner_session")

# Railway Volume path (مهم!)
DATA_DIR = os.getenv("DATA_DIR", "/data")
if not os.path.exists(DATA_DIR):
    # لوکال یا وقتی Volume هنوز مانت نشده
    DATA_DIR = os.path.dirname(os.path.abspath(__file__))

MEDIA_DIR = os.path.join(DATA_DIR, "media")
SESSIONS_DIR = os.path.join(DATA_DIR, "sessions")
DB_PATH = os.path.join(DATA_DIR, "banners.db")

os.makedirs(MEDIA_DIR, exist_ok=True)
os.makedirs(SESSIONS_DIR, exist_ok=True)

# پروکسی اختیاری (برای Railway معمولاً None)
# مثال: PROXY_HOST=127.0.0.1  PROXY_PORT=12334  PROXY_TYPE=socks5
PROXY = None
proxy_host = os.getenv("PROXY_HOST")
if proxy_host:
    proxy_type = os.getenv("PROXY_TYPE", "socks5")
    proxy_port = int(os.getenv("PROXY_PORT", "1080"))
    PROXY = (proxy_type, proxy_host, proxy_port)

# پروکسی برای check-host (اختیاری)
HTTP_PROXY_URL = os.getenv("HTTP_PROXY_URL")  # مثلاً http://127.0.0.1:12334 یا None

# ==================== دیتابیس ====================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS banners (
                name TEXT PRIMARY KEY,
                chat_id INTEGER,
                message_id INTEGER,
                media_path TEXT,
                caption TEXT,
                media_type TEXT,
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                banner_name TEXT,
                chat_id INTEGER,
                send_at REAL,
                repeat_interval INTEGER DEFAULT 0,
                remaining_repeats INTEGER DEFAULT 1,
                status TEXT DEFAULT 'pending',
                created_at TEXT
            )
        """)
        await db.commit()


async def add_banner(name, chat_id, message_id, media_path, caption, media_type):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT OR REPLACE INTO banners
               (name, chat_id, message_id, media_path, caption, media_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (name, chat_id, message_id, media_path, caption, media_type, datetime.utcnow().isoformat())
        )
        await db.commit()


async def get_banner(name):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM banners WHERE name = ?", (name,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return {
                    "name": row[0],
                    "chat_id": row[1],
                    "message_id": row[2],
                    "media_path": row[3],
                    "caption": row[4],
                    "media_type": row[5],
                }
            return None


async def list_banners():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT name, media_type, created_at FROM banners ORDER BY created_at DESC"
        ) as cursor:
            return await cursor.fetchall()


async def delete_banner(name):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM banners WHERE name = ?", (name,))
        await db.commit()


async def add_schedule(banner_name, chat_id, send_at, repeat_interval=0, remaining_repeats=1):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO schedules
               (banner_name, chat_id, send_at, repeat_interval, remaining_repeats, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'pending', ?)""",
            (banner_name, chat_id, send_at, repeat_interval, remaining_repeats, datetime.utcnow().isoformat())
        )
        await db.commit()
        return cursor.lastrowid


async def get_pending_schedules():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT * FROM schedules WHERE status = 'pending' ORDER BY send_at"
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "id": r[0],
                    "banner_name": r[1],
                    "chat_id": r[2],
                    "send_at": r[3],
                    "repeat_interval": r[4],
                    "remaining_repeats": r[5],
                    "status": r[6],
                }
                for r in rows
            ]


async def update_schedule_status(schedule_id, status):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE schedules SET status = ? WHERE id = ?", (status, schedule_id)
        )
        await db.commit()


async def update_schedule_after_send(schedule_id, new_send_at, remaining):
    async with aiosqlite.connect(DB_PATH) as db:
        if remaining <= 0:
            await db.execute(
                "UPDATE schedules SET status = 'done' WHERE id = ?", (schedule_id,)
            )
        else:
            await db.execute(
                "UPDATE schedules SET send_at = ?, remaining_repeats = ? WHERE id = ?",
                (new_send_at, remaining, schedule_id)
            )
        await db.commit()


async def cancel_schedule(schedule_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE schedules SET status = 'cancelled' WHERE id = ?", (schedule_id,)
        )
        await db.commit()


async def list_active_schedules():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT id, banner_name, chat_id, send_at, remaining_repeats
               FROM schedules WHERE status = 'pending' ORDER BY send_at"""
        ) as cursor:
            return await cursor.fetchall()


# ==================== ابزارها ====================
async def safe_send(client, chat_id, file=None, caption=None, message=None):
    """ارسال امن با هندل FloodWait"""
    try:
        if file:
            return await client.send_file(chat_id, file, caption=caption or "")
        else:
            return await client.send_message(chat_id, message or caption or "")
    except FloodWaitError as e:
        await asyncio.sleep(e.seconds + 1)
        return await safe_send(client, chat_id, file=file, caption=caption, message=message)
    except (ChatWriteForbiddenError, ChannelPrivateError, UserBannedInChannelError):
        raise
    except Exception:
        raise


async def notify_owner(client, text: str):
    """ارسال پیام وضعیت/خطا به Saved Messages"""
    try:
        await client.send_message("me", f"⚠️ BannerBot\n\n{text}")
    except Exception:
        pass


# ==================== زمان‌بند ====================
class BannerScheduler:
    def __init__(self, client):
        self.client = client
        self.running = False
        self.tasks = {}

    async def start(self):
        self.running = True
        await self.reload_pending()

    async def reload_pending(self):
        """لود زمان‌بندی‌های باقی‌مانده بعد از ری‌استارت"""
        schedules = await get_pending_schedules()
        now = time.time()
        for sch in schedules:
            delay = max(0, sch["send_at"] - now)
            task = asyncio.create_task(self._run_schedule(sch, delay))
            self.tasks[sch["id"]] = task

    async def schedule_once(self, schedule_id, banner_name, chat_id, delay_seconds, repeat_interval=0, remaining_repeats=1):
        sch = {
            "id": schedule_id,
            "banner_name": banner_name,
            "chat_id": chat_id,
            "send_at": time.time() + delay_seconds,
            "repeat_interval": repeat_interval,
            "remaining_repeats": remaining_repeats,
        }
        task = asyncio.create_task(self._run_schedule(sch, delay_seconds))
        self.tasks[schedule_id] = task
        return schedule_id

    async def _run_schedule(self, sch, delay):
        await asyncio.sleep(delay)
        await self._execute(
            sch["id"],
            sch["banner_name"],
            sch["chat_id"],
            sch["repeat_interval"],
            sch["remaining_repeats"],
        )

    async def _execute(self, schedule_id, banner_name, chat_id, interval=0, remaining=1):
        banner = await get_banner(banner_name)
        if not banner:
            await notify_owner(
                self.client,
                f"بنر `{banner_name}` پیدا نشد. زمان‌بندی {schedule_id} لغو شد."
            )
            await update_schedule_status(schedule_id, "failed")
            return

        try:
            if banner["media_path"] and os.path.exists(banner["media_path"]):
                await safe_send(
                    self.client, chat_id,
                    file=banner["media_path"],
                    caption=banner["caption"]
                )
            else:
                await safe_send(
                    self.client, chat_id,
                    message=banner["caption"]
                )

            remaining -= 1
            if remaining > 0 and interval > 0:
                new_send_at = time.time() + (interval * 60)
                await update_schedule_after_send(schedule_id, new_send_at, remaining)
                task = asyncio.create_task(
                    self._run_schedule(
                        {
                            "id": schedule_id,
                            "banner_name": banner_name,
                            "chat_id": chat_id,
                            "send_at": new_send_at,
                            "repeat_interval": interval,
                            "remaining_repeats": remaining,
                        },
                        interval * 60,
                    )
                )
                self.tasks[schedule_id] = task
            else:
                await update_schedule_status(schedule_id, "done")

        except Exception as e:
            await notify_owner(
                self.client,
                f"خطا در ارسال بنر `{banner_name}` به چت {chat_id}:\n`{str(e)}`"
            )
            await update_schedule_status(schedule_id, "failed")

    def cancel(self, schedule_id):
        task = self.tasks.pop(schedule_id, None)
        if task and not task.done():
            task.cancel()


# ==================== ابزار پینگ ====================
CHECK_HOST = "https://check-host.net"
GLOBALPING = "https://api.globalping.io/v1/measurements"


def _http_json(url: str, payload=None):
    """درخواست GET/POST و برگردوندن JSON. اول با پروکسی (اگر باشه)، بعد مستقیم."""
    last_err = None
    proxies = [HTTP_PROXY_URL, None] if HTTP_PROXY_URL else [None]
    for proxy in proxies:
        try:
            handlers = []
            if proxy:
                handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
            opener = urllib.request.build_opener(*handlers)
            headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
            data = None
            if payload is not None:
                data = json.dumps(payload).encode("utf-8")
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(url, data=data, headers=headers)
            with opener.open(req, timeout=20) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "ignore")[:300]
            raise RuntimeError(f"HTTP {e.code}: {body}")
        except Exception as e:
            last_err = e
    raise last_err


def _flag(code: str) -> str:
    code = (code or "").upper()
    if len(code) != 2 or not code.isalpha():
        return "🌐"
    return chr(0x1F1E6 + ord(code[0]) - 65) + chr(0x1F1E6 + ord(code[1]) - 65)


def format_ping_result(host: str, nodes: dict, results: dict, link: str = "") -> str:
    lines = [f"🏓 **Ping** `{host}`\n"]
    for node, info in nodes.items():
        country_code = info[0] if len(info) > 0 else ""
        country = info[1] if len(info) > 1 else ""
        city = info[2] if len(info) > 2 else ""
        label = f"{_flag(country_code)} {country}" + (f" - {city}" if city else "")

        res = results.get(node)
        if res is None:
            lines.append(f"{label}\n⏳ جوابی نیومد\n")
            continue
        pings = res[0] if res and res[0] else []
        if not pings:
            lines.append(f"{label}\n❌ بدون نتیجه\n")
            continue

        parts, times, ip = [], [], ""
        for p in pings:
            status = p[0] if p else "?"
            if status == "OK":
                ms = round(float(p[1]) * 1000)
                times.append(ms)
                parts.append(f"{ms}ms")
                if len(p) > 2 and p[2]:
                    ip = p[2]
            else:
                parts.append("✗")
        summary = ""
        if times:
            summary = f" | میانگین {sum(times) // len(times)}ms"
        lines.append(
            f"{label}" + (f" (`{ip}`)" if ip else "") + f"\n{'  '.join(parts)}{summary}\n"
        )
    if link:
        lines.append(f"🔗 {link}")
    return "\n".join(lines)


def format_globalping(host: str, res: dict, title: str = None) -> str:
    lines = [title or f"🏓 **Ping** `{host}` (globalping.io)\n"]
    for r in res.get("results", []):
        pr = r.get("probe", {}) or {}
        rs = r.get("result", {}) or {}
        country = pr.get("country", "")
        city = pr.get("city", "")
        label = f"{_flag(country)} {country}" + (f" - {city}" if city else "")

        if rs.get("status") != "finished":
            lines.append(f"{label}\n❌ {rs.get('status', 'نتیجه‌ای نیست')}\n")
            continue

        timings = rs.get("timings") or []
        parts = []
        for t in timings:
            rtt = t.get("rtt")
            parts.append(f"{round(rtt)}ms" if rtt is not None else "✗")
        stats = rs.get("stats") or {}
        extra = ""
        if stats.get("avg") is not None:
            extra += f" | میانگین {round(stats['avg'])}ms"
        if stats.get("loss") is not None:
            extra += f" | افت {stats['loss']}%"
        ip = rs.get("resolvedAddress") or ""
        lines.append(
            f"{label}" + (f" (`{ip}`)" if ip else "")
            + f"\n{'  '.join(parts) if parts else '✗'}{extra}\n"
        )
    lines.append(f"🔗 https://globalping.io/?measurement={res.get('id', '')}")
    return "\n".join(lines)


async def globalping_ping(host: str, limit: int, locations=None, title: str = None) -> str:
    payload = {"type": "ping", "target": host, "limit": limit,
               "measurementOptions": {"packets": 4}}
    if locations:
        payload["locations"] = locations
    start = await asyncio.to_thread(_http_json, GLOBALPING, payload)
    mid = start["id"]
    res = {}
    for _ in range(20):
        await asyncio.sleep(2)
        res = await asyncio.to_thread(_http_json, f"{GLOBALPING}/{mid}")
        if res.get("status") != "in-progress":
            break
    return format_globalping(host, res, title)


def parse_ping_output(text: str, count: int):
    parts, ip = [], ""
    for line in text.splitlines():
        m = re.search(r"time[=<]\s*([\d.]+)\s*ms", line, re.IGNORECASE)
        if m:
            parts.append(f"{max(1, round(float(m.group(1))))}ms")
        elif re.search(r"timed out|timeout|unreachable|no answer", line, re.IGNORECASE):
            parts.append("✗")
    m = re.search(r"\[([0-9a-fA-F:.]+)\]", text) or re.search(r"\(([0-9a-fA-F:.]+)\)", text)
    if m:
        ip = m.group(1)
    while len(parts) < count:
        parts.append("✗")
    return parts[:count], ip


async def local_ping(host: str, count: int = 4) -> str:
    is_win = os.name == "nt"
    cmd = ["ping", "-n" if is_win else "-c", str(count), host]
    kwargs = {"creationflags": 0x08000000} if is_win else {}
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, **kwargs
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=45)
    except asyncio.TimeoutError:
        proc.kill()
        return "❌ زمان ping تموم شد."
    text = out.decode("utf-8", "ignore")
    parts, ip = parse_ping_output(text, count)
    ok = [int(x[:-2]) for x in parts if x != "✗"]
    if not ok:
        first = next((l.strip() for l in text.splitlines() if l.strip()), "")
        return f"❌ هیچ جوابی نیومد.\n`{first[:200]}`"
    avg = sum(ok) // len(ok)
    loss = round((count - len(ok)) * 100 / count)
    return (f"{'  '.join(parts)} | میانگین {avg}ms | افت {loss}%"
            + (f"\nIP: `{ip}`" if ip else ""))


def split_text(text: str, limit: int = 4000):
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > limit:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    if cur.strip():
        chunks.append(cur)
    return chunks


def register_handlers(client, scheduler):

    @client.on(events.NewMessage(pattern=r"^\.addbanner\s+(\w+)$", outgoing=True))
    async def add_banner_handler(event):
        if not event.is_reply:
            await event.edit("❌ باید روی یک پیام ریپلای کنی.")
            return

        name = event.pattern_match.group(1).lower()
        reply = await event.get_reply_message()

        media_path = None
        media_type = "text"
        caption = reply.text or reply.message or ""

        if reply.media:
            if isinstance(reply.media, MessageMediaPhoto):
                media_type = "photo"
            elif isinstance(reply.media, MessageMediaDocument):
                media_type = "document"
            else:
                media_type = "media"

            filename = f"{name}_{int(time.time())}"
            path = await reply.download_media(file=os.path.join(MEDIA_DIR, filename))
            media_path = path

        await add_banner(
            name=name,
            chat_id=event.chat_id,
            message_id=reply.id,
            media_path=media_path,
            caption=caption,
            media_type=media_type,
        )
        await event.edit(f"✅ بنر `{name}` ذخیره شد.")

    @client.on(events.NewMessage(pattern=r"^\.sendbanner\s+(\w+)\s+(\d+)$", outgoing=True))
    async def send_banner_handler(event):
        name = event.pattern_match.group(1).lower()
        minutes = int(event.pattern_match.group(2))

        banner = await get_banner(name)
        if not banner:
            await event.edit(f"❌ بنر `{name}` وجود ندارد.")
            return

        send_at = time.time() + (minutes * 60)
        schedule_id = await add_schedule(name, event.chat_id, send_at, remaining_repeats=1)
        await scheduler.schedule_once(schedule_id, name, event.chat_id, minutes * 60)

        await event.edit(
            f"⏱ بنر `{name}` بعد از {minutes} دقیقه در این گپ ارسال می‌شود.\nID: `{schedule_id}`"
        )

    @client.on(events.NewMessage(pattern=r"^\.repeatbanner\s+(\w+)\s+(\d+)\s+(\d+)$", outgoing=True))
    async def repeat_banner_handler(event):
        name = event.pattern_match.group(1).lower()
        interval = int(event.pattern_match.group(2))
        times = int(event.pattern_match.group(3))

        banner = await get_banner(name)
        if not banner:
            await event.edit(f"❌ بنر `{name}` وجود ندارد.")
            return

        send_at = time.time() + (interval * 60)
        schedule_id = await add_schedule(
            name, event.chat_id, send_at,
            repeat_interval=interval, remaining_repeats=times
        )
        await scheduler.schedule_once(
            schedule_id, name, event.chat_id, interval * 60,
            repeat_interval=interval, remaining_repeats=times
        )

        await event.edit(
            f"🔁 بنر `{name}` هر {interval} دقیقه، {times} بار ارسال می‌شود.\nID: `{schedule_id}`"
        )

    @client.on(events.NewMessage(pattern=r"^\.banners$", outgoing=True))
    async def list_banners_handler(event):
        rows = await list_banners()
        if not rows:
            await event.edit("لیست بنرها خالیه.")
            return
        text = "📋 **بنرهای ذخیره‌شده:**\n\n"
        for name, mtype, created in rows:
            text += f"• `{name}` ({mtype}) — {created[:16]}\n"
        await event.edit(text)

    @client.on(events.NewMessage(pattern=r"^\.schedules$", outgoing=True))
    async def list_schedules_handler(event):
        rows = await list_active_schedules()
        if not rows:
            await event.edit("هیچ زمان‌بندی فعالی نیست.")
            return
        now = time.time()
        text = "🗓 **زمان‌بندی‌های فعال:**\n\n"
        for sid, name, chat_id, send_at, remaining in rows:
            left = max(0, int(send_at - now))
            mins = left // 60
            secs = left % 60
            text += (
                f"ID `{sid}` | `{name}` | چت `{chat_id}` | "
                f"باقی‌مانده: {mins}m {secs}s | تکرار: {remaining}\n"
            )
        await event.edit(text)

    @client.on(events.NewMessage(pattern=r"^\.cancel\s+(\d+)$", outgoing=True))
    async def cancel_handler(event):
        sid = int(event.pattern_match.group(1))
        await cancel_schedule(sid)
        scheduler.cancel(sid)
        await event.edit(f"✅ زمان‌بندی `{sid}` لغو شد.")

    @client.on(events.NewMessage(pattern=r"^\.delbanner\s+(\w+)$", outgoing=True))
    async def del_banner_handler(event):
        name = event.pattern_match.group(1).lower()
        banner = await get_banner(name)
        if not banner:
            await event.edit(f"❌ بنر `{name}` پیدا نشد.")
            return
        if banner["media_path"] and os.path.exists(banner["media_path"]):
            try:
                os.remove(banner["media_path"])
            except Exception:
                pass
        await delete_banner(name)
        await event.edit(f"🗑 بنر `{name}` حذف شد.")

    @client.on(events.NewMessage(pattern=r"(?i)^\s*\.?\s*(لیست\s*دستورات|help|commands)\s*$", outgoing=True))
    async def help_handler(event):
        text = (
            "📖 **لیست دستورات**\n\n"
            "**ذخیره و ارسال بنر**\n"
            "• `.addbanner <اسم>` ← روی پیام ریپلای کن تا به‌عنوان بنر ذخیره بشه\n"
            "• `.sendbanner <اسم> <دقیقه>` ← ارسال بنر بعد از N دقیقه در همین گپ\n"
            "• `.repeatbanner <اسم> <فاصله> <تعداد>` ← ارسال تکراری (فاصله به دقیقه)\n\n"
            "**مدیریت**\n"
            "• `.banners` ← لیست بنرهای ذخیره‌شده\n"
            "• `.schedules` ← لیست ارسال‌های فعال\n"
            "• `.cancel <id>` ← لغو یک ارسال زمان‌بندی‌شده\n"
            "• `.delbanner <اسم>` ← حذف بنر\n"
            "• `لیست دستورات` ← نمایش همین پیام\n"
            "• `del all` ← حذف همه پیام‌های خودت در همین گپ\n"
            "• `ping <آدرس> [تعداد نود]` ← پینگ از چند کشور\n"
            "• `pingir <آدرس>` ← پینگ از ایران (اینترنت خودت)\n\n"
            "**مثال‌ها**\n"
            "`.addbanner promo1`\n"
            "`.sendbanner promo1 15`\n"
            "`.repeatbanner promo1 5 12`\n"
            "`ping google.com`"
        )
        await event.edit(text)

    @client.on(events.NewMessage(pattern=r"(?i)^\s*\.?\s*(del\s*all|حذف\s*همه)\s*$", outgoing=True))
    async def del_all_handler(event):
        chat_id = event.chat_id
        ids = [event.id]
        try:
            async for msg in client.iter_messages(chat_id, from_user="me"):
                ids.append(msg.id)
        except Exception as e:
            await event.edit(f"❌ خطا در خوندن پیام‌ها: {e}")
            return

        deleted = 0
        try:
            for i in range(0, len(ids), 100):
                batch = ids[i:i + 100]
                await client.delete_messages(chat_id, batch)
                deleted += len(batch)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            await client.send_message("me", f"⚠️ خطا هنگام del all در چت `{chat_id}`: {e}")
            return

        note = await client.send_message(chat_id, f"🗑 {deleted} پیام حذف شد.")
        await asyncio.sleep(3)
        try:
            await note.delete()
        except Exception:
            pass

    @client.on(events.NewMessage(pattern=r"(?i)^\s*\.?\s*ping\s+(\S+)(?:\s+(\d{1,2}))?\s*$", outgoing=True))
    async def ping_handler(event):
        host = event.pattern_match.group(1).strip()
        max_nodes = int(event.pattern_match.group(2) or 20)
        max_nodes = max(1, min(max_nodes, 50))

        host = re.sub(r"^[a-zA-Z]+://", "", host).split("/")[0]
        if not re.fullmatch(r"[A-Za-z0-9.\-:]+", host):
            await event.edit("❌ آدرس نامعتبره.")
            return

        await event.edit(f"⏳ در حال گرفتن پینگ `{host}` از check-host.net ...")
        text = None
        reason = ""
        try:
            q = urllib.parse.quote(host)
            start = await asyncio.to_thread(
                _http_json, f"{CHECK_HOST}/check-ping?host={q}&max_nodes={max_nodes}"
            )
            if not start.get("ok"):
                reason = f"جواب خطا از check-host: {start}"
            else:
                req_id = start["request_id"]
                nodes = start.get("nodes", {})
                link = start.get("permanent_link", f"{CHECK_HOST}/check-report/{req_id}")

                results = {}
                await asyncio.sleep(4)
                for _ in range(10):
                    results = await asyncio.to_thread(
                        _http_json, f"{CHECK_HOST}/check-result/{req_id}"
                    )
                    if isinstance(results, dict) and all(results.get(n) is not None for n in nodes):
                        break
                    await asyncio.sleep(3)

                if not isinstance(results, dict):
                    results = {}
                if any(results.get(n) is not None for n in nodes):
                    text = format_ping_result(host, nodes, results, link)
                else:
                    reason = "check-host نتیجه‌ای برنگردوند"
        except Exception as e:
            reason = f"خطا در check-host: {e}"

        if text is None:
            await event.edit(f"⚠️ {reason}\n⏳ در حال گرفتن پینگ از globalping.io ...")
            try:
                text = await globalping_ping(host, max_nodes)
            except Exception as e:
                await event.edit(f"❌ هر دو سرویس جواب ندادن.\n• {reason}\n• globalping: {e}")
                return

        chunks = split_text(text)
        await event.edit(chunks[0])
        for c in chunks[1:]:
            await event.respond(c)

    @client.on(events.NewMessage(pattern=r"(?i)^\s*\.?\s*(?:pingir|irping)\s+(\S+)\s*$", outgoing=True))
    async def pingir_handler(event):
        host = event.pattern_match.group(1).strip()
        host = re.sub(r"^[a-zA-Z]+://", "", host).split("/")[0]
        if not re.fullmatch(r"[A-Za-z0-9.\-:]+", host):
            await event.edit("❌ آدرس نامعتبره.")
            return

        await event.edit(f"⏳ در حال گرفتن پینگ `{host}` از ایران ...")
        local_res, gp_res = await asyncio.gather(
            local_ping(host),
            globalping_ping(host, 10, locations=[{"country": "IR"}],
                            title="🇮🇷 **پروب‌های ایران (globalping.io)**\n"),
            return_exceptions=True,
        )

        lines = [f"🏓 **Ping** `{host}` از ایران\n"]
        lines.append("🖥 **از اینترنت خودت (همین کامپیوتر):**")
        lines.append(local_res if isinstance(local_res, str) else f"❌ خطا: {local_res}")
        lines.append("")
        if isinstance(gp_res, str):
            lines.append(gp_res)
        else:
            msg = str(gp_res)
            if "422" in msg or "no_probes" in msg:
                lines.append("🇮🇷 globalping تو ایران پروبی نداره.")
            else:
                lines.append(f"🇮🇷 globalping: خطا ({msg[:150]})")

        chunks = split_text("\n".join(lines))
        await event.edit(chunks[0])
        for c in chunks[1:]:
            await event.respond(c)


# ==================== نقطه شروع ====================
async def main():
    if not API_ID or not API_HASH:
        print("❌ API_ID و API_HASH رو داخل Variables تنظیم کن.")
        return

    session_path = os.path.join(SESSIONS_DIR, SESSION_NAME)
    print(f"📁 DATA_DIR = {DATA_DIR}")
    print(f"📁 Session path = {session_path}")
    print(f"🔌 Proxy = {PROXY}")

    client = TelegramClient(session_path, API_ID, API_HASH, proxy=PROXY)

    await client.start(phone=PHONE)
    print("✅ لاگین شد.")

    me = await client.get_me()
    print(f"کاربر: {me.first_name} (@{me.username})")

    await init_db()
    scheduler = BannerScheduler(client)
    await scheduler.start()

    register_handlers(client, scheduler)

    await notify_owner(client, "🚀 BannerBot روشن شد و آماده است (Railway).")
    print("ربات در حال اجراست... (Ctrl+C برای خروج)")

    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
