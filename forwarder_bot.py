import asyncio
import logging
import re
import os
import signal
from typing import Optional, List, Tuple

import aiosqlite
from aiohttp import web
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ================================
#  Database handling
# ================================
DB_PATH = "forwarder.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                source_chat_id INTEGER NOT NULL,
                source_thread_id INTEGER,
                dest_chat_id INTEGER NOT NULL,
                UNIQUE(user_id, source_chat_id, source_thread_id, dest_chat_id)
            )
        """)
        await db.commit()

async def add_rule(user_id: int, src_chat: int, src_thread: Optional[int], dest_chat: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO rules (user_id, source_chat_id, source_thread_id, dest_chat_id) VALUES (?,?,?,?)",
            (user_id, src_chat, src_thread, dest_chat)
        )
        await db.commit()

async def remove_rule(rule_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM rules WHERE id=? AND user_id=?",
            (rule_id, user_id)
        )
        await db.commit()
        return cursor.rowcount > 0

async def get_rules_for_source(src_chat: int) -> List[Tuple[Optional[int], int]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT source_thread_id, dest_chat_id FROM rules WHERE source_chat_id=?",
            (src_chat,)
        )
        return await cursor.fetchall()

async def get_user_rules(user_id: int) -> List[Tuple[int, int, Optional[int], int]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, source_chat_id, source_thread_id, dest_chat_id FROM rules WHERE user_id=?",
            (user_id,)
        )
        return await cursor.fetchall()

# ================================
#  Link parsing
# ================================
LINK_PATTERN = re.compile(
    r"(?:https?://)?t(?:elegram)?\.me/(?:c/)?(?P<chat>[^/]+)/(?P<msg>\d+)(?:\?thread=(?P<thread>\d+))?"
)

def parse_message_link(link: str) -> Tuple[Optional[int], int, Optional[int]]:
    match = LINK_PATTERN.search(link)
    if not match:
        raise ValueError("Invalid message link format.")
    chat_str = match.group("chat")
    message_id = int(match.group("msg"))
    thread_str = match.group("thread")
    thread_id = int(thread_str) if thread_str else None

    if chat_str.startswith("@"):
        return None, message_id, thread_id
    try:
        chat_id = int(chat_str)
        if chat_id > 0:
            chat_id = -100_000_000_000 - chat_id
        else:
            if chat_id > -100_000_000_000:
                chat_id = -100_000_000_000 + chat_id
        return chat_id, message_id, thread_id
    except ValueError:
        raise ValueError("Chat part must be a number (private) or a username (public).")

async def resolve_public_chat(chat_username: str, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat = await context.bot.get_chat(chat_username)
    return chat.id

# ================================
#  Command handlers (now using HTML)
# ================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 <b>Message Forwarder Bot</b>\n\n"
        "I can forward messages from private groups/channels to any chat you choose.\n\n"
        "🔹 <b>Add a source:</b>\n"
        "   /add_forward – step‑by‑step guide\n"
        "🔹 <b>Bulk add</b> (multiple links at once):\n"
        "   /bulk_add\n"
        "🔹 <b>List your rules:</b>\n"
        "   /list\n"
        "🔹 <b>Remove a rule:</b>\n"
        "   /remove",
        parse_mode=ParseMode.HTML,
    )

async def add_forward_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Send me a link to <b>any message</b> from the source chat.\n"
        "Example: <code>https://t.me/c/123456789/1234?thread=567</code>\n\n"
        "For topic groups, include the <code>?thread=</code> part if you want to forward only that topic.\n"
        "If you omit the thread, I will forward only the <i>General</i> topic.",
        parse_mode=ParseMode.HTML,
    )
    context.user_data["awaiting_link"] = True

async def handle_link_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_link") and not context.user_data.get("awaiting_bulk"):
        return
    text = update.message.text.strip()
    links = [l.strip() for l in text.splitlines() if l.strip()] if context.user_data.get("awaiting_bulk") else [text]
    if not links:
        await update.message.reply_text("Please send at least one valid message link.")
        return
    context.user_data["pending_links"] = links
    context.user_data.pop("awaiting_link", None)
    context.user_data.pop("awaiting_bulk", None)
    await update.message.reply_text(
        "Where should I forward the messages?\n"
        "Reply with:\n"
        "• <code>here</code> → forward to this chat\n"
        "• a chat ID (e.g. <code>-1001234567890</code>) or username (e.g. <code>@my_channel</code>)\n\n"
        "<i>(Make sure I am a member of the destination chat)</i>",
        parse_mode=ParseMode.HTML,
    )
    context.user_data["awaiting_dest"] = True

async def handle_dest_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_dest"):
        return
    dest_text = update.message.text.strip()
    destination = None
    if dest_text.lower() == "here":
        destination = update.effective_chat.id
    elif dest_text.startswith("@"):
        try:
            destination = await resolve_public_chat(dest_text, context)
        except Exception as e:
            await update.message.reply_text(f"❌ Cannot resolve chat: {e}")
            return
    else:
        try:
            destination = int(dest_text)
        except ValueError:
            await update.message.reply_text("Invalid chat ID. Use a number, @username, or 'here'.")
            return

    pending = context.user_data.get("pending_links", [])
    successes = 0
    errors = []
    for link in pending:
        try:
            chat_id, msg_id, thread_id = parse_message_link(link)
            if chat_id is None:
                match = re.search(r"t\.me/(@?\w+)/", link)
                if match:
                    username = match.group(1)
                    if not username.startswith("@"):
                        username = "@" + username
                    chat_id = await resolve_public_chat(username, context)
                else:
                    raise ValueError("Cannot extract username from link")
            await add_rule(update.effective_user.id, chat_id, thread_id, destination)
            successes += 1
        except Exception as e:
            errors.append(f"{link}: {e}")

    context.user_data.pop("awaiting_dest", None)
    context.user_data.pop("pending_links", None)
    msg = f"✅ Added {successes} forwarding rule(s)."
    if errors:
        msg += f"\n❌ Errors:\n" + "\n".join(errors)
    await update.message.reply_text(msg)

async def bulk_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Paste all message links (one per line).\n\n"
        "Example:\n"
        "<code>https://t.me/c/111/222</code>\n"
        "<code>https://t.me/c/333/444?thread=55</code>\n"
        "<code>https://t.me/username/666</code>\n\n"
        "Then I will ask for the destination chat.",
        parse_mode=ParseMode.HTML,
    )
    context.user_data["awaiting_bulk"] = True

async def list_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rules = await get_user_rules(user_id)
    if not rules:
        await update.message.reply_text("You have no forwarding rules.")
        return
    lines = ["<b>Your forwarding rules:</b>"]
    for idx, (rule_id, src_chat, thread, dest_chat) in enumerate(rules, 1):
        thread_str = "All topics" if thread == -1 else f"Thread {thread}" if thread is not None else "General"
        lines.append(f"{idx}. <code>{src_chat}</code> ({thread_str}) → <code>{dest_chat}</code>  (ID: {rule_id})")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def remove_rule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Usage: /remove <rule_id>\nUse /list to see IDs.")
        return
    try:
        rule_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Rule ID must be a number.")
        return
    removed = await remove_rule(rule_id, user_id)
    if removed:
        await update.message.reply_text(f"✅ Rule {rule_id} removed.")
    else:
        await update.message.reply_text("❌ Rule not found or not yours.")

# ================================
#  Forwarding logic (unchanged)
# ================================
async def forward_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or msg.chat.type == "private":
        return
    src_chat_id = msg.chat.id
    src_thread_id = getattr(msg, "message_thread_id", None)
    rules = await get_rules_for_source(src_chat_id)
    if not rules:
        return
    destinations = set()
    for rule_thread, dest in rules:
        if rule_thread == -1:
            destinations.add(dest)
        elif rule_thread is None:
            if src_thread_id is None:
                destinations.add(dest)
        else:
            if src_thread_id == rule_thread:
                destinations.add(dest)
    for dest in destinations:
        try:
            await context.bot.forward_message(
                chat_id=dest,
                from_chat_id=src_chat_id,
                message_id=msg.message_id,
                disable_notification=True,
            )
            await asyncio.sleep(0.1)
        except Exception as e:
            logging.error(f"Forward error {src_chat_id}→{dest}: {e}")

# ================================
#  Health check endpoint
# ================================
async def health(request):
    return web.Response(text="Bot is running")

# ================================
#  Main entry point
# ================================
async def main():
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    TOKEN = os.getenv("BOT_TOKEN")
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable not set")

    PORT = int(os.environ.get("PORT", "8443"))

    await init_db()

    app = Application.builder().token(TOKEN).build()

    # Command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add_forward", add_forward_start))
    app.add_handler(CommandHandler("bulk_add", bulk_add_start))
    app.add_handler(CommandHandler("list", list_rules))
    app.add_handler(CommandHandler("remove", remove_rule_cmd))

    # Conversation‑style message handlers
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link_message), group=1)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_dest_reply), group=1)

    # Global forwarder
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, forward_handler), group=2)

    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    logging.info("Bot polling started")

    # Health server
    health_app = web.Application()
    health_app.router.add_get("/", health)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"Health server started on port {PORT}")

    # Graceful shutdown
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass
    await stop_event.wait()

    logging.info("Shutting down...")
    await app.updater.stop()
    await app.stop()
    await app.shutdown()
    await runner.cleanup()
    logging.info("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
