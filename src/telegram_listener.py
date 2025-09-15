import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from sqlalchemy import create_engine, text

# Get bot token from environment
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("# TELEGRAM_BOT_TOKEN is not set. Add it to .env or export manually!")

# Database connection for storing chat_ids
DB_URL = "postgresql://airflow:airflow@postgres:5432/airflow"

engine = create_engine(DB_URL)

# --- /start command: subscribe user ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    chat_id = chat.id
    username = chat.username
    first_name = chat.first_name
    last_name = chat.last_name

    # Insert into DB if not exists
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO telegram_users (chat_id, username, first_name, last_name)
                VALUES (:chat_id, :username, :first_name, :last_name)
                ON CONFLICT (chat_id) DO NOTHING
            """),
            {"chat_id": chat_id, "username": username, "first_name": first_name, "last_name": last_name},
        )

    # Send confirmation message to user
    await context.bot.send_message(chat_id=chat_id, text="+ Ви підписались на сповіщення!")

# Build bot application and register handlers
app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler("start", start))

# --- /stop command: unsubscribe user ---
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM telegram_users WHERE chat_id = :chat_id"),
            {"chat_id": chat_id}
        )
    await context.bot.send_message(chat_id=chat_id, text="# Ви відписались від сповіщень.")

app.add_handler(CommandHandler("stop", stop))

# Start polling for messages
print("+ Bot is listening...")
app.run_polling()
