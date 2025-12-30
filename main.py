import discord
import pandas as pd
import requests
from io import BytesIO
from wcwidth import wcswidth
import os
from keep_alive import keep_alive
import random
import time
import json

# ---------------- CONFIG ----------------
DRIVE_FILE_ID = "1YMzE4FXjH4wctFektINwhCDjzZ0xqCP6"
TANKS_JSON_FILE_ID = "1pGcmeDcTqx2h_HXA_R24JbaqQiBHhYMQ"

COLUMNS_DEFAULT = ["Ņ", "Score", "True Name", "Tank Type", "Date"]
COLUMNS_C = ["Ņ", "Tank Type", "True Name", "Score", "Date"]
FIRST_COLUMN = "Score"
LEGENDS = 1000

COOLDOWN_SECONDS = 5
EXCEL_CACHE_SECONDS = 60

user_cooldowns = {}
_excel_cache = None
_excel_cache_time = 0

# ---------------- DISCORD ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

# ---------------- DATA LOADERS ----------------
def read_excel_cached():
    global _excel_cache, _excel_cache_time

    now = time.time()
    if _excel_cache is not None and now - _excel_cache_time < EXCEL_CACHE_SECONDS:
        return _excel_cache.copy()

    try:
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"
        r = requests.get(url)
        r.raise_for_status()
        df = pd.read_excel(BytesIO(r.content))
        _excel_cache = df
        _excel_cache_time = now
        return df.copy()
    except Exception as e:
        print(f"Excel download failed: {e}")
        if _excel_cache is not None:
            return _excel_cache.copy()
        return pd.DataFrame()

def load_tanks_from_drive(file_id):
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.text)

tank_data = load_tanks_from_drive(TANKS_JSON_FILE_ID)
TANK_NAMES = tank_data["tanks"]

# ---------------- HELPERS ----------------
def is_tejm(user):
    return str(user.name).lower() == "tejm_of_curonia"

def add_index(df):
    df = df.copy().reset_index(drop=True)
    df["Ņ"] = range(1, len(df) + 1)
    return df

# ---------------- TABLE FORMAT ----------------
def dataframe_to_markdown_aligned(df, shorten_tank=False):
    df = df.copy()

    if FIRST_COLUMN in df.columns:
        df[FIRST_COLUMN] = pd.to_numeric(
            df[FIRST_COLUMN].astype(str).str.replace(',', ''),
            errors='coerce'
        ).fillna(0)
        df[FIRST_COLUMN] = df[FIRST_COLUMN].apply(
            lambda v: f"{v/1_000_000:,.3f} Mil"
        )

    if "Date" in df.columns:
        df["Date"] = df["Date"].astype(str).str[:10]

    if shorten_tank and "Tank Type" in df.columns:
        def shorten(name):
            s = str(name).lower()
            s = s.replace("triple", "T").replace("auto", "A").replace("hexa", "H")
            return s.title()[:8]
        df["Tank Type"] = df["Tank Type"].apply(shorten)

    rows = [df.columns.tolist()] + df.values.tolist()
    widths = [max(wcswidth(str(r[i])) for r in rows) for i in range(len(rows[0]))]

    def fmt(r):
        return "  ".join(
            str(c) + " " * (widths[i] - wcswidth(str(c)))
            for i, c in enumerate(r)
        )

    lines = [fmt(df.columns), "-" * wcswidth(fmt(df.columns))]
    lines += [fmt(r) for r in df.values]
    return lines

# ---------------- BOT EVENTS ----------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    txt = message.content.strip()
    if not txt.startswith("!olymp;"):
        return

    # ---- cooldown ----
    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts = txt.split(";")
    if len(parts) < 2:
        return

    cmd = parts[1].lower()

    # ---- HELP ----
    if cmd == "help":
        await message.channel.send(
            "!olymp;a\n"
            "!olymp;b;1-15\n"
            "!olymp;b;Player;1-15\n"
            "!olymp;c;1-15\n"
            "!olymp;p;1-15\n"
            "!olymp;t;Tank;1-15\n"
            "!olymp;d;YYYY-MM-DD\n"
            "!olymp;r"
        )
        return

    df = read_excel_cached()
    if df.empty:
        await message.channel.send("Excel unavailable.")
        return

    df.columns = df.columns.str.strip()
    output_df = None
    shorten_tank = True

    # ---- RANDOM (EXACT ORIGINAL LOGIC) ----
    if cmd == "r":
        if len(parts) == 2:
            await message.channel.send(
                "!olymp;r;a\n"
                "!olymp;r;b\n"
                "!olymp;r;r"
            )
            return

        subcmd = parts[2].lower()

        if subcmd == "a":
            row = df.sample(1).iloc[0]
            await message.channel.send(
                f"{row['Name in game']} recommends {row['Tank Type']}"
            )
            return

        if subcmd == "b":
            excel_tanks = set(df["Tank Type"].astype(str).str.lower())
            available = [t for t in TANK_NAMES if t.lower() not in excel_tanks]
            if not available:
                await message.channel.send("No unused tanks.")
                return
            await message.channel.send(random.choice(available))
            return

        if subcmd == "r":
            await message.channel.send(random.choice(TANK_NAMES))
            return

        await message.channel.send("Unknown r command.")
        return

    # ---- A (TEJM ONLY) ----
    if cmd == "a":
        if not is_tejm(message.author):
            await message.channel.send("Only Tejm may use this.")
            return
        df2 = add_index(df)
        output_df = df2[COLUMNS_DEFAULT]

    # ---- FINAL OUTPUT ----
    if output_df is None:
        return

    lines = dataframe_to_markdown_aligned(output_df, shorten_tank)
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) < 1900:
            chunk += line + "\n"
        else:
            await message.channel.send(f"```\n{chunk}\n```")
            chunk = line + "\n"
    if chunk:
        await message.channel.send(f"```\n{chunk}\n```")

# ---------------- RUN ----------------
if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
