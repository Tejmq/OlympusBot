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
user_cooldowns = {}

# ---------------- DISCORD ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

# ---------------- EXCEL CACHE ----------------
EXCEL_CACHE = {
    "df": None,
    "time": 0
}
EXCEL_CACHE_SECONDS = 30

# ---------------- DATA LOADERS ----------------
def read_excel():
    now = time.time()
    if EXCEL_CACHE["df"] is not None and now - EXCEL_CACHE["time"] < EXCEL_CACHE_SECONDS:
        return EXCEL_CACHE["df"].copy()

    try:
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"
        r = requests.get(url)
        r.raise_for_status()
        df = pd.read_excel(BytesIO(r.content))
        EXCEL_CACHE["df"] = df
        EXCEL_CACHE["time"] = now
        return df.copy()
    except Exception as e:
        print(f"❌ Failed to download Excel: {e}")
        return pd.DataFrame()

def load_tanks():
    url = f"https://drive.google.com/uc?export=download&id={TANKS_JSON_FILE_ID}"
    return json.loads(requests.get(url).text)["tanks"]

TANK_NAMES = load_tanks()

# ---------------- HELPERS ----------------
def is_tejm(user):
    return str(user.name).lower() == "tejm_of_curonia"

def add_index(df):
    df = df.copy().reset_index(drop=True)
    df["Ņ"] = range(1, len(df) + 1)
    return df

def dataframe_to_markdown_aligned(df, shorten_tank=True):
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
            s = str(name)
            for k, v in {'triple':'T','auto':'A','hexa':'H'}.items():
                s = s.lower().replace(k, v)
            return s.title()[:8]
        df["Tank Type"] = df["Tank Type"].apply(shorten)

    rows = [df.columns.tolist()] + df.values.tolist()
    widths = [max(wcswidth(str(r[i])) for r in rows) for i in range(len(df.columns))]

    def fmt(r):
        return "| " + " | ".join(
            str(c) + " " * (widths[i] - wcswidth(str(c)))
            for i, c in enumerate(r)
        ) + " |"

    header = fmt(df.columns)
    sep = "| " + " | ".join("-" * w for w in widths) + " |"
    body = [fmt(r) for r in df.values]

    return [header, sep] + body

# ---------------- EVENTS ----------------
@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    txt = message.content.strip()
    if not txt.startswith("!olymp;"):
        return

    # ---- COOLDOWN ----
    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts = txt.split(";")
    if len(parts) < 2:
        return

    cmd = parts[1].lower()
    df = read_excel()
    if df.empty and cmd != "r":
        await message.channel.send("❌ Failed to load Excel.")
        return

    df.columns = [str(c).strip() for c in df.columns]
    output_df = None
    shorten_tank = True

    # ---------------- HELP ----------------
    if cmd == "help":
        await message.channel.send(
            "Commands:\n"
            "!olymp;a;1-10 (Tejm only)\n"
            "!olymp;b;1-10\n"
            "!olymp;b;Player;1-10\n"
            "!olymp;c;1-10\n"
            "!olymp;p;1-10\n"
            "!olymp;t;Tank;1-10\n"
            "!olymp;d;YYYY-MM-DD\n"
            "!olymp;r"
        )
        return

    # ---------------- RANDOM ----------------
    if cmd == "r":
        if len(parts) < 3:
            await message.channel.send(
                "**!olymp;r;a** – random tank with player record\n"
                "**!olymp;r;b** – tank with no score\n"
                "**!olymp;r;r** – fully random tank"
            )
            return

        sub = parts[2].lower()

        if sub == "a":
            row = df.sample(1).iloc[0]
            await message.channel.send(
                f"🏔️ **The Mountain** recommends **{row['Tank Type']}** "
                f"used by **{row['Name in game']}**."
            )
            return

        if sub == "b":
            excel_tanks = set(df["Tank Type"].str.lower())
            available = [t for t in TANK_NAMES if t.lower() not in excel_tanks]
            chosen = random.choice(available)
            await message.channel.send(
                f"🏔️ **The Mountain** recommends **{chosen}**."
            )
            return

        if sub == "r":
            chosen = random.choice(TANK_NAMES)
            await message.channel.send(
                f"🏔️ **The Mountain** recommends **{chosen}**."
            )
            return

        return

    # ---------------- RANGE ----------------
    range_arg = (1, 10)
    for p in parts:
        if "-" in p:
            try:
                a, b = map(int, p.split("-"))
                range_arg = (a, b)
            except:
                pass

    a, b = range_arg

    # ---------------- COMMANDS ----------------
    if cmd == "a":
        if not is_tejm(message.author):
            await message.channel.send("Only Tejm can use this command.")
            return
        df2 = add_index(df)
        output_df = df2[(df2["Ņ"] >= a) & (df2["Ņ"] <= b)][COLUMNS_DEFAULT]

    elif cmd == "b":
        df2 = df.copy()
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        if len(parts) > 2 and "-" not in parts[2]:
            df2 = df2[df2["True Name"].str.lower() == parts[2].lower()]
        else:
            df2 = df2.sort_values("Score", ascending=False).drop_duplicates("True Name")
        df2 = add_index(df2)
        output_df = df2[(df2["Ņ"] >= a) & (df2["Ņ"] <= b)][COLUMNS_DEFAULT]

    elif cmd == "c":
        df2 = df.copy()
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        df2 = df2.sort_values("Score", ascending=False).drop_duplicates("Tank Type")
        df2 = add_index(df2)
        output_df = df2[(df2["Ņ"] >= a) & (df2["Ņ"] <= b)][COLUMNS_C]

    elif cmd == "p":
        df2 = add_index(df)
        output_df = df2[(df2["Ņ"] >= a) & (df2["Ņ"] <= b)][COLUMNS_DEFAULT]

    elif cmd == "t":
        if len(parts) < 3:
            return
        tank = parts[2].lower()
        df2 = df[df["Tank Type"].str.lower() == tank]
        df2 = add_index(df2)
        output_df = df2[(df2["Ņ"] >= a) & (df2["Ņ"] <= b)][COLUMNS_C]

    elif cmd == "d":
        if len(parts) < 3:
            return
        df2 = df[df["Date"].astype(str).str[:10] == parts[2]]
        df2 = add_index(df2)
        output_df = df2[COLUMNS_DEFAULT]
        shorten_tank = False

    if output_df is None or output_df.empty:
        return

    lines = dataframe_to_markdown_aligned(output_df, shorten_tank)
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) > 1900:
            await message.channel.send(f"```\n{chunk}\n```")
            chunk = ""
        chunk += line + "\n"
    if chunk:
        await message.channel.send(f"```\n{chunk}\n```")

# ---------------- RUN ----------------
if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
