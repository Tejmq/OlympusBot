import discord
import pandas as pd
import requests
from io import BytesIO
from wcwidth import wcswidth
import os
import time
import json
import random
from keep_alive import keep_alive

# ─── CONFIG ──────────────────────────────────────────────────────────────
DRIVE_FILE_ID = "1YMzE4FXjH4wctFektINwhCDjzZ0xqCP6"
TANKS_JSON_FILE_ID = "1pGcmeDcTqx2h_HXA_R24JbaqQiBHhYMQ"

COLUMNS_DEFAULT = ["Ņ", "Score", "True Name", "Tank Type", "Date"]
COLUMNS_C = ["Ņ", "Tank Type", "True Name", "Score", "Date"]

FIRST_COLUMN = "Score"
LEGENDS = 1000
COOLDOWN_SECONDS = 5

user_cooldowns = {}

# ─── BOT SETUP ────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

# ─── DATA LOADERS ─────────────────────────────────────────────────────────
def read_excel():
    try:
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"
        r = requests.get(url)
        r.raise_for_status()
        return pd.read_excel(BytesIO(r.content))
    except Exception as e:
        print("Excel load error:", e)
        return pd.DataFrame()

def load_tanks():
    url = f"https://drive.google.com/uc?export=download&id={TANKS_JSON_FILE_ID}"
    r = requests.get(url)
    r.raise_for_status()
    return json.loads(r.text)["tanks"]

TANK_NAMES = load_tanks()

# ─── HELPERS ──────────────────────────────────────────────────────────────
def is_tejm(user):
    return user.name.lower() == "tejm_of_curonia"

def normalize_score(df):
    df = df.copy()
    df["Score"] = (
        pd.to_numeric(
            df["Score"].astype(str).str.replace(",", ""),
            errors="coerce"
        ).fillna(0)
    )
    return df

def add_index(df):
    df = df.reset_index(drop=True)
    df["Ņ"] = range(1, len(df) + 1)
    return df

def parse_range(text, max_range=15):
    try:
        a, b = map(int, text.split("-"))
        if b - a + 1 > max_range or a > LEGENDS or b > LEGENDS:
            return None
        return a, b
    except:
        return None

# ─── TABLE FORMATTER ──────────────────────────────────────────────────────
def dataframe_to_markdown_aligned(df, shorten_tank=True):
    df = df.copy()

    if FIRST_COLUMN in df:
        df[FIRST_COLUMN] = df[FIRST_COLUMN].apply(
            lambda v: f"{float(v)/1_000_000:,.3f} Mil"
        )

    if "Date" in df:
        df["Date"] = df["Date"].astype(str).str[:10]

    if shorten_tank and "Tank Type" in df:
        df["Tank Type"] = (
            df["Tank Type"]
            .astype(str)
            .str.lower()
            .replace({"triple": "t", "auto": "a", "hexa": "h"}, regex=True)
            .str.title()
            .str[:8]
        )

    rows = [df.columns.tolist()] + df.values.tolist()
    widths = [max(wcswidth(str(r[i])) for r in rows) for i in range(len(df.columns))]

    def fmt(r):
        return "| " + " | ".join(
            str(v) + " " * (widths[i] - wcswidth(str(v)))
            for i, v in enumerate(r)
        ) + " |"

    return (
        [fmt(df.columns),
         "| " + " | ".join("-" * w for w in widths) + " |"]
        + [fmt(r) for r in df.values]
    )

# ─── COMMAND HELPERS ──────────────────────────────────────────────────────
def handle_best(df, parts):
    df = normalize_score(df)
    player = parts[2] if len(parts) > 2 and "-" not in parts[2] else None

    if player:
        df = df[df["True Name"].str.lower() == player.lower()]
        df = df.sort_values("Score", ascending=False)
    else:
        df = (
            df.sort_values("Score", ascending=False)
            .drop_duplicates("True Name")
        )

    return df

def handle_tank(df, tank):
    df = normalize_score(df)
    return (
        df[df["Tank Type"].str.lower() == tank.lower()]
        .sort_values("Score", ascending=False)
    )

# ─── EVENTS ───────────────────────────────────────────────────────────────
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return

    if not message.content.startswith("!olymp;"):
        return

    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts = message.content.split(";")
    cmd = parts[1].lower() if len(parts) > 1 else ""

    if cmd == "help":
        await message.channel.send(
            "!olymp;b;1-15\n"
            "!olymp;b;Player;1-15\n"
            "!olymp;c;1-15\n"
            "!olymp;p;1-15\n"
            "!olymp;t;Tank;1-15\n"
            "!olymp;d;YYYY-MM-DD\n"
            "!olymp;r"
        )
        return

    df = read_excel()
    if df.empty:
        await message.channel.send("Failed to load data.")
        return

    df.columns = df.columns.str.strip()
    output = None
    shorten_tank = True

    # ─── COMMAND SWITCH ───────────────────────────────────────────────────
    if cmd == "a":
        if not is_tejm(message.author):
            await message.channel.send("Restricted command.")
            return
        output = df

    elif cmd == "b":
        output = handle_best(df, parts)

    elif cmd == "c":
        output = (
            normalize_score(df)
            .sort_values("Score", ascending=False)
            .drop_duplicates("Tank Type")
        )

    elif cmd == "p":
        output = df

    elif cmd == "t":
        if len(parts) < 3:
            await message.channel.send("Tank name required.")
            return
        output = handle_tank(df, parts[2])

    elif cmd == "d":
        if len(parts) < 3:
            await message.channel.send("❌ Provide date as YYYY-MM-DD")
            return

        target = parts[2].strip()
        df2 = df.copy()
        df2["Date"] = df2["Date"].astype(str).str[:10]

        output = df2[df2["Date"] == target]

        if output.empty:
            await message.channel.send("❌ No scores found for that date")
            return

        shorten_tank = False

    elif cmd == "r":
        if len(parts) == 2:
            await message.channel.send("!olymp;r;a | b | r")
            return

        sub = parts[2].lower()

        if sub == "a":
            row = df.sample(1).iloc[0]
            await message.channel.send(
                f"{row['True Name']} recommends {row['Tank Type']}"
            )
            return

        elif sub == "b":
            used = set(df["Tank Type"].str.lower())
            unused = [t for t in TANK_NAMES if t.lower() not in used]

            if not unused:
                await message.channel.send("No tanks left.")
                return

            await message.channel.send(
                f"Mountain recommends {random.choice(unused)}"
            )
            return

        elif sub == "r":
            await message.channel.send(
                f"Mountain recommends {random.choice(TANK_NAMES)}"
            )
            return

    else:
        await message.channel.send("Unknown command.")
        return

    # ─── RANGE & OUTPUT ────────────────────────────────────────────────────
    if output is None or output.empty:
        await message.channel.send("No results.")
        return

    output = add_index(output)

    range_arg = None
    for p in parts:
        if "-" in p:
            range_arg = parse_range(p)
            break

    if range_arg:
        a, b = range_arg
        output = output[(output["Ņ"] >= a) & (output["Ņ"] <= b)]
    else:
        if cmd == "p":
            await message.channel.send(
                "❌ p command requires a range (example: 1-10)"
            )
            return

    cols = COLUMNS_C if cmd in {"c", "t"} else COLUMNS_DEFAULT
    output = output[[c for c in cols if c in output]].head(LEGENDS)

    lines = dataframe_to_markdown_aligned(output, shorten_tank)
    chunk = ""

    for line in lines:
        if len(chunk) + len(line) > 1900:
            await message.channel.send(f"```\n{chunk}\n```")
            chunk = ""
        chunk += line + "\n"

    if chunk:
        await message.channel.send(f"```\n{chunk}\n```")

# ─── RUN ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN missing")
    keep_alive()
    bot.run(token)
