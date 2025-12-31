import discord
import pandas as pd
import requests
from io import BytesIO
from wcwidth import wcswidth
import os, time, json, random, re
from keep_alive import keep_alive

DRIVE_FILE_ID = "1YMzE4FXjH4wctFektINwhCDjzZ0xqCP6"
TANKS_JSON_FILE_ID = "1pGcmeDcTqx2h_HXA_R24JbaqQiBHhYMQ"

COLUMNS_DEFAULT = ["Ņ", "Score", "True Name", "Tank Type", "Date"]
COLUMNS_C = ["Ņ", "Tank Type", "True Name", "Score", "Date"]

FIRST_COLUMN = "Score"
LEGENDS = 1000
COOLDOWN_SECONDS = 5
user_cooldowns = {}

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

def read_excel():
    try:
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"
        r = requests.get(url)
        r.raise_for_status()
        return pd.read_excel(BytesIO(r.content))
    except:
        return pd.DataFrame()

def load_tanks():
    url = f"https://drive.google.com/uc?export=download&id={TANKS_JSON_FILE_ID}"
    r = requests.get(url)
    r.raise_for_status()
    return json.loads(r.text)["tanks"]

TANK_NAMES = load_tanks()

def normalize_score(df):
    df = df.copy()
    df["Score"] = pd.to_numeric(df["Score"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    return df

def add_index(df):
    df = df.reset_index(drop=True)
    df["Ņ"] = range(1, len(df) + 1)
    return df

def parse_range(text, max_range=15):
    try:
        a, b = map(int, text.split("-"))
        if b - a + 1 > max_range:
            return None
        return a, b
    except:
        return None

def dataframe_to_markdown_aligned(df):
    rows = [df.columns.tolist()] + df.values.tolist()
    widths = [max(wcswidth(str(r[i])) for r in rows) for i in range(len(df.columns))]
    def fmt(r):
        return "| " + " | ".join(str(v) + " "*(widths[i]-wcswidth(str(v))) for i,v in enumerate(r)) + " |"
    return [fmt(df.columns), "| " + " | ".join("-"*w for w in widths) + " |"] + [fmt(r) for r in df.values]

@bot.event
async def on_message(message):
    if message.author == bot.user: return
    if not message.content.startswith("!olymp;"): return

    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts = message.content.split(";")
    cmd = parts[1].lower()

    df = read_excel()
    if df.empty:
        await message.channel.send("Failed to load data.")
        return
    df.columns = df.columns.str.strip()

    output = None

    # ---- P COMMAND (FIXED) ----
    if cmd == "p":
        df = normalize_score(df).sort_values("Score", ascending=False)
        df = add_index(df)

        rng = None
        for p in parts:
            if "-" in p:
                rng = parse_range(p)
                break

        a, b = rng if rng else (1, 15)
        output = df[(df["Ņ"] >= a) & (df["Ņ"] <= b)]

    # ---- D COMMAND (FIXED) ----
    elif cmd == "d":
        if len(parts) < 3:
            await message.channel.send("❌ Use !olymp;d;YYYY-MM-DD or DD-MM-YYYY")
            return

        raw = parts[2]
        if re.match(r"\d{2}-\d{2}-\d{4}", raw):
            d,m,y = raw.split("-")
            target = f"{y}-{m}-{d}"
        elif re.match(r"\d{4}-\d{2}-\d{2}", raw):
            target = raw
        else:
            await message.channel.send("❌ Invalid date format")
            return

        df["Date"] = df["Date"].astype(str).str[:10]
        output = df[df["Date"] == target]

        if output.empty:
            await message.channel.send(f"❌ No results for {target}")
            return

        output = add_index(output)

    else:
        return

    if output is None or output.empty:
        await message.channel.send("No results.")
        return

    output = output[COLUMNS_DEFAULT]
    lines = dataframe_to_markdown_aligned(output)

    msg = ""
    for line in lines:
        if len(msg) + len(line) > 1900:
            await message.channel.send(f"```\n{msg}\n```")
            msg = ""
        msg += line + "\n"
    if msg:
        await message.channel.send(f"```\n{msg}\n```")

@bot.event
async def on_ready():
    print("Bot ready")

if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
