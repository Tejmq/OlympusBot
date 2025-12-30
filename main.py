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

# ---------------- DATA LOADERS ----------------
def read_excel():
    try:
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"
        r = requests.get(url)
        r.raise_for_status()
        return pd.read_excel(BytesIO(r.content))
    except Exception as e:
        print(f"Excel error: {e}")
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
            s = str(name).lower()
            s = s.replace("triple", "T").replace("auto", "A").replace("hexa", "H")
            return s.title()[:10]
        df["Tank Type"] = df["Tank Type"].apply(shorten)

    rows = [df.columns.tolist()] + df.values.tolist()
    col_widths = [
        max(wcswidth(str(r[i])) for r in rows)
        for i in range(len(df.columns))
    ]

    def fmt_row(row):
        return "  ".join(
            str(cell) + " " * (col_widths[i] - wcswidth(str(cell)))
            for i, cell in enumerate(row)
        )

    lines = [fmt_row(df.columns)]
    lines.append("-" * wcswidth(lines[0]))
    for r in df.values:
        lines.append(fmt_row(r))

    return lines

# ---------------- UI EMBED ----------------
async def send_table_embed(channel, title, lines, page, total):
    text = "\n".join(lines)
    embed = discord.Embed(
        title=title,
        description=f"```text\n{text}\n```",
        color=discord.Color.dark_gold()
    )
    embed.set_footer(text=f"Page {page}/{total}")
    await channel.send(embed=embed)

# ---------------- BOT EVENTS ----------------
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

    # ---------------- HELP ----------------
    if cmd == "help":
        embed = discord.Embed(
            title="Olympus Bot Commands",
            color=discord.Color.blurple()
        )
        embed.add_field(
            name="Leaderboards",
            value=(
                "`!olymp;b;1-15` Best players\n"
                "`!olymp;c;1-15` Best tanks\n"
                "`!olymp;p;1-15` Score slice\n"
                "`!olymp;t;Tank;1-15` Tank scores\n"
                "`!olymp;d;YYYY-MM-DD` Daily results"
            ),
            inline=False
        )
        embed.add_field(
            name="Random",
            value=(
                "`!olymp;r;a` Random recorded tank\n"
                "`!olymp;r;b` Unused tank\n"
                "`!olymp;r;r` Fully random tank"
            ),
            inline=False
        )
        embed.set_footer(text="Olympus Statistics")
        await message.channel.send(embed=embed)
        return

    df = read_excel()
    if df.empty:
        await message.channel.send("Data unavailable.")
        return

    df.columns = [str(c).strip() for c in df.columns]

    # ---------------- RANGE ----------------
    range_arg = (1, 1)
    if parts[-1].count("-") == 1:
        try:
            a, b = map(int, parts[-1].split("-"))
            range_arg = (a, b)
        except:
            pass

    # ---------------- COMMANDS ----------------
    if cmd == "b":
        df["Score"] = pd.to_numeric(df["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        df = df.sort_values("Score", ascending=False).drop_duplicates("True Name")
        df = add_index(df)
        a, b = range_arg
        out = df[(df["Ņ"] >= a) & (df["Ņ"] <= b)][COLUMNS_DEFAULT]
        title = "Best Player Scores"

    elif cmd == "c":
        df["Score"] = pd.to_numeric(df["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        df = df.sort_values("Score", ascending=False).drop_duplicates("Tank Type")
        df = add_index(df)
        a, b = range_arg
        out = df[(df["Ņ"] >= a) & (df["Ņ"] <= b)][COLUMNS_C]
        title = "Best Tank Scores"

    elif cmd == "p":
        df = add_index(df)
        a, b = range_arg
        out = df[(df["Ņ"] >= a) & (df["Ņ"] <= b)][COLUMNS_DEFAULT]
        title = "Scoreboard Slice"

    elif cmd == "d":
        date = parts[2]
        df["Date"] = df["Date"].astype(str).str[:10]
        df = df[df["Date"] == date]
        df = add_index(df)
        out = df[COLUMNS_DEFAULT]
        title = f"Scores for {date}"

    else:
        await message.channel.send("Unknown command.")
        return

    # ---------------- OUTPUT ----------------
    lines = dataframe_to_markdown_aligned(out)
    chunks = []
    block = []

    for l in lines:
        if sum(len(x) for x in block) + len(l) < 1800:
            block.append(l)
        else:
            chunks.append(block)
            block = [l]
    if block:
        chunks.append(block)

    for i, c in enumerate(chunks, 1):
        await send_table_embed(message.channel, title, c, i, len(chunks))

# ---------------- RUN ----------------
if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    keep_alive()
    bot.run(token)
