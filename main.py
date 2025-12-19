import discord
import pandas as pd
import requests
from io import BytesIO
from wcwidth import wcswidth
import os
from keep_alive import keep_alive
import random

# --- CONFIG ---
DRIVE_FILE_ID = "1YMzE4FXjH4wctFektINwhCDjzZ0xqCP6"
COLUMNS_DEFAULT = ["Ņ", "Score", "True Name", "Tank Type", "Date"]
COLUMNS_C = ["Ņ", "Tank Type", "True Name", "Score", "Date"]
FIRST_COLUMN = "Score"
LEGENDS = 500

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

# --- READ EXCEL ---
def read_excel():
    try:
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"
        r = requests.get(url)
        r.raise_for_status()
        return pd.read_excel(BytesIO(r.content))
    except Exception as e:
        print(f"❌ Failed to download Excel from Drive: {e}")
        return pd.DataFrame()

# --- FORMAT TABLE ---
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
        def shorten_name(name):
            s = str(name)
            repl_map = {'triple':'T','auto':'A','hexa':'H'}
            low = s.lower()
            for k,v in repl_map.items():
                low = low.replace(k,v)
            return low.title()[:8]
        df["Tank Type"] = df["Tank Type"].apply(shorten_name)

    rows = [df.columns.tolist()] + df.values.tolist()
    col_widths = [max(wcswidth(str(r[i])) for r in rows) for i in range(len(df.columns))]

    def fmt_row(r):
        cells = []
        for i, val in enumerate(r):
            s = str(val)
            pad = col_widths[i] - wcswidth(s)
            cells.append(s + " " * pad)
        return "| " + " | ".join(cells) + " |"

    header = fmt_row(df.columns)
    separator = "| " + " | ".join("-"*w for w in col_widths) + " |"
    body = [fmt_row(r) for r in df.values]

    return [header, separator] + body

# --- HELPERS ---
def is_tejm(user):
    return str(user.name).lower() == "tejm_of_curonia"

def add_index(df):
    df = df.copy().reset_index(drop=True)
    df["Ņ"] = range(1, len(df)+1)
    return df

# --- BOT EVENTS ---
@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")
    print("Bot ready and listening...")

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return

    txt = message.content.strip()
    if not txt.startswith("!olymp;"):
        return

    parts = txt.split(";")
    if len(parts) < 2:
        await message.channel.send("Not a command buddy")
        return

    cmd = parts[1].strip().lower()

    if cmd == "help":
        await message.channel.send(
        "Commands:\n"
        "!olymp;a;1-15 - Full scoreboard (Tejm only)\n"
        "!olymp;b;1-15 - Best score of each player\n"
        "!olymp;b;Player;1-15 - Best scores of a specific player\n"
        "!olymp;c;1-15 - Best score per tank (short tank names)\n"
        "!olymp;p;1-15 - Part of scoreboard\n"
        "!olymp;t;TankName;1-15 - Best score of a tank\n"
        "!olymp;d;YYYY-MM-DD - Scores from that date (short tank names)\n"
        "!olymp;r;1-15 - Raw scoreboard (NO tank shortening)\n"
    )
        return

    df = read_excel()
    if df.empty:
        await message.channel.send("❌ Failed to fetch Excel!")
        return

    df.columns = [str(c).strip() for c in df.columns]
    output_df = None
    range_arg = None

    # --- RANGE PARSING ---
    if cmd in ['c','p','r']:
        if len(parts) > 2 and '-' in parts[-1]:
            a,b = map(int, parts[-1].split('-'))
            range_arg = (a,b)
        else:
            await message.channel.send("Input range! (1-15)")
            return

    # --- COMMANDS ---
    if cmd == "d":
        target_date = parts[2].strip()
        df2 = df.copy()
        df2["Date"] = df2["Date"].astype(str).str[:10]
        df2 = df2[df2["Date"] == target_date]
        df2 = add_index(df2)
        output_df = df2[COLUMNS_DEFAULT]

    elif cmd == "r":
        df2 = add_index(df)
        a,b = range_arg
        df2 = df2[(df2['Ņ'] >= a) & (df2['Ņ'] <= b)]
        if df2.empty:
            await message.channel.send("No data to recommend!")
            return

        # pick random row
        row = df2.sample(n=1).iloc[0]
        recommendation = f"{row['True Name']} recommends you {row['Tank Type']}"
        await message.channel.send(recommendation)
        return

    # --- FINAL FORMAT ---
    shorten_tank = True if cmd in ['a','b','c','p','t','d'] else False
    lines = dataframe_to_markdown_aligned(output_df, shorten_tank=shorten_tank)

    chunk = ""
    for line in lines:
        if len(chunk) + len(line) < 1900:
            chunk += line + "\n"
        else:
            await message.channel.send(f"```{chunk}```")
            chunk = line + "\n"
    if chunk:
        await message.channel.send(f"```{chunk}```")

# --- RUN ---
if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    keep_alive()
    bot.run(token)
