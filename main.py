import discord
import pandas as pd
import requests
from io import BytesIO
from wcwidth import wcswidth
import os
from keep_alive import keep_alive

# --- CONFIG ---
DRIVE_FILE_ID = "1YMzE4FXjH4wctFektINwhCDjzZ0xqCP6"
COLUMNS_DEFAULT = ["Ņ", "Score", "True Name", "Tank Type", "Date"]
COLUMNS_C = ["Ņ", "Tank Type", "True Name", "Score", "Date"]
FIRST_COLUMN = "Score"
LEGENDS = 221

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

# --- READ EXCEL FROM GOOGLE DRIVE ---
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
            errors='coerce').fillna(0)
        df[FIRST_COLUMN] = df[FIRST_COLUMN].apply(
            lambda v: f"{v/1_000_000:,.3f} Mil")

    if "Date" in df.columns:
        df["Date"] = df["Date"].astype(str).str[:10]

    if shorten_tank and "Tank Type" in df.columns:
        def shorten_name(name):
            n = str(name)
            parts = n.split('-')
            for i in range(min(2, len(parts))):
                if parts[i].lower() == 'triple':
                    parts[i] = 'T'
                elif parts[i].lower() == 'auto':
                    parts[i] = 'A'
            return '-'.join(parts)[:8]

        df["Tank Type"] = df["Tank Type"].apply(shorten_name)

    rows = [df.columns.tolist()] + df.values.tolist()
    col_widths = [
        max(wcswidth(str(r[i])) for r in rows) for i in range(len(df.columns))
    ]

    def fmt_row(r):
        cells = []
        for i, val in enumerate(r):
            s = str(val)
            pad = col_widths[i] - wcswidth(s)
            cells.append(s + " " * pad)
        return "| " + " | ".join(cells) + " |"

    header = fmt_row(df.columns)
    separator = "| " + " | ".join("-" * w for w in col_widths) + " |"
    body = [fmt_row(r) for r in df.values]
    return [header, separator] + body

# --- HELPERS ---
def is_tejm(user):
    return str(user.name).lower() == "tejm_of_curonia"

def add_index(df):
    df = df.copy().reset_index(drop=True)
    df["Ņ"] = range(1, len(df) + 1)
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

    # --- HELP ---
    if cmd == "help":
        help_message = (
            "Commands:\n"
            "!olymp;b;1-10    - Show best scores of each player (requires range n-m)\n"
            "!olymp;c;1-10    - Show best scores per unique tank (requires range n-m)\n"
            "!olymp;p;1-15    - Show part of the score leaderboard (max range 15)\n"
            "!olymp;t;TankName;1-10    - Show best score of a tank (range optional) if not specified will show top 1\n"
        )
        await message.channel.send(help_message)
        return

    df = read_excel()
    if df.empty:
        await message.channel.send("❌ Failed to fetch Excel from Google Drive!")
        return
    df.columns = [str(c).strip() for c in df.columns]
    output_df = None

    # --- RANGE ARGUMENT ---
    range_arg = None
    if cmd in ['b', 'c', 'p']:
        if len(parts) > 2 and '-' in parts[2]:
            try:
                a, b = map(int, parts[2].split('-'))
                if b - a + 1 > 10:
                    await message.channel.send("Range is too big!")
                    return
                if a > LEGENDS or b > LEGENDS:
                    await message.channel.send("Not enough scores for that!")
                    return
                range_arg = (a, b)
            except:
                pass
        if range_arg is None:
            await message.channel.send("Input range! For example (1-10)")
            return

    # --- COMMANDS ---
    if cmd == "a":
        if not is_tejm(message.author):
            await message.channel.send("Only Tejm is allowed to use that one")
            return
        df2 = add_index(df)
        if range_arg:
            a, b = range_arg
            df2 = df2[(df2['Ņ'] >= a) & (df2['Ņ'] <= b)]
        output_df = df2[[c for c in COLUMNS_DEFAULT if c in df2.columns]]

    elif cmd == "b":
        df2 = df.copy()
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        df2 = df2.sort_values("Score", ascending=False).drop_duplicates(subset=["True Name"], keep="first")
        df2 = add_index(df2)
        a, b = range_arg
        df2 = df2[(df2['Ņ'] >= a) & (df2['Ņ'] <= b)]
        output_df = df2[[c for c in COLUMNS_DEFAULT if c in df2.columns]]

    elif cmd == "c":
        df2 = df.copy()
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        df2 = df2.sort_values("Score", ascending=False).drop_duplicates(subset=["Tank Type"], keep="first")
        df2 = add_index(df2)
        a, b = range_arg
        df2 = df2[(df2['Ņ'] >= a) & (df2['Ņ'] <= b)]
        output_df = df2[[c for c in COLUMNS_C if c in df2.columns]]

    elif cmd == "p":
        a, b = range_arg
        df2 = add_index(df)
        df2 = df2[(df2['Ņ'] >= a) & (df2['Ņ'] <= b)]
        output_df = df2[[c for c in COLUMNS_DEFAULT if c in df2.columns]]

    elif cmd == "t":
        if len(parts) < 3:
            await message.channel.send("Provide a tank name!")
            return
        tank_query = parts[2].strip().lower()
        df2 = df.copy()
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        df2 = df2.sort_values("Score", ascending=False)
        df_filtered = df2[df2["Tank Type"].str.lower() == tank_query]
        if df_filtered.empty:
            await message.channel.send("No such tank found!")
            return
        df2 = add_index(df_filtered)
        if len(parts) > 3 and '-' in parts[3]:
            try:
                a, b = map(int, parts[3].split('-'))
                if b - a + 1 > 15:
                    await message.channel.send("Range is too big!")
                    return
                if a > LEGENDS or b > LEGENDS:
                    await message.channel.send("Not enough scores for that!")
                    return
                df2 = df2[(df2['Ņ'] >= a) & (df2['Ņ'] <= b)]
            except:
                pass
        else:
            df2 = df2[df2['Ņ'] == 1]
        output_df = df2[[c for c in COLUMNS_C if c in df2.columns]]

    else:
        await message.channel.send("Not a command buddy")
        return

    # --- FINAL FORMAT ---
    output_df = output_df.head(LEGENDS)
    shorten_tank = True if cmd in ['c', 'b', 'a', 't'] else False
    lines = dataframe_to_markdown_aligned(output_df, shorten_tank=shorten_tank)

    chunk = ""
    chunks = []
    for line in lines:
        if len(chunk) + len(line) + 1 <= 1900:
            chunk += line + "\n"
        else:
            chunks.append(chunk)
            chunk = line + "\n"
    if chunk:
        chunks.append(chunk)

    for c in chunks:
        await message.channel.send(f"```\n{c}\n```")

# --- RUN BOT ---
if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        print("❌ ERROR: DISCORD_TOKEN environment variable not set!")
        exit(1)
    keep_alive()
    bot.run(token)
