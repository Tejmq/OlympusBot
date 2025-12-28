import discord
import pandas as pd
import requests
from io import BytesIO
from wcwidth import wcswidth
import os
from keep_alive import keep_alive
import re
import random  # added for random recommendation
import time
import json

user_cooldowns = {}
COOLDOWN_SECONDS = 5

# --- CONFIG ---
DRIVE_FILE_ID = "1YMzE4FXjH4wctFektINwhCDjzZ0xqCP6"
COLUMNS_DEFAULT = ["Ņ", "Score", "True Name", "Tank Type", "Date"]
COLUMNS_C = ["Ņ", "Tank Type", "True Name", "Score", "Date"]
FIRST_COLUMN = "Score"
LEGENDS = 1000
TANKS_JSON_FILE_ID = "1pGcmeDcTqx2h_HXA_R24JbaqQiBHhYMQ"


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



# --- tank json reader ---
def load_tanks_from_drive(file_id):
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.text)

tank_data = load_tanks_from_drive(TANKS_JSON_FILE_ID)
TANK_NAMES = tank_data["tanks"]




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

    # shorten tank types if requested
    if shorten_tank and "Tank Type" in df.columns:
        def shorten_name(name):
            s = str(name)
            repl_map = {'triple': 'T', 'auto': 'A', 'hexa': 'H'}
            low = s.lower()
            for key, val in repl_map.items():
                low = low.replace(key, val)
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
            # 🔒 COOLDOWN GOES HERE
    now = time.time()
    last = user_cooldowns.get(message.author.id, 0)

    if now - last < COOLDOWN_SECONDS:
        return  # ignore spam

    user_cooldowns[message.author.id] = now
    # 🔒 END COOLDOWN

    parts = txt.split(";")
    if len(parts) < 2:
        await message.channel.send("Not a command buddy")
        return

    cmd = parts[1].strip().lower()

    # --- HELP ---
    if cmd == "help":
        help_message = (
            "Commands:\n"
            "!olymp;b;1-15                 - Best scores of each player\n"
            "!olymp;b;Player;1-15          - Best scores of specific player\n"
            "!olymp;c;1-15                 - Best per tank\n"
            "!olymp;p;1-15                 - Part of scoreboard\n"
            "!olymp;t;TankName;1-15        - Best score of a tank\n"
            "!olymp;d;YYYY-MM-DD           - Scores from that date\n"
            "!olymp;r                      - Random recommendation\n"
        )
        await message.channel.send(help_message)
        return

    df = read_excel()
    if df.empty:
        await message.channel.send("❌ Failed to fetch Excel from Google Drive!")
        return
    df.columns = [str(c).strip() for c in df.columns]
    output_df = None

    range_arg = None
    if cmd in ['c', 'p']:
        if len(parts) > 2 and '-' in parts[-1]:
            try:
                a, b = map(int, parts[-1].split('-'))
                if b - a + 1 > 15:
                    await message.channel.send("Range is too big!")
                    return
                if a > LEGENDS or b > LEGENDS:
                    await message.channel.send("Not enough scores for that!")
                    return
                range_arg = (a, b)
            except:
                pass
        if range_arg is None:
            await message.channel.send("Input range! (1-15)")
            return

    if cmd == 'b':
        if len(parts) > 2 and '-' in parts[2]:
            try:
                a, b = map(int, parts[2].split('-'))
                range_arg = (a, b)
            except:
                pass
        elif len(parts) > 3 and '-' in parts[3]:
            try:
                a, b = map(int, parts[3].split('-'))
                range_arg = (a, b)
            except:
                pass
        if range_arg is None:
            range_arg = (1, 1)

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
        player_name = None
        if len(parts) > 2 and '-' not in parts[2]:
            player_name = parts[2].strip()
        if player_name:
            df2 = df2[df2["True Name"].str.lower() == player_name.lower()]
            df2 = df2.sort_values("Score", ascending=False)
        else:
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
        df_filtered = df2[df2["Tank Type"].astype(str).str.lower() == tank_query]
        if df_filtered.empty:
            await message.channel.send("No such tank found!")
            return
        df2 = add_index(df_filtered)
        if len(parts) > 3 and '-' in parts[3]:
            try:
                a, b = map(int, parts[3].split('-'))
                df2 = df2[(df2['Ņ'] >= a) & (df2['Ņ'] <= b)]
            except:
                pass
        else:
            df2 = df2[df2['Ņ'] == 1]
        output_df = df2[[c for c in COLUMNS_C if c in df2.columns]]

    elif cmd == "d":
        if len(parts) < 3:
            await message.channel.send("Provide a date in DD-MM-YYYY format!")
            return
        target_date = parts[2].strip()
        df2 = df.copy()
        df2["Date"] = df2["Date"].astype(str).str[:10]
        df2 = df2[df2["Date"] == target_date]
        if df2.empty:
            await message.channel.send("No scores found for that date!")
            return
        df2 = add_index(df2)
        output_df = df2[[c for c in COLUMNS_DEFAULT if c in df2.columns]]
        # Disable tank type shortening for d command
        shorten_tank = True



    elif cmd == "r":

        # No subcommand → show help
        if len(parts) == 2:
            await message.channel.send(
                "!olymp;r;a for a tank with a player record!\n"
                "!olymp;r;b for the tank with no score!"
            )
            return

        subcmd = parts[2].strip().lower()

        # --- r;a ---
        if subcmd == "a":
            if df.empty:
                await message.channel.send("No data available for recommendation!")
                return

            row = df.sample(1).iloc[0]
await message.channel.send(
    f"**{row['Name in game']}** recommends you **{row['Tank Type']}**."
)

            return

        # --- r;b ---
        elif subcmd == "b":
            excel_tanks = set(
                df["Tank Type"].astype(str).str.lower().str.strip()
            )

            available_tanks = [
                t for t in TANK_NAMES
                if t.lower().strip() not in excel_tanks
            ]

            if not available_tanks:
                await message.channel.send(
                    "🏔️ The Mountain has no new tanks left to recommend."
                )
                return

            chosen_tank = random.choice(available_tanks)
            await message.channel.send(
                f"🏔️ **The Mountain recommends you {chosen_tank}.**"
            )
            return

        else:
            await message.channel.send(
                "Unknown r command. Use !olymp;r to see options."
            )
            return





    # --- FINAL FORMAT ---
    output_df = output_df.head(LEGENDS)
    if cmd != 'd':
        shorten_tank = True
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
