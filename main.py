import discord
from discord import ui
import pandas as pd
import requests
from io import BytesIO
from wcwidth import wcswidth
import os
import time
import random
import json
from keep_alive import keep_alive

# ---------------- CONFIG ----------------
DRIVE_FILE_ID = "1YMzE4FXjH4wctFektINwhCDjzZ0xqCP6"
TANKS_JSON_FILE_ID = "1pGcmeDcTqx2h_HXA_R24JbaqQiBHhYMQ"

COLUMNS_DEFAULT = ["Ņ", "Score", "True Name", "Tank Type", "Date"]
COLUMNS_C = ["Ņ", "Tank Type", "True Name", "Score", "Date"]
FIRST_COLUMN = "Score"
LEGENDS = 1000
MAX_RANGE = 15

PAGE_SIZE = 15
CACHE_SECONDS = 30
COOLDOWN_SECONDS = 5

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

# ---------------- STATE ----------------
user_cooldowns = {}
excel_cache = {"df": None, "time": 0}
day_cache = {}

# ---------------- LOADERS ----------------
def read_excel():
    now = time.time()
    if excel_cache["df"] is not None and now - excel_cache["time"] < CACHE_SECONDS:
        return excel_cache["df"].copy()
    url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"
    r = requests.get(url)
    r.raise_for_status()
    df = pd.read_excel(BytesIO(r.content))
    excel_cache["df"] = df
    excel_cache["time"] = now
    return df.copy()

def load_tanks():
    url = f"https://drive.google.com/uc?export=download&id={TANKS_JSON_FILE_ID}"
    return json.loads(requests.get(url).text)["tanks"]

TANK_NAMES = load_tanks()

# ---------------- HELPERS ----------------
def is_tejm(user):
    return user.name.lower() == "tejm_of_curonia"

def add_index(df):
    df = df.copy().reset_index(drop=True)
    df["Ņ"] = range(1, len(df) + 1)
    return df

def dataframe_to_markdown(df):
    df = df.copy()
    if FIRST_COLUMN in df.columns:
        df[FIRST_COLUMN] = pd.to_numeric(
            df[FIRST_COLUMN].astype(str).str.replace(',', ''), errors='coerce'
        ).fillna(0).apply(lambda v: f"{v/1_000_000:.3f} Mil")

    if "Date" in df.columns:
        df["Date"] = df["Date"].astype(str).str[:10]

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
    return "\n".join([header, sep] + body)

# ---------------- PAGINATION ----------------
class PageView(ui.View):
    def __init__(self, pages):
        super().__init__(timeout=300)
        self.pages = pages
        self.page = 0

    async def update(self, interaction):
        await interaction.response.edit_message(content=f"```\n{self.pages[self.page]}\n```", view=self)

    @ui.button(label="⬅ Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _):
        if self.page > 0:
            self.page -= 1
        await self.update(interaction)

    @ui.button(label="Next ➡", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, _):
        if self.page < len(self.pages) - 1:
            self.page += 1
        await self.update(interaction)

# ---------------- RANDOM VIEW ----------------
class RandomTankView(ui.View):
    def __init__(self, mode, df):
        super().__init__(timeout=120)
        self.mode = mode
        self.df = df

    @ui.button(label="Reroll", style=discord.ButtonStyle.primary)
    async def reroll(self, interaction: discord.Interaction, _):
        if self.mode == "a":
            row = self.df.sample(1).iloc[0]
            text = f"The Mountain recommends **{row['Tank Type']}** by **{row['Name in game']}**."
        elif self.mode == "b":
            excel_tanks = set(self.df["Tank Type"].str.lower())
            available = [t for t in TANK_NAMES if t.lower() not in excel_tanks]
            text = f"The Mountain recommends **{random.choice(available)}**."
        else:
            text = f"The Mountain recommends **{random.choice(TANK_NAMES)}**."
        await interaction.response.edit_message(content=text, view=self)

# ---------------- BOT ----------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

@bot.event
async def on_message(message):
    if message.author.bot:
        return
    if not message.content.startswith("!olymp;"):
        return

    # ---------------- COOLDOWN ----------------
    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts = message.content.split(";")
    cmd = parts[1].lower()
    df = read_excel()
    df.columns = [str(c).strip() for c in df.columns]

    # ---------------- HELP ----------------
    if cmd == "help":
        await message.channel.send(
            "**Commands:**\n"
            "!olymp;a;start-end - All scores (Tejm only)\n"
            "!olymp;b;start-end - Best players\n"
            "!olymp;n;Player;start-end - Player scores\n"
            "!olymp;c;start-end - Best per tank\n"
            "!olymp;p;start-end - Part of leaderboard\n"
            "!olymp;t;Tank;start-end - Best tank scores\n"
            "!olymp;d;YYYY-MM-DD - Scores from date\n"
            "!olymp;r;a|b|r - Random recommendation"
        )
        return

    # ---------------- RANDOM ----------------
    if cmd == "r":
        if len(parts) < 3:
            await message.channel.send("Use `!olymp;r;a|b|r`")
            return
        mode = parts[2].lower()
        view = RandomTankView(mode, df)
        await view.reroll.callback(view, None)
        return

    # ---------------- PARSE RANGE ----------------
    start, end = 1, PAGE_SIZE
    for p in parts:
        if '-' in p:
            try:
                s, e = map(int, p.split('-'))
                if e - s + 1 > MAX_RANGE:
                    await message.channel.send(f"Range too big! Max {MAX_RANGE}")
                    return
                start, end = s, e
            except: pass

    df2 = None
    shorten_tank = True
    current_day = None

    # ---------------- COMMANDS ----------------
    if cmd == "a":  # All scores Tejm
        if not is_tejm(message.author):
            await message.channel.send("Only Tejm allowed!")
            return
        df2 = add_index(df)[COLUMNS_DEFAULT]
        df2 = df2[(df2['Ņ'] >= start) & (df2['Ņ'] <= end)]

    elif cmd == "b":  # Top players
        df2 = df.copy()
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        df2 = df2.sort_values("Score", ascending=False).drop_duplicates("True Name")
        df2 = add_index(df2)[COLUMNS_DEFAULT]
        df2 = df2[(df2['Ņ'] >= start) & (df2['Ņ'] <= end)]

    elif cmd == "n":  # Player name
        if len(parts) < 3:
            return
        name = parts[2].lower()
        df2 = df[df["True Name"].str.lower() == name]
        df2 = add_index(df2)[COLUMNS_DEFAULT]
        df2 = df2[(df2['Ņ'] >= start) & (df2['Ņ'] <= end)]

    elif cmd == "c":  # Best per tank
        df2 = df.copy()
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        df2 = df2.sort_values("Score", ascending=False).drop_duplicates("Tank Type")
        df2 = add_index(df2)[COLUMNS_C]
        df2 = df2[(df2['Ņ'] >= start) & (df2['Ņ'] <= end)]

    elif cmd == "p":  # Leaderboard slice
        df2 = add_index(df)[COLUMNS_DEFAULT]
        df2 = df2[(df2['Ņ'] >= start) & (df2['Ņ'] <= end)]

    elif cmd == "t":  # Tank
        if len(parts) < 3:
            return
        tank = parts[2].lower()
        df2 = df[df["Tank Type"].str.lower() == tank]
        df2 = add_index(df2)[COLUMNS_C]
        df2 = df2[(df2['Ņ'] >= start) & (df2['Ņ'] <= end)]

    elif cmd == "d":  # Date
        if len(parts) < 3:
            return
        current_day = parts[2]
        df2 = df[df["Date"].astype(str).str[:10] == current_day]
        df2 = add_index(df2)[COLUMNS_DEFAULT]

    if df2 is None or df2.empty:
        return

    # ---------------- PAGINATE ----------------
    pages = []
    for i in range(0, len(df2), PAGE_SIZE):
        pages.append(dataframe_to_markdown(df2.iloc[i:i+PAGE_SIZE]))

    view = PageView(pages)
    await message.channel.send(f"```\n{pages[0]}\n```", view=view)

# ---------------- RUN ----------------
if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
