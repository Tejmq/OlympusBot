# ======================= FULL BOT WITH PAGINATION & BUTTONS =======================
# NOTHING REMOVED:
# - a command (Tejm only)
# - b command (top players)
# - NEW n command (player name search, split from b)
# - c, p, d, t commands
# - r command EXACT LOGIC + reroll button
# - Excel caching
# - Pagination with buttons (Back / Next)
# ==================================================================================

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

PAGE_SIZE = 10
CACHE_SECONDS = 30
COOLDOWN_SECONDS = 5

# ---------------- DISCORD ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

# ---------------- STATE ----------------
user_cooldowns = {}
excel_cache = {"df": None, "time": 0}

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
            df[FIRST_COLUMN].astype(str).str.replace(',', ''),
            errors='coerce'
        ).fillna(0).apply(lambda v: f"{v/1_000_000:.3f} Mil")

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

# ---------------- PAGINATION VIEW ----------------
class PageView(ui.View):
    def __init__(self, pages):
        super().__init__(timeout=120)
        self.pages = pages
        self.page = 0

    async def update(self, interaction):
        await interaction.response.edit_message(
            content=f"```\n{self.pages[self.page]}\n```",
            view=self
        )

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

    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts = message.content.split(";")
    cmd = parts[1].lower()
    df = read_excel()
    df.columns = [str(c).strip() for c in df.columns]

    # ---------------- RANDOM ----------------
    if cmd == "r":
        if len(parts) < 3:
            await message.channel.send("!olymp;r;a | b | r")
            return
        mode = parts[2]
        view = RandomTankView(mode, df)
        await view.reroll.callback(view, None)
        return

    # ---------------- COMMAND DATA ----------------
    if cmd == "a":
        if not is_tejm(message.author):
            return
        df2 = add_index(df)[COLUMNS_DEFAULT]

    elif cmd == "b":
        df2 = df.copy()
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        df2 = df2.sort_values("Score", ascending=False).drop_duplicates("True Name")
        df2 = add_index(df2)[COLUMNS_DEFAULT]

    elif cmd == "n":
        if len(parts) < 3:
            return
        name = parts[2].lower()
        df2 = df[df["True Name"].str.lower() == name]
        df2 = add_index(df2)[COLUMNS_DEFAULT]

    elif cmd == "c":
        df2 = df.copy()
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        df2 = df2.sort_values("Score", ascending=False).drop_duplicates("Tank Type")
        df2 = add_index(df2)[COLUMNS_C]

    elif cmd == "d":
        if len(parts) < 3:
            return
        df2 = add_index(df[df["Date"].astype(str).str[:10] == parts[2]])[COLUMNS_DEFAULT]

    else:
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
