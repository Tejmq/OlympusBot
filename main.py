import discord
from discord import ui, Embed
import pandas as pd
import requests
from io import BytesIO
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

def dataframe_to_embed(df, title="Leaderboard"):
    embed = Embed(title=title)
    for i, row in df.iterrows():
        value = f"Score: {row.get('Score','')}\nTank: {row.get('Tank Type','')}\nDate: {row.get('Date','')}"
        embed.add_field(name=f"{row.get('Ņ','')} - {row.get('True Name','')}", value=value, inline=False)
    return embed

# ---------------- PAGINATION VIEW ----------------
class PageView(ui.View):
    def __init__(self, df, start, end, range_size, columns, title):
        super().__init__(timeout=300)
        self.df = df.copy()
        self.start = start
        self.end = end
        self.range_size = range_size
        self.columns = columns
        self.title = title

    def current_slice(self):
        return self.df.iloc[self.start-1:self.end]

    async def update(self, interaction):
        embed = dataframe_to_embed(self.current_slice(), self.title)
        await interaction.response.edit_message(embed=embed, view=self)

    @ui.button(label="⬅ Prev", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _):
        if self.start > 1:
            self.start = max(1, self.start - self.range_size)
            self.end = self.start + self.range_size - 1
            if self.end > len(self.df):
                self.end = len(self.df)
        await self.update(interaction)

    @ui.button(label="Next ➡", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, _):
        if self.end < len(self.df):
            self.start = self.end + 1
            self.end = self.start + self.range_size - 1
            if self.end > len(self.df):
                self.end = len(self.df)
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
            text = f"**{row['Name in game']}** recommends you **{row['Tank Type']}**."
        elif self.mode == "b":
            excel_tanks = set(self.df["Tank Type"].astype(str).str.lower())
            available = [t for t in TANK_NAMES if t.lower() not in excel_tanks]
            if not available:
                text = "The Mountain has no new tanks left to recommend."
            else:
                text = f"The Mountain recommends you **{random.choice(available)}**."
        else:
            text = f"The Mountain recommends you **{random.choice(TANK_NAMES)}**."
        embed = Embed(title="Random Recommendation", description=text)
        await interaction.response.edit_message(embed=embed, view=self)

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

    # ---------------- HELP ----------------
    if cmd == "help":
        help_text = (
            "**Commands:**\n"
            "!olymp;a;start-end - All scores (Tejm only)\n"
            "!olymp;b;start-end - Best players\n"
            "!olymp;n;Player;start-end - Player scores\n"
            "!olymp;c;start-end - Best per tank\n"
            "!olymp;p;start-end - Part of leaderboard\n"
            "!olymp;t;Tank;start-end - Best tank scores\n"
            "!olymp;d;YYYY-MM-DD - Scores from date\n"
            "!olymp;r;a|b|r - Random recommendation\n"
        )
        await message.channel.send(help_text)
        return

    # ---------------- RANDOM ----------------
    if cmd == "r":
        if len(parts) == 2:
            await message.channel.send(
                "**!olymp;r;a** for a tank with a player record!\n"
                "**!olymp;r;b** for the tank with no score!\n"
                "**!olymp;r;r** for a fully random tank!"
            )
            return
        subcmd = parts[2].strip().lower()
        view = RandomTankView(subcmd, df)
        # initial display
        await view.reroll.callback(view, None)
        return

    # ---------------- RANGE ----------------
    start, end = 1, MAX_RANGE
    range_size = MAX_RANGE
    for p in parts:
        if '-' in p:
            try:
                s, e = map(int, p.split('-'))
                if e - s + 1 > MAX_RANGE:
                    await message.channel.send(f"Range too big! Max {MAX_RANGE}")
                    return
                start, end = s, e
                range_size = e - s + 1
            except: pass

    df2 = None
    shorten_tank = True
    title = "Leaderboard"

    # ---------------- COMMANDS ----------------
    if cmd == "a":
        if not is_tejm(message.author):
            await message.channel.send("Only Tejm allowed!")
            return
        df2 = add_index(df)[COLUMNS_DEFAULT]

    elif cmd == "b":
        df2 = df.copy()
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
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
        df2["Score"] = pd.to_numeric(df2["Score"].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        df2 = df2.sort_values("Score", ascending=False).drop_duplicates("Tank Type")
        df2 = add_index(df2)[COLUMNS_C]

    elif cmd == "p":
        df2 = add_index(df)[COLUMNS_DEFAULT]

    elif cmd == "t":
        if len(parts) < 3:
            return
        tank = parts[2].lower()
        df2 = df[df["Tank Type"].str.lower() == tank]
        df2 = add_index(df2)[COLUMNS_C]

    elif cmd == "d":
        if len(parts) < 3:
            return
        current_day = parts[2]
        df2 = df[df["Date"].astype(str).str[:10] == current_day]
        df2 = add_index(df2)[COLUMNS_DEFAULT]

    if df2 is None or df2.empty:
        await message.channel.send("No results found.")
        return

    # ---------------- PAGINATION ----------------
    view = PageView(df2, start, min(end, len(df2)), range_size, df2.columns, title)
    embed = dataframe_to_embed(df2.iloc[start-1:min(end,len(df2))], title)
    await message.channel.send(embed=embed, view=view)

# ---------------- RUN ----------------
if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
