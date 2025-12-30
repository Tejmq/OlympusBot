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
TANK_NAMES = []

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
    global TANK_NAMES
    url = f"https://drive.google.com/uc?export=download&id={TANKS_JSON_FILE_ID}"
    TANK_NAMES = json.loads(requests.get(url).text)["tanks"]

# ---------------- TABLE TO MARKDOWN ----------------
from wcwidth import wcswidth

def dataframe_to_markdown_aligned(df, shorten_tank=False):
    df = df.copy()
    if FIRST_COLUMN in df.columns:
        df[FIRST_COLUMN] = pd.to_numeric(
            df[FIRST_COLUMN].astype(str).str.replace(',', ''), errors='coerce'
        ).fillna(0).apply(lambda v: f"{v/1_000_000:,.3f} Mil")
    if "Date" in df.columns:
        df["Date"] = df["Date"].astype(str).str[:10]
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
    lines = [fmt_row(r) for r in [df.columns] + list(df.values)]
    return lines

# ---------------- EMBED SENDER ----------------
async def send_leaderboard(channel, title, lines, page=1, total=1):
    text = "\n".join(lines)
    embed = Embed(
        title=title,
        description=f"```text\n{text}\n```",
        color=discord.Color.dark_grey()
    )
    embed.set_footer(text=f"Page {page}/{total}")
    await channel.send(embed=embed)

# ---------------- HELPERS ----------------
def is_tejm(user):
    return str(user.name).lower() == "tejm_of_curonia"

def add_index(df):
    df = df.copy().reset_index(drop=True)
    df["Ņ"] = range(1, len(df)+1)
    return df

# ---------------- PAGINATION ----------------
class PageView(ui.View):
    def __init__(self, df, start, end, range_size, title):
        super().__init__(timeout=300)
        self.df = df
        self.start = start
        self.end = end
        self.range_size = range_size
        self.title = title
        self.total_pages = (len(df) + range_size -1)//range_size
        self.page_num = (start-1)//range_size +1

    async def update(self, interaction):
        slice_df = self.df.iloc[self.start-1:self.end]
        lines = dataframe_to_markdown_aligned(slice_df)
        await send_leaderboard(interaction.channel, self.title, lines, self.page_num, self.total_pages)

    @ui.button(label="⬅ Prev", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _):
        if self.start > 1:
            self.start = max(1, self.start - self.range_size)
            self.end = self.start + self.range_size -1
            if self.end > len(self.df):
                self.end = len(self.df)
            self.page_num = (self.start-1)//self.range_size +1
        await self.update(interaction)

    @ui.button(label="Next ➡", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, _):
        if self.end < len(self.df):
            self.start = self.end +1
            self.end = self.start + self.range_size -1
            if self.end > len(self.df):
                self.end = len(self.df)
            self.page_num = (self.start-1)//self.range_size +1
        await self.update(interaction)

# ---------------- RANDOM ----------------
class RandomTankView(ui.View):
    def __init__(self, mode, df):
        super().__init__(timeout=120)
        self.mode = mode
        self.df = df

    @ui.button(label="Reroll", style=discord.ButtonStyle.primary)
    async def reroll(self, interaction: discord.Interaction, _):
        text = ""
        if self.mode=="a":
            if self.df.empty:
                text="No data available for recommendation!"
            else:
                row = self.df.sample(1).iloc[0]
                text=f"**{row['Name in game']}** recommends you **{row['Tank Type']}**."
        elif self.mode=="b":
            excel_tanks=set(self.df["Tank Type"].astype(str).str.lower())
            available=[t for t in TANK_NAMES if t.lower() not in excel_tanks]
            if not available:
                text="The Mountain has no new tanks left to recommend."
            else:
                text=f"The Mountain recommends **{random.choice(available)}**."
        elif self.mode=="r":
            if not TANK_NAMES:
                text="The Mountain has no tanks to recommend."
            else:
                text=f"The Mountain recommends **{random.choice(TANK_NAMES)}**."
        embed=Embed(title="Random Recommendation", description=text, color=discord.Color.dark_grey())
        await interaction.response.edit_message(embed=embed, view=self)

# ---------------- BOT ----------------
@bot.event
async def on_ready():
    load_tanks()
    print(f"Logged in as {bot.user}")

@bot.event
async def on_message(message):
    if message.author.bot:
        return
    if not message.content.startswith("!olymp;"):
        return

    # ---------------- COOLDOWN ----------------
    now = time.time()
    if now - user_cooldowns.get(message.author.id,0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts=message.content.split(";")
    cmd=parts[1].lower()
    df=read_excel()
    df.columns=[str(c).strip() for c in df.columns]

    # ---------------- HELP ----------------
    if cmd=="help":
        help_text=(
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
    if cmd=="r":
        if len(parts)==2:
            await message.channel.send(
                "**!olymp;r;a** for a tank with a player record!\n"
                "**!olymp;r;b** for the tank with no score!\n"
                "**!olymp;r;r** for a fully random tank!"
            )
            return
        subcmd=parts[2].lower()
        view=RandomTankView(subcmd, df)
        await view.reroll.callback(view,None)
        return

    # ---------------- RANGE ----------------
    start,end=1,MAX_RANGE
    range_size=MAX_RANGE
    for p in parts:
        if '-' in p:
            try:
                s,e=map(int,p.split('-'))
                if e-s+1>MAX_RANGE:
                    await message.channel.send(f"Range too big! Max {MAX_RANGE}")
                    return
                start,end=s,e
                range_size=e-s+1
            except: pass

    df2=None
    title="Leaderboard"

    # ---------------- COMMANDS ----------------
    if cmd=="a":
        if not is_tejm(message.author):
            await message.channel.send("Only Tejm allowed!")
            return
        df2=add_index(df)[COLUMNS_DEFAULT]

    elif cmd=="b":
        df2=df.copy()
        df2["Score"]=pd.to_numeric(df2["Score"].astype(str).str.replace(',',''),errors='coerce').fillna(0)
        df2=df2.sort_values("Score",ascending=False).drop_duplicates("True Name")
        df2=add_index(df2)[COLUMNS_DEFAULT]

    elif cmd=="n":
        if len(parts)<3:
            return
        name=parts[2].lower()
        df2=df[df["True Name"].str.lower()==name]
        df2=add_index(df2)[COLUMNS_DEFAULT]

    elif cmd=="c":
        df2=df.copy()
        df2["Score"]=pd.to_numeric(df2["Score"].astype(str).str.replace(',',''),errors='coerce').fillna(0)
        df2=df2.sort_values("Score",ascending=False).drop_duplicates("Tank Type")
        df2=add_index(df2)[COLUMNS_C]

    elif cmd=="p":
        df2=add_index(df)[COLUMNS_DEFAULT]

    elif cmd=="t":
        if len(parts)<3:
            return
        tank=parts[2].lower()
        df2=df[df["Tank Type"].str.lower()==tank]
        df2=add_index(df2)[COLUMNS_C]

    elif cmd=="d":
        if len(parts)<3:
            return
        current_day=parts[2]
        df2=df[df["Date"].astype(str).str[:10]==current_day]
        df2=add_index(df2)[COLUMNS_DEFAULT]

    if df2 is None or df2.empty:
        await message.channel.send("No results found.")
        return

    # ---------------- PAGINATION ----------------
    view=PageView(df2,start,min(end,len(df2)),range_size,title)
    lines=dataframe_to_markdown_aligned(df2.iloc[start-1:min(end,len(df2))])
    await send_leaderboard(message.channel,title,lines,view.page_num,view.total_pages)
    await message.channel.send(view=view)

# ---------------- RUN ----------------
if __name__=="__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
