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
    url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"
    r = requests.get(url)
    r.raise_for_status()
    return pd.read_excel(BytesIO(r.content))

def load_tanks_from_drive(file_id):
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.text)

TANK_NAMES = load_tanks_from_drive(TANKS_JSON_FILE_ID)["tanks"]

# ---------------- HELPERS ----------------
def add_index(df):
    df = df.copy().reset_index(drop=True)
    df["Ņ"] = range(1, len(df) + 1)
    return df

# ---------------- TABLE FORMAT ----------------
def dataframe_to_lines(df, shorten_tank=True):
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
        df["Tank Type"] = (
            df["Tank Type"]
            .astype(str)
            .str.replace("triple", "T", case=False)
            .str.replace("auto", "A", case=False)
            .str.replace("hexa", "H", case=False)
            .str.title()
            .str[:10]
        )

    rows = [df.columns.tolist()] + df.values.tolist()
    widths = [max(wcswidth(str(r[i])) for r in rows) for i in range(len(rows[0]))]

    def fmt(r):
        return "  ".join(
            str(c) + " " * (widths[i] - wcswidth(str(c)))
            for i, c in enumerate(r)
        )

    lines = [fmt(df.columns), "-" * wcswidth(fmt(df.columns))]
    lines += [fmt(r) for r in df.values]
    return lines

# ---------------- PAGINATED VIEW ----------------
class TableView(discord.ui.View):
    def __init__(self, user_id, title, pages):
        super().__init__(timeout=90)
        self.user_id = user_id
        self.title = title
        self.pages = pages
        self.index = 0

    def embed(self):
        embed = discord.Embed(
            title=self.title,
            description=f"```text\n{self.pages[self.index]}\n```",
            color=discord.Color.dark_gold()
        )
        embed.set_footer(text=f"Page {self.index + 1}/{len(self.pages)}")
        return embed

    async def interaction_check(self, interaction):
        return interaction.user.id == self.user_id

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction, button):
        if self.index > 0:
            self.index -= 1
            await interaction.response.edit_message(embed=self.embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next(self, interaction, button):
        if self.index < len(self.pages) - 1:
            self.index += 1
            await interaction.response.edit_message(embed=self.embed(), view=self)

    async def on_timeout(self):
        for c in self.children:
            c.disabled = True

# ---------------- BOT EVENTS ----------------
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

    if cmd == "help":
        embed = discord.Embed(title="Olympus Commands", color=discord.Color.blurple())
        embed.add_field(
            name="Leaderboards",
            value=(
                "`!olymp;b;1-15`\n"
                "`!olymp;c;1-15`\n"
                "`!olymp;p;1-15`\n"
                "`!olymp;t;Tank;1-15`\n"
                "`!olymp;d;YYYY-MM-DD`"
            ),
            inline=False
        )
        await message.channel.send(embed=embed)
        return

    df = read_excel()
    df.columns = df.columns.str.strip()

    # Range
    a, b = 1, 10
    if "-" in parts[-1]:
        try:
            a, b = map(int, parts[-1].split("-"))
        except:
            pass

    if cmd == "b":
        df["Score"] = pd.to_numeric(df["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        df = df.sort_values("Score", ascending=False).drop_duplicates("True Name")
        df = add_index(df)
        out = df[(df["Ņ"] >= a) & (df["Ņ"] <= b)][COLUMNS_DEFAULT]
        title = "Best Player Scores"

    elif cmd == "c":
        df["Score"] = pd.to_numeric(df["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)
        df = df.sort_values("Score", ascending=False).drop_duplicates("Tank Type")
        df = add_index(df)
        out = df[(df["Ņ"] >= a) & (df["Ņ"] <= b)][COLUMNS_C]
        title = "Best Tank Scores"

    else:
        await message.channel.send("Unknown command.")
        return

    lines = dataframe_to_lines(out)
    pages, buf = [], ""

    for l in lines:
        if len(buf) + len(l) < 1800:
            buf += l + "\n"
        else:
            pages.append(buf)
            buf = l + "\n"
    pages.append(buf)

    view = TableView(message.author.id, title, pages)
    await message.channel.send(embed=view.embed(), view=view)

# ---------------- RUN ----------------
if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
