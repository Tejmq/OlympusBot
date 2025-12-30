import discord
import pandas as pd
import requests
from io import BytesIO
from wcwidth import wcswidth
import os
import random
import time
import json
from keep_alive import keep_alive

# ---------------- CONFIG ----------------
DRIVE_FILE_ID = "1YMzE4FXjH4wctFektINwhCDjzZ0xqCP6"
TANKS_JSON_FILE_ID = "1pGcmeDcTqx2h_HXA_R24JbaqQiBHhYMQ"

COLUMNS_DEFAULT = ["Ņ", "Score", "True Name", "Tank Type", "Date"]
COLUMNS_C = ["Ņ", "Tank Type", "True Name", "Score", "Date"]
FIRST_COLUMN = "Score"
PAGE_SIZE = 10
COOLDOWN_SECONDS = 5

user_cooldowns = {}

# ---------------- DISCORD ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

# ---------------- DATA ----------------
def read_excel():
    url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"
    r = requests.get(url)
    r.raise_for_status()
    return pd.read_excel(BytesIO(r.content))

def load_tanks():
    url = f"https://drive.google.com/uc?export=download&id={TANKS_JSON_FILE_ID}"
    return json.loads(requests.get(url).text)["tanks"]

TANK_NAMES = load_tanks()

# ---------------- HELPERS ----------------
def add_index(df):
    df = df.reset_index(drop=True)
    df["Ņ"] = range(1, len(df) + 1)
    return df

def dataframe_to_text(df):
    rows = [df.columns.tolist()] + df.values.tolist()
    widths = [max(wcswidth(str(r[i])) for r in rows) for i in range(len(rows[0]))]

    def fmt(r):
        return "  ".join(
            str(c) + " " * (widths[i] - wcswidth(str(c)))
            for i, c in enumerate(r)
        )

    lines = [fmt(df.columns), "-" * wcswidth(fmt(df.columns))]
    lines += [fmt(r) for r in df.values]
    return "\n".join(lines)

# ---------------- VIEW ----------------
class LeaderboardView(discord.ui.View):
    def __init__(self, user_id, cmd, start, param=None):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.cmd = cmd
        self.start = start
        self.param = param

    async def interaction_check(self, interaction):
        return interaction.user.id == self.user_id

    async def render(self):
        df = read_excel()
        df.columns = df.columns.str.strip()
        df["Score"] = pd.to_numeric(df["Score"].astype(str).str.replace(',', ''), errors="coerce").fillna(0)

        # ---- COMMAND LOGIC ----
        if self.cmd == "a":
            df = add_index(df)
            title = "Full Scoreboard"
            cols = COLUMNS_DEFAULT

        elif self.cmd == "b":
            df = df.sort_values("Score", ascending=False).drop_duplicates("True Name")
            df = add_index(df)
            title = "Best Player Scores"
            cols = COLUMNS_DEFAULT

        elif self.cmd == "n":
            df = df[df["True Name"].str.lower() == self.param.lower()]
            df = df.sort_values("Score", ascending=False)
            df = add_index(df)
            title = f"Player: {self.param}"
            cols = COLUMNS_DEFAULT

        elif self.cmd == "c":
            df = df.sort_values("Score", ascending=False).drop_duplicates("Tank Type")
            df = add_index(df)
            title = "Best Tank Scores"
            cols = COLUMNS_C

        else:
            return None

        page = df.iloc[self.start:self.start + PAGE_SIZE][cols]
        text = dataframe_to_text(page)

        embed = discord.Embed(
            title=title,
            description=f"```text\n{text}\n```",
            color=discord.Color.dark_gold()
        )
        embed.set_footer(text=f"Rows {self.start+1}–{self.start+len(page)}")
        return embed

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction, _):
        await interaction.response.defer()
        self.start = max(0, self.start - PAGE_SIZE)
        await interaction.message.edit(embed=await self.render(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next(self, interaction, _):
        await interaction.response.defer()
        self.start += PAGE_SIZE
        await interaction.message.edit(embed=await self.render(), view=self)

# ---------------- BOT ----------------
@bot.event
async def on_message(message):
    if message.author.bot or not message.content.startswith("!olymp;"):
        return

    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts = message.content.split(";")
    cmd = parts[1].lower()

    # -------- HELP --------
    if cmd == "help":
        embed = discord.Embed(title="Olympus Commands", color=discord.Color.blurple())
        embed.add_field(name="Leaderboards",
            value=(
                "`!olymp;a`\n"
                "`!olymp;b`\n"
                "`!olymp;n;PlayerName`\n"
                "`!olymp;c`\n"
                "`!olymp;p;1-10`\n"
                "`!olymp;t;Tank`\n"
                "`!olymp;d;YYYY-MM-DD`"
            ), inline=False)
        embed.add_field(name="Random",
            value="`!olymp;r;a` `!olymp;r;b` `!olymp;r;r`", inline=False)
        await message.channel.send(embed=embed)
        return

    # -------- RANDOM --------
    if cmd == "r":
        if parts[2] == "r":
            await message.channel.send(f"Recommendation: {random.choice(TANK_NAMES)}")
        return

    # -------- START VIEW --------
    param = parts[2] if len(parts) > 2 else None
    view = LeaderboardView(message.author.id, cmd, 0, param)
    embed = await view.render()

    if embed:
        await message.channel.send(embed=embed, view=view)

# ---------------- RUN ----------------
if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
