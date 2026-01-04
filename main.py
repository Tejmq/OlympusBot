import discord
import pandas as pd
import requests
from io import BytesIO
from wcwidth import wcswidth
import os, time, json, random, re
from keep_alive import keep_alive
from discord import Embed
from discord import ui, Interaction
from threading import Lock

FETCH_LOCK = Lock()

DRIVE_FILE_ID = "1YMzE4FXjH4wctFektINwhCDjzZ0xqCP6"
TANKS_JSON_URL = "https://raw.githubusercontent.com/Tejmq/OlympusBot/refs/heads/main/data/tanks.json"


COLUMNS_DEFAULT = ["Ņ", "Score", "True Name", "Tank Type", "Date"]
COLUMNS_C = ["Ņ", "Tank Type", "True Name", "Score", "Date"]

FIRST_COLUMN = "Score"
LEGENDS = 1000
COOLDOWN_SECONDS = 7
user_cooldowns = {}

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

DATAFRAME_CACHE = None
LAST_FETCH = 0
CACHE_TTL = 300  # 5 minutes

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DiscordBot/1.0)",
    "Accept": "*/*"
}

def read_excel_cached():
    global DATAFRAME_CACHE, LAST_FETCH

    now = time.time()
    url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"

    with FETCH_LOCK:
        if DATAFRAME_CACHE is None or now - LAST_FETCH > CACHE_TTL:
            try:
                r = requests.get(url, headers=HEADERS, timeout=10)
                r.raise_for_status()
                DATAFRAME_CACHE = pd.read_excel(BytesIO(r.content))
                LAST_FETCH = now
                print("Excel cache refreshed")
            except Exception as e:
                print("Excel fetch failed:", e)
                return pd.DataFrame()

    return DATAFRAME_CACHE.copy()




TANK_NAMES = []

def load_tanks():
    global TANK_NAMES
    # Return cached list if already loaded
    if TANK_NAMES:
        return TANK_NAMES

    try:
        r = requests.get(TANKS_JSON_URL, timeout=10)
        r.raise_for_status()
        TANK_NAMES = r.json()["tanks"]
        print("Tank list loaded from GitHub")
    except Exception as e:
        print("Tank list load failed:", e)
        TANK_NAMES = []

    return TANK_NAMES


@bot.event
async def on_ready():
    try:
        read_excel_cached()
        print("Initial data load OK")
    except Exception as e:
        print("Initial data load failed:", e)

    global TANK_NAMES
    TANK_NAMES = load_tanks() 
    print(f"Logged in as {bot.user}")



class RangePaginationView(ui.View):
    def __init__(self, df, start_index, range_size, title, shorten_tank):
        super().__init__(timeout=300)
        self.df = df.reset_index(drop=True)
        self.range_size = range_size
        self.title = title
        self.shorten_tank = shorten_tank

        # Start page calculation
        self.page = (start_index - 1) // range_size
        self.max_page = (len(self.df) - 1) // range_size

    def get_slice(self):
        start = self.page * self.range_size
        end = min(start + self.range_size, len(self.df))
        # Clamp in case start < 0
        if start < 0:
            start, end = 0, min(self.range_size, len(self.df))
        return self.df.iloc[start:end], start, end

    async def update(self, interaction: Interaction):
        slice_df, start, end = self.get_slice()

        # Global numbering
        slice_df = slice_df.copy()
        slice_df["Ņ"] = range(start + 1, end + 1)

        lines = dataframe_to_markdown_aligned(slice_df, self.shorten_tank)

        embed = Embed(
            title=self.title,
            description=f"```text\n{chr(10).join(lines)}\n```",
            color=discord.Color.dark_grey()
        )
        embed.set_footer(text=f"Rows {start+1}-{end} / {len(self.df)}")

        await interaction.response.edit_message(embed=embed, view=self)

    @ui.button(label="⬅ Prev", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction: Interaction, _):
        # Decrement page but clamp at 0
        self.page = max(self.page - 1, 0)
        await self.update(interaction)

    @ui.button(label="Next ➡", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: Interaction, _):
        self.page = min(self.page + 1, self.max_page)
        await self.update(interaction)






def shorten_name(name: str, max_len: int = 10) -> str:
    """
    Shortens player name to max_len characters.
    Capitalizes each word for readability.
    """
    name = str(name).strip()
    # Capitalize each word
    name = " ".join(w.capitalize() for w in name.split())
    # Truncate to max_len
    if len(name) > max_len:
        name = name[:max_len]
    return name



def is_tejm(user):
    return user.name.lower() == "tejm_of_curonia"

def normalize_score(df):
    df = df.copy()
    df["Score"] = pd.to_numeric(df["Score"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    return df

def add_index(df):
    df = df.reset_index(drop=True)
    df["Ņ"] = range(1, len(df) + 1)
    return df

def parse_range(text, max_range=15):
    try:
        a, b = map(int, text.split("-"))
        if b - a + 1 > max_range:
            return None
        return a, b
    except:
        return None

def dataframe_to_markdown_aligned(df, shorten_tank=True):
    df = df.copy()

    if FIRST_COLUMN in df.columns:
        df[FIRST_COLUMN] = df[FIRST_COLUMN].apply(
            lambda v: f"{float(v) / 1_000_000:,.3f} M"
        )

    if "Date" in df.columns:
        df["Date"] = df["Date"].astype(str).str[:10]
        
    if "True Name" in df.columns:
        df["True Name"] = df["True Name"].apply(lambda n: shorten_name(n, 10))
    
    if shorten_tank and "Tank Type" in df.columns:
        df["Tank Type"] = (
            df["Tank Type"]
            .astype(str)
            .str.lower()
            .replace({"triple": "t", "auto": "a", "hexa": "h"}, regex=True)
            .str.title()
            .str[:8]
        )

    rows = [df.columns.tolist()] + df.values.tolist()
    widths = [max(wcswidth(str(r[i])) for r in rows) for i in range(len(df.columns))]

    def fmt(row):
        return " " + " | ".join(
            str(v) + " " * (widths[i] - wcswidth(str(v)))
            for i, v in enumerate(row)
        ) + " "

    return (
        [fmt(df.columns)]
        + ["-" + "-".join("-" * w for w in widths) + " -"]
        + [fmt(r) for r in df.values]
    )




async def send_embed_table(channel, title, lines, page=1, total=1):
    text = "\n".join(lines)

    embed = Embed(
        title=title,
        description=f"```text\n{text}\n```",
        color=discord.Color.dark_grey()
    )

    embed.set_footer(text=f"Page {page}/{total}")
    await channel.send(embed=embed)





def handle_best(df):
    df = normalize_score(df)
    return (
        df.sort_values("Score", ascending=False)
          .drop_duplicates("True Name")
    )


def handle_name(df, name):
    df = normalize_score(df)
    return (
        df[df["True Name"].str.lower() == name.lower()]
        .sort_values("Score", ascending=False)
    )


def handle_tank(df, tank):
    df = normalize_score(df)
    return df[df["Tank Type"].str.lower() == tank.lower()].sort_values("Score", ascending=False)

def extract_range(parts, max_range=15, total_len=0):
    """
    Extract start, end, and size from user input like '1-5'.
    Returns (start, end, size, warning)
    """
    warning = None
    start, end = 1, 1  # default to 1 row
    size = 1

    for p in parts:
        if "-" in p:
            try:
                a, b = map(int, p.split("-"))
                if b - a + 1 > max_range:
                    warning = f"❌ Max range is {max_range}!"
                    b = a + max_range - 1
                start, end = a, min(b, total_len)
                size = end - start + 1
                return start, end, size, warning
            except:
                pass
    # Make sure end does not exceed total_len
    end = min(end, total_len)
    size = end - start + 1
    return start, end, size, warning

@bot.event
async def on_message(message):
    print("Received message:", message.content)  # <<< debug

    if message.author == bot.user:
        print("Ignoring self")
        return

    if not message.content.startswith("!o;"):
        print("Ignoring non-command")
        return

    # Continue with rest of command logic...

    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts = message.content.split(";")
    cmd = parts[1].lower()

    df = read_excel_cached()
    print("Excel rows loaded:", len(df))
    if df.empty:
        await message.channel.send("Curses, data rate-limited! Try again in a few minutes.")
        return
    df.columns = df.columns.str.strip()

    output = None
    shorten_tank = True

    if cmd == "a":
        if not is_tejm(message.author):
            await message.channel.send("Restricted command.")
            return
        output = df.copy()

    elif cmd == "b":
        output = handle_best(df)
        
    elif cmd == "n":
        if len(parts) < 3:
            await message.channel.send("❌ Usage: !o;n;PlayerName;1-15")
            return
        name = parts[2].strip()
        output = handle_name(df, name)

    elif cmd == "c":
        output = normalize_score(df).sort_values("Score", ascending=False).drop_duplicates("Tank Type")
        
    elif cmd == "p":
        output = normalize_score(df).sort_values("Score", ascending=False)

    elif cmd == "t":
        if len(parts) < 3:
            await message.channel.send("Tank name required.")
            return
        output = handle_tank(df, parts[2])

    
    elif cmd == "d":
        if len(parts) < 3:
            await message.channel.send("❌ Usage: !o;d;YYYY-MM-DD or DD-MM-YYYY")
            return
        raw = parts[2]
        if re.match(r"\d{2}-\d{2}-\d{4}", raw):
            d,m,y = raw.split("-")
            target = f"{y}-{m}-{d}"
        elif re.match(r"\d{4}-\d{2}-\d{2}", raw):
            target = raw
        else:
            await message.channel.send("❌ Invalid date format")
            return
        df["Date"] = df["Date"].astype(str).str[:10]
        output = normalize_score(df[df["Date"] == target])
        if output.empty:
            await message.channel.send(f"❌ No results for {target}")
            return
        output = add_index(output)
        shorten_tank = True

    
        # --- HELP ---
    elif cmd == "help":
        help_message = (
                "Commands:\n"
                "!o;p;1-15               - Part of the scoreboard\n"            
                "!o;t;TankName;1-15      - Best score of a tank\n"
                "!o;n;Player;1-15        - Best scores of a specific player\n"
                "!o;d;YYYY-MM-DD         - Scores from a specific date\n"
            
                "!o;c;1-15               - Best tank list\n"
                "!o;b;1-15               - Best player list\n"

                "!o;r                    - Random recommendation\n"
                
            )
        await message.channel.send(help_message)
        return
            
    elif cmd == "r":
        if len(parts) == 2:
            await message.channel.send(
                "**!o;r;a** for a tank with a player record!\n"
                "**!o;r;b** for the tank with no score!\n"
                "**!o;r;r** for a fully random tank!"
            )
            return
        sub = parts[2].lower()
        if sub == "a":
            row = df.sample(1).iloc[0]
            await message.channel.send(f"{row['True Name']} recommends {row['Tank Type']}")
            return
        if sub == "b":
            used = set(df["Tank Type"].str.lower())
            unused = [t for t in TANK_NAMES if t.lower() not in used]
            if not unused:
                await message.channel.send("No tanks left.")
                return
            await message.channel.send(f"Mountain recommends {random.choice(unused)}")
            return
            
        if sub == "r":
            await message.channel.send(f"Mountain recommends {random.choice(TANK_NAMES)}")
            return
        await message.channel.send("Unknown r command.")
        return

    else:
        return

    if output is None or output.empty:
        await message.channel.send("No results.")
        return

    cols = COLUMNS_C if cmd in {"c", "t"} else COLUMNS_DEFAULT
    output = output[[c for c in cols if c in output]]

    title_map = {
        "a": "All Scores",
        "b": "Best Players",
        "n": "Player Scores",
        "c": "Best Per Tank",
        "p": "Leaderboard",
        "t": "Tank Scores",
        "d": "Scores by Date"
    }

    title = title_map.get(cmd, "Olymp Leaderboard")

    start, end, range_size, warning = extract_range(parts, max_range=15, total_len=len(output))
    if warning:
        await message.channel.send(warning)

    view = RangePaginationView(
        df=output,
        start_index=start,
        range_size=range_size,
        title=title,
        shorten_tank=shorten_tank
    )
    slice_df = output.iloc[start-1:end]
    slice_df["Ņ"] = range(start, min(end, len(output)) + 1)
    lines = dataframe_to_markdown_aligned(slice_df, shorten_tank)
    embed = Embed(
        title=title,
        description=f"```text\n{chr(10).join(lines)}\n```",
        color=discord.Color.dark_grey()
    )
    embed.set_footer(text=f"Rows {start}-{min(end, len(output))} / {len(output)}")
    await message.channel.send(embed=embed, view=view)



if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
