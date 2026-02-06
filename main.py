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
import asyncio
from discord.errors import HTTPException
import re
from difflib import get_close_matches

FETCH_LOCK = Lock()

EXCEL_GITHUB_URL = "https://raw.githubusercontent.com/Tejmq/OlympusBot/main/data/Olympus.xlsx"
LOCAL_EXCEL_PATH = "data/Olympus.xlsx"
TANKS_JSON_URL = "https://raw.githubusercontent.com/Tejmq/OlympusBot/refs/heads/main/data/tanks.json"


COLUMNS_DEFAULT = ["Ņ", "Score", "Name", "Tank", "Id"]
COLUMNS_C = ["Ņ", "Tank", "Name", "Score", "Id"]

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
    "User-Agent": "OlympusBot/1.0 (contact: tejm)",
    "Accept": "application/octet-stream"
}

async def safe_send(channel, **kwargs):
    try:
        return await channel.send(**kwargs)
    except HTTPException as e:
        # Check if this is a Cloudflare block (HTML 429)
        text = getattr(e, "text", "") or ""
        if e.status == 429 and "DOCTYPE html" in text:
            print("Blocked by Cloudflare, cannot send message.")
            return None  # Don't crash; just skip
        # Normal Discord 429 handling
        if e.status == 429:
            retry_after = getattr(e, "retry_after", 5)
            print(f"Rate limited — sleeping {retry_after}s")
            await asyncio.sleep(retry_after)
            try:
                return await channel.send(**kwargs)
            except Exception as inner_e:
                print("Retry failed:", inner_e)
                return None
        raise  # re-raise any other exception


def safe_val(row, key, default="Unknown"):
    try:
        v = row.get(key, default)
        if pd.isna(v) or v in ("?", "", None):
            return default
        return v
    except Exception:
        return default


def fuzzy_matches(query, choices, max_results=5, cutoff=0.7):
    """
    Returns up to max_results close matches.
    cutoff ~ similarity (0.0–1.0)
    """
    query = query.lower()
    choices_lower = {c.lower(): c for c in choices}
    matches = get_close_matches(
        query,
        choices_lower.keys(),
        n=max_results,
        cutoff=cutoff
    )
    return [choices_lower[m] for m in matches]




class DidYouMeanView(ui.View):
    def __init__(self, bot, original_message, cmd, suggestions):
        super().__init__(timeout=30)
        self.bot = bot
        self.original_message = original_message
        self.cmd = cmd

        for s in suggestions:
            self.add_item(DidYouMeanButton(s, cmd))



class DidYouMeanButton(ui.Button):
    def __init__(self, suggestion, cmd):
        super().__init__(
            label=suggestion,
            style=discord.ButtonStyle.primary
        )
        self.suggestion = suggestion
        self.cmd = cmd

    async def callback(self, interaction: Interaction):
        fixed_command = f"!o;{self.cmd};{self.suggestion}"

        await interaction.response.send_message(
            content=f"🔁 Running: `{fixed_command}`",
            ephemeral=True
        )

        # Fake a new message object
        fake = interaction.message
        fake.content = fixed_command
        fake.author = interaction.user

        await interaction.client.dispatch("message", fake)




def read_excel_cached():
    global DATAFRAME_CACHE, LAST_FETCH

    now = time.time()

    with FETCH_LOCK:
        # 1️⃣ Use in-memory cache if valid
        if DATAFRAME_CACHE is not None and now - LAST_FETCH < CACHE_TTL:
            return DATAFRAME_CACHE.copy()

        # 2️⃣ Load from disk if exists
        if os.path.isfile(LOCAL_EXCEL_PATH):
            try:
                DATAFRAME_CACHE = pd.read_excel(LOCAL_EXCEL_PATH)
                LAST_FETCH = now
                print("Excel loaded from disk")
                return DATAFRAME_CACHE.copy()
            except Exception as e:
                print("Failed to load Excel from disk:", e)

        # 3️⃣ Fetch from GitHub RAW (last resort)
        try:
            print("Fetching Excel from GitHub...")
            r = requests.get(EXCEL_GITHUB_URL, headers=HEADERS, timeout=15)
            r.raise_for_status()

            content_type = r.headers.get("Content-Type", "").lower()
            if "html" in content_type:
                print("GitHub returned HTML — blocked or rate-limited")
                return "html_error"

            with open(LOCAL_EXCEL_PATH, "wb") as f:
                f.write(r.content)

            DATAFRAME_CACHE = pd.read_excel(BytesIO(r.content))
            LAST_FETCH = now
            print("Excel fetched and cached")

            return DATAFRAME_CACHE.copy()

        except Exception as e:
            print("Excel fetch failed:", e)
            return "fetch_error"
            

def extract_gt(parts, valid=None):
    """
    Extract GT filter letter (A, R, F, etc.)
    Returns (gt_letter or None)
    """
    if valid is None:
        valid = {"a", "r", "f"}

    for p in parts:
        p = p.strip().lower()
        if len(p) == 1 and p in valid:
            return p.upper()
    return None



async def send_screenshot(channel, df, screenshot_id):
    # Ensure Id column exists
    if "Id" not in df.columns:
        await safe_send(channel, content="❌ No Id column in data.")
        return

    # Match Id as string
    row = df[df["Id"].astype(str) == str(screenshot_id)]
    if row.empty:
        await safe_send(channel, content="❌ No screenshot with that Id.")
        return

    row = row.iloc[0]

    cdn_url = safe_val(row, "CDN", None)
    if not cdn_url or not isinstance(cdn_url, str):
        await safe_send(channel, content="❌ No screenshot available for this entry.")
        return

    embed = Embed(
        title=f"Screenshot ID: {screenshot_id}",
        color=discord.Color.dark_grey()
    )
    embed.set_image(url=cdn_url)

    await safe_send(channel, embed=embed)


def parse_playtime(v):
    try:
        if pd.isna(v) or v in ("?", "", None):
            return 0.0
        # If pandas already converted it to timedelta
        if isinstance(v, pd.Timedelta):
            return v.total_seconds()
        # String format [h]:mm:ss
        parts = str(v).split(":")
        if len(parts) == 3:
            h, m, s = map(int, parts)
            return h * 3600 + m * 60 + s
        return 0.0
    except:
        return 0.0



def parse_score(v):
    try:
        if pd.isna(v) or v in ("?", "", None):
            return 0.0
        return float(str(v).replace(",", ""))
    except:
        return 0.0



async def send_info_embed(channel, df, info_id):
    safe_id = re.sub(r"[^A-Za-z0-9_-]", "_", str(info_id))
    # Ensure Id column exists
    if "Id" not in df.columns:
        await safe_send(channel, content="❌ No Id column in data.")
        return
    # Match base64 Id as string
    row = df[df["Id"].astype(str) == str(info_id)]
    if row.empty:
        await safe_send(channel, content="❌ No entry with that Id.")
        return
    row = row.iloc[0]
    name1 = safe_val(row, "Name", "Unknown")
    name = safe_val(row, "Name in game", "Unknown")
    tank = safe_val(row, "Tank", "Unknown")
    killer = safe_val(row, "Killer", "Unknown")
    # Numeric fields (safe)
    try:
        score = parse_score(safe_val(row, "Score", 0))
    except:
        score = 0
    try:
        playtime = parse_playtime(safe_val(row, "Playtime", 0))
    except:
        playtime = 0

    date = str(safe_val(row, "Date", "Unknown"))[:10]
    playtime1 = round((playtime / 3600), 2) 
    ratio = round(score / (playtime / 3600), 0) if playtime > 0 else 0

    # Playtime display
    if playtime > 0:
        playtime_display = f"{round(playtime / 3600, 2)}"
    else:
        playtime_display = "Unknown"


    # Ratio display
    if ratio > 0:
        ratio_display = f"{int(ratio):,}"
    else:
        ratio_display = "Unknown"

    
    description = (
        f"**{name1}**\n"
        f"{name} got **{int(score):,}** with **{tank}**.\n"
        f"It took **{playtime_display}** hours, on **{date}**, "
        f"with a ratio of **{ratio_display}** per hour.\n"
        f"{name} died to **{killer}**."
    )
    embed = Embed(
        title=f"Score id: {info_id}",
        description=description,
        color=discord.Color.dark_grey()
    )
    # Image 
    cdn_url = safe_val(row, "CDN", None)
    if cdn_url and isinstance(cdn_url, str):
        embed.set_image(url=cdn_url)

    await safe_send(channel, embed=embed)





TANK_NAMES = []
def load_tanks():
    global TANK_NAMES
    if TANK_NAMES:
        return TANK_NAMES

    try:
        r = requests.get(TANKS_JSON_URL, timeout=10)
        r.raise_for_status()

        if "html" in r.headers.get("Content-Type", "").lower():
            print("Tank JSON fetch returned HTML — possible GitHub/Cloudflare issue")
            return "html_error"

        TANK_NAMES = r.json()["tanks"]
        print("Tank list loaded from GitHub")
    except Exception as e:
        print("Tank list load failed:", e)
        return "fetch_error"

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
        super().__init__(timeout=180)
        self.df = df.reset_index(drop=True)
        self.range_size = range_size
        self.title = title
        self.shorten_tank = shorten_tank

        # Start page calculation
        self.page = (start_index - 1) // range_size
        self.max_page = (len(self.df) - 1) // range_size

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        try:
            await self.message.edit(view=self)
        except:
            pass
    
    def get_slice(self):
        start = self.page * self.range_size
        end = min(start + self.range_size, len(self.df))
        # Clamp in case start < 0
        if start < 0:
            start, end = 0, min(self.range_size, len(self.df))
        return self.df.iloc[start:end], start, end


    async def update(self, interaction: Interaction):
        if interaction.response.is_done():
            return
        slice_df, start, end = self.get_slice()
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
        await asyncio.sleep(0.8)
    

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
    name = str(name).strip()
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
        
    if "Name" in df.columns:
        df["Name"] = df["Name"].apply(lambda n: shorten_name(n, 10))
    
    if shorten_tank and "Tank" in df.columns:
        df["Tank"] = (
            df["Tank"]
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


from difflib import get_close_matches





async def fuzzy_or_abort(
    *,
    channel,
    bot,
    original_message,
    cmd,
    user_input,
    choices,
    max_results=5,
    cutoff=0.65,
    title="Did you mean…?"
):
    """
    Returns corrected value OR None if suggestions were shown.
    """
    # Exact match (case-insensitive)
    lookup = {c.lower(): c for c in choices}
    if user_input.lower() in lookup:
        return lookup[user_input.lower()]
    # Fuzzy matches
    matches = get_close_matches(
        user_input.lower(),
        lookup.keys(),
        n=max_results,
        cutoff=cutoff
    )
    if not matches:
        await safe_send(
            channel,
            content=f"❌ No match found for `{user_input}`."
        )
        return None
    suggestions = [lookup[m] for m in matches]
    embed = Embed(
        title=title,
        description="\n".join(f"• **{s}**" for s in suggestions),
        color=discord.Color.orange()
    )
    view = DidYouMeanView(
        bot=bot,
        original_message=original_message,
        cmd=cmd,
        suggestions=suggestions
    )
    await safe_send(channel, embed=embed, view=view)
    return None




class DidYouMeanView(ui.View):
    def __init__(self, bot, original_message, cmd, suggestions):
        super().__init__(timeout=30)
        self.bot = bot
        self.original_message = original_message
        self.cmd = cmd
        for s in suggestions:
            self.add_item(DidYouMeanButton(s, cmd))


class DidYouMeanButton(ui.Button):
    def __init__(self, suggestion, cmd):
        super().__init__(
            label=suggestion,
            style=discord.ButtonStyle.primary
        )
        self.suggestion = suggestion
        self.cmd = cmd
    async def callback(self, interaction: Interaction):
        fixed_command = f"!o;{self.cmd};{self.suggestion}"
        await interaction.response.send_message(
            content=f"🔁 Running `{fixed_command}`",
            ephemeral=True
        )
        fake = interaction.message
        fake.content = fixed_command
        fake.author = interaction.user
        await interaction.client.dispatch("message", fake)




def handle_best(df):
    df = normalize_score(df)
    return (
        df.sort_values("Score", ascending=False)
          .drop_duplicates("Name")
    )


def handle_name(df, name):
    df = normalize_score(df)
    return (
        df[df["Name"].str.lower() == name.lower()]
        .sort_values("Score", ascending=False)
    )


def handle_tank(df, tank):
    df = normalize_score(df)
    return df[df["Tank"].str.lower() == tank.lower()].sort_values("Score", ascending=False)

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
    if message.author == bot.user:
        return

    if not message.content.startswith("!o;"):
        return

    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts = message.content.split(";")
    if len(parts) < 2:
        return
    cmd = parts[1].lower()
    df = read_excel_cached()
    



    
    if isinstance(df, str):
        if df == "html_error":
            await safe_send(message.channel, content="Curses, data rate-limited! Try again in a few minutes.")
        else:
            await safe_send(message.channel, content="If you are reading this, Tejm messed up.")
        return
    if df.empty:
        await safe_send(message.channel, content="Curses, data rate-limited! Try again in a few minutes.")
        return
    
    df.columns = df.columns.str.strip()

    output = None
    shorten_tank = True

    if cmd == "a":
        if not is_tejm(message.author):
            await safe_send(message.channel, content="Restricted command.")
            return
        output = df.copy()

    elif cmd == "b":
        output = handle_best(df)
        
    elif cmd == "n":
        if len(parts) < 3:
            await safe_send(message.channel, content="❌ Usage: !o;n;PlayerName;1-15")
            return
        name = parts[2].strip()
        output = handle_name(df, name)

    elif cmd == "c":
        output = normalize_score(df).sort_values("Score", ascending=False).drop_duplicates("Tank")
        
    elif cmd == "p":
        output = normalize_score(df).sort_values("Score", ascending=False)

    
    elif cmd == "t":
        if len(parts) < 3:
            await safe_send(message.channel, content="Tank name required.")
            return
        tank_input = parts[2].strip()
        tank = await fuzzy_or_abort(
            channel=message.channel,
            bot=bot,
            original_message=message,
            cmd="t",
            user_input=tank_input,
            choices=df["Tank"].dropna().unique()
        )
        if tank is None:
            return
        output = handle_tank(df, tank)



    
    elif cmd == "s":
        if len(parts) < 3:
            await safe_send(
                message.channel,
                content="❌ Usage: !o;s;<Id>"
            )
            return

        df = read_excel_cached()
        if isinstance(df, str) or df.empty:
            await safe_send(message.channel, content="❌ Data unavailable.")
            return

        df.columns = df.columns.str.strip()
        screenshot_id = parts[2].strip()
        await send_screenshot(message.channel, df, screenshot_id)
        return



    elif cmd == "i":
        if len(parts) < 3:
            await safe_send(
                message.channel,
                content="❌ Usage: !o;i;<Id>"
            )
            return
        info_id = parts[2].strip()
        df = read_excel_cached()
        if isinstance(df, str) or df.empty:
            await safe_send(message.channel, content="❌ Data unavailable.")
            return
        df.columns = df.columns.str.strip()
        await send_info_embed(message.channel, df, info_id)
        return   


    elif cmd == "d":
        if len(parts) < 3:
            await safe_send(message.channel, content="❌ Usage: !o;d;YYYY-MM-DD or DD-MM-YYYY")
            return
        raw = parts[2]
        if re.match(r"\d{2}-\d{2}-\d{4}", raw):
            d,m,y = raw.split("-")
            target = f"{y}-{m}-{d}"
        elif re.match(r"\d{4}-\d{2}-\d{2}", raw):
            target = raw
        else:
            await safe_send(message.channel, content="❌ Invalid date format")
            return
        df["Date"] = df["Date"].astype(str).str[:10]
        output = normalize_score(df[df["Date"] == target])
        if output.empty:
            await safe_send(message.channel, content=f"❌ No results for {target}")
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
        await safe_send(message.channel, content=help_message)
        return
            
    elif cmd == "r":
        if len(parts) == 2:
            await safe_send(
                message.channel,
                content=(
                    "**!o;r;a** for a tank with a player record!\n"
                    "**!o;r;b** for the tank with no score!\n"
                    "**!o;r;r** for a fully random tank!"
                )
            )
            return
        sub = parts[2].lower()
        if sub == "a":
            row = df.sample(1).iloc[0]
            await safe_send(message.channel, content=f"{row['Name in game']} recommends {row['Tank']}")
            return
        if sub == "b":
            used = set(df["Tank"].str.lower())
            unused = [t for t in TANK_NAMES if t.lower() not in used]
            if not unused:
                await safe_send(message.channel, content="No tanks left.")
                return
            await safe_send(message.channel, content=f"Mountain recommends {random.choice(unused)}")
            return
            
        if sub == "r":
            await safe_send(message.channel, content=f"Siege Emperor recommends {random.choice(TANK_NAMES)}")           
            return
        await safe_send(message.channel, content="Unknown r command.")
        return

    else:
        return

    if output is None or output.empty:
        await safe_send(message.channel, content="No results.")
        return


    # ---------------- GT FILTER HERE ----------------
    gt_filter = extract_gt(parts)
    if gt_filter and "GT" in output.columns:
        output = output[
            output["GT"].astype(str).str.upper() == gt_filter
        ]
    if output.empty:
        await safe_send(
            message.channel,
            content=f"No results for GT={gt_filter}."
        )
        return
    # ------------------------------------------------

    
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
    footer = f"Rows {start}-{min(end, len(output))} / {len(output)}"
    if warning:
        footer = f"{warning} • {footer}"

    embed.set_footer(text=footer)


    msg = await safe_send(message.channel, embed=embed, view=view)
    view.message = msg



if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
