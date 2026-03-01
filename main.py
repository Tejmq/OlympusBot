import discord
import pandas as pd
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

LOCAL_EXCEL_PATH = "data/Olympus.xlsx"

COLUMNS_DEFAULT = ["Ņ", "Score", "Name", "Tank", "Id"]
COLUMNS_C = ["Ņ", "Tank", "Name", "Score", "Id"]

FIRST_COLUMN = "Score"
LEGENDS = 1000
COOLDOWN_SECONDS = 7
user_cooldowns = {}

intents = discord.Intents.default()
intents.message_content = True

from discord.ext import commands
from discord import app_commands

bot = commands.Bot(command_prefix="!", intents=intents)

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
    def __init__(
        self,
        *,
        cmd,
        channel,
        df,
        parts,
        index,
        resolver,      
        title,
        columns
    ):
        super().__init__(timeout=30)  # 30s timeout
        self.cmd = cmd
        self.channel = channel
        self.df = df
        self.parts = parts
        self.index = index
        self.resolver = resolver
        self.title = title
        self.columns = columns
        self.message = None  # will store the sent message for editing

    async def on_timeout(self):
        # Disable all buttons when timeout occurs
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception as e:
                print("DidYouMeanView timeout edit failed:", e)



class DidYouMeanButton(ui.Button):
    def __init__(self, label: str):
        super().__init__(label=label, style=discord.ButtonStyle.secondary)
    async def callback(self, interaction: Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer()
        view: DidYouMeanView = self.view
        output = view.resolver(view.df, self.label)
        view: DidYouMeanView = self.view
        # 1️⃣ Get the full corrected dataset from resolver
        output = view.resolver(view.df, self.label)
    # 🔀 BRANCH MODE (resolver returned a list)
        if isinstance(output, list):
            await handle_branch_command(
                interaction.message,
                self.label,
                interaction=interaction
            )
            return
        if output is None or output.empty:
            await interaction.response.edit_message(
                content="❌ No results after correction.",
                embed=None,
                view=None
            )
            return

        # 2️⃣ Re-apply GT filter if present
        gt_filter = extract_gt(view.parts)
        if gt_filter and "GT" in output.columns:
            output = output[output["GT"].astype(str).str.upper() == gt_filter]
        if output.empty:
            await interaction.response.edit_message(
                content=f"❌ No results for GT={gt_filter}.",
                embed=None,
                view=None
            )
            return
        # 3️⃣ Columns & title
        cols = [c for c in view.columns if c in output.columns]
        output = output[cols]
        # 4️⃣ Extract range from original command parts
        start, end, range_size, warning = extract_range(
            view.parts,
            max_range=15,
            total_len=len(output)
        )
        # 5️⃣ Pagination
        paged_view = RangePaginationView(
            df=output,
            start_index=start,
            range_size=range_size,
            title=view.title,
            shorten_tank=True
        )

        slice_df = output.iloc[start-1:end].copy()
        slice_df["Ņ"] = range(start, min(end, len(output)) + 1)

        lines = dataframe_to_markdown_aligned(slice_df)

        embed = Embed(
            title=view.title,
            description=f"```text\n{chr(10).join(lines)}\n```",
            color=discord.Color.dark_grey()
        )

        footer = f"Rows {start}-{min(end, len(output))} / {len(output)}"
        if warning:
            footer = f"{warning} • {footer}"

        embed.set_footer(text=footer)

        await interaction.edit_original_response(embed=embed, view=paged_view)
        paged_view.message = await interaction.original_response()




def read_excel_cached():
    global DATAFRAME_CACHE

    if DATAFRAME_CACHE is not None:
        return DATAFRAME_CACHE.copy()

    try:
        DATAFRAME_CACHE = pd.read_excel("data/Olympus.xlsx")
        print("Excel loaded locally")
        return DATAFRAME_CACHE.copy()
    except Exception as e:
        print("Excel load failed:", e)
        return "fetch_error"




def extract_gt(parts, valid=None):
    """
    Extract GT filter letter (A, R, F, etc.)
    Returns (gt_letter or None)
    """
    if valid is None:
        valid = {"a", "r", "f", "l"}

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




BRANCHES_JSON = []
def load_branches():
    global BRANCHES_JSON

    if BRANCHES_JSON:
        return BRANCHES_JSON

    try:
        with open("data/branches.json", "r") as f:
            BRANCHES_JSON = json.load(f)
        print("Branches loaded locally")
    except Exception as e:
        print("Branch load failed:", e)
        return "fetch_error"

    return BRANCHES_JSON



def handle_branch(df, branch_key):
    branches = load_branches()
    if not isinstance(branches, dict):
        return None
    return branches.get(branch_key)



async def handle_branch_command(
    message,
    branch_name: str,
    interaction: Interaction | None = None
):
    # Load branches
    branches = load_branches()
    if not isinstance(branches, dict):
        content = "❌ Branch list unavailable."

        if interaction:
            await interaction.edit_original_response(
                content=content,
                embed=None,
                view=None
            )
        else:
            await safe_send(message.channel, content=content)
        return

    # --- FUZZY BRANCH MATCHING ---
    branch_key = await fuzzy_or_abort(
        message=message,
        interaction=interaction,
        df=None,  # branches are not dataframe-based
        user_input=branch_name,
        choices=branches.keys(),
        arg_index=2,
        resolver=handle_branch,
        title="Branch not found — did you mean?",
        result_title="Branch Highscores",
        columns=["Ņ", "Tank", "Name", "Score", "Id"],
        cutoff=0.6
    )
    if branch_key is None:
        return
    # --------------------------------

    branch_tanks = branches.get(branch_key)
    if not branch_tanks:
        content = "❌ Branch has no tanks defined."

        if interaction:
            await interaction.edit_original_response(
                content=content,
                embed=None,
                view=None
            )
        else:
            await safe_send(message.channel, content=content)
        return

    # Load Excel
    df = read_excel_cached()
    if isinstance(df, str) or df.empty:
        content = "❌ Data unavailable."

        if interaction:
            await interaction.edit_original_response(
                content=content,
                embed=None,
                view=None
            )
        else:
            await safe_send(message.channel, content=content)
        return

    df.columns = df.columns.str.strip()
    df = normalize_score(df)

    # Build rows: top score per tank
    rows = []
    for tank in branch_tanks:
        tank_rows = df[df["Tank"].str.lower() == tank.lower()]
        if tank_rows.empty:
            rows.append({"Tank": tank, "Score": 0, "Name": "", "Id": ""})
        else:
            best = tank_rows.sort_values("Score", ascending=False).iloc[0]
            rows.append({
                "Tank": tank,
                "Score": int(best["Score"]),
                "Name": best.get("Name", ""),
                "Id": best.get("Id", "")
            })

    # Sort + limit
    rows.sort(key=lambda x: x["Score"], reverse=True)
    rows = rows[:16]

    display_df = pd.DataFrame(rows)
    display_df["Ņ"] = range(1, len(display_df) + 1)
    display_df = display_df[["Ņ", "Tank", "Name", "Score", "Id"]]

    lines = dataframe_to_markdown_aligned(display_df)

    embed = Embed(
        title=f"{branch_key} Branch",
        description=f"```text\n{chr(10).join(lines)}\n```",
        color=discord.Color.dark_grey()
    )
    embed.set_footer(text=f"{len(display_df)} tanks in this branch")

    # FINAL SEND / EDIT
    if interaction:
        await interaction.edit_original_response(
            embed=embed,
            view=None
        )
    else:
        await safe_send(message.channel, embed=embed)





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
    ratio = score / (playtime / 3600) if playtime > 0 else None

    # Playtime display
    if playtime > 0:
        playtime_display = f"{round(playtime / 3600, 2)}"
    else:
        playtime_display = "Unknown"


    if ratio is not None:
        ratio_display = f"{ratio:,.0f}"
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
        with open("data/tanks.json", "r") as f:
            TANK_NAMES = json.load(f)["tanks"]
        print("Tank list loaded locally")
    except Exception as e:
        print("Tank load failed:", e)
        return "fetch_error"

    return TANK_NAMES



@bot.event
async def on_ready():
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash commands")
    except Exception as e:
        print("Slash sync failed:", e)

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
    message,
    interaction: Interaction | None = None,
    df,
    user_input,
    choices,
    arg_index,
    resolver,
    title,
    result_title,
    columns,
    max_results=5,
    cutoff=0.65
):
    lookup = {str(c).lower(): str(c) for c in choices if pd.notna(c)}

    key = user_input.lower()
    if key in lookup:
        return lookup[key]
    matches = get_close_matches(
        key,
        lookup.keys(),
        n=max_results,
        cutoff=cutoff
    )
    # ❌ No matches at all
    if not matches:
        await safe_send(
            message.channel,
            content=f"❌ `{user_input}` not found."
        )
        return None
    #  Did you mean?
    embed = Embed(
        title=title,
        description="Did you mean one of these?",
        color=discord.Color.dark_grey()
    )
    view = DidYouMeanView(
        cmd=message.content,
        channel=message.channel,
        df=df,
        parts=message.content.split(";"),
        index=arg_index,
        resolver=resolver,
        title=result_title,
        columns=columns
    )
    for m in matches:
        original = lookup[m]
        # Don't use a value; buttons are interactive
        embed.add_field(name=original, value="\u200b", inline=True)  # optional, just to keep field
        view.add_item(DidYouMeanButton(original))
    if interaction:
        await interaction.edit_original_response(embed=embed, view=view)
        view.message = await interaction.original_response()
    else:
        msg = await safe_send(message.channel, embed=embed, view=view)
        view.message = msg
        await bot.process_commands(message) 
    return None





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

def extract_range(parts, max_range=20, total_len=0):
    """
    Extract start, end, and size from user input like '1-5'.
    Returns (start, end, size, warning)
    """
    warning = None
    start = 1
    end = min(20, total_len)
    size = end - start + 1


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
    # --- Debug: show every message received ---
    print(f"[DEBUG] Received message from {message.author}: {message.content}")
    if message.author == bot.user:
        return
    if not message.content.startswith("!o;"):
        return
    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        print(f"[DEBUG] Cooldown active for {message.author}")
        return
    user_cooldowns[message.author.id] = now
    parts = message.content.split(";")
    if len(parts) < 2:
        return
    cmd = parts[1].lower()
    # --- Debug: check Excel load ---
    df = read_excel_cached()
    print(f"[DEBUG] read_excel_cached returned type: {type(df)}")
    if isinstance(df, pd.DataFrame):
        print(f"[DEBUG] DataFrame shape: {df.shape}, columns: {df.columns.tolist()}")
    else:
        print(f"[DEBUG] Excel load returned: {df}")
    



    
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
            await safe_send(
                message.channel,
                content="❌ Usage: !o;n;PlayerName"
            )
            return
        name_input = parts[2].strip()
        name = await fuzzy_or_abort(
            message=message,
            df=df,
            user_input=name_input,
            choices=df["Name"].dropna().unique(),
            arg_index=2,
            resolver=handle_name,
            title="Player not found — did you mean?",
            result_title="Player Scores",
            columns=["Ņ", "Tank", "Score", "Date", "Id"]
        )
        if name is None:
            return
        output = handle_name(df, name)
        # ✅ SET TITLE HERE
        title = f"All scores of {name}"




    elif cmd == "c":
        output = normalize_score(df).sort_values("Score", ascending=False).drop_duplicates("Tank")
        
    elif cmd == "p":
        output = normalize_score(df).sort_values("Score", ascending=False)

    elif cmd == "t":
        tank_input = parts[2].strip()
        tank = await fuzzy_or_abort(
            message=message,
            df=df,
            user_input=tank_input,
            choices=df["Tank"].dropna().unique(),
            arg_index=2,
            resolver=handle_tank,
            title="Tank not found — did you mean?",
            result_title="Tank Scores",
            columns=["Ņ", "Name", "Score", "date", "Id"]
        )
        if tank is None:
            return
        output = handle_tank(df, tank)
        # ✅ SET TITLE HERE
        title = f"All scores of {tank}"

    
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



    # --- Call in on_message ---
    elif cmd == "bch":
        if len(parts) < 3:
            await safe_send(message.channel, content="❌ Usage: !o;bch;<branchname>")
            return
        branch_name = parts[2].strip()
        await handle_branch_command(message, branch_name)
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
                "!o;p              - Part of the scoreboard\n"            
                "!o;t;TankName     - Best score of a tank\n"
                "!o;n;Player       - Best scores of a specific player\n"
                "!o;d;YYYY-MM-DD         - Scores from a specific date\n"
                "!o;bch;BranchName    - Highscores of every tank in a branch\n"
            
                "!o;c             - Best tank list\n"
                "!o;b              - Best player list\n"

                ";1-15    -to imput range      ;r    -to pick category \n"
            
                "!o;s;id                 - Screenshot of the score\n"
                "!o;i;id                 - Detailed description\n"
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

    if cmd == "n":  # player
        cols = ["Ņ", "Score", "Tank", "Date", "Id"]
    elif cmd == "t":  # tank
        cols = ["Ņ", "Score", "Name", "Date", "Id"]
    elif cmd == "c":
        cols = COLUMNS_C.copy()
    else:
        cols = COLUMNS_DEFAULT.copy()
    cols = [c for c in cols if c in output.columns]
    output = output[cols]


    # after output is finalized
    if 'title' not in locals() or title is None:
        title_map = {
            "a": "All Scores",
            "b": "Best Players",
            "c": "Best Per Tank",
            "p": "Leaderboard",
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
    await bot.process_commands(message) 




    # -------------!o;p;1-10------------------------

from discord import app_commands
@bot.tree.command(name="leaderboard", description="Show leaderboard with optional range")
@app_commands.describe(
    start="Starting rank (default: 1)",
    end="Ending rank (default: 15)"
)
async def leaderboard(
    interaction: discord.Interaction,
    start: int = 1,
    end: int = 15
):
    await interaction.response.defer()
    # Safety checks
    if start < 1:
        start = 1
    if end < start:
        end = start
    df = read_excel_cached()
    if isinstance(df, str) or df.empty:
        await interaction.followup.send("Data unavailable.")
        return
    df = normalize_score(df).sort_values("Score", ascending=False)
    df = add_index(df)
    # Convert to zero-based slicing
    start_index = start - 1
    end_index = end
    sliced = df.iloc[start_index:end_index]
    if sliced.empty:
        await interaction.followup.send("No results in that range.")
        return
    sliced = sliced[["Ņ", "Score", "Name", "Tank", "Id"]]
    lines = dataframe_to_markdown_aligned(sliced)
    embed = discord.Embed(
        title=f"Leaderboard ({start}-{end})",
        description=f"```text\n{chr(10).join(lines)}\n```",
        color=discord.Color.dark_grey()
    )
    await interaction.followup.send(embed=embed)










if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
