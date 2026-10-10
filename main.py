import discord
import pandas as pd
from wcwidth import wcswidth
import os, time, json, random, re
from keep_alive import keep_alive
from discord import Embed
from discord import ui, Interaction
import asyncio
from discord.errors import HTTPException
from difflib import get_close_matches
from datetime import datetime, time as dt_time
import copy

# Centralized display order: change a command's list here instead of in its handler.
COLUMN_ORDER = {
    "default": ["Ņ", "Score", "Name", "Tank", "Id"],
    "c": ["Ņ", "Tank", "Name", "Score", "Id"],
    "b": ["Ņ", "Score", "Tank", "Name", "Id"],
    "n": ["Ņ", "Score", "Tank", "Date", "Id"],
    "t": ["Ņ", "Score", "Name", "Date", "Id"],
    "e": ["Ņ", "Score", "Tank", "LB", "Tank LB", "Id"],
}
FIRST_COLUMN = "Score"
# Per-command behavior switches. Edit these instead of duplicating logic.
COMMAND_OPTIONS = {
    "n": {"used_lb_type": True, "used_fuzzy_matching": True, "fuzzy_column": "Name", "allow_range": True, "formatting_type": "v2"},
    "t": {"used_lb_type": True, "used_fuzzy_matching": True, "fuzzy_column": "Tank", "allow_range": True, "formatting_type": "v2"},
    "e": {"used_lb_type": True, "used_fuzzy_matching": True, "fuzzy_column": "Name", "allow_range": True, "formatting_type": "v2"},
    "c": {"used_lb_type": True, "used_fuzzy_matching": False, "fuzzy_column": None, "allow_range": True, "formatting_type": "v3"},
    "b": {"used_lb_type": True, "used_fuzzy_matching": False, "fuzzy_column": None, "allow_range": True, "formatting_type": "v3"},
    "p": {"used_lb_type": True, "used_fuzzy_matching": False, "fuzzy_column": None, "allow_range": True, "formatting_type": "v2"},
    "br": {"used_lb_type": True, "used_fuzzy_matching": True, "fuzzy_column": "Branch", "allow_range": False, "formatting_type": "v2"},
}
COOLDOWN_SECONDS = 2
user_cooldowns = {}
CU_ACTIVE = set()

intents = discord.Intents.default()
intents.message_content = True

from discord.ext import commands

bot = commands.Bot(command_prefix="!!", intents=intents)

DATAFRAME_CACHE = None
CACHE_TTL = 300  # 5 minutes

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



RANDOM_MESSAGES = []
def load_messages():
    global RANDOM_MESSAGES
    if RANDOM_MESSAGES:
        return RANDOM_MESSAGES
    try:
        with open("data/messages.json", "r") as f:
            RANDOM_MESSAGES = json.load(f)["messages"]
        print("Random messages loaded")
    except Exception as e:
        print("Message load failed:", e)
        RANDOM_MESSAGES = []
    return RANDOM_MESSAGES



async def maybe_send_random_message(channel, chance=0.5):
    """
    chance = probability between 0 and 1
    example: 0.25 = 25%
    """
    if random.random() <= chance:
        msgs = load_messages()
        if msgs:
            await safe_send(channel, content=random.choice(msgs))









class DidYouMeanView(ui.View):
    def __init__(
        self,
        *,
        cmd,
        message_source,
        channel,
        df,
        parts,
        index,
        resolver,
        title,
        columns
    ):
        super().__init__(timeout=30)
        self.cmd = cmd
        self.message_source = message_source
        self.channel = channel
        self.df = df
        self.parts = parts
        self.index = index
        self.resolver = resolver
        self.title = title
        self.columns = columns
        self.message = None
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception as e:
                print(
                    "DidYouMeanView timeout edit failed:",
                    e
                )




def make_embed(title, lines, color=discord.Color.red()):
    return Embed(
        title=title,
        description=f"```text\n{chr(10).join(lines)}\n```",
        color=color
    )


def make_leaderboard_embed(title, frame, footer=None, formatting_type="v2", shorten_tank=True, row_layout=None):
    """v2 = original layout; v2.5 = previous aligned code-block layout; v3 = spaced regular embed text."""
    if row_layout is not None:  # Backward compatibility for existing callers.
        formatting_type = "v3" if row_layout else "v2"

    display = frame.copy()
    if "Score" in display.columns:
        def format_score(value):
            try:
                return f"{float(value) / 1_000_000:,.3f} M"
            except (TypeError, ValueError):
                return str(value)
        display["Score"] = display["Score"].apply(format_score)
    if "Date" in display.columns:
        display["Date"] = display["Date"].astype(str).str[:10]
    if "Name" in display.columns:
        display["Name"] = display["Name"].astype(str).map(lambda value: shorten_name(value, 16))
    if shorten_tank and "Tank" in display.columns:
        display["Tank"] = display["Tank"].astype(str).str[:18]

    if formatting_type == "v2":
        embed = make_embed(title, dataframe_to_markdown_aligned(display, shorten_tank))
    else:
        rank_col = "Ņ" if "Ņ" in display.columns else None
        data_cols = [col for col in display.columns if col != rank_col]
        headers = [rank_col or "Rank", *data_cols]
        data_rows = []
        for _, row in display.iterrows():
            rank = str(row[rank_col]) if rank_col else "•"
            values = [str(row[col]).replace("\n", " ").replace("|", "/") for col in data_cols]
            data_rows.append([rank, *values])

        if formatting_type == "v2.5":
            # Preserve the previous v3 implementation as an optional style.
            widths = [
                min(22, max(3, max([wcswidth(str(headers[i]))] + [wcswidth(r[i]) for r in data_rows])))
                for i in range(len(headers))
            ]
            def pad_cell(value, width):
                value = str(value)
                while wcswidth(value) > width and value:
                    value = value[:-1]
                return value + (" " * max(0, width - wcswidth(value)))
            lines = ["  ".join(pad_cell(value, widths[i]) for i, value in enumerate(headers)).rstrip()]
            for row in data_rows:
                lines.append("  ".join(pad_cell(value, widths[i]) for i, value in enumerate(row)).rstrip())
            embed = Embed(
                title=title,
                description="```text\n" + "\n".join(lines)[:4080] + "\n```",
                color=discord.Color.red(),
            )
        else:
            # v3: ordinary embed text, not a Markdown/ASCII table or code block.
            # Use non-breaking spaces for calculated padding because Markdown
            # otherwise collapses runs of ordinary spaces in embed descriptions.
            widths = [
                min(24, max(wcswidth(str(headers[i])), max([wcswidth(r[i]) for r in data_rows], default=0)))
                for i in range(len(headers))
            ]
            def fit_cell(value, width):
                value = str(value)
                while wcswidth(value) > width and value:
                    value = value[:-1]
                return value + ("\u00a0" * max(0, width - wcswidth(value)))

            heading = "**" + " | ".join(fit_cell(h, widths[i]) for i, h in enumerate(headers)).rstrip() + "**"
            lines = [heading]
            for row in data_rows:
                rank = row[0]
                cells = [fit_cell(value, widths[i + 1]) for i, value in enumerate(row[1:])]
                lines.append(f"**{rank}.** " + " | ".join(cells).rstrip())
            embed = Embed(
                title=title,
                description="\n".join(lines)[:4080],
                color=discord.Color.red(),
            )

    if footer:
        embed.set_footer(text=footer)
    return embed




@bot.event
async def on_ready():
    print("Bot starting...")
    try:
        read_excel_cached()
        print("Initial data load OK")
    except Exception as e:
        print("Initial data load failed:", e)
    global TANK_NAMES
    TANK_NAMES = load_tanks()
    print(f"Logged in as {bot.user}")





class RangePaginationView(ui.View):
    def __init__(self, df, start_index, range_size, title, shorten_tank, row_layout=False, formatting_type=None):
        super().__init__(timeout=180)
        self.df = df.reset_index(drop=True)
        self.range_size = range_size
        self.title = title
        self.shorten_tank = shorten_tank
        self.row_layout = row_layout
        self.formatting_type = formatting_type or ("v3" if row_layout else "v2")

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
        footer = f"Rows {start+1}-{end} / {len(self.df)}"
        embed = make_leaderboard_embed(
            self.title, slice_df, footer=footer,
            formatting_type=self.formatting_type, shorten_tank=self.shorten_tank
        )
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




def handle_record_each(df, name, personal=False):
    """
    personal=False -> tanks where the player holds the global #1 score
    personal=True  -> player's personal best score on each tank
    """
    df = normalize_score(df)
    if personal:
        # Only this player's scores
        player_df = df[df["Name"].str.lower() == name.lower()].copy()
        # Keep only their best score per tank
        return (
            player_df.sort_values("Score", ascending=False)
                     .drop_duplicates("Tank")
                     .sort_values("Score", ascending=False)
        )
    # Existing global-record mode
    best_per_tank = (
        df.sort_values("Score", ascending=False)
          .drop_duplicates("Tank")
    )
    return best_per_tank[
        best_per_tank["Name"].str.lower() == name.lower()
    ].sort_values("Score", ascending=False)





async def handle_records_player(message, df, parts):
    if len(parts) < 3 or not parts[2].strip():
        await safe_send(
            message.channel,
            content="❌ Usage: re!!PlayerName (add /+ for personal bests)",
        )
        return
    # Detect + anywhere after the player name.
    personal_mode = any(p.strip() == "+" for p in parts[3:])
    name_input = parts[2].strip()
    name = await fuzzy_or_abort(
        message=message,
        df=df,
        user_input=name_input,
        choices=df["Name"].dropna().unique(),
        arg_index=2,
        resolver=lambda d, n: handle_record_each(d, n, personal=personal_mode),
        title="Player not found — did you mean?",
        result_title="Player Records",
        columns=["Ņ", "Score", "Tank", "Date", "Id"]
    )
    if name is None:
        return
    df_filtered = handle_record_each(df, name, personal=personal_mode)
    if df_filtered.empty:
        if personal_mode:
            await safe_send(
                message.channel,
                content=f"❌ **{name}** has no personal tank records."
            )
        else:
            await safe_send(
                message.channel,
                content=f"❌ **{name}** holds no global records."
            )
        return
    df_filtered = add_index(df_filtered)
    cols = ["Ņ", "Score", "Tank", "Date", "Id"]
    df_filtered = df_filtered[cols]
    start, end, range_size, warning = extract_range(
        parts,
        max_range=20,
        total_len=len(df_filtered)
    )
    title = (
        f"{name}'s Personal Bests"
        if personal_mode
        else f"{name}'s Tank Records"
    )
    view = RangePaginationView(
        df=df_filtered,
        start_index=start,
        range_size=range_size,
        title=title,
        shorten_tank=True
    )
    slice_df = df_filtered.iloc[start-1:end].copy()
    slice_df["Ņ"] = range(start, min(end, len(df_filtered)) + 1)
    lines = dataframe_to_markdown_aligned(slice_df)
    embed = make_embed(title, lines)
    footer = f"Rows {start}-{min(end, len(df_filtered))} / {len(df_filtered)}"
    if warning:
        footer = f"{warning} • {footer}"
    embed.set_footer(text=footer)
    msg = await safe_send(message.channel, embed=embed, view=view)
    view.message = msg














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
    columns=None,
    max_results=5,
    cutoff=0.65,
    used_fuzzy_matching=True,
    fuzzy_column=None,
):
    if user_input is None or not str(user_input).strip():
        await safe_send(
            message.channel,
            content=f"❌ Usage: {message.content.split('!!', 1)[0]}!!<name>",
        )
        return None
    if used_fuzzy_matching and fuzzy_column and df is not None and fuzzy_column in df.columns:
        choices = df[fuzzy_column].dropna().unique()
    lookup = {str(c).lower(): str(c) for c in choices if pd.notna(c)}

    key = str(user_input).lower().strip()
    if not used_fuzzy_matching:
        return lookup.get(key)
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
        description="Choose the correct option below.",
        color=discord.Color.red()
    )
    view = DidYouMeanView(
        cmd=message.content,
        message_source=message,
        channel=message.channel,
        df=df,
        parts=parse_command_parts(message.content),
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
    return None




class RandomAnalysisView(ui.View):
    def __init__(self, df, mode):
        super().__init__(timeout=180)
        self.df = df
        self.mode = mode
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        try:
            await self.message.edit(view=self)
        except:
            pass
    @ui.button(label="🎲 Reroll", style=discord.ButtonStyle.secondary)
    async def reroll(self, interaction: discord.Interaction, _):
        output = handle_random_analysis(self.df, self.mode)
        lines = dataframe_to_markdown_aligned(output, shorten_tank=False)
        # 14-character tank names only for this command
        output = output.copy()
        output["Tank"] = output["Tank"].astype(str).str[:14]
        lines = dataframe_to_markdown_aligned(output, shorten_tank=False)
        embed = make_embed("Random Recommendations", lines)
        embed.set_footer(text="very!")
        await interaction.response.edit_message(embed=embed, view=self)





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


async def handle_player_command(message, df, parts):
    """Resolve a player name and return (scores, title) for the dispatcher."""
    if len(parts) < 3 or not parts[2].strip():
        await safe_send(message.channel, content="❌ Usage: p!!PlayerName")
        return None, None
    name = await fuzzy_or_abort(
        message=message,
        df=df,
        user_input=parts[2].strip(),
        choices=df["Name"].dropna().unique(),
        arg_index=2,
        resolver=handle_name,
        title="Player not found — did you mean?",
        result_title="Player Scores",
        columns=["Ņ", "Tank", "Score", "Date", "Id"],
        used_fuzzy_matching=COMMAND_OPTIONS["n"]["used_fuzzy_matching"],
        fuzzy_column=COMMAND_OPTIONS["n"]["fuzzy_column"],
    )
    if name is None:
        return None, None
    return handle_name(df, name), f"All scores of {name}"


async def handle_tank_command(message, df, parts):
    """Resolve a tank name and return (scores, title) for the dispatcher."""
    if len(parts) < 3 or not parts[2].strip():
        await safe_send(message.channel, content="❌ Usage: t!!TankName")
        return None, None
    tank = await fuzzy_or_abort(
        message=message,
        df=df,
        user_input=parts[2].strip(),
        choices=df["Tank"].dropna().unique(),
        arg_index=2,
        resolver=handle_tank,
        title="Tank not found — did you mean?",
        result_title="Tank Scores",
        columns=["Ņ", "Score", "Name", "Date", "Id"],
        used_fuzzy_matching=COMMAND_OPTIONS["t"]["used_fuzzy_matching"],
        fuzzy_column=COMMAND_OPTIONS["t"]["fuzzy_column"],
    )
    if tank is None:
        return None, None
    await maybe_send_random_message(message.channel, 0.05)
    return handle_tank(df, tank), f"All scores of {tank}"


async def handle_extended_player_command(message, df, parts):
    """Resolve an extended player search and return (scores, title)."""
    if len(parts) < 3 or not parts[2].strip():
        await safe_send(message.channel, content="❌ Usage: e!!PlayerName")
        return None, None
    name = await fuzzy_or_abort(
        message=message,
        df=df,
        user_input=parts[2].strip(),
        choices=df["Name"].dropna().unique(),
        arg_index=2,
        resolver=handle_name_extended,
        title="Player not found — did you mean?",
        result_title="Player Scores",
        columns=["Ņ", "Score", "Tank", "LB", "Tank LB", "Id"],
        used_fuzzy_matching=COMMAND_OPTIONS["e"]["used_fuzzy_matching"],
        fuzzy_column=COMMAND_OPTIONS["e"]["fuzzy_column"],
    )
    if name is None:
        return None, None
    output = handle_name_extended(df, name)
    if output.empty:
        await safe_send(message.channel, content=f"❌ No scores found for **{name}**.")
        return None, None
    output = output[["Score", "Tank", "LB", "Tank LB", "Id"]].copy()
    output.insert(0, "Ņ", range(1, len(output) + 1))
    return output, f"All scores of {name} — Extended"

def extract_range(parts, max_range=20, total_len=0):
    """
    Extract start, end, and size from user input like '1-5'.
    Returns (start, end, size, warning)
    """
    warning = None
    start = 1
    end = min(15, total_len)
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





def extract_date_filter(parts):
    """Return (operator, ISO date) from helper arguments, or (None, None)."""
    pattern = re.compile(r"([<>=]?)(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4})")
    for part in parts:
        match = pattern.fullmatch(str(part).strip())
        if not match:
            continue
        operator, date_text = match.groups()
        if re.fullmatch(r"\d{2}-\d{2}-\d{4}", date_text):
            try:
                date_text = datetime.strptime(date_text, "%d-%m-%Y").strftime("%Y-%m-%d")
            except ValueError:
                return None, None
        else:
            try:
                datetime.strptime(date_text, "%Y-%m-%d")
            except ValueError:
                return None, None
        return operator or "=", date_text
    return None, None


def apply_date_filter(df, parts):
    """Apply an optional date helper without mutating the source frame."""
    operator, target = extract_date_filter(parts)
    if not target or "Date" not in df.columns:
        return df.copy(), None
    result = df.copy()
    # Parse both ISO dates and day-first dates from the spreadsheet consistently.
    parsed_dates = pd.to_datetime(result["Date"], errors="coerce", format="mixed", dayfirst=True)
    normalized_dates = parsed_dates.dt.strftime("%Y-%m-%d")
    if operator == "<":
        result = result[normalized_dates < target]
    elif operator == ">":
        result = result[normalized_dates > target]
    else:
        result = result[normalized_dates == target]
    return result, f"{operator}{target}"


def split_helpers(text):
    """Split command arguments on any supported helper separator."""
    return [part.strip() for part in re.split(r"[;:/\\|]+", text) if part.strip()]


def parse_command_parts(content):
    """Parse public <command>!!<arg>/<helper> syntax into internal parts."""
    match = re.match(r"^([a-z]+)!!(.*)$", content.strip(), flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    command, raw = match.groups()
    public_command = command.lower()
    # Public commands only; player searches use p!! and branches use br!!.
    supported_public_commands = {
        "p", "t", "i", "l", "br", "player", "tank", "info", "leaderboard",
        "records", "best", "a", "b", "c", "e", "nt", "r", "ra", "re",
        "s", "d", "say", "w", "cu", "cm", "cua", "help", "help2",
    }
    if public_command not in supported_public_commands:
        return []
    aliases = {
        "p": "n", "t": "t", "i": "i", "l": "p", "br": "br",
        "player": "n", "tank": "t", "info": "i", "leaderboard": "p",
        "records": "re", "best": "b",
    }
    command = aliases.get(public_command, public_command)
    return ["", command, *split_helpers(raw)]


def normalize_command_message(message):
    """Return a copy only when the message uses the public !! command syntax."""
    if not re.match(r"^[a-z]+!!", message.content.strip(), flags=re.IGNORECASE):
        return None
    return copy.copy(message)


async def handle_w_command(message, df, parts):
    if "nu" not in df.columns:
        await safe_send(message.channel, content="❌ No 'nu' column found in data.")
        return
    start_nu, end_nu, warning = 1, 15, None
    for value in parts[2:]:
        match = re.fullmatch(r"(\d+)-(\d+)", value.strip())
        if not match:
            continue
        start_nu, end_nu = map(int, match.groups())
        if start_nu > end_nu:
            start_nu, end_nu = end_nu, start_nu
        if end_nu - start_nu > 20:
            warning = "❌ Max NU range is 20!"
            end_nu = start_nu + 20
        break
    output = handle_nu_range(df)
    if output.empty:
        await safe_send(message.channel, content="❌ No valid nu data found.")
        return
    output = output[(output["nu"] >= start_nu) & (output["nu"] <= end_nu)].copy()
    if output.empty:
        await safe_send(message.channel, content="❌ No rows found in that nu range.")
        return
    cols = [col for col in ["Tank", "Name", "Score", "Id", "nu"] if col in output.columns]
    output = output[cols].reset_index(drop=True)
    page_size = 15
    view = RangePaginationView(
        df=output,
        start_index=1,
        range_size=page_size,
        title=f"NU Leaderboard ({start_nu}-{end_nu})",
        shorten_tank=True,
        formatting_type="v2",
    )
    # The range filters NU values, while the buttons paginate the resulting rows.
    first_page = output.iloc[:page_size].copy()
    first_page["Ņ"] = range(1, len(first_page) + 1)
    footer = f"NU range {start_nu}-{end_nu} • Rows 1-{len(first_page)} / {len(output)}"
    if warning:
        footer = f"{warning} • {footer}"
    embed = make_leaderboard_embed(
        f"NU Leaderboard ({start_nu}-{end_nu})",
        first_page,
        footer=footer,
        formatting_type="v2",
        shorten_tank=True,
    )
    msg = await safe_send(message.channel, embed=embed, view=view)
    view.message = msg


async def handle_screenshot_command(message, parts):
    if len(parts) < 3 or not parts[2].strip():
        await safe_send(message.channel, content="❌ Usage: s!!ID")
        return
    df = read_excel_cached()
    if isinstance(df, str) or df.empty:
        await safe_send(message.channel, content="❌ Data unavailable.")
        return
    df.columns = df.columns.str.strip()
    await send_screenshot(message.channel, df, parts[2].strip())


async def handle_description_command(message, parts):
    if len(parts) < 3 or not parts[2].strip():
        await safe_send(message.channel, content="❌ Usage: d!!ID")
        return
    df = read_excel_cached()
    if isinstance(df, str) or df.empty:
        await safe_send(message.channel, content="❌ Data unavailable.")
        return
    df.columns = df.columns.str.strip()
    await send_description_embed(message.channel, df, parts[2].strip())


async def handle_info_command(message, parts):
    if len(parts) < 3 or not parts[2].strip():
        await safe_send(message.channel, content="❌ Usage: i!!ID")
        return
    df = read_excel_cached()
    if isinstance(df, str) or df.empty:
        await safe_send(message.channel, content="❌ Data unavailable.")
        return
    df.columns = df.columns.str.strip()
    await send_info_embed(message.channel, df, parts[2].strip())


async def handle_random_analysis_command(message, df, parts):
    if len(parts) < 3 or not parts[2].strip():
        await safe_send(message.channel, content=(
            "ra!!0 - 10 random unscored tanks\n"
            "ra!!1 - 10 random tanks with records from 1Mil-5Mil\n"
            "ra!!2 - 10 random tanks with records from 5Mil-10Mil\n"
            "ra!!3 - 10 completely random tanks"
        ))
        return
    try:
        mode = int(parts[2])
        if mode not in (0, 1, 2, 3):
            raise ValueError
    except (TypeError, ValueError):
        await safe_send(message.channel, content="❌ Invalid mode. Use ra!!0, ra!!1, ra!!2, or ra!!3.")
        return
    output = handle_random_analysis(df, mode).copy()
    output["Tank"] = output["Tank"].astype(str).str[:14]
    embed = make_embed("Random Recommendations", dataframe_to_markdown_aligned(output, shorten_tank=False))
    embed.set_footer(text="🎲 Click the button to reroll")
    view = RandomAnalysisView(df, mode)
    msg = await safe_send(message.channel, embed=embed, view=view)
    view.message = msg


async def handle_branch_request(message, parts):
    if len(parts) < 3 or not parts[2].strip() or extract_date_filter([parts[2]])[1]:
        await safe_send(message.channel, content="❌ Usage: br!!BranchName[/a|/r][/YYYY-MM-DD]")
        return
    branch_name = parts[2].strip()
    branch_mode = next((value.strip().lower() for value in parts[3:] if value.strip().lower() in {"a", "r"}), None)
    if branch_mode == "a":
        await handle_branch_command(message, branch_name, gt_filter="A", branches_loader=load_branches, branch_resolver=handle_branch)
    elif branch_mode == "r":
        await handle_branch_command(message, branch_name, gt_filter="R", branches_loader=load_branches2, branch_resolver=handle_branch2)
    else:
        await handle_branch_command(message, branch_name, gt_filter=None, branches_loader=load_combined_branches, branch_resolver=resolve_combined_branch)


async def process_olympus_command(
    message,
    bypass_cooldown=False
):
    if message.author == bot.user:
        return

    # Every command, including fuzzy-button reruns, uses public !! syntax.
    compact = normalize_command_message(message)
    if compact is None:
        return
    message = compact

    
    if not bypass_cooldown:    
        now = time.time()
        if (
            now - user_cooldowns.get(message.author.id, 0)
            < COOLDOWN_SECONDS
        ):
            print(
                f"[DEBUG] Cooldown active for {message.author}"
            )
            return
        user_cooldowns[message.author.id] = now
    parts = parse_command_parts(message.content)
    if len(parts) < 2:
        return
    cmd = parts[1].lower()

    # --- Load Excel first ---
    df = read_excel_cached()
    if not isinstance(df, pd.DataFrame):
        await safe_send(message.channel, content="❌ Data unavailable.")
        return

    df.columns = df.columns.str.strip()
    # Date helper (YYYY-MM-DD or DD-MM-YYYY, optionally prefixed by <, >, =).
    df, date_filter = apply_date_filter(df, parts[2:])
    if date_filter and df.empty:
        await safe_send(message.channel, content=f"❌ No results for {date_filter}")
        return

    # ... continue with your normal cmd handling (p, b, n, t, etc.)
    



    
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
    title = None

    if cmd == "a":
        if not is_tejm(message.author):
            await safe_send(message.channel, content="Restricted command.")
            return
        output = df.copy()

    elif cmd == "b":
        output = handle_best(df)
        
    elif cmd == "n":
        output, title = await handle_player_command(message, df, parts)
        if output is None:
            return

    elif cmd == "nt":
        await handle_name_tank(message, df, parts)
        return

    elif cmd == "c":
        output = normalize_score(df).sort_values("Score", ascending=False).drop_duplicates("Tank")
        await maybe_send_random_message(message.channel, 0.99)
        
    elif cmd == "p":
        output = normalize_score(df).sort_values("Score", ascending=False)
        await maybe_send_random_message(message.channel, 0.05)

    elif cmd == "t":
        output, title = await handle_tank_command(message, df, parts)
        if output is None:
            return

    elif cmd == "e":
        output, title = await handle_extended_player_command(message, df, parts)
        if output is None:
            return
        shorten_tank = True

    elif cmd == "say":
        msgs = load_messages()
        if not msgs:
            await safe_send(message.channel, content="❌ No messages loaded.")
            return

        await message.delete()
        await safe_send(message.channel, content=random.choice(msgs))
        return


    elif cmd == "w":
        await handle_w_command(message, df, parts)
        return

    elif cmd == "cu":
        await handle_collective_score(message, df, parts)
        return

    elif cmd == "cm":
        await handle_cumulative_monthly_top20(message, df, parts)
        return

    elif cmd == "cua":
        await handle_cumulative_top10(message, df)
        return
    
    elif cmd == "s":
        await handle_screenshot_command(message, parts)
        return

    elif cmd == "d":
        await handle_description_command(message, parts)
        return

    elif cmd == "ra":
        await handle_random_analysis_command(message, df, parts)
        return

    elif cmd == "i":
        await handle_info_command(message, parts)
        return

    elif cmd == "re":
        if len(parts) < 3 or not parts[2].strip():
            await safe_send(
                message.channel,
                content="❌ Usage: re!!PlayerName (add /+ for personal bests)",
            )
            return
        await handle_records_player(message, df, parts)
        return

    
    # --- Call in on_message ---
    elif cmd == "br":
        await handle_branch_request(message, parts)
        return


    
        # --- HELP ---
    elif cmd == "help":
        help_message = (
                "Commands:\n"
                "l!!                - Leaderboard\n"            
                "t!!TankName        - Tank scores\n"
                "p!!PlayerName      - Player scores\n"
                "i!!ID              - Score info\n"
                "br!!BranchName/a  - Every tank in an AR branch\n"
                "r!!a             - Random recommendation\n"            

                "help2!!            -for more commands\n"
            )
        await safe_send(message.channel, content=help_message)
        return

    elif cmd == "help2":
        help_message = (
            "Commands:\n"
            "re!!PlayerName       - Score records of a player\n"
            "br!!BranchName/a     - AR branch highscores\n"
            "br!!BranchName/r     - non-AR branch highscores\n"
            "c!!                  - Top tank list\n"
            "b!!                  - Top player list\n"
            "e!!PlayerName        - Player scores with global + tank ranks\n"
            "s!!ID                - Screenshot of a score\n"
            "d!!ID                - Score description\n"
            "cu!!PlayerName       - Cumulative leaderboard for a player\n"
            "cua!!                - Cumulative leaderboard of all time\n"
            "cm!!YYYY-MM          - Cumulative leaderboard for a month\n"
            "w!!1-15              - See newly added scores\n"
            "ra!!0/1/2/3          - Random recommendations\n"
            "r!!a/b/r             - Random tank recommendation modes\n"
            "nt!!Player/Tank      - Search a player and tank together\n"
            "say!!                - Random text\n"
            "Add /1-15 to limit results, or /YYYY-MM-DD, /<YYYY-MM-DD, />YYYY-MM-DD to filter by date."
        )
        await safe_send(message.channel, content=help_message)
        return

    elif cmd == "r":
        if len(parts) == 2:
            await safe_send(
                message.channel,
                content=(
                    "**r!!a** for a tank with a player record!\n"
                    "**r!!b** for the tank with no score!\n"
                    "**r!!r** for a fully random tank!"
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
        await bot.process_commands(message)
        return


    # ---------------- GT FILTER HERE ----------------
    command_options = COMMAND_OPTIONS.get(cmd, {"allow_range": True, "used_lb_type": True})
    gt_filter = extract_gt(parts) if command_options.get("used_lb_type", True) else None
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

    cols = COLUMN_ORDER.get(cmd, COLUMN_ORDER["default"]).copy()
    cols = [c for c in cols if c in output.columns]
    output = output[cols]


    # after output is finalized
    if 'title' not in locals() or title is None:
        title_map = {
            "a": "All Scores",
            "b": "Best Players",
            "c": "Best Per Tank",
            "p": "Leaderboard"
        }
        title = title_map.get(cmd, "Olymp Leaderboard")

    if command_options.get("allow_range", True):
        start, end, range_size, warning = extract_range(parts, max_range=20, total_len=len(output))
    else:
        start, end, range_size, warning = 1, min(15, len(output)), min(15, len(output)), None


    formatting_type = COMMAND_OPTIONS.get(cmd, {}).get("formatting_type", "v2")
    view = RangePaginationView(
        df=output,
        start_index=start,
        range_size=range_size,
        title=title,
        shorten_tank=shorten_tank,
        formatting_type=formatting_type,
    )
    slice_df = output.iloc[start-1:end].copy()
    slice_df["Ņ"] = range(start, min(end, len(output)) + 1)
    footer = f"Rows {start}-{min(end, len(output))} / {len(output)}"
    if warning:
        footer = f"{warning} • {footer}"
    embed = make_leaderboard_embed(
        title, slice_df, footer=footer,
        formatting_type=formatting_type, shorten_tank=shorten_tank
    )


    msg = await safe_send(message.channel, embed=embed, view=view)
    view.message = msg
    await bot.process_commands(message) 
    



    # -------------l!!1-10------------------------

@bot.event
async def on_message(message):
    try:
        await process_olympus_command(message)
    except Exception as exc:
        # Keep a malformed command from producing an unhandled event traceback.
        print(f"[COMMAND ERROR] {type(exc).__name__}: {exc}")
        if message.author != bot.user and "!!" in message.content:
            await safe_send(
                message.channel,
                content="❌ Something went wrong while processing that command. Check the command syntax and try again.",
            )


if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
