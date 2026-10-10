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
    "n": ["Ņ", "Score", "Tank", "Date", "Id"],
    "t": ["Ņ", "Score", "Name", "Date", "Id"],
    "e": ["Ņ", "Score", "Tank", "LB", "Tank LB", "Id"],
}
FIRST_COLUMN = "Score"
# Per-command behavior switches. Edit these instead of duplicating logic.
COMMAND_OPTIONS = {
    "n": {"used_lb_type": True, "used_fuzzy_matching": True, "fuzzy_column": "Name", "allow_range": True},
    "t": {"used_lb_type": True, "used_fuzzy_matching": True, "fuzzy_column": "Tank", "allow_range": True},
    "e": {"used_lb_type": True, "used_fuzzy_matching": True, "fuzzy_column": "Name", "allow_range": True},
    "c": {"used_lb_type": True, "used_fuzzy_matching": False, "fuzzy_column": None, "allow_range": True},
    "b": {"used_lb_type": True, "used_fuzzy_matching": False, "fuzzy_column": None, "allow_range": True},
    "p": {"used_lb_type": True, "used_fuzzy_matching": False, "fuzzy_column": None, "allow_range": True},
    "bch": {"used_lb_type": True, "used_fuzzy_matching": True, "fuzzy_column": "Branch", "allow_range": False},
}
COOLDOWN_SECONDS = 7
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


def make_leaderboard_embed(title, frame, footer=None, row_layout=False, shorten_tank=True):
    """Render either the compact text table or a readable row-by-row embed."""
    if not row_layout:
        embed = make_embed(title, dataframe_to_markdown_aligned(frame, shorten_tank))
    else:
        embed = Embed(title=title, color=discord.Color.red())
        display = frame.copy()
        if "Score" in display.columns:
            display["Score"] = display["Score"].apply(
                lambda value: f"{float(value) / 1_000_000:,.3f} M"
            )
        if "Date" in display.columns:
            display["Date"] = display["Date"].astype(str).str[:10]
        if "Name" in display.columns:
            display["Name"] = display["Name"].astype(str).map(lambda value: shorten_name(value, 16))
        if shorten_tank and "Tank" in display.columns:
            display["Tank"] = display["Tank"].astype(str).str[:18]
        rank_col = "Ņ" if "Ņ" in display.columns else None
        header_cols = [col for col in display.columns if col != rank_col]
        # Column names appear once; each result stays on one line. Inline-code
        # padding preserves approximate column alignment in Discord Markdown.
        widths = {}
        for col in header_cols:
            values = [str(value).replace("`", "ˋ") for value in display[col].tolist()]
            widths[col] = min(20, max([len(col)] + [len(value) for value in values]))

        def cell(value, width):
            value = str(value).replace("`", "ˋ")
            return f"`{value[:width]:<{width}}`"

        header = " | ".join(cell(col, widths[col]) for col in header_cols)
        lines = [f"**{rank_col or 'Rank'}** | {header}"]
        for _, row in display.iterrows():
            rank = str(row[rank_col]) if rank_col else "•"
            values = [cell(row[col], widths[col]) for col in header_cols]
            lines.append(f"**{rank}.** | " + " | ".join(values))
        embed.description = "\n".join(lines)[:4096]
    if footer:
        embed.set_footer(text=footer)
    return embed







async def handle_collective_score(message, df, parts):
    if len(parts) < 3:
        await safe_send(
            message.channel,
            content="Almost, usage: cu!!PlayerName"
        )
        return
    # Prevent the same user from running CU twice at once
    user_id = message.author.id
    if user_id in CU_ACTIVE:
        return
    CU_ACTIVE.add(user_id)
    cooking_msg = None
    try:
        cooking_msg = await safe_send(
            message.channel,
            content="Cooking up"
        )
        name_input = parts[2].strip()
        # Player name lookup
        names = {
            str(name).lower(): str(name)
            for name in df["Name"].dropna().unique()
        }
        name_key = name_input.lower()
        if name_key not in names:
            matches = get_close_matches(
                name_key,
                names.keys(),
                n=1,
                cutoff=0.65
            )

            if not matches:
                if cooking_msg:
                    await cooking_msg.edit(
                        content=f"`{name_input}` not found."
                    )
                return
            name = names[matches[0]]
        else:
            name = names[name_key]
        # Get every score by player
        player_df = df[
            df["Name"].astype(str).str.lower() == name.lower()
        ].copy()
        if player_df.empty:
            if cooking_msg:
                await cooking_msg.edit(
                    content=f"No scores found for **{name}**."
                )
            return
        player_df = normalize_score(player_df)
        # Sum ALL scores
        total_score = player_df["Score"].sum()
        # Random tank they played
        played_tanks = (
            player_df["Tank"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        random_tank = (
            random.choice(played_tanks)
            if played_tanks
            else "Unknown"
        )
        total_mil = total_score / 1_000_000
        result = (
            f"All together **{name}** got **{total_mil:,.3f} M**, "
            f"and the most integral tank to that was **{random_tank}**."
        )
        if cooking_msg:
            await cooking_msg.edit(content=result)
    except Exception as e:
        print("[CU ERROR]", e)
        if cooking_msg:
            try:
                await cooking_msg.edit(
                    content="Failed cooking that up."
                )
            except:
                pass
    finally:
        CU_ACTIVE.discard(user_id)






class DidYouMeanButton(ui.Button):
    def __init__(self, label: str):
        super().__init__(
            label=label,
            style=discord.ButtonStyle.secondary
        )

    
    async def callback(self, interaction: Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer()
        view: DidYouMeanView = self.view
        # Parts are parsed as ["", internal_command, arg1, helper1, ...].
        corrected_parts = list(view.parts)
        if view.index < 2 or view.index >= len(corrected_parts):
            await interaction.followup.send(
                "❌ I couldn't apply that suggestion because the command arguments changed. Please run the command again.",
                ephemeral=True,
            )
            return
        corrected_parts[view.index] = self.label
        internal_to_public = {"n": "p", "bch": "br", "p": "l"}
        public_command = internal_to_public.get(
            corrected_parts[1].lower(), corrected_parts[1].lower()
        )
        corrected_command = public_command + "!!" + "/".join(corrected_parts[2:])
        print(f"[FUZZY] {view.cmd} -> {corrected_command}")
        # Remove old "Did you mean?" message
        await interaction.edit_original_response(
            content=f" Cooking...",
            embed=None,
            view=None
        )
        # Create a fake message using the ORIGINAL message
        fake_message = copy.copy(view.message_source)
        fake_message.content = corrected_command
        # Execute the whole command again
        await process_olympus_command(
            fake_message,
            bypass_cooldown=True
        )





def handle_random_analysis(df, mode):
    df = normalize_score(df)
    best = (
        df.sort_values("Score", ascending=False)
          .drop_duplicates("Tank")
    )
    used = set(best["Tank"].str.lower())
    unused = [t for t in TANK_NAMES if t.lower() not in used]
    if mode == 0:
        rows = [{
            "Score": 0,
            "Tank": t,
            "Name": "Noone!",
            "Id": "-"
        } for t in random.sample(unused, min(10, len(unused)))]
    elif mode == 1:
        pool = best[(best["Score"] >= 1_000_000) &
                    (best["Score"] < 5_000_000)]
        rows = pool.sample(min(10, len(pool)))[["Score","Tank","Name","Id"]].to_dict("records")
    elif mode == 2:
        pool = best[(best["Score"] >= 5_000_000) &
                    (best["Score"] < 10_000_000)]
        rows = pool.sample(min(10, len(pool)))[["Score","Tank","Name","Id"]].to_dict("records")
    else:
        rows = best[["Score","Tank","Name","Id"]].to_dict("records")
        rows.extend({
            "Score": 0,
            "Tank": t,
            "Name": "Noone!",
            "Id": "-"
        } for t in unused)
        rows = random.sample(rows, min(10, len(rows)))
    return pd.DataFrame(rows)[["Score","Tank","Name","Id"]]



def handle_name_extended(df, name):
    """
    Same as p!!PlayerName, but adds:
      LB       = global leaderboard rank for the score
      Tank LB  = leaderboard rank within that tank
    """
    df = normalize_score(df).copy()
    # Sort every score globally, highest first
    df = df.sort_values("Score", ascending=False).reset_index(drop=True)
    # Overall leaderboard rank
    df["LB"] = range(1, len(df) + 1)
    # Rank within each tank
    df["Tank LB"] = (
        df.groupby("Tank")["Score"]
          .rank(method="min", ascending=False)
          .astype(int)
    )
    # Only this player's scores
    player_df = df[
        df["Name"].astype(str).str.lower() == name.lower()
    ].copy()
    # Keep the player's scores ordered by global score
    player_df = player_df.sort_values("Score", ascending=False)
    return player_df






def handle_nu_range(df):
    """
    Uses the 'nu' column as the filter source.
    Example:
    w!!1-15
    Means:
    show rows where nu is between 1 and 15
    Output columns:
    Tank, Name, Score, Id, nu
    """
    if "nu" not in df.columns:
        return pd.DataFrame()
    df = normalize_score(df).copy()
    # make nu numeric
    df["nu"] = pd.to_numeric(df["nu"], errors="coerce")
    # remove invalid nu rows
    df = df.dropna(subset=["nu"])
    # sort by nu ascending
    df = df.sort_values("nu", ascending=True).reset_index(drop=True)
    return df




# --- Helper for nt!! ---
async def handle_name_tank(message, df, parts):
    if len(parts) < 4:
        await safe_send(message.channel, content="❌ Usage: nt!!PlayerName/TankName")
        return
    input1, input2 = parts[2].strip(), parts[3].strip()
    name_choices = df["Name"].dropna().unique()
    tank_choices = df["Tank"].dropna().unique()
    # Detect which is name / tank
    name = input1 if any(input1.lower() == n.lower() for n in name_choices) else input2
    tank = input2 if name == input1 else input1
    # Fuzzy name
    name = await fuzzy_or_abort(
        message=message,
        df=df,
        user_input=name,
        choices=name_choices,
        arg_index=2,
        resolver=handle_name,
        title="Player not found — did you mean?",
        result_title="Player Scores",
        columns=["Ņ", "Tank", "Score", "Date", "Id"]
    )
    if name is None:
        return
    # Fuzzy tank
    tank = await fuzzy_or_abort(
        message=message,
        df=df,
        user_input=tank,
        choices=tank_choices,
        arg_index=3,
        resolver=handle_tank,
        title="Tank not found — did you mean?",
        result_title="Tank Scores",
        columns=["Ņ", "Name", "Score", "Date", "Id"]
    )
    if tank is None:
        return
    # Filter results
    df_filtered = df[
        (df["Name"].str.lower() == name.lower()) &
        (df["Tank"].str.lower() == tank.lower())
    ].copy()
    if df_filtered.empty:
        await safe_send(
            message.channel,
            content=f"❌ No scores for **{name}** with **{tank}**."
        )
        return
    df_filtered = normalize_score(df_filtered)
    df_filtered = df_filtered.sort_values("Score", ascending=False)
    df_filtered = add_index(df_filtered)
    cols = ["Ņ", "Score", "Date", "Id"]
    df_filtered = df_filtered[cols]
    # ---------- RANGE ----------
    start, end, range_size, warning = extract_range(
        parts,
        max_range=20,
        total_len=len(df_filtered)
    )
    # ---------- PAGINATION ----------
    view = RangePaginationView(
        df=df_filtered,
        start_index=start,
        range_size=range_size,
        title=f"Scores for {name} with {tank}",
        shorten_tank=True
    )
    slice_df = df_filtered.iloc[start-1:end].copy()
    slice_df["Ņ"] = range(start, min(end, len(df_filtered)) + 1)
    lines = dataframe_to_markdown_aligned(slice_df)
    title = f"Scores for {name} with {tank}"
    embed = make_embed(title, lines)
    footer = f"Rows {start}-{min(end, len(df_filtered))} / {len(df_filtered)}"
    if warning:
        footer = f"{warning} • {footer}"
    embed.set_footer(text=footer)
    msg = await safe_send(message.channel, embed=embed, view=view)
    view.message = msg
    





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
        color=discord.Color.red()
    )
    embed.set_image(url=cdn_url)

    await safe_send(channel, embed=embed)




BRANCHES_JSON = []
BRANCHES2_JSON = []


def load_branches():
    """Load the normal branch definitions from data/branches.json."""
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


def load_branches2():
    """Load the ;r branch definitions from data/branches2.json."""
    global BRANCHES2_JSON

    if BRANCHES2_JSON:
        return BRANCHES2_JSON

    try:
        with open("data/branches2.json", "r") as f:
            BRANCHES2_JSON = json.load(f)
        print("Branches2 loaded locally")
    except Exception as e:
        print("Branches2 load failed:", e)
        return "fetch_error"

    return BRANCHES2_JSON


def handle_branch(df, branch_key):
    branches = load_branches()
    if not isinstance(branches, dict):
        return None
    return branches.get(branch_key)


def handle_branch2(df, branch_key):
    branches = load_branches2()
    if not isinstance(branches, dict):
        return None
    return branches.get(branch_key)


async def handle_branch_command(
    message,
    branch_name: str,
    gt_filter: str,
    interaction: Interaction | None = None,
    branches_loader=load_branches,
    branch_resolver=handle_branch
):
    # The caller must explicitly choose GT=A or GT=R; there is no default mode.
    branches = branches_loader()
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
        df=None,
        user_input=branch_name,
        choices=branches.keys(),
        arg_index=2,
        resolver=branch_resolver,
        title="Branch not found — did you mean?",
        result_title="Branch Highscores",
        columns=["Ņ", "Tank", "Name", "Score", "Id"],
        cutoff=0.6,
        used_fuzzy_matching=COMMAND_OPTIONS["bch"]["used_fuzzy_matching"],
        fuzzy_column=COMMAND_OPTIONS["bch"]["fuzzy_column"],
    )
    if branch_key is None:
        return

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

    # Branch commands always go through the GT filter:
    # normal bch = A, bch;r = R.
    if "GT" not in df.columns:
        content = "❌ No 'GT' column found in data."

        if interaction:
            await interaction.edit_original_response(
                content=content,
                embed=None,
                view=None
            )
        else:
            await safe_send(message.channel, content=content)
        return

    df = df[
        df["GT"].astype(str).str.strip().str.upper() == gt_filter.upper()
    ].copy()

    if df.empty:
        content = f"❌ No results for GT={gt_filter.upper()}."

        if interaction:
            await interaction.edit_original_response(
                content=content,
                embed=None,
                view=None
            )
        else:
            await safe_send(message.channel, content=content)
        return

    df = normalize_score(df)

    # Build rows: top score per tank after the GT filter.
    rows = []
    for tank in branch_tanks:
        tank_rows = df[
            df["Tank"].astype(str).str.lower() == str(tank).lower()
        ]
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

    rows.sort(key=lambda x: x["Score"], reverse=True)
    rows = rows[:16]

    display_df = pd.DataFrame(rows)
    display_df["Ņ"] = range(1, len(display_df) + 1)
    display_df = display_df[["Ņ", "Tank", "Name", "Score", "Id"]]

    lines = dataframe_to_markdown_aligned(display_df)

    title = f"{branch_key} Branch"
    embed = make_embed(title, lines)
    embed.set_footer(
        text=f"{len(display_df)} tanks in this branch • GT={gt_filter.upper()}"
    )

    if interaction:
        await interaction.edit_original_response(
            embed=embed,
            view=None
        )
    else:
        await safe_send(message.channel, embed=embed)


async def handle_cumulative_monthly_top20(message, df, parts):
    """
    cm!!YYYY-MM

    Build a cumulative top-20 leaderboard using only scores whose
    Date falls within the requested calendar month.
    """
    if len(parts) < 3 or not re.fullmatch(r"\d{4}-\d{2}", parts[2].strip()):
        await safe_send(
            message.channel,
            content="❌ Usage: cm!!YYYY-MM  (example: cm!!2026-07)"
        )
        return

    month = parts[2].strip()

    # Validate that YYYY-MM is an actual calendar month.
    try:
        datetime.strptime(month, "%Y-%m")
    except ValueError:
        await safe_send(
            message.channel,
            content=f"❌ Invalid month: `{month}`. Use YYYY-MM, e.g. `2026-07`."
        )
        return

    if "Date" not in df.columns:
        await safe_send(
            message.channel,
            content="❌ No Date column found in the data."
        )
        return

    cooking_msg = await safe_send(
        message.channel,
        content="Cooking up"
    )

    try:
        month_df = df.copy()
        month_df["Date"] = month_df["Date"].astype(str).str[:10]
        month_df = month_df[
            month_df["Date"].str.match(r"^\d{4}-\d{2}-\d{2}$", na=False) &
            month_df["Date"].str[:7].eq(month)
        ].copy()

        if month_df.empty:
            if cooking_msg:
                await cooking_msg.edit(
                    content=f"❌ No scores found for **{month}**."
                )
            return

        month_df = normalize_score(month_df)
        month_df = month_df.dropna(subset=["Name"])
        month_df["Name"] = month_df["Name"].astype(str)

        # ---------------- TOTAL SCORES ----------------
        totals = (
            month_df.groupby("Name", as_index=False)["Score"]
                    .sum()
        )

        # ---------------- FAVOURITE TANK ----------------
        fave_counts = (
            month_df.dropna(subset=["Tank"])
                    .groupby(["Name", "Tank"])
                    .size()
                    .reset_index(name="Uses")
        )

        fave_counts = (
            fave_counts
            .sort_values(["Name", "Uses"], ascending=[True, False])
            .drop_duplicates("Name")
        )

        # ---------------- MERGE ----------------
        output = totals.merge(
            fave_counts[["Name", "Tank"]],
            on="Name",
            how="left"
        )
        output = output.rename(columns={"Tank": "Fave"})
        output["Fave"] = output["Fave"].fillna("?")

        # Top 20 cumulative scores for this month only.
        output = (
            output.sort_values("Score", ascending=False)
                  .head(20)
                  .reset_index(drop=True)
        )

        output["Ņ"] = range(1, len(output) + 1)
        output = output[["Ņ", "Name", "Score", "Fave"]]

        output["Fave"] = (
            output["Fave"]
            .astype(str)
            .str[:12]
        )

        lines = dataframe_to_markdown_aligned(
            output,
            shorten_tank=False
        )

        embed = make_embed(
            f"Top 20 Cumulative Scores — {month}",
            lines
        )
        embed.set_footer(
            text=f"All scores from {month} combined • {len(month_df)} scores"
        )

        if cooking_msg:
            await cooking_msg.edit(
                content=None,
                embed=embed
            )

    except Exception as e:
        print("[CM ERROR]", e)
        if cooking_msg:
            try:
                await cooking_msg.edit(
                    content="❌ Failed cooking that up."
                )
            except:
                pass


async def handle_cumulative_top10(message, df):
    cooking_msg = await safe_send(
        message.channel,
        content="Cooking up"
    )

    try:
        df = normalize_score(df).copy()
        # Remove invalid names
        df = df.dropna(subset=["Name"])
        df["Name"] = df["Name"].astype(str)

        # ---------------- TOTAL SCORES ----------------
        totals = (
            df.groupby("Name", as_index=False)["Score"]
              .sum()
        )
        # ---------------- FAVOURITE TANK ----------------
        # Tank used the most = most score entries with that tank
        fave_counts = (
            df.dropna(subset=["Tank"])
              .groupby(["Name", "Tank"])
              .size()
              .reset_index(name="Uses")
        )
        fave_counts = (
            fave_counts
            .sort_values(
                ["Name", "Uses"],
                ascending=[True, False]
            )
            .drop_duplicates("Name")
        )
        # ---------------- MERGE ----------------
        output = totals.merge(
            fave_counts[["Name", "Tank"]],
            on="Name",
            how="left"
        )
        output = output.rename(
            columns={"Tank": "Fave"}
        )
        output["Fave"] = output["Fave"].fillna("?")
        # Top 15 cumulative scores
        output = (
            output
            .sort_values("Score", ascending=False)
            .head(20)
            .reset_index(drop=True)
        )
        output["Ņ"] = range(1, len(output) + 1)
        output = output[
            ["Ņ", "Name", "Score", "Fave"]
        ]
        # Shorten favourite tank for table
        output["Fave"] = (
            output["Fave"]
            .astype(str)
            .str[:12]
        )
        lines = dataframe_to_markdown_aligned(
            output,
            shorten_tank=False
        )
        embed = make_embed(
            "Top 15 Cumulative Scores",
            lines
        )
        embed.set_footer(
            text="All scores combined and most played tank"
        )
        if cooking_msg:
            await cooking_msg.edit(
                content=None,
                embed=embed
            )
    except Exception as e:
        print("[CU15 ERROR]", e)
        if cooking_msg:
            await cooking_msg.edit(
                content="❌ Failed cooking that up."
            )











def parse_playtime(v):
    try:
        if pd.isna(v) or v in ("?", "", None):
            return 0.0
        # Debug (remove later)
        print(f"Playtime value: {v!r}")
        print(f"Playtime type : {type(v)}")
        # Timedelta
        if isinstance(v, pd.Timedelta):
            return v.total_seconds()
        # Excel datetime (1900 system)
        if isinstance(v, (pd.Timestamp, datetime)):
            base = datetime(1899, 12, 30)
            return (v.to_pydatetime() if isinstance(v, pd.Timestamp) else v - base).total_seconds()
        # datetime.time (<24h)
        if isinstance(v, dt_time):
            return v.hour * 3600 + v.minute * 60 + v.second
        # Excel serial number (days)
        if isinstance(v, (int, float)):
            return float(v) * 86400
        # String
        s = str(v).strip()
        # "1 day, 2:34:56"
        if "day" in s:
            td = pd.to_timedelta(s)
            return td.total_seconds()
        # "26:15:10"
        if ":" in s:
            h, m, sec = map(int, s.split(":"))
            return h * 3600 + m * 60 + sec
        return 0.0
    except Exception as e:
        print("parse_playtime error:", e)
        return 0.0




def parse_score(v):
    try:
        if pd.isna(v) or v in ("?", "", None):
            return 0.0
        return float(str(v).replace(",", ""))
    except:
        return 0.0




async def send_info_embed(channel, df, info_id, interaction=None):
    # Ensure Id column exists
    if "Id" not in df.columns:
        await safe_send(channel, content="❌ No Id column in data.")
        return
    # Match base64 Id as string
    row = df[df["Id"].astype(str) == str(info_id)]
    if row.empty:
        await safe_send(channel, content="❌ No entry with that Id.")
        await maybe_send_random_message(channel, 0.99)
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
  #      f"**{name1}**\n"
        f"{name} got **{int(score):,}** with **{tank}**.\n"
        f"It took **{playtime_display}** hours, on **{date}**, "
        f"with a ratio of **{ratio_display}** per hour.\n"
        f"{name} died to **{killer}**."
    )
    embed = Embed(
        title=f"{tank} by {name1}",        #        title=f"{tank} — {int(score):,}",
        description=description,
        color=discord.Color.green()
    )
    # Image 
    # Image
    DEFAULT_SCREENSHOT = "https://cdn.discordapp.com/attachments/1466759427955888160/1466762183378604248/id_A.jpg?ex=6a5773bb&is=6a56223b&hm=66161e5238ce024020580aa4346a10e912563c3f4f175c79a2ae2f9a1088292a"
    cdn_url = safe_val(row, "CDN", None)
    if (
        not cdn_url
        or not isinstance(cdn_url, str)
        or not cdn_url.strip().startswith(("http://", "https://"))
    ):
        cdn_url = DEFAULT_SCREENSHOT
    # Healer column → Embed footer
    row_healer = safe_val(row, "Heal", None)
    if row_healer is None or str(row_healer).strip() in ("", "None", "nan"):
        embed.set_footer(text="Healers Unknown")
    else:
        embed.set_footer(text=f"Healers: {row_healer}")
    embed.set_image(url=cdn_url)
    if interaction:
        await interaction.edit_original_response(
            content=None,
            embed=embed
        )
    else:
        await safe_send(channel, embed=embed)
async def send_description_embed(channel, df, info_id, interaction=None):
    # Ensure Id column exists
    if "Id" not in df.columns:
        await safe_send(channel, content="❌ No Id column in data.")
        return
    # Find row by Id
    row = df[df["Id"].astype(str) == str(info_id)]
    if row.empty:
        await safe_send(
            channel,
            content="❌ No entry with that Id."
        )
        return
    row = row.iloc[0]
    # Get values safely
    name1 = safe_val(row, "Name", "Unknown")
    name = safe_val(row, "Name in game", "Unknown")
    tank = safe_val(row, "Tank", "Unknown")
    score = safe_val(row, "Score", 0)
    date = safe_val(row, "Date", "Unknown")
    killer = safe_val(row, "Killer", "Unknown")
    healer = safe_val(row, "Heal", None)
    description = safe_val(row, "Description", "No description.")
    # Format score
    try:
        score = int(float(str(score).replace(",", "")))
        score_display = f"{score:,}"
    except:
        score_display = str(score)
    # Healer
    if healer is None or str(healer).strip() in ("", "None", "nan", "?"):
        healer_display = "Healers Unknown"
    else:
        healer_display = f"Thanks {healer} for heals"
    embed_description = (
        f"**Name:** {name1}\n"
        f"**Tank:** {tank}\n"
        f"**In-Game Name:** {name}\n"
        f"**Score:** {score_display}\n"
        f"**Date:** {str(date)[:10]}\n"
        f"**Killer:** {killer}\n"
        f"**{healer_display}**\n\n"
        f"**Description:**\n"
        f"{description}"
    )
    embed = Embed(
        title=f"{tank} by {name1}",
        description=embed_description,
        color=discord.Color.green()
    )
    if interaction:
        await interaction.edit_original_response(
            content=None,
            embed=embed
        )
    else:
        await safe_send(
            channel,
            embed=embed
        )







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
    def __init__(self, df, start_index, range_size, title, shorten_tank, row_layout=False):
        super().__init__(timeout=180)
        self.df = df.reset_index(drop=True)
        self.range_size = range_size
        self.title = title
        self.shorten_tank = shorten_tank
        self.row_layout = row_layout

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
            row_layout=self.row_layout, shorten_tank=self.shorten_tank
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
    result["Date"] = result["Date"].astype(str).str[:10]
    if operator == "<":
        result = result[result["Date"] < target]
    elif operator == ">":
        result = result[result["Date"] > target]
    else:
        result = result[result["Date"] == target]
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
    aliases = {
        "p": "n", "t": "t", "i": "i", "l": "p", "br": "bch",
        "player": "n", "tank": "t", "info": "i", "leaderboard": "p",
        "branch": "bch", "records": "re", "best": "b",
    }
    command = aliases.get(command.lower(), command.lower())
    return ["", command, *split_helpers(raw)]


def normalize_command_message(message):
    """Return a copy only when the message uses the public !! command syntax."""
    if not re.match(r"^[a-z]+!!", message.content.strip(), flags=re.IGNORECASE):
        return None
    return copy.copy(message)


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
        """
        w!!1-15
        Means:
        show all rows where nu >= 1 and nu <= 15
        Max allowed range:
        20
        Examples:
        w!!1-15
        w!!40-50
        w!!100-120
        """
        if "nu" not in df.columns:
            await safe_send(
                message.channel,
                content="❌ No 'nu' column found in data."
            )
            return
        # default values
        start_nu = 1
        end_nu = 15
        warning = None
        # read explicit nu range from command
        for p in parts:
            if "-" in p:
                try:
                    a, b = map(int, p.split("-"))
                    if a > b:
                        a, b = b, a
                    # MAX RANGE = 20
                    if (b - a) > 20:
                        warning = "❌ Max NU range is 20!"
                        b = a + 20
                    start_nu = a
                    end_nu = b
                    break
                except:
                    pass
        output = handle_nu_range(df)
        if output.empty:
            await safe_send(
                message.channel,
                content="❌ No valid nu data found."
            )
            return
        # FILTER BY nu VALUE
        output = output[
            (output["nu"] >= start_nu) &
            (output["nu"] <= end_nu)
        ].copy()
        if output.empty:
            await safe_send(
                message.channel,
                content="❌ No rows found in that nu range."
            )
            return
        # display columns
        cols = ["Tank", "Name", "Score", "Id", "nu"]
        cols = [c for c in cols if c in output.columns]
        output = output[cols]
        title = f"NU Leaderboard ({start_nu}-{end_nu})"
        shorten_tank = True
        # embed output (same style as your other commands)
        lines = dataframe_to_markdown_aligned(output, shorten_tank)
        embed = make_embed(title, lines)
        footer = f"NU range {start_nu}-{end_nu} • {len(output)} rows"
        if warning:
            footer = f"{warning} • {footer}"
        embed.set_footer(text=footer)
        await safe_send(
            message.channel,
            embed=embed
        )

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
        if len(parts) < 3:
            await safe_send(
                message.channel,
                content="❌ Usage: s!!ID"
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

    elif cmd == "d":
        if len(parts) < 3:
            await safe_send(
                message.channel,
                content="❌ Usage: d!!ID"
            )
            return
        info_id = parts[2].strip()
        df = read_excel_cached()
        if isinstance(df, str) or df.empty:
            await safe_send(
                message.channel,
                content="❌ Data unavailable."
            )
            return
        df.columns = df.columns.str.strip()
        await send_description_embed(
            message.channel,
            df,
            info_id
        )
        return


    elif cmd == "ra":
        if len(parts) == 2:
            await safe_send(
                message.channel,
                content=(
                    "ra!!0 - 10 random unscored tanks\n"
                    "ra!!1 - 10 random tanks with records from 1Mil-5Mil\n"
                    "ra!!2 - 10 random tanks with records from 5Mil-10Mil\n"
                    "ra!!3 - 10 completely random tanks"
                )
            )
            return
        try:
            mode = int(parts[2])
            if mode not in (0,1,2,3):
                raise ValueError
        except:
            await safe_send(message.channel, content="❌ Invalid mode.")
            return
        output = handle_random_analysis(df, mode)
        output = output.copy()
        output["Tank"] = output["Tank"].astype(str).str[:14]
        lines = dataframe_to_markdown_aligned(output, shorten_tank=False)
        embed = make_embed("Random Recommendations", lines)
        embed.set_footer(text="🎲 Click the button to reroll")
        view = RandomAnalysisView(df, mode)
        msg = await safe_send(message.channel, embed=embed, view=view)
        view.message = msg
        return

    



    elif cmd == "i":
        if len(parts) < 3:
            await safe_send(
                message.channel,
                content="❌ Usage: i!!ID"
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
    elif cmd == "bch":
        if len(parts) < 3:
            await safe_send(
                message.channel,
                content="❌ Usage: br!!BranchName/a or br!!BranchName/r"
            )
            return

        branch_name = parts[2].strip()
        branch_mode = next((p.strip().lower() for p in parts[3:] if p.strip().lower() in {"a", "r"}), None)
        if not branch_name or branch_mode is None:
            await safe_send(
                message.channel,
                content="❌ Choose a branch and type: br!!BranchName/a or br!!BranchName/r",
            )
            return

        if branch_mode == "a":
            await handle_branch_command(
                message,
                branch_name,
                gt_filter="A",
                branches_loader=load_branches,
                branch_resolver=handle_branch
            )
        else:
            await handle_branch_command(
                message,
                branch_name,
                gt_filter="R",
                branches_loader=load_branches2,
                branch_resolver=handle_branch2
            )
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


    row_layout = cmd in {"c", "b"}  # Toggle here to compare row-style vs text-table embeds.
    view = RangePaginationView(
        df=output,
        start_index=start,
        range_size=range_size,
        title=title,
        shorten_tank=shorten_tank,
        row_layout=row_layout,
    )
    slice_df = output.iloc[start-1:end].copy()
    slice_df["Ņ"] = range(start, min(end, len(output)) + 1)
    footer = f"Rows {start}-{min(end, len(output))} / {len(output)}"
    if warning:
        footer = f"{warning} • {footer}"
    embed = make_leaderboard_embed(
        title, slice_df, footer=footer,
        row_layout=row_layout, shorten_tank=shorten_tank
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
