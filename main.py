import discord
import pandas as pd
import requests
from io import BytesIO
from wcwidth import wcswidth
import os, time, json, random, re
from keep_alive import keep_alive

DRIVE_FILE_ID = "1YMzE4FXjH4wctFektINwhCDjzZ0xqCP6"
TANKS_JSON_FILE_ID = "1pGcmeDcTqx2h_HXA_R24JbaqQiBHhYMQ"

COLUMNS_DEFAULT = ["Ņ", "Score", "True Name", "Tank Type", "Date"]
COLUMNS_C = ["Ņ", "Tank Type", "True Name", "Score", "Date"]

FIRST_COLUMN = "Score"
LEGENDS = 1000
COOLDOWN_SECONDS = 5
user_cooldowns = {}

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

def read_excel():
    try:
        url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}&export=download"
        r = requests.get(url)
        r.raise_for_status()
        return pd.read_excel(BytesIO(r.content))
    except:
        return pd.DataFrame()

def load_tanks():
    url = f"https://drive.google.com/uc?export=download&id={TANKS_JSON_FILE_ID}"
    r = requests.get(url)
    r.raise_for_status()
    return json.loads(r.text)["tanks"]

TANK_NAMES = load_tanks()

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
    if FIRST_COLUMN in df:
        df[FIRST_COLUMN] = df[FIRST_COLUMN].apply(lambda v: f"{float(v)/1_000_000:,.3f} Mil")
    if "Date" in df:
        df["Date"] = df["Date"].astype(str).str[:10]
    if shorten_tank and "Tank Type" in df:
        df["Tank Type"] = df["Tank Type"].astype(str).str.lower().replace(
            {"triple": "t", "auto": "a", "hexa": "h"}, regex=True
        ).str.title().str[:8]
    rows = [df.columns.tolist()] + df.values.tolist()
    widths = [max(wcswidth(str(r[i])) for r in rows) for i in range(len(df.columns))]
    def fmt(r):
        return "| " + " | ".join(str(v) + " "*(widths[i]-wcswidth(str(v))) for i,v in enumerate(r)) + " |"
    return [fmt(df.columns), "| " + " | ".join("-"*w for w in widths) + " |"] + [fmt(r) for r in df.values]

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

def apply_range(df, parts, default_range=(1,1)):
    df = add_index(df)
    rng = None
    for p in parts:
        if "-" in p:
            rng = parse_range(p)
            break
    a, b = rng if rng else default_range
    return df[(df["Ņ"] >= a) & (df["Ņ"] <= b)]

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

@bot.event
async def on_message(message):
    if message.author == bot.user: return
    if not message.content.startswith("!olymp;"): return

    now = time.time()
    if now - user_cooldowns.get(message.author.id, 0) < COOLDOWN_SECONDS:
        return
    user_cooldowns[message.author.id] = now

    parts = message.content.split(";")
    cmd = parts[1].lower()

    df = read_excel()
    if df.empty:
        await message.channel.send("Failed to load data.")
        return
    df.columns = df.columns.str.strip()

    output = None
    shorten_tank = True

    if cmd == "a":
        if not is_tejm(message.author):
            await message.channel.send("Restricted command.")
            return
        output = apply_range(df, parts)

    elif cmd == "b":
        output = apply_range(handle_best(df), parts)
        
    elif cmd == "n":
        if len(parts) < 3:
            await message.channel.send("❌ Usage: !olymp;n;PlayerName;1-15")
            return
        name = parts[2].strip()
        output = apply_range(handle_name(df, name), parts)


    elif cmd == "c":
        output = apply_range(
            normalize_score(df).sort_values("Score", ascending=False).drop_duplicates("Tank Type"),
            parts
        )

    elif cmd == "p":
        dfp = normalize_score(df).sort_values("Score", ascending=False)
        output = apply_range(dfp, parts, default_range=(1,1))

    elif cmd == "t":
        if len(parts) < 3:
            await message.channel.send("Tank name required.")
            return
        output = apply_range(handle_tank(df, parts[2]), parts)

    
    elif cmd == "d":
        if len(parts) < 3:
            await message.channel.send("❌ Usage: !olymp;d;YYYY-MM-DD or DD-MM-YYYY")
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
        shorten_tank = False

    
        # --- HELP ---
    elif cmd == "help":
        help_message = (
                "Commands:\n"
                "!olymp;b;1-15               - Best scores of each player\n"
                "!olymp;n;Player;1-15        - Best scores of specific player\n"
                "!olymp;c;1-15               - Best per tank\n"
                "!olymp;p;1-15               - Part of scoreboard\n"
                "!olymp;t;TankName;1-15      - Best score of a tank\n"
                "!olymp;d;YYYY-MM-DD         - Scores from that date\n"
                "!olymp;r                    - Random recommendation\n"
                
            )
        await message.channel.send(help_message)
        return
            
    elif cmd == "r":
        if len(parts) == 2:
            await message.channel.send("!olymp;r;a | b | r")
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

    cols = COLUMNS_C if cmd in {"c","t"} else COLUMNS_DEFAULT
    output = output[[c for c in cols if c in output]]

    lines = dataframe_to_markdown_aligned(output, shorten_tank)
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) > 1900:
            await message.channel.send(f"```\n{chunk}\n```")
            chunk = ""
        chunk += line + "\n"
    if chunk:
        await message.channel.send(f"```\n{chunk}\n```")

if __name__ == "__main__":
    keep_alive()
    bot.run(os.getenv("DISCORD_TOKEN"))
