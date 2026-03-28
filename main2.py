"""
SeanBot 1.0 - Rusty Kuntz Dynasty League
Monthly prize tracker using daily started-lineup accounting.

Flow (nightly after Force Sync):
  1. Get today's Fantrax scoring day + all team lineups
  2. Mark each player active (started) or inactive (bench/IL/NA)
  3. For each active player, fetch their stat for today from MLB game log
  4. Store: scoring_days, daily_lineups, daily_player_stats
  5. Recompute monthly_totals by joining lineups + stats (active only)
  6. Write leaderboard to team_stats for frontend display

Stat categories by month:
  March/April = RBI, May = K, June = H, July = SB, August = HR, Sept = manual
"""

import os
import asyncio
import calendar
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("seanbot")
ET = ZoneInfo("America/New_York")

app = FastAPI(title="SeanBot 1.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"],
    allow_methods=["GET", "POST"], allow_headers=["*"]
)

FANTRAX_USERNAME  = os.environ.get("FANTRAX_USERNAME", "")
FANTRAX_PASSWORD  = os.environ.get("FANTRAX_PASSWORD", "")
FANTRAX_LEAGUE_ID = os.environ.get("FANTRAX_LEAGUE_ID", "38fsaq9emigy6d4z")
SUPABASE_URL      = os.environ.get("SUPABASE_URL", "https://haaaaugigaryryqjuztx.supabase.co")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhhYWFhdWdpZ2FyeXJ5cWp1enR4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDY2OTk3NywiZXhwIjoyMDkwMjQ1OTc3fQ.1mPiznawXi-2AtDqfIQET7hjWw5fuk-zRU9aPgQnNDQ")
ADMIN_SECRET      = os.environ.get("ADMIN_SECRET", "changeme123")

LEAGUE_START_DATE = date(2026, 3, 27)

# Slots that count as INACTIVE — stats from these do NOT count
INACTIVE_SLOTS = {"BN", "IL", "IL+", "NA", "MINORS", "RES", "IR", "INJURED_RESERVE"}

# Month -> stat category
def active_stat_for_month(m: int) -> str:
    return {3: "rbi", 4: "rbi", 5: "k", 6: "h", 7: "sb", 8: "hr"}.get(m, "rbi")

TEAMS = [
    "Possibilities", "Yoshi\u2019s Islanders", "thebigfur", "Red Birds",
    "Daddy Yankee", "\u00a1pinto!", "JoanMacias", "Xavier", "Los Jankees",
    "Momin8421", "ericliaci", "Sho Me The Money",
    "Designated Shitters", "Arraezed & Hoerny"
]

supabase: Optional[Client] = None
sync_state = {"last_sync_date": None, "syncing": False, "last_sync_time": None, "status": "idle"}
COOKIES = {}
FANTRAX_LOGGED_IN = False
MLBAM_CACHE: dict = {}


# ─────────────────────────────────────────────
# Fantrax auth + HTTP helpers
# ─────────────────────────────────────────────

async def fantrax_login() -> bool:
    global FANTRAX_LOGGED_IN, COOKIES
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            resp = await c.post(
                "https://www.fantrax.com/fxea/general/login",
                json={"email": FANTRAX_USERNAME, "password": FANTRAX_PASSWORD},
                headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
            )
        if resp.status_code == 200:
            COOKIES = dict(resp.cookies)
            FANTRAX_LOGGED_IN = True
            logger.info(f"Fantrax login OK. Cookies: {list(COOKIES.keys())}")
            return True
    except Exception as e:
        logger.error(f"Login error: {e}")
    return False


def fantrax_headers() -> dict:
    h = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Referer": "https://www.fantrax.com/",
    }
    if COOKIES:
        h["Cookie"] = "; ".join(f"{k}={v}" for k, v in COOKIES.items())
    return h


async def fantrax_get(path: str, params: dict = None) -> Optional[dict]:
    try:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as c:
            resp = await c.get(
                f"https://www.fantrax.com{path}",
                params=params, headers=fantrax_headers()
            )
        for k, v in resp.cookies.items():
            COOKIES[k] = v
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"Fantrax {path} returned {resp.status_code}")
    except Exception as e:
        logger.error(f"Fantrax GET error {path}: {e}")
    return None


# ─────────────────────────────────────────────
# Team name normalisation
# ─────────────────────────────────────────────

def norm_team(s: str) -> str:
    """Normalise apostrophes/quotes for fuzzy team matching."""
    return s.replace("\u2019", "'").replace("\u2018", "'").lower().strip()


def match_team(fantrax_name: str) -> Optional[str]:
    if fantrax_name in TEAMS:
        return fantrax_name
    for t in TEAMS:
        if norm_team(t) == norm_team(fantrax_name):
            return t
    return None


# ─────────────────────────────────────────────
# STEP 1 — Fetch lineups for a scoring date
# ─────────────────────────────────────────────

async def fetch_lineups_for_date(score_date: str) -> list[dict]:
    """
    Call Fantrax getTeamRosters for today.
    Returns list of dicts:
      {fantrax_player_id, player_name, fantasy_team, mlb_team, slot, is_active}
    """
    roster_data = await fantrax_get(
        "/fxea/general/getTeamRosters",
        {"leagueId": FANTRAX_LEAGUE_ID}
    )
    if not roster_data or not isinstance(roster_data.get("rosters"), dict):
        logger.error("getTeamRosters returned no data")
        return []

    # Build player id -> name/team/pos from getPlayerIds
    player_ids_data = await fantrax_get("/fxea/general/getPlayerIds", {"sport": "MLB"})

    player_info = {}
    if isinstance(player_ids_data, dict):
        for pid, pdata in player_ids_data.items():
            if isinstance(pdata, dict):
                raw = pdata.get("name", "")
                if raw and "," in raw:
                    parts = raw.split(",", 1)
                    name = f"{parts[1].strip()} {parts[0].strip()}"
                else:
                    name = raw
                player_info[str(pid)] = {
                    "name": name,
                    "team": pdata.get("team", ""),
                    "position": pdata.get("position", ""),
                }
        del player_ids_data

    rows = []
    for team_id, team_info in roster_data["rosters"].items():
        if not isinstance(team_info, dict):
            continue
        fantrax_team_name = team_info.get("teamName", "")
        matched = match_team(fantrax_team_name)
        if not matched:
            logger.warning(f"No team match: '{fantrax_team_name}'")
            continue

        for player in team_info.get("rosterItems", []):
            if not isinstance(player, dict):
                continue
            pid = str(player.get("id", ""))
            if not pid:
                continue

            # Slot determines active vs inactive
            # Fantrax returns slot info in various fields — check all
            slot = (
                player.get("lineupStatus") or
                player.get("slot") or
                player.get("slotName") or
                player.get("status") or
                "BN"
            ).upper().strip()

            is_active = slot not in INACTIVE_SLOTS

            pinfo = player_info.get(pid, {})
            rows.append({
                "fantrax_player_id": pid,
                "player_name": pinfo.get("name", pid),
                "fantasy_team": matched,
                "mlb_team": pinfo.get("team", ""),
                "slot": slot,
                "is_active": is_active,
                "score_date": score_date,
                "updated_at": score_date,
            })

    logger.info(f"Lineups fetched: {len(rows)} players, "
                f"{sum(1 for r in rows if r['is_active'])} active")
    return rows


# ─────────────────────────────────────────────
# STEP 2 — Fetch MLB stat for one player on one date
# ─────────────────────────────────────────────

async def get_mlbam_id(name: str, mlb_team: str = "") -> Optional[int]:
    cache_key = f"{name}|{mlb_team}"
    if cache_key in MLBAM_CACHE:
        return MLBAM_CACHE[cache_key]
    try:
        parts = name.strip().split()
        last = parts[-1] if parts else ""
        first = parts[0] if len(parts) > 1 else ""
        url = f"https://statsapi.mlb.com/api/v1/people/search?names={last}&sportId=1"
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(url)
        people = resp.json().get("people", [])
        name_lower = name.lower()

        # Priority 1: exact name + team
        if mlb_team:
            for p in people:
                if (p.get("fullName", "").lower() == name_lower and
                        p.get("currentTeam", {}).get("abbreviation", "").upper() == mlb_team.upper()):
                    mid = p.get("id")
                    if mid:
                        MLBAM_CACHE[cache_key] = mid
                        return mid
        # Priority 2: exact name
        for p in people:
            if p.get("fullName", "").lower() == name_lower:
                mid = p.get("id")
                if mid:
                    MLBAM_CACHE[cache_key] = mid
                    return mid
        # Priority 3: first + last + team
        if first and mlb_team:
            for p in people:
                full = p.get("fullName", "").lower()
                abbr = p.get("currentTeam", {}).get("abbreviation", "").upper()
                if first.lower() in full and last.lower() in full and abbr == mlb_team.upper():
                    mid = p.get("id")
                    if mid:
                        MLBAM_CACHE[cache_key] = mid
                        return mid
        # Priority 4: first + last
        if first:
            for p in people:
                full = p.get("fullName", "").lower()
                if first.lower() in full and last.lower() in full:
                    mid = p.get("id")
                    if mid:
                        MLBAM_CACHE[cache_key] = mid
                        return mid
    except Exception as e:
        logger.error(f"MLBAM lookup error {name}: {e}")
    return None


async def fetch_player_stat_for_date(
    mlbam_id: int, stat_type: str, score_date: str
) -> int:
    """Fetch a single player's stat for one date from MLB game log."""
    year = score_date[:4]
    group = "pitching" if stat_type == "k" else "hitting"
    stat_key = {
        "rbi": "rbi", "h": "hits", "hr": "homeRuns",
        "sb": "stolenBases", "k": "strikeOuts"
    }[stat_type]

    url = (f"https://statsapi.mlb.com/api/v1/people/{mlbam_id}/stats"
           f"?stats=gameLog&season={year}&group={group}"
           f"&startDate={score_date}&endDate={score_date}")
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(url)
        total = 0
        for sg in resp.json().get("stats", []):
            for split in sg.get("splits", []):
                total += int(split.get("stat", {}).get(stat_key, 0) or 0)
        return total
    except Exception as e:
        logger.error(f"MLB stat error mlbam={mlbam_id} {stat_type} {score_date}: {e}")
        return 0


# ─────────────────────────────────────────────
# STEP 3 — Recompute monthly totals from stored data
# ─────────────────────────────────────────────

def recompute_monthly_totals(month_str: str, stat_type: str) -> dict:
    """
    Join daily_lineups (is_active=true) with daily_player_stats for the month.
    Returns {fantasy_team: total_value}
    """
    if not supabase:
        return {}

    year, mon = int(month_str[:4]), int(month_str[5:7])
    last_day = calendar.monthrange(year, mon)[1]
    month_start = f"{month_str}-01"
    month_end = f"{month_str}-{last_day:02d}"

    # Get all active lineup rows for the month
    lineups = supabase.table("daily_lineups")\
        .select("score_date,fantrax_player_id,fantasy_team")\
        .eq("is_active", True)\
        .gte("score_date", month_start)\
        .lte("score_date", month_end)\
        .execute().data

    if not lineups:
        return {t: 0 for t in TEAMS}

    # Get all player stats for the month
    stats = supabase.table("daily_player_stats")\
        .select("score_date,fantrax_player_id,value")\
        .eq("stat_type", stat_type)\
        .gte("score_date", month_start)\
        .lte("score_date", month_end)\
        .execute().data

    # Build lookup: (date, player_id) -> value
    stat_lookup = {(r["score_date"], r["fantrax_player_id"]): r["value"] for r in stats}

    # Sum: for each active lineup row, add stat value
    totals = {t: 0 for t in TEAMS}
    for row in lineups:
        team = row["fantasy_team"]
        if team not in totals:
            continue
        key = (row["score_date"], row["fantrax_player_id"])
        totals[team] += stat_lookup.get(key, 0)

    return totals


# ─────────────────────────────────────────────
# Main sync function
# ─────────────────────────────────────────────

async def run_fantrax_sync():
    global FANTRAX_LOGGED_IN
    if sync_state["syncing"]:
        logger.info("Sync already running, skipping")
        return
    sync_state["syncing"] = True
    sync_state["status"] = "syncing"
    logger.info("SeanBot sync starting...")

    try:
        if not FANTRAX_LOGGED_IN:
            ok = await fantrax_login()
            if not ok:
                sync_state["status"] = "login_failed"
                return

        today_str = date.today().isoformat()
        today_month = date.today().month
        stat_type = active_stat_for_month(today_month)
        month_str = today_str[:7]

        logger.info(f"Sync date: {today_str} | Active stat: {stat_type}")

        # ── STEP 1: Fetch lineups ──
        lineup_rows = await fetch_lineups_for_date(today_str)
        if not lineup_rows:
            logger.error("No lineup data returned — aborting sync")
            sync_state["status"] = "error"
            return

        # Store scoring_day
        if supabase:
            supabase.table("scoring_days").upsert({
                "score_date": today_str,
                "season": 2026,
                "processed": False,
                "updated_at": today_str,
            }, on_conflict="score_date").execute()

        # Store daily_lineups (upsert so rerun is safe)
        if supabase:
            supabase.table("daily_lineups").upsert(
                lineup_rows,
                on_conflict="score_date,fantasy_team,fantrax_player_id"
            ).execute()
            logger.info(f"Stored {len(lineup_rows)} lineup rows for {today_str}")

        # ── STEP 2: Fetch stats for active players only ──
        active_rows = [r for r in lineup_rows if r["is_active"]]
        logger.info(f"Fetching {stat_type} for {len(active_rows)} active players...")

        stat_rows = []
        api_calls = 0

        for row in active_rows:
            name = row["player_name"]
            mlb_team = row["mlb_team"]
            pid = row["fantrax_player_id"]

            if not name or name == pid:
                continue

            mlbam_id = await get_mlbam_id(name, mlb_team)
            if not mlbam_id:
                continue

            value = await fetch_player_stat_for_date(mlbam_id, stat_type, today_str)
            api_calls += 1

            stat_rows.append({
                "score_date": today_str,
                "fantrax_player_id": pid,
                "player_name": name,
                "stat_type": stat_type,
                "value": value,
                "updated_at": today_str,
            })

            if value > 0:
                logger.info(f"  {name} ({mlb_team}): {stat_type}={value}")

        logger.info(f"Stats fetched: {api_calls} API calls, "
                    f"{sum(r['value'] for r in stat_rows)} total {stat_type}")

        # Store daily_player_stats
        if supabase and stat_rows:
            supabase.table("daily_player_stats").upsert(
                stat_rows,
                on_conflict="score_date,fantrax_player_id,stat_type"
            ).execute()

        # Mark scoring day processed
        if supabase:
            supabase.table("scoring_days").upsert({
                "score_date": today_str,
                "season": 2026,
                "processed": True,
                "updated_at": today_str,
            }, on_conflict="score_date").execute()

        # ── STEP 3: Recompute monthly totals from source data ──
        logger.info(f"Recomputing monthly totals for {month_str}...")
        totals = recompute_monthly_totals(month_str, stat_type)

        stat_col = {"rbi": "rbi", "k": "strikeouts", "h": "hits",
                    "sb": "stolen_bases", "hr": "home_runs"}[stat_type]

        if supabase:
            for team_name, total in totals.items():
                # Write to monthly_totals (auditable)
                supabase.table("monthly_totals").upsert({
                    "month": month_str,
                    "fantasy_team": team_name,
                    "stat_type": stat_type,
                    "total": total,
                    "updated_at": today_str,
                }, on_conflict="month,fantasy_team,stat_type").execute()

                # Write to team_stats (frontend display)
                supabase.table("team_stats").upsert({
                    "team_name": team_name,
                    "season": 2026,
                    stat_col: total,
                    "updated_at": today_str,
                }, on_conflict="team_name,season").execute()

        now_et = datetime.now(ET).strftime("%b %d %I:%M %p ET")
        sync_state["last_sync_date"] = today_str
        sync_state["last_sync_time"] = now_et
        sync_state["status"] = "done"

        top = sorted(totals.items(), key=lambda x: -x[1])[:3]
        logger.info(f"Sync complete [{now_et}] | Top 3: {top}")

    except Exception as e:
        logger.error(f"Sync error: {e}", exc_info=True)
        sync_state["status"] = "error"
    finally:
        sync_state["syncing"] = False


# ─────────────────────────────────────────────
# MLB scores + news helpers
# ─────────────────────────────────────────────

async def get_todays_games() -> list:
    today = date.today().strftime("%Y-%m-%d")
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            resp = await c.get(
                f"https://statsapi.mlb.com/api/v1/schedule"
                f"?sportId=1&date={today}&hydrate=linescore,team,decisions"
            )
        return resp.json().get("dates", [{}])[0].get("games", [])
    except:
        return []


async def fetch_mlb_scores() -> list:
    games = await get_todays_games()
    today = date.today().isoformat()
    out = []
    for g in games:
        away = g["teams"]["away"]
        home = g["teams"]["home"]
        out.append({
            "game_id": g["gamePk"],
            "status": g["status"]["abstractGameState"],
            "status_detail": g["status"]["detailedState"],
            "inning": g.get("linescore", {}).get("currentInningOrdinal", ""),
            "away_team": away["team"]["name"],
            "away_abbr": away["team"].get("abbreviation", ""),
            "away_score": away.get("score"),
            "away_winner": away.get("isWinner", False),
            "home_team": home["team"]["name"],
            "home_abbr": home["team"].get("abbreviation", ""),
            "home_score": home.get("score"),
            "home_winner": home.get("isWinner", False),
            "venue": g.get("venue", {}).get("name", ""),
            "game_date": today,
        })
    return out


async def fetch_mlb_news() -> list:
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            resp = await c.get(
                "https://api.rss2json.com/v1/api.json"
                "?rss_url=https://www.mlb.com/feeds/news/rss.xml"
            )
        return [
            {
                "title": i.get("title", ""),
                "link": i.get("link", ""),
                "pub_date": i.get("pubDate", ""),
                "description": (i.get("description", "") or "")[:200]
            }
            for i in resp.json().get("items", [])[:12]
        ]
    except:
        return []


async def cache_mlb_scores():
    if not supabase:
        return
    scores = await fetch_mlb_scores()
    today_str = date.today().isoformat()
    if scores:
        supabase.table("mlb_scores_cache").delete().neq("game_date", today_str).execute()
        supabase.table("mlb_scores_cache").upsert(scores, on_conflict="game_id").execute()


async def check_and_sync():
    await cache_mlb_scores()
    games = await get_todays_games()
    if not games:
        return
    non_final = [
        g for g in games
        if g["status"]["abstractGameState"] not in ("Final", "Postponed", "Cancelled")
    ]
    if not non_final:
        await run_fantrax_sync()
    else:
        live = sum(1 for g in games if g["status"]["abstractGameState"] == "Live")
        logger.info(f"{live} games live, {len(non_final)} not final — waiting")


# ─────────────────────────────────────────────
# Startup / shutdown
# ─────────────────────────────────────────────

scheduler = AsyncIOScheduler(timezone="America/New_York")


@app.on_event("startup")
async def startup():
    global supabase
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase connected")
    if FANTRAX_USERNAME and FANTRAX_PASSWORD:
        await fantrax_login()
    scheduler.add_job(check_and_sync, "cron", hour="22,23,0,1,2", minute="0,30")
    scheduler.add_job(cache_mlb_scores, "cron", hour="13-21", minute="0,15,30,45")
    scheduler.start()
    logger.info("SeanBot 1.0 online")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ─────────────────────────────────────────────
# API endpoints
# ─────────────────────────────────────────────

@app.get("/")
async def root():
    return {"app": "SeanBot 1.0", "status": "online", "sync": sync_state}


@app.get("/api/status")
async def get_status():
    games = await get_todays_games()
    return {
        "sync_status": sync_state["status"],
        "last_sync_time": sync_state["last_sync_time"],
        "last_sync_date": sync_state["last_sync_date"],
        "games_today": len(games),
        "games_final": sum(1 for g in games if g["status"]["abstractGameState"] == "Final"),
        "games_live": sum(1 for g in games if g["status"]["abstractGameState"] == "Live"),
        "time_et": datetime.now(ET).strftime("%I:%M %p ET"),
    }


@app.get("/api/team-stats")
async def get_team_stats(season: int = 2026):
    if not supabase:
        raise HTTPException(503, "DB not configured")
    return {"data": supabase.table("team_stats").select("*").eq("season", season).execute().data}


@app.get("/api/monthly-totals")
async def get_monthly_totals(month: str = None):
    """Return monthly leaderboard from monthly_totals table (auditable source)."""
    if not supabase:
        raise HTTPException(503, "DB not configured")
    if not month:
        month = date.today().isoformat()[:7]
    stat_type = active_stat_for_month(int(month[5:7]))
    rows = supabase.table("monthly_totals")\
        .select("fantasy_team,total,stat_type")\
        .eq("month", month)\
        .eq("stat_type", stat_type)\
        .order("total", desc=True)\
        .execute().data
    return {"month": month, "stat_type": stat_type, "data": rows}


@app.get("/api/daily-breakdown")
async def get_daily_breakdown(team: str, date_str: str = None):
    """
    Commissioner view: show which active players contributed stats
    for a given team on a given date.
    """
    if not supabase:
        raise HTTPException(503, "DB not configured")
    if not date_str:
        date_str = date.today().isoformat()
    stat_type = active_stat_for_month(int(date_str[5:7]))

    # Get active players for this team on this date
    lineups = supabase.table("daily_lineups")\
        .select("fantrax_player_id,player_name,slot,is_active")\
        .eq("score_date", date_str)\
        .eq("fantasy_team", team)\
        .execute().data

    # Get stats for those players on that date
    pids = [r["fantrax_player_id"] for r in lineups]
    stats_map = {}
    if pids:
        stats = supabase.table("daily_player_stats")\
            .select("fantrax_player_id,value")\
            .eq("score_date", date_str)\
            .eq("stat_type", stat_type)\
            .in_("fantrax_player_id", pids)\
            .execute().data
        stats_map = {r["fantrax_player_id"]: r["value"] for r in stats}

    result = []
    for p in lineups:
        result.append({
            "player": p["player_name"],
            "slot": p["slot"],
            "is_active": p["is_active"],
            "stat_type": stat_type,
            "value": stats_map.get(p["fantrax_player_id"], 0),
        })
    result.sort(key=lambda x: (-x["value"], not x["is_active"]))

    total = sum(r["value"] for r in result if r["is_active"])
    return {
        "team": team, "date": date_str,
        "stat_type": stat_type, "total": total,
        "players": result
    }


@app.get("/api/player-leaders/{stat}")
async def get_player_leaders(stat: str, limit: int = 10):
    if stat not in ["rbi", "k", "h", "sb", "hr"]:
        raise HTTPException(400, "Invalid stat")
    if not supabase:
        raise HTTPException(503, "DB not configured")
    # Pull from daily_player_stats for current month, sum by player
    month_str = date.today().isoformat()[:7]
    stat_type = active_stat_for_month(date.today().month)
    year, mon = int(month_str[:4]), int(month_str[5:7])
    last_day = calendar.monthrange(year, mon)[1]
    month_start = f"{month_str}-01"
    month_end = f"{month_str}-{last_day:02d}"

    rows = supabase.table("daily_player_stats")\
        .select("fantrax_player_id,player_name,value")\
        .eq("stat_type", stat_type)\
        .gte("score_date", month_start)\
        .lte("score_date", month_end)\
        .execute().data

    # Sum by player
    player_totals: dict = {}
    for r in rows:
        pid = r["fantrax_player_id"]
        if pid not in player_totals:
            player_totals[pid] = {"name": r["player_name"], "value": 0}
        player_totals[pid]["value"] += r["value"]

    sorted_players = sorted(player_totals.values(), key=lambda x: -x["value"])[:limit]
    return {"data": sorted_players}


@app.get("/api/mlb-scores")
async def get_mlb_scores():
    if supabase:
        today_str = date.today().isoformat()
        result = supabase.table("mlb_scores_cache").select("*").eq("game_date", today_str).execute()
        if result.data:
            return {"data": result.data}
    return {"data": await fetch_mlb_scores()}


@app.get("/api/mlb-news")
async def get_mlb_news():
    return {"data": await fetch_mlb_news()}


@app.get("/api/chirps")
async def get_chirps(limit: int = 50):
    if not supabase:
        raise HTTPException(503, "DB not configured")
    return {
        "data": supabase.table("chirps").select("*")
        .order("created_at", desc=True).limit(limit).execute().data
    }


@app.post("/api/chirps")
async def post_chirp(request: Request):
    if not supabase:
        raise HTTPException(503, "DB not configured")
    body = await request.json()
    author  = (body.get("author")  or "Anonymous")[:40]
    message = (body.get("message") or "")[:280]
    team    = (body.get("team")    or "")[:60]
    if not message.strip():
        raise HTTPException(400, "Empty message")
    return {
        "ok": True,
        "data": supabase.table("chirps").insert(
            {"author": author, "team": team, "message": message}
        ).execute().data
    }


@app.get("/api/history")
async def get_history():
    if not supabase:
        raise HTTPException(503, "DB not configured")
    return {
        "data": supabase.table("prize_history").select("*")
        .order("year", desc=True).execute().data
    }


@app.post("/api/history")
async def save_history(request: Request):
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Unauthorized")
    if not supabase:
        raise HTTPException(503, "DB not configured")
    record = {
        "year": int(body["year"]),
        "cat_idx": int(body["cat_idx"]),
        "cat_name": body["cat_name"],
        "period": body["period"],
        "winner_team": body["winner_team"],
        "total": int(body["total"])
    }
    return {
        "ok": True,
        "data": supabase.table("prize_history")
        .upsert(record, on_conflict="year,cat_idx").execute().data
    }


@app.post("/api/sync-now")
async def trigger_sync(request: Request):
    global FANTRAX_LOGGED_IN
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Unauthorized")

    # Manual stat override
    override = body.get("team_override")
    if override and supabase:
        team = override.get("team")
        stat = override.get("stat")
        val  = int(override.get("value", 0))
        if stat in {"rbi", "strikeouts", "hits", "stolen_bases", "home_runs"} and team:
            supabase.table("team_stats").upsert(
                {"team_name": team, "season": 2026, stat: val,
                 "updated_at": date.today().isoformat()},
                on_conflict="team_name,season"
            ).execute()
            return {"ok": True, "message": f"Override: {team} {stat}={val}"}

    sync_state["last_sync_date"] = None
    FANTRAX_LOGGED_IN = False
    asyncio.create_task(run_fantrax_sync())
    return {"ok": True, "message": "Sync triggered"}
