"""
SeanBot 1.0 - Rusty Kuntz Dynasty League
Uses getPlayerIds for real names + MLB Stats API for season stats
"""

import os
import asyncio
import logging
import json
from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from supabase import create_client, Client

try:
    from fantraxapi import FanTraxAPI
    FANTRAXAPI_AVAILABLE = True
except ImportError:
    FANTRAXAPI_AVAILABLE = False
    logger = None  # will be set below

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("seanbot")
ET = ZoneInfo("America/New_York")

app = FastAPI(title="SeanBot 1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET","POST"], allow_headers=["*"])

FANTRAX_USERNAME  = os.environ.get("FANTRAX_USERNAME", "")
FANTRAX_PASSWORD  = os.environ.get("FANTRAX_PASSWORD", "")
FANTRAX_LEAGUE_ID = os.environ.get("FANTRAX_LEAGUE_ID", "38fsaq9emigy6d4z")
SUPABASE_URL      = os.environ.get("SUPABASE_URL", "https://haaaaugigaryryqjuztx.supabase.co")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhhYWFhdWdpZ2FyeXJ5cWp1enR4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDY2OTk3NywiZXhwIjoyMDkwMjQ1OTc3fQ.1mPiznawXi-2AtDqfIQET7hjWw5fuk-zRU9aPgQnNDQ")
ADMIN_SECRET      = os.environ.get("ADMIN_SECRET", "changeme123")

supabase: Optional[Client] = None
TEAMS = [
    "Possibilities", "Yoshi’s Islanders", "thebigfur", "Red Birds",
    "Daddy Yankee", "¡pinto!", "JoanMacias", "Xavier", "Los Jankees",
    "Momin8421", "ericliaci", "Sho Me The Money",
    "Designated Shitters", "Arraezed & Hoerny"
]
sync_state = {"last_sync_date": None, "syncing": False, "last_sync_time": None, "status": "idle"}
COOKIES = {}
FANTRAX_LOGGED_IN = False


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
            logger.info(f"Login OK. Cookies: {list(COOKIES.keys())}")
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
    except Exception as e:
        logger.error(f"GET error {path}: {e}")
    return None


# Cache: player name -> MLBAM ID
MLBAM_ID_CACHE: dict = {}

async def get_mlbam_id(name: str, mlb_team: str = "") -> Optional[int]:
    """Search MLB Stats API by full name + team to get correct MLBAM player ID."""
    cache_key = f"{name}|{mlb_team}"
    if cache_key in MLBAM_ID_CACHE:
        return MLBAM_ID_CACHE[cache_key]
    try:
        parts = name.strip().split()
        last = parts[-1] if parts else ""
        first = parts[0] if len(parts) > 1 else ""
        url = f"https://statsapi.mlb.com/api/v1/people/search?names={last}&sportId=1"
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(url)
        people = resp.json().get("people", [])
        name_lower = name.lower()

        # Priority 1: exact full name + team abbreviation match
        if mlb_team:
            for p in people:
                full = p.get("fullName", "").lower()
                team_abbr = p.get("currentTeam", {}).get("abbreviation", "")
                if full == name_lower and team_abbr.upper() == mlb_team.upper():
                    mlbam_id = p.get("id")
                    if mlbam_id:
                        MLBAM_ID_CACHE[cache_key] = mlbam_id
                        return mlbam_id

        # Priority 2: exact full name match (any team)
        for p in people:
            if p.get("fullName", "").lower() == name_lower:
                mlbam_id = p.get("id")
                if mlbam_id:
                    MLBAM_ID_CACHE[cache_key] = mlbam_id
                    return mlbam_id

        # Priority 3: first + last both in name + team match
        if first and mlb_team:
            for p in people:
                full = p.get("fullName", "").lower()
                team_abbr = p.get("currentTeam", {}).get("abbreviation", "")
                if (first.lower() in full and last.lower() in full
                        and team_abbr.upper() == mlb_team.upper()):
                    mlbam_id = p.get("id")
                    if mlbam_id:
                        MLBAM_ID_CACHE[cache_key] = mlbam_id
                        return mlbam_id

        # Priority 4: first + last both in name (no team filter)
        if first:
            for p in people:
                full = p.get("fullName", "").lower()
                if first.lower() in full and last.lower() in full:
                    mlbam_id = p.get("id")
                    if mlbam_id:
                        MLBAM_ID_CACHE[cache_key] = mlbam_id
                        return mlbam_id

    except Exception as e:
        logger.error(f"MLBAM lookup error for {name} ({mlb_team}): {e}")
    return None


async def get_player_stats_for_date(mlbam_id: int, stat_date: str) -> dict:
    """Get a player's stats for a specific date using MLB game log."""
    result = {"rbi": 0, "h": 0, "hr": 0, "sb": 0, "k": 0}
    try:
        year = stat_date[:4]
        hit_url = (f"https://statsapi.mlb.com/api/v1/people/{mlbam_id}/stats"
                   f"?stats=gameLog&season={year}&group=hitting"
                   f"&startDate={stat_date}&endDate={stat_date}")
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(hit_url)
        for sg in resp.json().get("stats", []):
            for split in sg.get("splits", []):
                s = split.get("stat", {})
                result["rbi"] += int(s.get("rbi", 0) or 0)
                result["h"]   += int(s.get("hits", 0) or 0)
                result["hr"]  += int(s.get("homeRuns", 0) or 0)
                result["sb"]  += int(s.get("stolenBases", 0) or 0)
        pit_url = (f"https://statsapi.mlb.com/api/v1/people/{mlbam_id}/stats"
                   f"?stats=gameLog&season={year}&group=pitching"
                   f"&startDate={stat_date}&endDate={stat_date}")
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(pit_url)
        for sg in resp.json().get("stats", []):
            for split in sg.get("splits", []):
                s = split.get("stat", {})
                result["k"] += int(s.get("strikeOuts", 0) or 0)
    except Exception as e:
        logger.error(f"Stats error for mlbam_id={mlbam_id} date={stat_date}: {e}")
    return result


async def get_mlb_stats(player_name: str, mlb_team: str = "") -> dict:
    """Kept for compatibility — returns today's stats."""
    empty = {"rbi": 0, "h": 0, "hr": 0, "sb": 0, "k": 0}
    mlbam_id = await get_mlbam_id(player_name, mlb_team)
    if not mlbam_id:
        return empty
    return await get_player_stats_for_date(mlbam_id, date.today().isoformat())


async def get_todays_games() -> list:
    today = date.today().strftime("%Y-%m-%d")
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            resp = await c.get(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&hydrate=linescore,team,decisions")
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
            "game_id": g["gamePk"], "status": g["status"]["abstractGameState"],
            "status_detail": g["status"]["detailedState"],
            "inning": g.get("linescore", {}).get("currentInningOrdinal", ""),
            "away_team": away["team"]["name"], "away_abbr": away["team"].get("abbreviation", ""),
            "away_score": away.get("score"), "away_winner": away.get("isWinner", False),
            "home_team": home["team"]["name"], "home_abbr": home["team"].get("abbreviation", ""),
            "home_score": home.get("score"), "home_winner": home.get("isWinner", False),
            "venue": g.get("venue", {}).get("name", ""), "game_date": today,
        })
    return out


async def fetch_mlb_news() -> list:
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            resp = await c.get("https://api.rss2json.com/v1/api.json?rss_url=https://www.mlb.com/feeds/news/rss.xml")
        return [{"title": i.get("title",""), "link": i.get("link",""), "pub_date": i.get("pubDate",""), "description": (i.get("description","") or "")[:200]} for i in resp.json().get("items", [])[:12]]
    except:
        return []


async def get_started_player_ids_fantraxapi() -> dict:
    """
    Use fantraxapi library to get started (non-bench, non-IL) player IDs
    for each team for today's scoring period.
    Returns: {fantrax_player_id: fantasy_team_name}
    """
    if not FANTRAXAPI_AVAILABLE:
        logger.warning("fantraxapi not installed, skipping started filter")
        return {}
    try:
        loop = asyncio.get_event_loop()
        def _get_lineups():
            api = FanTraxAPI(FANTRAX_LEAGUE_ID, username=FANTRAX_USERNAME, password=FANTRAX_PASSWORD)
            league = api.league()
            started = {}
            today = date.today()
            # Find today's scoring period number
            period_num = None
            for num, d in league.scoring_dates.items():
                if d == today:
                    period_num = num
                    break
            if period_num is None:
                logger.warning(f"No scoring period found for {today}")
                return {}
            logger.info(f"Today's scoring period: {period_num}")
            for team in league.teams:
                try:
                    roster = team.roster(period_num)
                    for row in roster.rows:
                        if row.player is None:
                            continue
                        pos = row.position.short_name if row.position else ""
                        # Skip bench and IL slots
                        if pos in ("BN", "IL", "IL+", "NA", "MINORS"):
                            continue
                        started[row.player.id] = team.name
                except Exception as e:
                    logger.error(f"Roster error for {team.name}: {e}")
            logger.info(f"Started players found: {len(started)}")
            return started
        started = await loop.run_in_executor(None, _get_lineups)
        return started
    except Exception as e:
        logger.error(f"fantraxapi lineup error: {e}")
        return {}


async def run_fantrax_sync():
    global FANTRAX_LOGGED_IN
    if sync_state["syncing"]:
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

        # Determine which stat to track based on current month
        month = date.today().month
        if month in (3, 4):
            active_stat = "rbi"
        elif month == 5:
            active_stat = "k"
        elif month == 6:
            active_stat = "h"
        elif month == 7:
            active_stat = "sb"
        elif month == 8:
            active_stat = "hr"
        else:
            active_stat = "rbi"
        logger.info(f"Active stat: {active_stat}")

        # Find all dates from league start to today that need processing
        LEAGUE_START = date(2026, 3, 25)  # Opening Day
        all_dates = []
        d = LEAGUE_START
        while d <= date.today():
            all_dates.append(d.isoformat())
            d = date.fromordinal(d.toordinal() + 1)

        # Find which dates already have data in daily_stats
        existing = supabase.table("daily_stats")            .select("stat_date")            .eq("stat_type", active_stat)            .execute().data if supabase else []
        existing_dates = {row["stat_date"] for row in existing}

        # Process missing dates + always reprocess today
        dates_to_process = [d for d in all_dates if d not in existing_dates or d == today_str]
        logger.info(f"Dates to process: {dates_to_process}")

        # Clean up stale/invalid team names from previous syncs
        if supabase:
            valid_teams = TEAMS
            all_team_rows = supabase.table("team_stats").select("team_name").execute().data
            for row in all_team_rows:
                if row["team_name"] not in valid_teams:
                    logger.info(f"Deleting stale team_stats: {row['team_name']}")
                    supabase.table("team_stats").delete().eq("team_name", row["team_name"]).execute()
            for t in ["pinto!", "Designated Shitters 🧻", "Designated Shitters 🚽"]:
                supabase.table("player_stats").delete().eq("fantasy_team", t).execute()

        # Get today's started players via fantraxapi (bench/IL excluded)
        started_map = await get_started_player_ids_fantraxapi()
        use_started_filter = len(started_map) > 0
        logger.info(f"Started filter active: {use_started_filter} ({len(started_map)} players)")

        # Step 1: Rosters — who owns which player
        roster_data = await fantrax_get(
            "/fxea/general/getTeamRosters",
            {"leagueId": FANTRAX_LEAGUE_ID}
        )
        player_team_map = {}   # fantrax_id -> team_name
        player_status_map = {} # fantrax_id -> status

        if roster_data and isinstance(roster_data.get("rosters"), dict):
            for team_id, team_info in roster_data["rosters"].items():
                if not isinstance(team_info, dict):
                    continue
                team_name = team_info.get("teamName", "Unknown")
                for player in team_info.get("rosterItems", []):
                    if not isinstance(player, dict):
                        continue
                    pid = str(player.get("id", ""))
                    status = player.get("status", "ACTIVE").upper()
                    if pid:
                        player_team_map[pid] = team_name
                        player_status_map[pid] = status
            logger.info(f"Roster: {len(player_team_map)} players across 14 teams")
            # Log a sample rosterItem to see all available fields
            for team_id, team_info in roster_data["rosters"].items():
                if isinstance(team_info, dict) and team_info.get("rosterItems"):
                    sample = team_info["rosterItems"][0]
                    logger.info(f"Sample rosterItem keys: {list(sample.keys())}")
                    logger.info(f"Sample rosterItem: {sample}")
                    break

        # Step 2: getPlayerIds — real names, teams, positions (2GB RAM — no problem)
        player_ids_data = await fantrax_get(
            "/fxea/general/getPlayerIds",
            {"sport": "MLB"}
        )

        rostered_ids = set(player_team_map.keys())
        player_info_map = {}
        if isinstance(player_ids_data, dict):
            for pid, pdata in player_ids_data.items():
                if str(pid) in rostered_ids and isinstance(pdata, dict):
                    player_info_map[str(pid)] = pdata
            logger.info(f"getPlayerIds: {len(player_info_map)} rostered players matched")
        del player_ids_data  # free memory


        # Step 3: For each missing date, fetch that day's stats for all rostered players
        processed = 0
        stats_calls = 0

        for process_date in dates_to_process:
            logger.info(f"Processing date: {process_date}")
            team_totals = {t: 0 for t in TEAMS}
            sample_logged = False

            for fantrax_pid, fantasy_team_name in player_team_map.items():
            # If we have started lineup data, only count started players
            if use_started_filter and fantrax_pid not in started_map:
                continue
            # Otherwise fall back to filtering out known bench/IR statuses
            if not use_started_filter:
                status = player_status_map.get(fantrax_pid, "ACTIVE").upper()
                if status in ("INJURED_RESERVE", "IL", "IR", "MINORS", "NA"):
                    continue

            # Exact match first, then normalize apostrophes/quotes
            matched_team = None
            if fantasy_team_name in TEAMS:
                matched_team = fantasy_team_name
            else:
                # Normalize curly/straight apostrophes for comparison
                def norm(s):
                    return s.replace('’', "'").replace('‘', "'").lower()
                for t in TEAMS:
                    if norm(t) == norm(fantasy_team_name):
                        matched_team = t
                        break
            if not matched_team:
                logger.warning(f"No team match for: '{fantasy_team_name}'")
                continue

            pinfo = player_info_map.get(fantrax_pid, {})
            raw_name = pinfo.get("name", "")
            if raw_name and "," in raw_name:
                parts = raw_name.split(",", 1)
                name = f"{parts[1].strip()} {parts[0].strip()}"
            else:
                name = raw_name or fantrax_pid

            mlb_team = pinfo.get("team", "")
            position = pinfo.get("position", "")

            # Get this date's stats for this player
            stat_value = 0
            if name and name != fantrax_pid:
                mlbam_id = await get_mlbam_id(name, mlb_team)
                if mlbam_id:
                    day_stats = await get_player_stats_for_date(mlbam_id, process_date)
                    stat_value = day_stats.get(active_stat, 0)
                    stats_calls += 1

            team_totals[matched_team] += stat_value

            if not sample_logged and stat_value > 0:
                logger.info(f"  {process_date} {name} {active_stat}={stat_value} ({matched_team})")
                sample_logged = True

            processed += 1

            # Update player_stats leaderboard for today only
            if process_date == today_str and supabase:
                supabase.table("player_stats").upsert({
                    "player_id": fantrax_pid, "name": name,
                    "mlb_team": mlb_team, "position": position,
                    "fantasy_team": matched_team,
                    "rbi": stat_value if active_stat == "rbi" else 0,
                    "k":   stat_value if active_stat == "k"   else 0,
                    "h":   stat_value if active_stat == "h"   else 0,
                    "sb":  stat_value if active_stat == "sb"  else 0,
                    "hr":  stat_value if active_stat == "hr"  else 0,
                    "updated_at": today_str,
                }, on_conflict="player_id").execute()

        # Save this date's team totals to daily_stats
        if supabase:
            for team_name, day_value in team_totals.items():
                supabase.table("daily_stats").upsert({
                    "stat_date": process_date,
                    "fantasy_team": team_name,
                    "stat_type": active_stat,
                    "value": int(day_value),
                    "updated_at": today_str
                }, on_conflict="stat_date,fantasy_team,stat_type").execute()
            logger.info(f"  Saved {process_date}: {dict(list(team_totals.items())[:3])}...")

        # End of date loop - recompute monthly totals from all daily_stats
        import calendar
        month_str = today_str[:7]
        year, mon = int(month_str[:4]), int(month_str[5:7])
        last_day = calendar.monthrange(year, mon)[1]
        if supabase:
            all_daily = supabase.table("daily_stats")                .select("fantasy_team,value")                .gte("stat_date", f"{month_str}-01")                .lte("stat_date", f"{month_str}-{last_day:02d}")                .eq("stat_type", active_stat)                .execute().data
            monthly_totals = {t: 0 for t in TEAMS}
            for row in all_daily:
                t = row["fantasy_team"]
                if t in monthly_totals:
                    monthly_totals[t] += row["value"]
            stat_col = {"rbi": "rbi", "k": "strikeouts", "h": "hits",
                        "sb": "stolen_bases", "hr": "home_runs"}[active_stat]
            for team_name, total in monthly_totals.items():
                supabase.table("team_stats").upsert({
                    "team_name": team_name, "season": 2026,
                    stat_col: total, "updated_at": today_str
                }, on_conflict="team_name,season").execute()
            logger.info(f"Monthly totals written. Top: {sorted(monthly_totals.items(), key=lambda x: -x[1])[:3]}")

        # Step 4: Write daily_stats rows (one per team per day)
        # Then recompute team_stats as sum of all daily_stats for the month
        if supabase:
            month_str = today_str[:7]  # e.g. "2026-03"
            for team_name, day_value in team_totals.items():
                # Upsert today's daily row
                supabase.table("daily_stats").upsert({
                    "stat_date": today_str,
                    "fantasy_team": team_name,
                    "stat_type": active_stat,
                    "value": int(day_value),
                    "updated_at": today_str
                }, on_conflict="stat_date,fantasy_team,stat_type").execute()

            # Recompute monthly totals from daily_stats
            # Use gte/lte instead of like — stat_date is a DATE column
            import calendar
            year, mon = int(month_str[:4]), int(month_str[5:7])
            last_day = calendar.monthrange(year, mon)[1]
            month_start = f"{month_str}-01"
            month_end = f"{month_str}-{last_day:02d}"
            all_daily = supabase.table("daily_stats")                .select("fantasy_team,value")                .gte("stat_date", month_start)                .lte("stat_date", month_end)                .eq("stat_type", active_stat)                .execute().data

            monthly_totals = {t: 0 for t in TEAMS}
            for row in all_daily:
                t = row["fantasy_team"]
                if t in monthly_totals:
                    monthly_totals[t] += row["value"]

            # Write to team_stats for display
            stat_col = {"rbi": "rbi", "k": "strikeouts", "h": "hits",
                        "sb": "stolen_bases", "hr": "home_runs"}[active_stat]
            for team_name, total in monthly_totals.items():
                supabase.table("team_stats").upsert({
                    "team_name": team_name, "season": 2026,
                    stat_col: total, "updated_at": today_str
                }, on_conflict="team_name,season").execute()

        now_et = datetime.now(ET).strftime("%b %d %I:%M %p ET")
        sync_state["last_sync_date"] = today_str
        sync_state["last_sync_time"] = now_et
        sync_state["status"] = "done"
        logger.info(f"Sync complete: {processed} players, {stats_calls} MLB API calls [{now_et}]")

    except Exception as e:
        logger.error(f"Sync error: {e}", exc_info=True)
        sync_state["status"] = "error"
    finally:
        sync_state["syncing"] = False


async def cache_mlb_scores():
    if not supabase:
        return
    scores = await fetch_mlb_scores()
    today_str = date.today().isoformat()
    if scores:
        supabase.table("mlb_scores_cache").delete().neq("game_date", today_str).execute()
        supabase.table("mlb_scores_cache").upsert(scores, on_conflict="game_id").execute()


async def check_and_sync():
    today_str = date.today().isoformat()
    await cache_mlb_scores()
    if sync_state["last_sync_date"] == today_str:
        return
    games = await get_todays_games()
    if not games:
        sync_state["last_sync_date"] = today_str
        return
    non_final = [g for g in games if g["status"]["abstractGameState"] not in ("Final","Postponed","Cancelled")]
    if not non_final:
        await run_fantrax_sync()
    else:
        live = sum(1 for g in games if g["status"]["abstractGameState"] == "Live")
        logger.info(f"{live} games live — waiting")


scheduler = AsyncIOScheduler(timezone="America/New_York")


@app.on_event("startup")
async def startup():
    global supabase
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase connected")
    if FANTRAX_USERNAME and FANTRAX_PASSWORD:
        await fantrax_login()
    scheduler.add_job(check_and_sync, "cron", hour="22-23,0,1,2", minute="0,30")
    scheduler.add_job(cache_mlb_scores, "cron", hour="13-21", minute="0,30")
    scheduler.start()
    logger.info("SeanBot 1.0 online")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


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


@app.get("/api/daily-stats")
async def get_daily_stats(month: str = None):
    """Return monthly cumulative stats from daily_stats table."""
    if not supabase:
        raise HTTPException(503, "DB not configured")
    if not month:
        month = date.today().isoformat()[:7]  # e.g. "2026-03"
    rows = supabase.table("daily_stats")        .select("fantasy_team,stat_type,value,stat_date")        .like("stat_date", f"{month}%")        .execute().data
    # Sum by team
    totals = {t: 0 for t in TEAMS}
    for row in rows:
        t = row["fantasy_team"]
        if t in totals:
            totals[t] += row["value"]
    return {"month": month, "data": [{"team": t, "total": v} for t, v in sorted(totals.items(), key=lambda x: -x[1])]}


@app.get("/api/team-stats")
async def get_team_stats(season: int = 2026):
    if not supabase:
        raise HTTPException(503, "DB not configured")
    return {"data": supabase.table("team_stats").select("*").eq("season", season).execute().data}


@app.get("/api/player-leaders/{stat}")
async def get_player_leaders(stat: str, limit: int = 10):
    if stat not in ["rbi","k","h","sb","hr"]:
        raise HTTPException(400, "Invalid stat")
    if not supabase:
        raise HTTPException(503, "DB not configured")
    return {"data": supabase.table("player_stats").select("*").order(stat, desc=True).limit(limit).execute().data}


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
    return {"data": supabase.table("chirps").select("*").order("created_at", desc=True).limit(limit).execute().data}


@app.post("/api/chirps")
async def post_chirp(request: Request):
    if not supabase:
        raise HTTPException(503, "DB not configured")
    body = await request.json()
    author  = (body.get("author")  or "Anonymous")[:40]
    message = (body.get("message") or "")[:280]
    team    = (body.get("team")    or "")[:60]
    if not message.strip():
        raise HTTPException(400, "Empty")
    return {"ok": True, "data": supabase.table("chirps").insert({"author": author, "team": team, "message": message}).execute().data}


@app.get("/api/history")
async def get_history():
    if not supabase:
        raise HTTPException(503, "DB not configured")
    return {"data": supabase.table("prize_history").select("*").order("year", desc=True).execute().data}


@app.post("/api/history")
async def save_history(request: Request):
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Unauthorized")
    if not supabase:
        raise HTTPException(503, "DB not configured")
    record = {"year": int(body["year"]), "cat_idx": int(body["cat_idx"]), "cat_name": body["cat_name"], "period": body["period"], "winner_team": body["winner_team"], "total": int(body["total"])}
    return {"ok": True, "data": supabase.table("prize_history").upsert(record, on_conflict="year,cat_idx").execute().data}


@app.post("/api/sync-now")
async def trigger_sync(request: Request):
    global FANTRAX_LOGGED_IN
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Unauthorized")
    override = body.get("team_override")
    if override and supabase:
        team = override.get("team")
        stat = override.get("stat")
        val  = int(override.get("value", 0))
        if stat in {"rbi","strikeouts","hits","stolen_bases","home_runs"} and team:
            supabase.table("team_stats").upsert(
                {"team_name": team, "season": 2026, stat: val, "updated_at": date.today().isoformat()},
                on_conflict="team_name,season"
            ).execute()
            return {"ok": True, "message": f"Override: {team} {stat}={val}"}
    sync_state["last_sync_date"] = None
    FANTRAX_LOGGED_IN = False
    asyncio.create_task(run_fantrax_sync())
    return {"ok": True, "message": "Sync triggered"}
