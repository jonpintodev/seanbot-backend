"""
SeanBot 1.0 — Rusty Kuntz Dynasty League
Backend API · Render.com Free Tier

Smart sync: polls MLB API every 30 min from 10pm ET,
triggers Fantrax pull only after ALL games finish.
"""

import os
import json
import asyncio
import logging
from datetime import date, datetime, timedelta
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

app = FastAPI(title="SeanBot 1.0 — Rusty Kuntz Dynasty League API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET","POST"], allow_headers=["*"])

# ── ENV VARS ──────────────────────────────────────────────────────────────────
FANTRAX_USERNAME  = os.environ.get("FANTRAX_USERNAME", "")
FANTRAX_PASSWORD  = os.environ.get("FANTRAX_PASSWORD", "")
FANTRAX_LEAGUE_ID = os.environ.get("FANTRAX_LEAGUE_ID", "")
SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY", "")
ADMIN_SECRET      = os.environ.get("ADMIN_SECRET", "changeme123")

supabase: Optional[Client] = None

# ── STAT CATEGORIES ───────────────────────────────────────────────────────────
PRIZE_CATS = {
    0: {"name": "RBI",          "fantrax_key": "rbi",  "months": [3, 4]},
    1: {"name": "Strikeouts",   "fantrax_key": "kp",   "months": [5]},
    2: {"name": "Hits",         "fantrax_key": "h",    "months": [6]},
    3: {"name": "Stolen Bases", "fantrax_key": "sb",   "months": [7]},
    4: {"name": "Home Runs",    "fantrax_key": "hr",   "months": [8]},
    5: {"name": "Single-Day",   "fantrax_key": None,   "months": [9]},
}

TEAMS = [
    "Possibilities", "Yoshi's Islanders", "thebigfur", "Red Birds",
    "Daddy Yankee", "¡pinto!", "JoanMacias", "Xavier", "Los Jankees",
    "Momin8241", "ericliaci", "Sho Me The Money",
    "Designated Shitters 🧻", "Arraezed & Hoerny"
]

# ── SYNC STATE (in-memory) ────────────────────────────────────────────────────
sync_state = {
    "last_sync_date": None,      # date string of last successful sync
    "syncing": False,            # lock to prevent double-sync
    "last_sync_time": None,      # full datetime string
    "games_checked": 0,
    "status": "idle",            # idle | waiting_for_games | syncing | done
}


# ── MLB API HELPERS ───────────────────────────────────────────────────────────
async def get_todays_games() -> list[dict]:
    """Fetch today's MLB schedule from the official MLB Stats API."""
    today = date.today().strftime("%Y-%m-%d")
    url = (
        f"https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&date={today}&hydrate=linescore,team,decisions"
    )
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            data = resp.json()
        return data.get("dates", [{}])[0].get("games", [])
    except Exception as e:
        logger.error(f"MLB schedule fetch error: {e}")
        return []


async def all_games_final() -> tuple[bool, list[dict]]:
    """
    Returns (all_done, games).
    all_done = True only when every scheduled game today is Final/Postponed/Cancelled.
    """
    games = await get_todays_games()
    if not games:
        return False, []
    non_final = [
        g for g in games
        if g["status"]["abstractGameState"] not in ("Final", "Postponed", "Cancelled")
    ]
    return len(non_final) == 0, games


async def fetch_mlb_scores_formatted() -> list[dict]:
    """Fetch today's scores and return serializable dicts."""
    games = await get_todays_games()
    today = date.today().isoformat()
    results = []
    for g in games:
        away = g["teams"]["away"]
        home = g["teams"]["home"]
        results.append({
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
    return results


async def fetch_mlb_news() -> list[dict]:
    """Fetch MLB news via allorigins proxy of the MLB RSS feed."""
    try:
        import xml.etree.ElementTree as ET
        rss = "https://www.mlb.com/feeds/news/rss.xml"
        proxy = f"https://api.allorigins.win/get?url={rss}"
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(proxy)
        root = ET.fromstring(resp.json()["contents"])
        return [
            {
                "title": item.findtext("title", ""),
                "link": item.findtext("link", ""),
                "pub_date": item.findtext("pubDate", ""),
                "description": (item.findtext("description", "") or "")[:200],
            }
            for item in root.findall(".//item")[:12]
        ]
    except Exception as e:
        logger.error(f"MLB news fetch error: {e}")
        return []


# ── FANTRAX CLIENT ────────────────────────────────────────────────────────────
class FantraxClient:
    BASE = "https://www.fantrax.com"

    def __init__(self):
        self.session = httpx.AsyncClient(
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=30,
            follow_redirects=True,
        )
        self.logged_in = False

    async def login(self) -> bool:
        try:
            resp = await self.session.post(
                f"{self.BASE}/universal/v1/auth/login",
                json={"username": FANTRAX_USERNAME, "password": FANTRAX_PASSWORD},
            )
            if resp.status_code == 200:
                self.logged_in = True
                logger.info("✅ Fantrax login successful")
                return True
            logger.error(f"❌ Fantrax login failed {resp.status_code}: {resp.text[:300]}")
            return False
        except Exception as e:
            logger.error(f"❌ Fantrax login exception: {e}")
            return False

    async def get_team_rosters(self) -> Optional[dict]:
        resp = await self.session.get(
            f"{self.BASE}/fxea/general/getTeamRosters",
            params={"leagueId": FANTRAX_LEAGUE_ID}
        )
        return resp.json() if resp.status_code == 200 else None

    async def get_player_stats(self) -> Optional[dict]:
        resp = await self.session.post(
            f"{self.BASE}/fxea/general/getPlayerStats",
            json={
                "leagueId": FANTRAX_LEAGUE_ID,
                "scoringPeriod": "SEASON",
                "statusOrTeamFilter": "ALL",
                "timeframeType": "BY_DATE",
                "pageNumber": 1,
                "rowsPerPage": 100,
                "scoringCategory": "BASEBALL_HITTER",
            }
        )
        return resp.json() if resp.status_code == 200 else None

    async def close(self):
        await self.session.aclose()


fantrax = FantraxClient()


# ── CORE SYNC ─────────────────────────────────────────────────────────────────
async def run_fantrax_sync():
    """
    Pull Fantrax data for started players only and write to Supabase.
    Called automatically once all games are final each night.
    """
    if sync_state["syncing"]:
        logger.info("Sync already in progress — skipping")
        return

    sync_state["syncing"] = True
    sync_state["status"] = "syncing"
    logger.info("🤖 SeanBot sync starting...")

    try:
        # Ensure logged in
        if not fantrax.logged_in:
            ok = await fantrax.login()
            if not ok:
                logger.error("Cannot sync — Fantrax login failed")
                return

        today_str = date.today().isoformat()

        # 1. Get rosters to find starters
        rosters_data = await fantrax.get_team_rosters()
        starter_ids: set[str] = set()
        player_team_map: dict[str, str] = {}  # player_id → fantasy team name

        if rosters_data:
            for team_info in rosters_data.get("rosters", []):
                team_name = team_info.get("name", "Unknown")
                for player in team_info.get("rosterItems", []):
                    slot = player.get("lineupStatus", "").upper()
                    pid = player.get("id", "")
                    if pid and slot not in ("BN", "IL", "NA", "SUSP"):
                        starter_ids.add(pid)
                        player_team_map[pid] = team_name

        logger.info(f"Found {len(starter_ids)} starters")

        # 2. Get player stats
        stats_data = await fantrax.get_player_stats()

        team_totals: dict[str, dict] = {
            t: {"rbi": 0, "strikeouts": 0, "hits": 0, "stolen_bases": 0, "home_runs": 0}
            for t in TEAMS
        }
        player_rows: list[dict] = []

        if stats_data:
            for p in stats_data.get("playerStats", []):
                pid = p.get("id", "")
                if pid not in starter_ids:
                    continue
                fantasy_team = player_team_map.get(pid)
                if not fantasy_team:
                    continue

                s = p.get("stats", {})
                rbi = float(s.get("rbi",  0) or 0)
                kp  = float(s.get("kp",   0) or 0)
                h   = float(s.get("h",    0) or 0)
                sb  = float(s.get("sb",   0) or 0)
                hr  = float(s.get("hr",   0) or 0)

                if fantasy_team in team_totals:
                    team_totals[fantasy_team]["rbi"]          += rbi
                    team_totals[fantasy_team]["strikeouts"]   += kp
                    team_totals[fantasy_team]["hits"]         += h
                    team_totals[fantasy_team]["stolen_bases"] += sb
                    team_totals[fantasy_team]["home_runs"]    += hr

                player_rows.append({
                    "player_id":    pid,
                    "name":         p.get("name", "Unknown"),
                    "mlb_team":     p.get("team", ""),
                    "position":     p.get("position", ""),
                    "fantasy_team": fantasy_team,
                    "rbi":  rbi,  "k": kp,  "h": h,  "sb": sb,  "hr": hr,
                    "updated_at":   today_str,
                })

        # 3. Write to Supabase
        if supabase:
            for team_name, stats in team_totals.items():
                supabase.table("team_stats").upsert(
                    {"team_name": team_name, "season": 2026, **{k: int(v) for k,v in stats.items()}, "updated_at": today_str},
                    on_conflict="team_name,season"
                ).execute()

            for p in player_rows:
                supabase.table("player_stats").upsert(p, on_conflict="player_id").execute()

        now_et = datetime.now(ET).strftime("%b %d, %Y %I:%M %p ET")
        sync_state["last_sync_date"] = today_str
        sync_state["last_sync_time"] = now_et
        sync_state["status"] = "done"
        logger.info(f"✅ SeanBot sync complete — {len(player_rows)} players, {len(team_totals)} teams [{now_et}]")

    except Exception as e:
        logger.error(f"Sync error: {e}")
        sync_state["status"] = "error"
    finally:
        sync_state["syncing"] = False


async def cache_mlb_scores():
    """Write today's MLB scores to Supabase cache."""
    if not supabase:
        return
    scores = await fetch_mlb_scores_formatted()
    today_str = date.today().isoformat()
    if scores:
        # Clear old dates first
        supabase.table("mlb_scores_cache").delete().neq("game_date", today_str).execute()
        supabase.table("mlb_scores_cache").upsert(scores, on_conflict="game_id").execute()
    return scores


# ── SMART POST-GAME SCHEDULER ─────────────────────────────────────────────────
async def check_and_sync():
    """
    Runs every 30 min from 10pm–2am ET.
    If all games are Final AND we haven't synced today → run Fantrax sync.
    """
    now_et = datetime.now(ET)
    today_str = date.today().isoformat()

    # Update MLB scores cache regardless
    await cache_mlb_scores()

    # Already synced today?
    if sync_state["last_sync_date"] == today_str:
        logger.info(f"Already synced today ({today_str}) — skipping Fantrax pull")
        return

    sync_state["status"] = "waiting_for_games"
    done, games = await all_games_final()
    sync_state["games_checked"] = len(games)

    if not games:
        logger.info("No games scheduled today — no sync needed")
        sync_state["last_sync_date"] = today_str  # mark done so we don't keep checking
        return

    if done:
        logger.info(f"All {len(games)} games Final ✅ — triggering Fantrax sync")
        await run_fantrax_sync()
    else:
        in_progress = [g for g in games if g["status"]["abstractGameState"] == "Live"]
        logger.info(f"{len(in_progress)} games still in progress — waiting...")


async def daytime_score_refresh():
    """Refresh MLB scores every 30 min during game hours (1pm–10pm ET)."""
    await cache_mlb_scores()
    logger.info("Daytime score cache refreshed")


# ── STARTUP / SHUTDOWN ────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler(timezone="America/New_York")

@app.on_event("startup")
async def startup():
    global supabase
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("✅ Supabase connected")
    else:
        logger.warning("⚠️  Supabase not configured — running without database")

    if FANTRAX_USERNAME and FANTRAX_PASSWORD:
        await fantrax.login()

    # Check for game completion every 30 min, 10pm–2am ET
    scheduler.add_job(check_and_sync, "cron", hour="22-23,0,1,2", minute="0,30", id="post_game_check")
    # Refresh scores every 30 min during the afternoon/evening
    scheduler.add_job(daytime_score_refresh, "cron", hour="13-21", minute="0,30", id="daytime_scores")
    scheduler.start()
    logger.info("🤖 SeanBot 1.0 is online — smart post-game sync active")

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()
    await fantrax.close()


# ── API ROUTES ────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    now_et = datetime.now(ET).strftime("%b %d, %Y %I:%M %p ET")
    return {
        "app": "SeanBot 1.0",
        "league": "Rusty Kuntz Dynasty League",
        "status": "online",
        "time_et": now_et,
        "sync": sync_state,
    }

@app.get("/api/status")
async def get_status():
    """Health check + sync status for the frontend."""
    games = await get_todays_games()
    final_count = sum(1 for g in games if g["status"]["abstractGameState"] == "Final")
    live_count  = sum(1 for g in games if g["status"]["abstractGameState"] == "Live")
    return {
        "sync_status":     sync_state["status"],
        "last_sync_time":  sync_state["last_sync_time"],
        "last_sync_date":  sync_state["last_sync_date"],
        "games_today":     len(games),
        "games_final":     final_count,
        "games_live":      live_count,
        "time_et":         datetime.now(ET).strftime("%I:%M %p ET"),
    }

@app.get("/api/team-stats")
async def get_team_stats(season: int = 2026):
    if not supabase:
        raise HTTPException(503, "Database not configured")
    result = supabase.table("team_stats").select("*").eq("season", season).execute()
    return {"data": result.data}

@app.get("/api/player-leaders/{stat}")
async def get_player_leaders(stat: str, limit: int = 10):
    valid = ["rbi", "k", "h", "sb", "hr"]
    if stat not in valid:
        raise HTTPException(400, f"stat must be one of {valid}")
    if not supabase:
        raise HTTPException(503, "Database not configured")
    result = supabase.table("player_stats").select("*").order(stat, desc=True).limit(limit).execute()
    return {"data": result.data, "stat": stat}

@app.get("/api/mlb-scores")
async def get_mlb_scores():
    if supabase:
        today_str = date.today().isoformat()
        result = supabase.table("mlb_scores_cache").select("*").eq("game_date", today_str).execute()
        if result.data:
            return {"data": result.data}
    # Fallback: live fetch
    return {"data": await fetch_mlb_scores_formatted()}

@app.get("/api/mlb-news")
async def get_mlb_news():
    return {"data": await fetch_mlb_news()}

@app.get("/api/chirps")
async def get_chirps(limit: int = 50):
    if not supabase:
        raise HTTPException(503, "Database not configured")
    result = supabase.table("chirps").select("*").order("created_at", desc=True).limit(limit).execute()
    return {"data": result.data}

@app.post("/api/chirps")
async def post_chirp(request: Request):
    if not supabase:
        raise HTTPException(503, "Database not configured")
    body = await request.json()
    author  = (body.get("author")  or "Anonymous")[:40]
    message = (body.get("message") or "")[:280]
    team    = (body.get("team")    or "")[:60]
    if not message.strip():
        raise HTTPException(400, "Message cannot be empty")
    result = supabase.table("chirps").insert({"author": author, "team": team, "message": message}).execute()
    return {"ok": True, "data": result.data}

@app.get("/api/history")
async def get_history():
    if not supabase:
        raise HTTPException(503, "Database not configured")
    result = supabase.table("prize_history").select("*").order("year", desc=True).execute()
    return {"data": result.data}

@app.post("/api/history")
async def save_history(request: Request):
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Unauthorized")
    if not supabase:
        raise HTTPException(503, "Database not configured")
    record = {
        "year":         int(body["year"]),
        "cat_idx":      int(body["cat_idx"]),
        "cat_name":     body["cat_name"],
        "period":       body["period"],
        "winner_team":  body["winner_team"],
        "total":        int(body["total"]),
    }
    result = supabase.table("prize_history").upsert(record, on_conflict="year,cat_idx").execute()
    return {"ok": True, "data": result.data}

@app.post("/api/sync-now")
async def trigger_sync(request: Request):
    """Admin endpoint to manually trigger a Fantrax sync."""
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Unauthorized")

    # Optional manual stat override (for September / manual corrections)
    override = body.get("team_override")
    if override and supabase:
        team = override.get("team")
        stat = override.get("stat")
        val  = int(override.get("value", 0))
        valid_stats = {"rbi", "strikeouts", "hits", "stolen_bases", "home_runs"}
        if stat in valid_stats and team:
            supabase.table("team_stats").upsert(
                {"team_name": team, "season": 2026, stat: val, "updated_at": date.today().isoformat()},
                on_conflict="team_name,season"
            ).execute()
            return {"ok": True, "message": f"Override saved: {team} {stat} = {val}"}

    # Full sync
    sync_state["last_sync_date"] = None  # reset so it runs even if already synced today
    asyncio.create_task(run_fantrax_sync())
    return {"ok": True, "message": "Full Fantrax sync triggered"}
