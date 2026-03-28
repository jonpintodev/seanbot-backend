"""
SeanBot 1.0 — Rusty Kuntz Dynasty League
Backend API · Render.com
"""

import os
import asyncio
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

app = FastAPI(title="SeanBot 1.0 — Rusty Kuntz Dynasty League")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET","POST"], allow_headers=["*"])

FANTRAX_USERNAME  = os.environ.get("FANTRAX_USERNAME", "")
FANTRAX_PASSWORD  = os.environ.get("FANTRAX_PASSWORD", "")
FANTRAX_LEAGUE_ID = os.environ.get("FANTRAX_LEAGUE_ID", "38fsaq9emigy6d4z")
SUPABASE_URL      = os.environ.get("SUPABASE_URL", "https://haaaaugigaryryqjuztx.supabase.co")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhhYWFhdWdpZ2FyeXJ5cWp1enR4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDY2OTk3NywiZXhwIjoyMDkwMjQ1OTc3fQ.1mPiznawXi-2AtDqfIQET7hjWw5fuk-zRU9aPgQnNDQ")
ADMIN_SECRET      = os.environ.get("ADMIN_SECRET", "changeme123")

supabase: Optional[Client] = None

TEAMS = [
    "Possibilities", "Yoshi's Islanders", "thebigfur", "Red Birds",
    "Daddy Yankee", "¡pinto!", "JoanMacias", "Xavier", "Los Jankees",
    "Momin8241", "ericliaci", "Sho Me The Money",
    "Designated Shitters 🧻", "Arraezed & Hoerny"
]

sync_state = {
    "last_sync_date": None,
    "syncing": False,
    "last_sync_time": None,
    "status": "idle",
}


# ── MLB HELPERS ───────────────────────────────────────────────────────────────
async def get_todays_games() -> list[dict]:
    today = date.today().strftime("%Y-%m-%d")
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&hydrate=linescore,team,decisions"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            data = resp.json()
        return data.get("dates", [{}])[0].get("games", [])
    except Exception as e:
        logger.error(f"MLB schedule error: {e}")
        return []


async def all_games_final() -> tuple[bool, list]:
    games = await get_todays_games()
    if not games:
        return False, []
    non_final = [g for g in games if g["status"]["abstractGameState"] not in ("Final","Postponed","Cancelled")]
    return len(non_final) == 0, games


async def fetch_mlb_scores_formatted() -> list[dict]:
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
    try:
        import xml.etree.ElementTree as ET
        proxy = "https://api.allorigins.win/get?url=https://www.mlb.com/feeds/news/rss.xml"
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(proxy)
        raw = resp.json().get("contents", "")
        # Strip BOM / leading whitespace that breaks XML parser
        raw = raw.strip().lstrip("\ufeff")
        root = ET.fromstring(raw)
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
        logger.error(f"MLB news error: {e}")
        return []


# ── FANTRAX CLIENT ────────────────────────────────────────────────────────────
class FantraxClient:
    BASE = "https://www.fantrax.com"

    def __init__(self):
        self.session = httpx.AsyncClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=30,
            follow_redirects=True,
        )
        self.logged_in = False

    async def login(self) -> bool:
        """Try multiple Fantrax login endpoints until one works."""
        endpoints = [
            {
                "url": f"{self.BASE}/loginRegister/v3/login",
                "body": {"email": FANTRAX_USERNAME, "password": FANTRAX_PASSWORD, "rememberMe": True},
            },
            {
                "url": f"{self.BASE}/fxea/general/login",
                "body": {"email": FANTRAX_USERNAME, "password": FANTRAX_PASSWORD},
            },
            {
                "url": f"{self.BASE}/universal/v2/auth/login",
                "body": {"username": FANTRAX_USERNAME, "password": FANTRAX_PASSWORD},
            },
        ]
        for ep in endpoints:
            try:
                resp = await self.session.post(ep["url"], json=ep["body"])
                logger.info(f"Fantrax login attempt {ep['url']}: {resp.status_code}")
                if resp.status_code in (200, 201):
                    self.logged_in = True
                    logger.info("✅ Fantrax login successful")
                    return True
            except Exception as e:
                logger.error(f"Login attempt error {ep['url']}: {e}")
        logger.error("❌ All Fantrax login attempts failed")
        return False

    async def get_team_rosters(self) -> Optional[dict]:
        resp = await self.session.get(
            f"{self.BASE}/fxea/general/getTeamRosters",
            params={"leagueId": FANTRAX_LEAGUE_ID}
        )
        if resp.status_code != 200:
            logger.error(f"getTeamRosters HTTP {resp.status_code}")
            return None
        try:
            data = resp.json()
            # Fantrax sometimes wraps response in a string — unwrap it
            if isinstance(data, str):
                import json
                data = json.loads(data)
            return data if isinstance(data, dict) else None
        except Exception as e:
            logger.error(f"getTeamRosters parse error: {e} | body: {resp.text[:300]}")
            return None

    async def get_player_stats(self) -> Optional[dict]:
        resp = await self.session.post(
            f"{self.BASE}/fxea/general/getPlayerStats",
            json={
                "leagueId": FANTRAX_LEAGUE_ID,
                "scoringPeriod": "SEASON",
                "statusOrTeamFilter": "ALL",
                "timeframeType": "BY_DATE",
                "pageNumber": 1,
                "rowsPerPage": 200,
                "scoringCategory": "BASEBALL_HITTER",
            }
        )
        if resp.status_code != 200:
            logger.error(f"getPlayerStats HTTP {resp.status_code}")
            return None
        try:
            data = resp.json()
            if isinstance(data, str):
                import json
                data = json.loads(data)
            return data if isinstance(data, dict) else None
        except Exception as e:
            logger.error(f"getPlayerStats parse error: {e} | body: {resp.text[:300]}")
            return None

    async def close(self):
        await self.session.aclose()


fantrax = FantraxClient()


# ── CORE SYNC ─────────────────────────────────────────────────────────────────
async def run_fantrax_sync():
    if sync_state["syncing"]:
        logger.info("Sync already running — skipping")
        return

    sync_state["syncing"] = True
    sync_state["status"] = "syncing"
    logger.info("🤖 SeanBot sync starting...")

    try:
        if not fantrax.logged_in:
            ok = await fantrax.login()
            if not ok:
                sync_state["status"] = "login_failed"
                return

        today_str = date.today().isoformat()

        # Get rosters — identify starters
        rosters_data = await fantrax.get_team_rosters()
        starter_ids: set[str] = set()
        player_team_map: dict[str, str] = {}

        if rosters_data:
            logger.info(f"Roster keys: {list(rosters_data.keys())[:10]}")
            roster_list = (rosters_data.get("rosters") or
                           rosters_data.get("rosterItems") or
                           rosters_data.get("teams") or [])
            for team_info in roster_list:
                if not isinstance(team_info, dict):
                    continue
                team_name = (team_info.get("name") or
                             team_info.get("teamName") or "Unknown")
                players = (team_info.get("rosterItems") or
                           team_info.get("players") or
                           team_info.get("roster") or [])
                for player in players:
                    if not isinstance(player, dict):
                        continue
                    slot = player.get("lineupStatus", "").upper()
                    pid = str(player.get("id") or player.get("playerId") or "")
                    if pid and slot not in ("BN", "IL", "NA", "SUSP"):
                        starter_ids.add(pid)
                        player_team_map[pid] = team_name
        else:
            logger.warning("No roster data — counting all players")

        logger.info(f"Starters found: {len(starter_ids)}")

        # Get player stats
        stats_data = await fantrax.get_player_stats()

        team_totals = {t: {"rbi":0,"strikeouts":0,"hits":0,"stolen_bases":0,"home_runs":0} for t in TEAMS}
        player_rows = []

        if stats_data:
            logger.info(f"Stats keys: {list(stats_data.keys())[:10]}")
            # Try common Fantrax player stats keys
            player_list = (stats_data.get("playerStats") or
                           stats_data.get("players") or
                           stats_data.get("rosterStats") or [])
            for p in player_list:
                if not isinstance(p, dict):
                    continue
                pid = str(p.get("id") or p.get("playerId") or "")
                # If no starters found, include everyone
                if starter_ids and pid not in starter_ids:
                    continue
                fantasy_team = player_team_map.get(pid)
                if not fantasy_team or fantasy_team not in team_totals:
                    continue
                s = p.get("stats", {})
                rbi = float(s.get("rbi", 0) or 0)
                kp  = float(s.get("kp",  0) or 0)
                h   = float(s.get("h",   0) or 0)
                sb  = float(s.get("sb",  0) or 0)
                hr  = float(s.get("hr",  0) or 0)
                team_totals[fantasy_team]["rbi"]          += rbi
                team_totals[fantasy_team]["strikeouts"]   += kp
                team_totals[fantasy_team]["hits"]         += h
                team_totals[fantasy_team]["stolen_bases"] += sb
                team_totals[fantasy_team]["home_runs"]    += hr
                player_rows.append({
                    "player_id": pid,
                    "name": p.get("name", "Unknown"),
                    "mlb_team": p.get("team", ""),
                    "position": p.get("position", ""),
                    "fantasy_team": fantasy_team,
                    "rbi": rbi, "k": kp, "h": h, "sb": sb, "hr": hr,
                    "updated_at": today_str,
                })

        if supabase:
            for team_name, stats in team_totals.items():
                supabase.table("team_stats").upsert(
                    {"team_name": team_name, "season": 2026,
                     **{k: int(v) for k, v in stats.items()},
                     "updated_at": today_str},
                    on_conflict="team_name,season"
                ).execute()
            for p in player_rows:
                supabase.table("player_stats").upsert(p, on_conflict="player_id").execute()

        now_et = datetime.now(ET).strftime("%b %d %I:%M %p ET")
        sync_state["last_sync_date"] = today_str
        sync_state["last_sync_time"] = now_et
        sync_state["status"] = "done"
        logger.info(f"✅ Sync complete — {len(player_rows)} players, {len(team_totals)} teams")

    except Exception as e:
        logger.error(f"Sync error: {e}")
        sync_state["status"] = "error"
    finally:
        sync_state["syncing"] = False


async def cache_mlb_scores():
    if not supabase:
        return
    scores = await fetch_mlb_scores_formatted()
    today_str = date.today().isoformat()
    if scores:
        supabase.table("mlb_scores_cache").delete().neq("game_date", today_str).execute()
        supabase.table("mlb_scores_cache").upsert(scores, on_conflict="game_id").execute()


async def check_and_sync():
    """Runs every 30 min 10pm–2am ET. Triggers Fantrax sync once all games are Final."""
    today_str = date.today().isoformat()
    await cache_mlb_scores()
    if sync_state["last_sync_date"] == today_str:
        return
    sync_state["status"] = "waiting_for_games"
    done, games = await all_games_final()
    if not games:
        sync_state["last_sync_date"] = today_str
        return
    if done:
        logger.info(f"All {len(games)} games Final — triggering sync")
        await run_fantrax_sync()
    else:
        live = sum(1 for g in games if g["status"]["abstractGameState"] == "Live")
        logger.info(f"{live} games still live — waiting...")


async def daytime_score_refresh():
    await cache_mlb_scores()


# ── SCHEDULER ─────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler(timezone="America/New_York")

@app.on_event("startup")
async def startup():
    global supabase
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("✅ Supabase connected")
    if FANTRAX_USERNAME and FANTRAX_PASSWORD:
        await fantrax.login()
    scheduler.add_job(check_and_sync, "cron", hour="22-23,0,1,2", minute="0,30")
    scheduler.add_job(daytime_score_refresh, "cron", hour="13-21", minute="0,30")
    scheduler.start()
    logger.info("🤖 SeanBot 1.0 is online")

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()
    await fantrax.close()


# ── ROUTES ────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"app": "SeanBot 1.0", "league": "Rusty Kuntz Dynasty League", "status": "online", "sync": sync_state}

@app.get("/api/status")
async def get_status():
    games = await get_todays_games()
    final_count = sum(1 for g in games if g["status"]["abstractGameState"] == "Final")
    live_count  = sum(1 for g in games if g["status"]["abstractGameState"] == "Live")
    return {
        "sync_status": sync_state["status"],
        "last_sync_time": sync_state["last_sync_time"],
        "last_sync_date": sync_state["last_sync_date"],
        "games_today": len(games),
        "games_final": final_count,
        "games_live": live_count,
        "time_et": datetime.now(ET).strftime("%I:%M %p ET"),
    }

@app.get("/api/team-stats")
async def get_team_stats(season: int = 2026):
    if not supabase:
        raise HTTPException(503, "Database not configured")
    result = supabase.table("team_stats").select("*").eq("season", season).execute()
    return {"data": result.data}

@app.get("/api/player-leaders/{stat}")
async def get_player_leaders(stat: str, limit: int = 10):
    if stat not in ["rbi","k","h","sb","hr"]:
        raise HTTPException(400, "Invalid stat")
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
        "year": int(body["year"]),
        "cat_idx": int(body["cat_idx"]),
        "cat_name": body["cat_name"],
        "period": body["period"],
        "winner_team": body["winner_team"],
        "total": int(body["total"]),
    }
    result = supabase.table("prize_history").upsert(record, on_conflict="year,cat_idx").execute()
    return {"ok": True, "data": result.data}

@app.post("/api/sync-now")
async def trigger_sync(request: Request):
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
            return {"ok": True, "message": f"Override saved: {team} {stat}={val}"}
    sync_state["last_sync_date"] = None
    asyncio.create_task(run_fantrax_sync())
    return {"ok": True, "message": "Fantrax sync triggered"}

