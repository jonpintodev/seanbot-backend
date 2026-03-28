"""
SeanBot 1.0 - Rusty Kuntz Dynasty League
Diagnostic: inspect playerInfo from getLeagueInfo
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
    "Possibilities", "Yoshi's Islanders", "thebigfur", "Red Birds",
    "Daddy Yankee", "pinto!", "JoanMacias", "Xavier", "Los Jankees",
    "Momin8241", "ericliaci", "Sho Me The Money",
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
        logger.error(f"GET {path}: {resp.status_code}")
    except Exception as e:
        logger.error(f"GET error {path}: {e}")
    return None


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

        # Get rosters — we know this works
        roster_data = await fantrax_get(
            "/fxea/general/getTeamRosters",
            {"leagueId": FANTRAX_LEAGUE_ID}
        )
        player_team_map = {}
        player_status_map = {}
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
            logger.info(f"Roster: {len(player_team_map)} players")

        # ── KEY: Inspect playerInfo from getLeagueInfo ──────────────────────
        league_info = await fantrax_get(
            "/fxea/general/getLeagueInfo",
            {"leagueId": FANTRAX_LEAGUE_ID}
        )

        team_totals = {t: {"rbi":0.0,"strikeouts":0.0,"hits":0.0,"stolen_bases":0.0,"home_runs":0.0} for t in TEAMS}
        player_rows = []

        if isinstance(league_info, dict):
            player_info = league_info.get("playerInfo", {})
            logger.info(f"playerInfo type: {type(player_info)}")

            if isinstance(player_info, dict):
                logger.info(f"playerInfo keys count: {len(player_info)}")
                # Log the first player entry fully
                first_pid = next(iter(player_info), None)
                if first_pid:
                    logger.info(f"First playerInfo key: {first_pid}")
                    logger.info(f"First playerInfo value: {json.dumps(player_info[first_pid])[:600]}")

                # Process all players
                for pid, pdata in player_info.items():
                    if not isinstance(pdata, dict):
                        continue
                    fantasy_team = player_team_map.get(pid)
                    if not fantasy_team:
                        continue

                    # Match to our team list
                    matched = None
                    for t in TEAMS:
                        if t.lower() == fantasy_team.lower() or fantasy_team.lower() in t.lower() or t.lower() in fantasy_team.lower():
                            matched = t
                            break
                    if not matched:
                        continue

                    # Skip injured/reserve
                    status = player_status_map.get(pid, "ACTIVE")
                    if status in ("INJURED_RESERVE",):
                        continue

                    # Log all available stat keys from first matched player
                    if not player_rows:
                        logger.info(f"Player data keys: {list(pdata.keys())}")
                        logger.info(f"Full player data: {json.dumps(pdata)[:600]}")

                    # Try every possible stat key
                    stats = pdata.get("stats", {}) or pdata.get("scoring", {}) or {}
                    rbi = float(stats.get("rbi", 0) or pdata.get("rbi", 0) or 0)
                    kp  = float(stats.get("kp",  0) or stats.get("k", 0) or pdata.get("k", 0) or 0)
                    h   = float(stats.get("h",   0) or pdata.get("h", 0) or 0)
                    sb  = float(stats.get("sb",  0) or pdata.get("sb", 0) or 0)
                    hr  = float(stats.get("hr",  0) or pdata.get("hr", 0) or 0)

                    team_totals[matched]["rbi"]          += rbi
                    team_totals[matched]["strikeouts"]   += kp
                    team_totals[matched]["hits"]         += h
                    team_totals[matched]["stolen_bases"] += sb
                    team_totals[matched]["home_runs"]    += hr

                    name = pdata.get("name","") or pdata.get("playerName","") or pid
                    mlb  = pdata.get("team","") or pdata.get("mlbTeam","") or pdata.get("nflTeam","")
                    pos  = pdata.get("position","") or pdata.get("pos","")

                    player_rows.append({
                        "player_id": pid,
                        "name": name,
                        "mlb_team": mlb,
                        "position": pos,
                        "fantasy_team": matched,
                        "rbi": rbi, "k": kp, "h": h, "sb": sb, "hr": hr,
                        "updated_at": today_str,
                    })

            elif isinstance(player_info, list):
                logger.info(f"playerInfo is a list with {len(player_info)} items")
                if player_info:
                    logger.info(f"First item: {json.dumps(player_info[0])[:600]}")

        logger.info(f"Players processed: {len(player_rows)}")
        logger.info(f"Team totals sample: {json.dumps({k: v for k,v in list(team_totals.items())[:3]})}")

        if supabase:
            for team_name, stats in team_totals.items():
                supabase.table("team_stats").upsert({
                    "team_name": team_name, "season": 2026,
                    "rbi": int(stats["rbi"]), "strikeouts": int(stats["strikeouts"]),
                    "hits": int(stats["hits"]), "stolen_bases": int(stats["stolen_bases"]),
                    "home_runs": int(stats["home_runs"]), "updated_at": today_str
                }, on_conflict="team_name,season").execute()
            for p in player_rows:
                supabase.table("player_stats").upsert(p, on_conflict="player_id").execute()

        now_et = datetime.now(ET).strftime("%b %d %I:%M %p ET")
        sync_state["last_sync_date"] = today_str
        sync_state["last_sync_time"] = now_et
        sync_state["status"] = "done"
        logger.info(f"Sync complete: {len(player_rows)} players [{now_et}]")

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
