"""
SeanBot 1.0 — Rusty Kuntz Dynasty League
- 2026 Standings: manual commissioner input, accumulates daily
- All other stats: live from MLB.com
"""

import os, asyncio, calendar, logging
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

SUPABASE_URL  = os.environ.get("SUPABASE_URL", "https://haaaaugigaryryqjuztx.supabase.co")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhhYWFhdWdpZ2FyeXJ5cWp1enR4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDY2OTk3NywiZXhwIjoyMDkwMjQ1OTc3fQ.1mPiznawXi-2AtDqfIQET7hjWw5fuk-zRU9aPgQnNDQ")
ADMIN_SECRET  = os.environ.get("ADMIN_SECRET", "changeme123")

TEAMS = [
    "Possibilities", "Yoshi\u2019s Islanders", "thebigfur", "Red Birds",
    "Daddy Yankee", "\u00a1pinto!", "JoanMacias", "Xavier", "Los Jankees",
    "Momin8421", "ericliaci", "Sho Me The Money",
    "Designated Shitters", "Arraezed & Hoerny"
]

supabase: Optional[Client] = None
scheduler = AsyncIOScheduler(timezone="America/New_York")


# ── MLB.com helpers ───────────────────────────────────────────────────────────

async def get_todays_games() -> list:
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            resp = await c.get(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date.today()}&hydrate=linescore,team,decisions")
        return resp.json().get("dates",[{}])[0].get("games",[])
    except: return []

async def fetch_mlb_scores() -> list:
    games = await get_todays_games()
    today = date.today().isoformat()
    out = []
    for g in games:
        away = g["teams"]["away"]; home = g["teams"]["home"]
        out.append({
            "game_id": g["gamePk"], "status": g["status"]["abstractGameState"],
            "status_detail": g["status"]["detailedState"],
            "inning": g.get("linescore",{}).get("currentInningOrdinal",""),
            "away_team": away["team"]["name"], "away_abbr": away["team"].get("abbreviation",""),
            "away_score": away.get("score"), "away_winner": away.get("isWinner",False),
            "home_team": home["team"]["name"], "home_abbr": home["team"].get("abbreviation",""),
            "home_score": home.get("score"), "home_winner": home.get("isWinner",False),
            "venue": g.get("venue",{}).get("name",""), "game_date": today,
        })
    return out

async def fetch_mlb_news() -> list:
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            resp = await c.get("https://api.rss2json.com/v1/api.json?rss_url=https://www.mlb.com/feeds/news/rss.xml")
        return [{"title":i.get("title",""),"link":i.get("link",""),"pub_date":i.get("pubDate",""),
                 "description":(i.get("description","") or "")[:200]} for i in resp.json().get("items",[])[:12]]
    except: return []

async def cache_mlb_scores():
    if not supabase: return
    scores = await fetch_mlb_scores()
    today_str = date.today().isoformat()
    if scores:
        supabase.table("mlb_scores_cache").delete().neq("game_date", today_str).execute()
        supabase.table("mlb_scores_cache").upsert(scores, on_conflict="game_id").execute()


# ── Startup / Shutdown ────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    global supabase
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase connected")
    scheduler.add_job(cache_mlb_scores, "cron", hour="13-23", minute="0,15,30,45")
    scheduler.start()
    logger.info("SeanBot 1.0 online")

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ── API Endpoints ─────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"app": "SeanBot 1.0", "status": "online"}

@app.get("/api/status")
async def get_status():
    games = await get_todays_games()
    return {
        "games_today": len(games),
        "games_final": sum(1 for g in games if g["status"]["abstractGameState"] == "Final"),
        "games_live":  sum(1 for g in games if g["status"]["abstractGameState"] == "Live"),
        "time_et":     datetime.now(ET).strftime("%I:%M %p ET"),
    }

@app.get("/api/team-stats")
async def get_team_stats():
    """Returns the manually maintained 2026 standings."""
    if not supabase: raise HTTPException(503, "DB not configured")
    return {"data": supabase.table("team_stats").select("*").eq("season", 2026).execute().data}

@app.get("/api/player-leaders/{stat}")
async def get_player_leaders(stat: str, limit: int = 10):
    """MLB season leaders from player_stats table."""
    if stat not in ["rbi","k","h","sb","hr"]: raise HTTPException(400, "Invalid stat")
    if not supabase: raise HTTPException(503, "DB not configured")
    return {"data": supabase.table("player_stats").select(f"name,fantasy_team,mlb_team,position,{stat}")
            .order(stat, desc=True).limit(limit).execute().data}

@app.get("/api/mlb-scores")
async def get_mlb_scores():
    if supabase:
        today_str = date.today().isoformat()
        result = supabase.table("mlb_scores_cache").select("*").eq("game_date", today_str).execute()
        if result.data: return {"data": result.data}
    return {"data": await fetch_mlb_scores()}

@app.get("/api/mlb-news")
async def get_mlb_news():
    return {"data": await fetch_mlb_news()}

@app.get("/api/chirps")
async def get_chirps(limit: int = 50):
    if not supabase: raise HTTPException(503, "DB not configured")
    return {"data": supabase.table("chirps").select("*").order("created_at", desc=True).limit(limit).execute().data}

@app.post("/api/chirps")
async def post_chirp(request: Request):
    if not supabase: raise HTTPException(503, "DB not configured")
    body = await request.json()
    author  = (body.get("author")  or "Anonymous")[:40]
    message = (body.get("message") or "")[:280]
    team    = (body.get("team")    or "")[:60]
    if not message.strip(): raise HTTPException(400, "Empty message")
    return {"ok": True, "data": supabase.table("chirps").insert(
        {"author": author, "team": team, "message": message}).execute().data}

@app.get("/api/history")
async def get_history():
    if not supabase: raise HTTPException(503, "DB not configured")
    return {"data": supabase.table("prize_history").select("*").order("year", desc=True).execute().data}

@app.post("/api/update-standings")
async def update_standings(request: Request):
    """
    Commissioner updates the 2026 standings.
    Accepts a list of {team_name, rbi} entries.
    Each team's value is ADDED to their existing total.
    To set a specific total, use the override flag.
    """
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET: raise HTTPException(403, "Unauthorized")
    if not supabase: raise HTTPException(503, "DB not configured")

    entries  = body.get("entries", [])   # [{team_name, stat, value}]
    override = body.get("override", False)  # if True, set absolute value; if False, add to existing
    today    = date.today().isoformat()
    updated  = 0

    for entry in entries:
        team  = entry.get("team_name", "")
        stat  = entry.get("stat", "rbi")   # rbi, strikeouts, hits, stolen_bases, home_runs
        value = int(entry.get("value", 0))
        if team not in TEAMS: continue
        if stat not in {"rbi","strikeouts","hits","stolen_bases","home_runs"}: continue

        if override:
            new_val = value
        else:
            # Get existing value and add to it
            existing = supabase.table("team_stats").select(stat).eq("team_name", team).eq("season", 2026).execute().data
            cur = existing[0].get(stat, 0) if existing else 0
            new_val = (cur or 0) + value

        supabase.table("team_stats").upsert({
            "team_name": team, "season": 2026,
            stat: new_val, "updated_at": today
        }, on_conflict="team_name,season").execute()
        updated += 1

    return {"ok": True, "updated": updated}

@app.post("/api/sync-now")
async def trigger_sync(request: Request):
    """Legacy endpoint — now just handles direct team stat overrides for admin panel."""
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET: raise HTTPException(403, "Unauthorized")
    if not supabase: raise HTTPException(503, "DB not configured")

    override = body.get("team_override")
    if override:
        team  = override.get("team", "")
        stat  = override.get("stat", "rbi")
        value = int(override.get("value", 0))
        today = date.today().isoformat()
        if team in TEAMS:
            # ADD to existing, never replace
            existing = supabase.table("team_stats").select(stat).eq("team_name", team).eq("season", 2026).execute().data
            cur = existing[0].get(stat, 0) if existing else 0
            new_val = (cur or 0) + value
            supabase.table("team_stats").upsert({
                "team_name": team, "season": 2026,
                stat: new_val, "updated_at": today
            }, on_conflict="team_name,season").execute()
            return {"ok": True, "message": f"{team} {stat}: {cur} + {value} = {new_val}"}

    return {"ok": True, "message": "No action taken"}
