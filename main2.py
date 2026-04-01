"""
RustyStats — Rusty Kuntz Dynasty League
Manual scoring ledger with monthly rollups.
MLB widgets pull live from MLB.com.
No Fantrax automation.
"""

import os, logging
from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("rusty")
ET = ZoneInfo("America/New_York")

app = FastAPI(title="RustyStats")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET","POST"], allow_headers=["*"])

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://haaaaugigaryryqjuztx.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhhYWFhdWdpZ2FyeXJ5cWp1enR4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDY2OTk3NywiZXhwIjoyMDkwMjQ1OTc3fQ.1mPiznawXi-2AtDqfIQET7hjWw5fuk-zRU9aPgQnNDQ")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "RustyKuntz2026!")

TEAMS = [
    "Possibilities", "Yoshi\u2019s Islanders", "thebigfur", "Red Birds",
    "Daddy Yankee", "\u00a1pinto!", "JoanMacias", "Xavier", "Los Jankees",
    "Momin8421", "ericliaci", "Sho Me The Money",
    "Designated Shitters", "Arraezed & Hoerny"
]

VALID_STATS = {"rbi", "strikeouts", "hits", "stolen_bases", "home_runs"}

supabase: Optional[Client] = None
scheduler = AsyncIOScheduler(timezone="America/New_York")


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_month_range(month_str: str):
    y, m = int(month_str[:4]), int(month_str[5:7])
    import calendar
    last = calendar.monthrange(y, m)[1]
    return f"{month_str}-01", f"{month_str}-{last:02d}"


# Prize period ranges — RBI covers Mar+Apr together, others are single months
STAT_DATE_RANGES = {
    "rbi":          ("2026-03-01", "2026-04-30"),
    "strikeouts":   ("2026-05-01", "2026-05-31"),
    "hits":         ("2026-06-01", "2026-06-30"),
    "stolen_bases": ("2026-07-01", "2026-07-31"),
    "home_runs":    ("2026-08-01", "2026-08-31"),
}

def compute_standings(stat: str, month_str: str) -> list:
    if stat in STAT_DATE_RANGES:
        start, end = STAT_DATE_RANGES[stat]
    else:
        start, end = get_month_range(month_str)
    rows = supabase.table("daily_stats") \
        .select("team_name,value") \
        .eq("stat", stat) \
        .gte("score_date", start) \
        .lte("score_date", end) \
        .execute().data

    totals = {t: 0 for t in TEAMS}
    for r in rows:
        t = r["team_name"]
        if t in totals:
            totals[t] += (r["value"] or 0)

    return sorted(
        [{"team_name": t, "total": v} for t, v in totals.items()],
        key=lambda x: -x["total"]
    )


def compute_daily(stat: str, score_date: str) -> list:
    rows = supabase.table("daily_stats") \
        .select("team_name,value") \
        .eq("stat", stat) \
        .eq("score_date", score_date) \
        .execute().data

    have = {r["team_name"]: r["value"] for r in rows}
    result = []
    for t in TEAMS:
        result.append({"team_name": t, "total": have.get(t, 0)})
    return sorted(result, key=lambda x: -x["total"])


# ── MLB helpers ───────────────────────────────────────────────────────────────

async def get_todays_games() -> list:
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f"https://statsapi.mlb.com/api/v1/schedule"
                f"?sportId=1&date={date.today()}&hydrate=linescore,team,decisions"
            )
        return r.json().get("dates", [{}])[0].get("games", [])
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
            "game_id":       g["gamePk"],
            "status":        g["status"]["abstractGameState"],
            "status_detail": g["status"]["detailedState"],
            "inning":        g.get("linescore", {}).get("currentInningOrdinal", ""),
            "away_team":     away["team"]["name"],
            "away_abbr":     away["team"].get("abbreviation", ""),
            "away_score":    away.get("score"),
            "away_winner":   away.get("isWinner", False),
            "home_team":     home["team"]["name"],
            "home_abbr":     home["team"].get("abbreviation", ""),
            "home_score":    home.get("score"),
            "home_winner":   home.get("isWinner", False),
            "venue":         g.get("venue", {}).get("name", ""),
            "game_date":     today,
        })
    return out


async def fetch_mlb_news() -> list:
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get("https://api.rss2json.com/v1/api.json?rss_url=https://www.mlb.com/feeds/news/rss.xml")
        return [
            {"title": i.get("title",""), "link": i.get("link",""), "pub_date": i.get("pubDate",""),
             "description": (i.get("description","") or "")[:200]}
            for i in r.json().get("items", [])[:15]
        ]
    except:
        return []


async def cache_mlb_scores():
    if not supabase:
        return
    scores    = await fetch_mlb_scores()
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
    logger.info("RustyStats online")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ── Public endpoints ──────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"app": "RustyStats", "status": "online"}


@app.get("/api/status")
async def get_status():
    games = await get_todays_games()
    return {
        "games_today": len(games),
        "games_final": sum(1 for g in games if g["status"]["abstractGameState"] == "Final"),
        "games_live":  sum(1 for g in games if g["status"]["abstractGameState"] == "Live"),
        "time_et":     datetime.now(ET).strftime("%I:%M %p ET"),
    }


@app.get("/api/standings")
async def get_standings(stat: str = "rbi", month: str = None):
    """Monthly leaderboard — sum of all daily_stats rows for the month."""
    if not supabase:
        raise HTTPException(503, "DB not configured")
    if stat not in VALID_STATS:
        raise HTTPException(400, "Invalid stat")
    if not month:
        month = date.today().isoformat()[:7]
    return {"stat": stat, "month": month, "data": compute_standings(stat, month)}


@app.get("/api/daily")
async def get_daily(stat: str = "rbi", score_date: str = None):
    """Single-day leaderboard for the given date."""
    if not supabase:
        raise HTTPException(503, "DB not configured")
    if stat not in VALID_STATS:
        raise HTTPException(400, "Invalid stat")
    if not score_date:
        score_date = date.today().isoformat()
    return {"stat": stat, "score_date": score_date, "data": compute_daily(stat, score_date)}


@app.get("/api/daily-entries")
async def get_daily_entries(score_date: str, stat: str = "rbi"):
    """Load saved entries for a specific date — used by admin to pre-fill the form."""
    if not supabase:
        raise HTTPException(503, "DB not configured")
    rows = supabase.table("daily_stats") \
        .select("team_name,value") \
        .eq("score_date", score_date) \
        .eq("stat", stat) \
        .execute().data
    return {"score_date": score_date, "stat": stat, "data": rows}


@app.get("/api/saved-dates")
async def get_saved_dates(stat: str = "rbi"):
    """Return all dates that have entries for this stat — for the calendar view."""
    if not supabase:
        raise HTTPException(503, "DB not configured")
    rows = supabase.table("daily_stats") \
        .select("score_date") \
        .eq("stat", stat) \
        .order("score_date", desc=True) \
        .execute().data
    dates = sorted(set(r["score_date"] for r in rows), reverse=True)
    return {"stat": stat, "dates": dates}


@app.get("/api/mlb-scores")
async def get_mlb_scores():
    if supabase:
        today_str = date.today().isoformat()
        result    = supabase.table("mlb_scores_cache").select("*").eq("game_date", today_str).execute()
        if result.data:
            return {"data": result.data}
    return {"data": await fetch_mlb_scores()}


@app.get("/api/mlb-news")
async def get_mlb_news():
    return {"data": await fetch_mlb_news()}


@app.get("/api/mlb-leaders")
async def get_mlb_leaders(stat: str = "homeRuns", limit: int = 10):
    """Live MLB season leaders from MLB Stats API."""
    try:
        group  = "pitching" if stat in ("strikeOuts", "era", "wins") else "hitting"
        season = date.today().year
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f"https://statsapi.mlb.com/api/v1/stats/leaders"
                f"?leaderCategories={stat}&season={season}&sportId=1"
                f"&limit={limit}&statGroup={group}"
            )
        leaders = r.json().get("leagueLeaders", [{}])[0].get("leaders", [])
        return {
            "stat": stat,
            "data": [
                {
                    "rank":  l.get("rank"),
                    "name":  l.get("person", {}).get("fullName", ""),
                    "team":  l.get("team", {}).get("abbreviation", ""),
                    "value": l.get("value"),
                }
                for l in leaders
            ],
        }
    except Exception as e:
        logger.error(f"MLB leaders error: {e}")
        return {"stat": stat, "data": []}


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
    body    = await request.json()
    author  = (body.get("author")  or "Anonymous")[:40]
    message = (body.get("message") or "")[:280]
    team    = (body.get("team")    or "")[:60]
    if not message.strip():
        raise HTTPException(400, "Empty message")
    return {
        "ok":   True,
        "data": supabase.table("chirps").insert(
            {"author": author, "team": team, "message": message}
        ).execute().data,
    }


# ── Commissioner ──────────────────────────────────────────────────────────────

@app.post("/api/save-day")
async def save_day(request: Request):
    """
    Save or update a full day of stats.
    One row per team+date+stat. Re-saving a date overwrites that day.
    Returns updated monthly standings immediately.

    Body: { secret, score_date, stat, note?, entries: [{team_name, value}] }
    """
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(403, "Unauthorized")
    if not supabase:
        raise HTTPException(503, "DB not configured")

    score_date = body.get("score_date", "")
    stat       = body.get("stat", "rbi")
    note       = body.get("note", "")
    entries    = body.get("entries", [])

    if not score_date:
        raise HTTPException(400, "score_date required")
    if stat not in VALID_STATS:
        raise HTTPException(400, f"Invalid stat: {stat}")

    today = date.today().isoformat()
    saved = 0

    for entry in entries:
        team  = entry.get("team_name", "")
        value = entry.get("value")
        if team not in TEAMS or value is None:
            continue
        value = int(value)
        if value < 0:
            continue

        supabase.table("daily_stats").upsert(
            {
                "score_date": score_date,
                "team_name":  team,
                "stat":       stat,
                "value":      value,
                "updated_at": today,
            },
            on_conflict="score_date,team_name,stat"
        ).execute()
        saved += 1

    month_str = score_date[:7]
    standings = compute_standings(stat, month_str)
    logger.info(f"save-day {score_date} {stat}: {saved} entries. Leader: {standings[0]['team_name']} {standings[0]['total']}")
    return {"ok": True, "saved": saved, "standings": standings}
