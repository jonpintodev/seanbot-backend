"""
SeanBot 1.0 — Rusty Kuntz Dynasty League
Monthly prize tracker: started-lineup-only daily accounting.

AUTOMATION STATUS:
  ✅ Fantrax login + API access
  ✅ getPlayerIds -> real player names + MLB teams
  ✅ MLB Stats API game log -> per-player stat for a specific date
  ✅ Supabase: scoring_days, daily_lineups, daily_player_stats, monthly_totals
  ✅ Monthly recomputation from stored daily rows (active starters only)
  ✅ Nightly sync pipeline — IMPLEMENTED, slot validation PENDING tonight's debug logs

  ⚠️  PENDING VALIDATION: is_active slot logic not confirmed until debug logs
      show real Fantrax slot field values from a completed scoring day.

  ❌ Historical dates (Mar 25-28): getTeamRosters has no date param — returns
      current state only. No confirmed historical lineup endpoint exists.
      These dates require commissioner manual entry via /api/manual-entry,
      stored with source='manual' and clearly labeled.

Stat categories: Mar/Apr=RBI | May=K | Jun=H | Jul=SB | Aug=HR | Sep=manual
"""

import os, asyncio, calendar, logging
from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import Optional
from collections import defaultdict

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

LEAGUE_START_DATE = date(2026, 3, 27)
INACTIVE_SLOTS = {"BN","IL","IL+","NA","MINORS","RES","IR","INJURED_RESERVE","RESERVE"}

def active_stat_for_month(m: int) -> str:
    return {3:"rbi",4:"rbi",5:"k",6:"h",7:"sb",8:"hr"}.get(m,"rbi")

TEAMS = [
    "Possibilities","Yoshi\u2019s Islanders","thebigfur","Red Birds",
    "Daddy Yankee","\u00a1pinto!","JoanMacias","Xavier","Los Jankees",
    "Momin8421","ericliaci","Sho Me The Money",
    "Designated Shitters","Arraezed & Hoerny"
]

supabase: Optional[Client] = None
sync_state = {"last_sync_date":None,"syncing":False,"last_sync_time":None,"status":"idle"}
COOKIES = {}
FANTRAX_LOGGED_IN = False
MLBAM_CACHE: dict = {}

# ── Fantrax auth ──────────────────────────────────────────────────────────────

async def fantrax_login() -> bool:
    global FANTRAX_LOGGED_IN, COOKIES
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            resp = await c.post(
                "https://www.fantrax.com/fxea/general/login",
                json={"email":FANTRAX_USERNAME,"password":FANTRAX_PASSWORD},
                headers={"User-Agent":"Mozilla/5.0","Content-Type":"application/json"}
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
    h = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
         "Content-Type":"application/json","Accept":"application/json","Referer":"https://www.fantrax.com/"}
    if COOKIES:
        h["Cookie"] = "; ".join(f"{k}={v}" for k,v in COOKIES.items())
    return h

async def fantrax_get(path: str, params: dict = None) -> Optional[dict]:
    try:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as c:
            resp = await c.get(f"https://www.fantrax.com{path}", params=params, headers=fantrax_headers())
        for k,v in resp.cookies.items(): COOKIES[k] = v
        if resp.status_code == 200: return resp.json()
        logger.warning(f"Fantrax {path} returned {resp.status_code}")
    except Exception as e:
        logger.error(f"Fantrax GET error {path}: {e}")
    return None

# ── Team matching ─────────────────────────────────────────────────────────────

def norm_team(s: str) -> str:
    return s.replace("\u2019","'").replace("\u2018","'").lower().strip()

def match_team(name: str) -> Optional[str]:
    if name in TEAMS: return name
    for t in TEAMS:
        if norm_team(t) == norm_team(name): return t
    return None

# ── STEP 1: Fetch lineups with full debug logging ─────────────────────────────
# NOTE: getTeamRosters returns CURRENT state only — not historical.
# Historical dates must use commissioner manual entry.

async def fetch_lineups_for_date(score_date: str) -> list[dict]:
    roster_data = await fantrax_get("/fxea/general/getTeamRosters", {"leagueId": FANTRAX_LEAGUE_ID})
    if not roster_data or not isinstance(roster_data.get("rosters"), dict):
        logger.error("getTeamRosters returned no data")
        return []

    player_ids_data = await fantrax_get("/fxea/general/getPlayerIds", {"sport": "MLB"})
    player_info = {}
    if isinstance(player_ids_data, dict):
        for pid, pdata in player_ids_data.items():
            if isinstance(pdata, dict):
                raw = pdata.get("name","")
                if raw and "," in raw:
                    parts = raw.split(",",1)
                    name = f"{parts[1].strip()} {parts[0].strip()}"
                else:
                    name = raw
                player_info[str(pid)] = {"name": name, "team": pdata.get("team",""), "position": pdata.get("position","")}
        del player_ids_data

    rows = []
    slot_field_used = None

    for team_id, team_info in roster_data["rosters"].items():
        if not isinstance(team_info, dict): continue
        matched = match_team(team_info.get("teamName",""))
        if not matched:
            logger.warning(f"No team match: '{team_info.get('teamName','')}'")
            continue
        for player in team_info.get("rosterItems", []):
            if not isinstance(player, dict): continue
            pid = str(player.get("id",""))
            if not pid: continue
            lineup_status = player.get("lineupStatus")
            slot_val      = player.get("slot")
            slot_name     = player.get("slotName")
            status_val    = player.get("status")
            if lineup_status:
                slot = str(lineup_status).upper().strip()
                slot_field_used = slot_field_used or "lineupStatus"
            elif slot_val:
                slot = str(slot_val).upper().strip()
                slot_field_used = slot_field_used or "slot"
            elif slot_name:
                slot = str(slot_name).upper().strip()
                slot_field_used = slot_field_used or "slotName"
            elif status_val:
                slot = str(status_val).upper().strip()
                slot_field_used = slot_field_used or "status"
            else:
                slot = "UNKNOWN"
            is_active = slot not in INACTIVE_SLOTS and slot != "UNKNOWN"
            pinfo = player_info.get(pid, {})
            rows.append({
                "fantrax_player_id": pid,
                "player_name": pinfo.get("name", pid),
                "fantasy_team": matched,
                "mlb_team": pinfo.get("team",""),
                "slot": slot,
                "is_active": is_active,
                "score_date": score_date,
                "updated_at": score_date,
            })

    # DEBUG: raw slot fields from first team
    logger.info(f"DEBUG slot_field_used: {slot_field_used!r}")
    for team_id, team_info in roster_data["rosters"].items():
        if isinstance(team_info, dict) and team_info.get("rosterItems"):
            sample = team_info["rosterItems"][0]
            logger.info(f"DEBUG rosterItem keys: {list(sample.keys())}")
            for p in team_info["rosterItems"][:6]:
                logger.info(
                    f"DEBUG player {p.get('id')} "
                    f"lineupStatus={p.get('lineupStatus')!r} "
                    f"slot={p.get('slot')!r} "
                    f"slotName={p.get('slotName')!r} "
                    f"status={p.get('status')!r}"
                )
            break

    # DEBUG: per-team active/inactive counts
    team_counts = defaultdict(lambda: {"active":0,"inactive":0,"slots":set()})
    for r in rows:
        t = r["fantasy_team"]
        if r["is_active"]: team_counts[t]["active"] += 1
        else: team_counts[t]["inactive"] += 1
        team_counts[t]["slots"].add(r["slot"])
    for team, counts in list(team_counts.items())[:4]:
        logger.info(f"DEBUG {team}: {counts['active']} active, {counts['inactive']} inactive, slots={counts['slots']}")

    total_active = sum(1 for r in rows if r["is_active"])
    logger.info(
        f"Lineups: {len(rows)} total, {total_active} active starters. "
        f"slot_field={slot_field_used!r} — "
        f"{'⚠️ SLOT UNKNOWN — is_active unreliable' if not slot_field_used else 'slot detected'}"
    )
    return rows

# ── STEP 2: MLB Stats API — player stat for one date ─────────────────────────

async def get_mlbam_id(name: str, mlb_team: str = "") -> Optional[int]:
    cache_key = f"{name}|{mlb_team}"
    if cache_key in MLBAM_CACHE: return MLBAM_CACHE[cache_key]
    try:
        parts = name.strip().split()
        last  = parts[-1] if parts else ""
        first = parts[0] if len(parts) > 1 else ""
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(f"https://statsapi.mlb.com/api/v1/people/search?names={last}&sportId=1")
        people = resp.json().get("people", [])
        name_lower = name.lower()
        for p in people:
            if p.get("fullName","").lower() == name_lower and mlb_team and p.get("currentTeam",{}).get("abbreviation","").upper() == mlb_team.upper():
                mid = p.get("id")
                if mid: MLBAM_CACHE[cache_key] = mid; return mid
        for p in people:
            if p.get("fullName","").lower() == name_lower:
                mid = p.get("id")
                if mid: MLBAM_CACHE[cache_key] = mid; return mid
        if first and mlb_team:
            for p in people:
                full = p.get("fullName","").lower()
                abbr = p.get("currentTeam",{}).get("abbreviation","").upper()
                if first.lower() in full and last.lower() in full and abbr == mlb_team.upper():
                    mid = p.get("id")
                    if mid: MLBAM_CACHE[cache_key] = mid; return mid
        if first:
            for p in people:
                full = p.get("fullName","").lower()
                if first.lower() in full and last.lower() in full:
                    mid = p.get("id")
                    if mid: MLBAM_CACHE[cache_key] = mid; return mid
    except Exception as e:
        logger.error(f"MLBAM lookup error {name}: {e}")
    return None

async def fetch_player_stat_for_date(mlbam_id: int, stat_type: str, score_date: str) -> int:
    year     = score_date[:4]
    group    = "pitching" if stat_type == "k" else "hitting"
    stat_key = {"rbi":"rbi","h":"hits","hr":"homeRuns","sb":"stolenBases","k":"strikeOuts"}[stat_type]
    url = (f"https://statsapi.mlb.com/api/v1/people/{mlbam_id}/stats"
           f"?stats=gameLog&season={year}&group={group}&startDate={score_date}&endDate={score_date}")
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(url)
        total = 0
        for sg in resp.json().get("stats",[]):
            for split in sg.get("splits",[]):
                total += int(split.get("stat",{}).get(stat_key,0) or 0)
        return total
    except Exception as e:
        logger.error(f"MLB stat error mlbam={mlbam_id} {stat_type} {score_date}: {e}")
        return 0

# ── STEP 3: Recompute monthly totals from stored rows ────────────────────────
# Joins daily_lineups (is_active=true) with daily_player_stats.
# Also includes manual_score_entries (source='manual'), clearly separate.

def recompute_monthly_totals(month_str: str, stat_type: str) -> dict:
    if not supabase: return {t:0 for t in TEAMS}
    year, mon = int(month_str[:4]), int(month_str[5:7])
    last_day  = calendar.monthrange(year, mon)[1]
    m_start   = f"{month_str}-01"
    m_end     = f"{month_str}-{last_day:02d}"

    lineups = supabase.table("daily_lineups").select("score_date,fantrax_player_id,fantasy_team") \
        .eq("is_active", True).gte("score_date", m_start).lte("score_date", m_end).execute().data
    stats = supabase.table("daily_player_stats").select("score_date,fantrax_player_id,value") \
        .eq("stat_type", stat_type).gte("score_date", m_start).lte("score_date", m_end).execute().data
    stat_lookup = {(r["score_date"], r["fantrax_player_id"]): r["value"] for r in stats}

    totals = {t:0 for t in TEAMS}
    for row in lineups:
        t = row["fantasy_team"]
        if t in totals:
            totals[t] += stat_lookup.get((row["score_date"], row["fantrax_player_id"]), 0)

    # Manual entries — source='manual', stored separately, clearly labeled
    try:
        manual_rows = supabase.table("manual_score_entries").select("fantasy_team,value") \
            .eq("stat_type", stat_type).gte("score_date", m_start).lte("score_date", m_end).execute().data
        for row in manual_rows:
            t = row["fantasy_team"]
            if t in totals: totals[t] += row["value"]
        logger.info(f"Recompute {month_str} {stat_type}: {len(lineups)} auto rows + {len(manual_rows)} manual rows")
    except Exception as e:
        logger.warning(f"manual_score_entries query failed (table may not exist yet): {e}")

    logger.info(f"Top 3: {sorted(totals.items(), key=lambda x:-x[1])[:3]}")
    return totals

# ── Main nightly sync ─────────────────────────────────────────────────────────
# STATUS: Implemented. Slot-field validation PENDING — not confirmed
# correct until debug logs from a completed scoring day are reviewed.

async def run_fantrax_sync():
    global FANTRAX_LOGGED_IN
    if sync_state["syncing"]: logger.info("Already syncing"); return
    sync_state["syncing"] = True
    sync_state["status"]  = "syncing"
    logger.info("SeanBot sync starting...")
    try:
        if not FANTRAX_LOGGED_IN:
            ok = await fantrax_login()
            if not ok: sync_state["status"] = "login_failed"; return

        today_str = date.today().isoformat()
        stat_type = active_stat_for_month(date.today().month)
        month_str = today_str[:7]
        logger.info(f"Sync date: {today_str} | Active stat: {stat_type}")

        lineup_rows = await fetch_lineups_for_date(today_str)
        if not lineup_rows:
            logger.error("No lineup rows — aborting"); sync_state["status"] = "error"; return

        active_rows = [r for r in lineup_rows if r["is_active"]]
        logger.info(f"Active starters: {len(active_rows)} / {len(lineup_rows)} total — NOTE: pending slot validation")

        if supabase:
            supabase.table("scoring_days").upsert(
                {"score_date":today_str,"season":2026,"processed":False,"updated_at":today_str},
                on_conflict="score_date").execute()
            supabase.table("daily_lineups").upsert(
                lineup_rows, on_conflict="score_date,fantasy_team,fantrax_player_id").execute()

        # Write season stats to player_stats for individual leaders display
        # This is separate from prize accounting — just for the leaderboard sidebar
        logger.info(f"Writing season stats to player_stats for {len(lineup_rows)} players...")
        for row in lineup_rows:
            name = row["player_name"]; mlb_team = row["mlb_team"]; pid = row["fantrax_player_id"]
            if not name or name == pid: continue
            mlbam_id = await get_mlbam_id(name, mlb_team)
            if not mlbam_id: continue
            # Get season totals for display
            try:
                year = date.today().year
                url = (f"https://statsapi.mlb.com/api/v1/people/{mlbam_id}/stats"
                       f"?stats=season&season={year}&group=hitting,pitching")
                async with httpx.AsyncClient(timeout=10) as c:
                    resp = await c.get(url)
                season_stats = {"rbi":0,"h":0,"hr":0,"sb":0,"k":0}
                for sg in resp.json().get("stats",[]):
                    splits = sg.get("splits",[])
                    if not splits: continue
                    s = splits[0].get("stat",{})
                    grp = sg.get("group",{}).get("displayName","")
                    if grp == "hitting":
                        season_stats["rbi"] = int(s.get("rbi",0) or 0)
                        season_stats["h"]   = int(s.get("hits",0) or 0)
                        season_stats["hr"]  = int(s.get("homeRuns",0) or 0)
                        season_stats["sb"]  = int(s.get("stolenBases",0) or 0)
                    elif grp == "pitching":
                        season_stats["k"]   = int(s.get("strikeOuts",0) or 0)
                if supabase:
                    supabase.table("player_stats").upsert({
                        "player_id": pid, "name": name,
                        "mlb_team": mlb_team, "position": row.get("slot",""),
                        "fantasy_team": row["fantasy_team"],
                        "rbi": season_stats["rbi"], "k": season_stats["k"],
                        "h": season_stats["h"], "sb": season_stats["sb"],
                        "hr": season_stats["hr"], "updated_at": today_str,
                    }, on_conflict="player_id").execute()
            except Exception as e:
                logger.error(f"Season stats error for {name}: {e}")

        logger.info(f"Fetching {stat_type} for {len(active_rows)} active players on {today_str}...")
        stat_rows   = []
        api_calls   = 0
        sample_rows = []

        for row in active_rows:
            name = row["player_name"]; mlb_team = row["mlb_team"]; pid = row["fantrax_player_id"]
            if not name or name == pid: continue
            mlbam_id = await get_mlbam_id(name, mlb_team)
            if not mlbam_id: continue
            value = await fetch_player_stat_for_date(mlbam_id, stat_type, today_str)
            api_calls += 1
            stat_rows.append({"score_date":today_str,"fantrax_player_id":pid,"player_name":name,
                               "stat_type":stat_type,"value":value,"updated_at":today_str})
            if len(sample_rows) < 10:
                sample_rows.append(f"{row['fantasy_team']} | {name} | slot={row['slot']} | {stat_type}={value}")
            if value > 0:
                logger.info(f"  {name} ({mlb_team}) {stat_type}={value}")

        logger.info(f"DEBUG sample joined rows:")
        for s in sample_rows: logger.info(f"  {s}")
        logger.info(f"Stats: {api_calls} API calls, {sum(r['value'] for r in stat_rows)} total {stat_type}")

        if supabase and stat_rows:
            supabase.table("daily_player_stats").upsert(
                stat_rows, on_conflict="score_date,fantrax_player_id,stat_type").execute()
            supabase.table("scoring_days").upsert(
                {"score_date":today_str,"season":2026,"processed":True,"updated_at":today_str},
                on_conflict="score_date").execute()

        totals   = recompute_monthly_totals(month_str, stat_type)
        stat_col = {"rbi":"rbi","k":"strikeouts","h":"hits","sb":"stolen_bases","hr":"home_runs"}[stat_type]
        nonzero = {t:v for t,v in totals.items() if v > 0}
        logger.info(f"Sync totals for {month_str}: {nonzero if nonzero else 'ALL ZERO'}")
        if supabase:
            for team_name, total in totals.items():
                supabase.table("monthly_totals").upsert(
                    {"month":month_str,"fantasy_team":team_name,"stat_type":stat_type,
                     "total":total,"updated_at":today_str},
                    on_conflict="month,fantasy_team,stat_type").execute()
                supabase.table("team_stats").upsert(
                    {"team_name":team_name,"season":2026,stat_col:total,"updated_at":today_str},
                    on_conflict="team_name,season").execute()

        now_et = datetime.now(ET).strftime("%b %d %I:%M %p ET")
        sync_state.update({"last_sync_date":today_str,"last_sync_time":now_et,"status":"done"})
        logger.info(f"Sync complete [{now_et}]")
    except Exception as e:
        logger.error(f"Sync error: {e}", exc_info=True)
        sync_state["status"] = "error"
    finally:
        sync_state["syncing"] = False

# ── MLB helpers ───────────────────────────────────────────────────────────────

async def get_todays_games() -> list:
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            resp = await c.get(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date.today()}&hydrate=linescore,team,decisions")
        return resp.json().get("dates",[{}])[0].get("games",[])
    except: return []

async def fetch_mlb_scores() -> list:
    games = await get_todays_games(); today = date.today().isoformat(); out = []
    for g in games:
        away = g["teams"]["away"]; home = g["teams"]["home"]
        out.append({"game_id":g["gamePk"],"status":g["status"]["abstractGameState"],
            "status_detail":g["status"]["detailedState"],"inning":g.get("linescore",{}).get("currentInningOrdinal",""),
            "away_team":away["team"]["name"],"away_abbr":away["team"].get("abbreviation",""),
            "away_score":away.get("score"),"away_winner":away.get("isWinner",False),
            "home_team":home["team"]["name"],"home_abbr":home["team"].get("abbreviation",""),
            "home_score":home.get("score"),"home_winner":home.get("isWinner",False),
            "venue":g.get("venue",{}).get("name",""),"game_date":today})
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
    scores = await fetch_mlb_scores(); today_str = date.today().isoformat()
    if scores:
        supabase.table("mlb_scores_cache").delete().neq("game_date",today_str).execute()
        supabase.table("mlb_scores_cache").upsert(scores,on_conflict="game_id").execute()

async def check_and_sync():
    await cache_mlb_scores()
    games = await get_todays_games()
    if not games: return
    non_final = [g for g in games if g["status"]["abstractGameState"] not in ("Final","Postponed","Cancelled")]
    if not non_final: await run_fantrax_sync()
    else:
        live = sum(1 for g in games if g["status"]["abstractGameState"] == "Live")
        logger.info(f"{live} live, {len(non_final)} not final — waiting")

# ── Scheduler ─────────────────────────────────────────────────────────────────

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

# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"app":"SeanBot 1.0","status":"online","sync":sync_state}

@app.get("/api/status")
async def get_status():
    games = await get_todays_games()
    return {"sync_status":sync_state["status"],"last_sync_time":sync_state["last_sync_time"],
            "last_sync_date":sync_state["last_sync_date"],"games_today":len(games),
            "games_final":sum(1 for g in games if g["status"]["abstractGameState"]=="Final"),
            "games_live":sum(1 for g in games if g["status"]["abstractGameState"]=="Live"),
            "time_et":datetime.now(ET).strftime("%I:%M %p ET")}

@app.get("/api/team-stats")
async def get_team_stats(season: int = 2026):
    if not supabase: raise HTTPException(503,"DB not configured")
    return {"data":supabase.table("team_stats").select("*").eq("season",season).execute().data}

@app.get("/api/monthly-totals")
async def get_monthly_totals(month: str = None):
    """Leaderboard from monthly_totals — includes automated + manual entries."""
    if not supabase: raise HTTPException(503,"DB not configured")
    if not month: month = date.today().isoformat()[:7]
    stat_type = active_stat_for_month(int(month[5:7]))
    rows = supabase.table("monthly_totals").select("fantasy_team,total,stat_type") \
        .eq("month",month).eq("stat_type",stat_type).order("total",desc=True).execute().data
    return {"month":month,"stat_type":stat_type,"data":rows}

@app.get("/api/daily-breakdown")
async def get_daily_breakdown(team: str, date_str: str = None):
    """Commissioner audit: which started players contributed stats on a given date."""
    if not supabase: raise HTTPException(503,"DB not configured")
    if not date_str: date_str = date.today().isoformat()
    stat_type = active_stat_for_month(int(date_str[5:7]))

    lineups = supabase.table("daily_lineups").select("fantrax_player_id,player_name,slot,is_active") \
        .eq("score_date",date_str).eq("fantasy_team",team).execute().data
    pids = [r["fantrax_player_id"] for r in lineups]
    stats_map = {}
    if pids:
        stats = supabase.table("daily_player_stats").select("fantrax_player_id,value") \
            .eq("score_date",date_str).eq("stat_type",stat_type).in_("fantrax_player_id",pids).execute().data
        stats_map = {r["fantrax_player_id"]:r["value"] for r in stats}

    players = [{"player":p["player_name"],"slot":p["slot"],"is_active":p["is_active"],
                "stat_type":stat_type,"value":stats_map.get(p["fantrax_player_id"],0),
                "source":"automated"} for p in lineups]
    players.sort(key=lambda x:(-x["value"],not x["is_active"]))

    manual = []
    try:
        manual = supabase.table("manual_score_entries").select("value,note,entered_at") \
            .eq("score_date",date_str).eq("fantasy_team",team).eq("stat_type",stat_type).execute().data
    except: pass

    auto_total   = sum(p["value"] for p in players if p["is_active"])
    manual_total = sum(r["value"] for r in manual)
    return {"team":team,"date":date_str,"stat_type":stat_type,
            "auto_total":auto_total,"manual_total":manual_total,"total":auto_total+manual_total,
            "manual_entries":manual,"players":players}

@app.post("/api/manual-entry")
async def post_manual_entry(request: Request):
    """
    Commissioner enters official stat total for a team/date where automated
    lineup data is unavailable (e.g. March 25-28).
    Stored with source='manual', clearly labeled. Recomputes monthly totals immediately.
    """
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET: raise HTTPException(403,"Unauthorized")
    if not supabase: raise HTTPException(503,"DB not configured")
    score_date   = body.get("score_date","")
    fantasy_team = body.get("fantasy_team","")
    stat_type    = body.get("stat_type","")
    value        = int(body.get("value",0))
    note         = body.get("note","Commissioner manual entry — lineup data unavailable for this date")
    if not score_date or not fantasy_team or not stat_type:
        raise HTTPException(400,"score_date, fantasy_team, and stat_type required")
    if fantasy_team not in TEAMS: raise HTTPException(400,f"Unknown team: {fantasy_team}")
    if stat_type not in ("rbi","k","h","sb","hr"): raise HTTPException(400,f"Invalid stat_type: {stat_type}")

    row = {"score_date":score_date,"fantasy_team":fantasy_team,"stat_type":stat_type,
           "value":value,"source":"manual","note":note,"entered_at":datetime.now(ET).isoformat()}
    supabase.table("manual_score_entries").upsert(row,on_conflict="score_date,fantasy_team,stat_type").execute()

    # Verify what's now in manual_score_entries for this month
    all_manual = supabase.table("manual_score_entries").select("score_date,fantasy_team,value")         .eq("stat_type", stat_type).gte("score_date", f"{score_date[:7]}-01").execute().data
    logger.info(f"All manual entries for {score_date[:7]}: {all_manual}")

    month_str = score_date[:7]
    totals    = recompute_monthly_totals(month_str, stat_type)
    logger.info(f"Recomputed totals after saving {fantasy_team} {score_date}: { {t:v for t,v in totals.items() if v > 0} }")
    stat_col  = {"rbi":"rbi","k":"strikeouts","h":"hits","sb":"stolen_bases","hr":"home_runs"}[stat_type]
    today_str = date.today().isoformat()
    for team_name, total in totals.items():
        supabase.table("monthly_totals").upsert(
            {"month":month_str,"fantasy_team":team_name,"stat_type":stat_type,"total":total,"updated_at":today_str},
            on_conflict="month,fantasy_team,stat_type").execute()
        supabase.table("team_stats").upsert(
            {"team_name":team_name,"season":2026,stat_col:total,"updated_at":today_str},
            on_conflict="team_name,season").execute()

    logger.info(f"Manual entry: {fantasy_team} {score_date} {stat_type}={value}")
    return {"ok":True,"entry":row,"monthly_totals":totals}

@app.get("/api/manual-entries")
async def get_manual_entries(month: str = None):
    """List all manual entries so commissioner can audit what's been entered."""
    if not supabase: raise HTTPException(503,"DB not configured")
    if not month: month = date.today().isoformat()[:7]
    year, mon = int(month[:4]), int(month[5:7])
    last_day  = calendar.monthrange(year,mon)[1]
    rows = supabase.table("manual_score_entries").select("*") \
        .gte("score_date",f"{month}-01").lte("score_date",f"{month}-{last_day:02d}") \
        .order("score_date").execute().data
    return {"month":month,"entries":rows}

@app.get("/api/player-leaders/{stat}")
async def get_player_leaders(stat: str, limit: int = 10):
    """
    MLB-wide player leaders from player_stats table (season totals).
    This is display-only — not used for prize accounting.
    Prize accounting uses daily_lineups + daily_player_stats.
    """
    if stat not in ["rbi","k","h","sb","hr"]: raise HTTPException(400,"Invalid stat")
    if not supabase: raise HTTPException(503,"DB not configured")
    rows = supabase.table("player_stats").select(
        f"name,fantasy_team,mlb_team,position,{stat}"
    ).order(stat, desc=True).limit(limit).execute().data
    return {"data": rows}

@app.post("/api/refresh-mlb-stats")
async def refresh_mlb_stats(request: Request):
    """
    Refresh player_stats table with current MLB season totals.
    Used only for display (Individual Leaders, Daily Digest).
    NOT used for prize accounting — that uses daily_lineups + daily_player_stats.
    """
    global FANTRAX_LOGGED_IN
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET: raise HTTPException(403,"Unauthorized")
    if not FANTRAX_LOGGED_IN:
        ok = await fantrax_login()
        if not ok: raise HTTPException(503,"Fantrax login failed")

    async def _run():
        roster_data = await fantrax_get("/fxea/general/getTeamRosters",{"leagueId":FANTRAX_LEAGUE_ID})
        player_ids_data = await fantrax_get("/fxea/general/getPlayerIds",{"sport":"MLB"})
        player_info = {}
        if isinstance(player_ids_data, dict):
            for pid, pdata in player_ids_data.items():
                if isinstance(pdata, dict):
                    raw = pdata.get("name","")
                    if raw and "," in raw:
                        parts = raw.split(",",1)
                        name = f"{parts[1].strip()} {parts[0].strip()}"
                    else:
                        name = raw
                    player_info[str(pid)] = {"name":name,"team":pdata.get("team",""),"position":pdata.get("position","")}
            del player_ids_data

        today = date.today().isoformat()
        processed = 0
        if roster_data and isinstance(roster_data.get("rosters"),dict):
            for team_id, team_info in roster_data["rosters"].items():
                if not isinstance(team_info,dict): continue
                matched = match_team(team_info.get("teamName",""))
                if not matched: continue
                for player in team_info.get("rosterItems",[]):
                    if not isinstance(player,dict): continue
                    pid = str(player.get("id",""))
                    if not pid: continue
                    pinfo = player_info.get(pid,{})
                    name = pinfo.get("name","")
                    mlb_team = pinfo.get("team","")
                    position = pinfo.get("position","")
                    if not name or name == pid: continue
                    mlbam_id = await get_mlbam_id(name, mlb_team)
                    if not mlbam_id: continue
                    # Get season totals for display
                    url = (f"https://statsapi.mlb.com/api/v1/people/{mlbam_id}/stats"
                           f"?stats=season&season={date.today().year}&group=hitting,pitching")
                    try:
                        async with httpx.AsyncClient(timeout=10) as c:
                            resp = await c.get(url)
                        result = {"rbi":0,"h":0,"hr":0,"sb":0,"k":0}
                        for sg in resp.json().get("stats",[]):
                            s = sg.get("splits",[{}])[0].get("stat",{}) if sg.get("splits") else {}
                            g = sg.get("group",{}).get("displayName","")
                            if g == "hitting":
                                result["rbi"] = s.get("rbi",0) or 0
                                result["h"]   = s.get("hits",0) or 0
                                result["hr"]  = s.get("homeRuns",0) or 0
                                result["sb"]  = s.get("stolenBases",0) or 0
                            elif g == "pitching":
                                result["k"]   = s.get("strikeOuts",0) or 0
                        if supabase:
                            supabase.table("player_stats").upsert({
                                "player_id":pid,"name":name,"mlb_team":mlb_team,
                                "position":position,"fantasy_team":matched,
                                "rbi":result["rbi"],"k":result["k"],"h":result["h"],
                                "sb":result["sb"],"hr":result["hr"],"updated_at":today
                            }, on_conflict="player_id").execute()
                        processed += 1
                    except Exception as e:
                        logger.error(f"MLB stats error {name}: {e}")
        logger.info(f"MLB stats refresh complete: {processed} players")

    asyncio.create_task(_run())
    return {"ok":True,"message":"MLB stats refresh started (display only)"}


@app.get("/api/mlb-scores")
async def get_mlb_scores():
    if supabase:
        today_str = date.today().isoformat()
        result = supabase.table("mlb_scores_cache").select("*").eq("game_date",today_str).execute()
        if result.data: return {"data":result.data}
    return {"data":await fetch_mlb_scores()}

@app.get("/api/mlb-news")
async def get_mlb_news():
    return {"data":await fetch_mlb_news()}

@app.get("/api/chirps")
async def get_chirps(limit: int = 50):
    if not supabase: raise HTTPException(503,"DB not configured")
    return {"data":supabase.table("chirps").select("*").order("created_at",desc=True).limit(limit).execute().data}

@app.post("/api/chirps")
async def post_chirp(request: Request):
    if not supabase: raise HTTPException(503,"DB not configured")
    body = await request.json()
    author = (body.get("author") or "Anonymous")[:40]
    message = (body.get("message") or "")[:280]
    team = (body.get("team") or "")[:60]
    if not message.strip(): raise HTTPException(400,"Empty message")
    return {"ok":True,"data":supabase.table("chirps").insert({"author":author,"team":team,"message":message}).execute().data}

@app.get("/api/history")
async def get_history():
    if not supabase: raise HTTPException(503,"DB not configured")
    return {"data":supabase.table("prize_history").select("*").order("year",desc=True).execute().data}

@app.post("/api/history")
async def save_history(request: Request):
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET: raise HTTPException(403,"Unauthorized")
    if not supabase: raise HTTPException(503,"DB not configured")
    record = {"year":int(body["year"]),"cat_idx":int(body["cat_idx"]),"cat_name":body["cat_name"],
              "period":body["period"],"winner_team":body["winner_team"],"total":int(body["total"])}
    return {"ok":True,"data":supabase.table("prize_history").upsert(record,on_conflict="year,cat_idx").execute().data}

@app.post("/api/sync-now")
async def trigger_sync(request: Request):
    global FANTRAX_LOGGED_IN
    body = await request.json()
    if body.get("secret") != ADMIN_SECRET: raise HTTPException(403,"Unauthorized")
    override = body.get("team_override")
    if override and supabase:
        team      = override.get("team")
        stat      = override.get("stat")
        val       = int(override.get("value", 0))
        score_date = override.get("score_date", date.today().isoformat())
        # Map team_stats col names back to stat_type keys
        stat_map  = {"rbi":"rbi","strikeouts":"k","hits":"h","stolen_bases":"sb","home_runs":"hr"}
        stat_type = stat_map.get(stat, stat)
        if stat_type in {"rbi","k","h","sb","hr"} and team:
            # Write to manual_score_entries so it accumulates correctly
            supabase.table("manual_score_entries").upsert({
                "score_date": score_date,
                "fantasy_team": team,
                "stat_type": stat_type,
                "value": val,
                "source": "manual",
                "note": f"Admin override for {score_date}",
                "entered_at": datetime.now(ET).isoformat(),
            }, on_conflict="score_date,fantasy_team,stat_type").execute()
            # Recompute monthly totals including all manual entries
            month_str = score_date[:7]
            totals    = recompute_monthly_totals(month_str, stat_type)
            stat_col  = {"rbi":"rbi","k":"strikeouts","h":"hits","sb":"stolen_bases","hr":"home_runs"}[stat_type]
            today_str = date.today().isoformat()
            for team_name, total in totals.items():
                supabase.table("monthly_totals").upsert({
                    "month":month_str,"fantasy_team":team_name,
                    "stat_type":stat_type,"total":total,"updated_at":today_str
                }, on_conflict="month,fantasy_team,stat_type").execute()
                supabase.table("team_stats").upsert({
                    "team_name":team_name,"season":2026,
                    stat_col:total,"updated_at":today_str
                }, on_conflict="team_name,season").execute()
            return {"ok":True,"message":f"Saved {team} {score_date} {stat_type}={val}, monthly total updated"}
    sync_state["last_sync_date"] = None
    FANTRAX_LOGGED_IN = False
    asyncio.create_task(run_fantrax_sync())
    return {"ok":True,"message":"Sync triggered"}
