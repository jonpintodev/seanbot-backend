-- ============================================================
-- SeanBot 1.0 — Rusty Kuntz Dynasty League
-- Paste this entire file into Supabase SQL Editor and run it
-- ============================================================

create table if not exists team_stats (
  id            bigserial primary key,
  team_name     text not null,
  season        integer not null default 2026,
  rbi           integer default 0,
  strikeouts    integer default 0,
  hits          integer default 0,
  stolen_bases  integer default 0,
  home_runs     integer default 0,
  updated_at    date default current_date,
  unique (team_name, season)
);

create table if not exists player_stats (
  player_id     text primary key,
  name          text not null,
  mlb_team      text,
  position      text,
  fantasy_team  text,
  rbi           numeric default 0,
  k             numeric default 0,
  h             numeric default 0,
  sb            numeric default 0,
  hr            numeric default 0,
  updated_at    date default current_date
);

create table if not exists mlb_scores_cache (
  game_id       bigint primary key,
  status        text,
  status_detail text,
  inning        text,
  away_team     text,
  away_abbr     text,
  away_score    integer,
  away_winner   boolean default false,
  home_team     text,
  home_abbr     text,
  home_score    integer,
  home_winner   boolean default false,
  venue         text,
  game_date     date default current_date
);

create table if not exists chirps (
  id         bigserial primary key,
  author     text not null,
  team       text,
  message    text not null,
  created_at timestamptz default now()
);

create table if not exists prize_history (
  id           bigserial primary key,
  year         integer not null,
  cat_idx      integer not null,
  cat_name     text not null,
  period       text not null,
  winner_team  text not null,
  total        integer default 0,
  unique (year, cat_idx)
);

-- Row Level Security
alter table team_stats       enable row level security;
alter table player_stats     enable row level security;
alter table mlb_scores_cache enable row level security;
alter table chirps           enable row level security;
alter table prize_history    enable row level security;

-- Public reads
create policy "read team_stats"    on team_stats    for select using (true);
create policy "read player_stats"  on player_stats  for select using (true);
create policy "read mlb_scores"    on mlb_scores_cache for select using (true);
create policy "read chirps"        on chirps        for select using (true);
create policy "read prize_history" on prize_history for select using (true);
create policy "insert chirps"      on chirps        for insert with check (true);

-- Seed all 14 teams
insert into team_stats (team_name, season) values
  ('Possibilities', 2026), ('Yoshi''s Islanders', 2026),
  ('thebigfur', 2026), ('Red Birds', 2026), ('Daddy Yankee', 2026),
  ('¡pinto!', 2026), ('JoanMacias', 2026), ('Xavier', 2026),
  ('Los Jankees', 2026), ('Momin8241', 2026), ('ericliaci', 2026),
  ('Sho Me The Money', 2026), ('Designated Shitters 🧻', 2026),
  ('Arraezed & Hoerny', 2026)
on conflict (team_name, season) do nothing;
