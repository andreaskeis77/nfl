from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from nfl_rag_db.db import connect, default_db_path
from nfl_rag_db.webapp.queries import (
    dashboard_payload,
    game_detail_payload,
    latest_runs,
    list_tables,
    list_players,
    list_teams,
    player_detail_payload,
    season_detail_payload,
    season_week_overview,
    team_detail_payload,
    week_detail_payload,
)

APP_TITLE = "nfl-rag-db"

app = FastAPI(
    title=APP_TITLE,
    version="0.7.0",
    docs_url="/docs",
    redoc_url=None,
)

templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


def resolve_db_path(db: str | None = None) -> str:
    if db:
        return db
    env_path = os.getenv("NFL_DB_PATH")
    if env_path:
        return env_path
    return str(default_db_path())


def _close_quietly(con: Any) -> None:
    try:
        con.close()
    except Exception:
        pass


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/dashboard", status_code=307)


@app.get("/health")
def health(db: str | None = Query(default=None)) -> dict[str, Any]:
    db_path = resolve_db_path(db)
    return {"ok": True, "db_path": db_path}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: str | None = Query(default=None)) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        payload = dashboard_payload(con, db_path=db_path)
        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "title": "Dashboard",
                "request": request,
                "db_path": db_path,
                **payload,
            },
        )
    finally:
        _close_quietly(con)


@app.get("/datasets", response_class=HTMLResponse)
def datasets(request: Request, db: str | None = Query(default=None)) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        payload = dashboard_payload(con, db_path=db_path)
        tables = list_tables(con)
        return templates.TemplateResponse(
            request,
            "datasets.html",
            {
                "title": "Datasets",
                "request": request,
                "db_path": db_path,
                "tables": tables,
                "coverage": payload["coverage"],
                "latest_table_stats": payload["latest_table_stats"],
            },
        )
    finally:
        _close_quietly(con)


@app.get("/runs", response_class=HTMLResponse)
def runs(request: Request, db: str | None = Query(default=None)) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        rows = latest_runs(con, limit=50)
        return templates.TemplateResponse(
            request,
            "runs.html",
            {
                "title": "Runs",
                "request": request,
                "db_path": db_path,
                "runs": rows,
            },
        )
    finally:
        _close_quietly(con)


@app.get("/seasons", response_class=HTMLResponse)
def seasons(request: Request, db: str | None = Query(default=None)) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        rows = season_week_overview(con)
        return templates.TemplateResponse(
            request,
            "seasons.html",
            {
                "title": "Seasons",
                "request": request,
                "db_path": db_path,
                "seasons": rows,
            },
        )
    finally:
        _close_quietly(con)


@app.get("/seasons/{season}", response_class=HTMLResponse)
def season_detail(request: Request, season: int, db: str | None = Query(default=None)) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        payload = season_detail_payload(con, season=season)
        if payload["summary"]["games_visible"] == 0 and payload["summary"]["weeks_visible"] == 0:
            raise HTTPException(status_code=404, detail=f"Season {season} not found")
        return templates.TemplateResponse(
            request,
            "season_detail.html",
            {
                "title": f"Season {season}",
                "request": request,
                "db_path": db_path,
                **payload,
            },
        )
    finally:
        _close_quietly(con)


@app.get("/seasons/{season}/weeks/{week}", response_class=HTMLResponse)
def week_detail(
    request: Request,
    season: int,
    week: int,
    db: str | None = Query(default=None),
) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        payload = week_detail_payload(con, season=season, week=week)
        if payload["summary"]["games_visible"] == 0:
            raise HTTPException(status_code=404, detail=f"Season {season} week {week} not found")
        return templates.TemplateResponse(
            request,
            "week_detail.html",
            {
                "title": f"Season {season} / Week {week}",
                "request": request,
                "db_path": db_path,
                **payload,
            },
        )
    finally:
        _close_quietly(con)


@app.get("/games/{game_id}", response_class=HTMLResponse)
def game_detail(request: Request, game_id: str, db: str | None = Query(default=None)) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        payload = game_detail_payload(con, game_id=game_id)
        if not payload["game"]:
            raise HTTPException(status_code=404, detail=f"Game {game_id} not found")
        return templates.TemplateResponse(
            request,
            "game_detail.html",
            {
                "title": payload["title"],
                "request": request,
                "db_path": db_path,
                **payload,
            },
        )
    finally:
        _close_quietly(con)


@app.get("/teams", response_class=HTMLResponse)
def teams(request: Request, db: str | None = Query(default=None), q: str | None = Query(default=None)) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        rows = list_teams(con, search=q)
        return templates.TemplateResponse(
            request,
            "teams.html",
            {
                "title": "Teams",
                "request": request,
                "db_path": db_path,
                "teams": rows,
                "query": q or "",
            },
        )
    finally:
        _close_quietly(con)


@app.get("/teams/{team_code}", response_class=HTMLResponse)
def team_detail(request: Request, team_code: str, db: str | None = Query(default=None)) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        payload = team_detail_payload(con, team_code=team_code)
        if not payload["team"]:
            raise HTTPException(status_code=404, detail=f"Team {team_code} not found")
        return templates.TemplateResponse(
            request,
            "team_detail.html",
            {
                "title": f"Team {payload['team'].get('team_code', team_code)}",
                "request": request,
                "db_path": db_path,
                **payload,
            },
        )
    finally:
        _close_quietly(con)


@app.get("/players", response_class=HTMLResponse)
def players(
    request: Request,
    db: str | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        rows = list_players(con, search=q, limit=limit)
        return templates.TemplateResponse(
            request,
            "players.html",
            {
                "title": "Players",
                "request": request,
                "db_path": db_path,
                "players": rows,
                "query": q or "",
                "limit": limit,
            },
        )
    finally:
        _close_quietly(con)


@app.get("/players/{player_id}", response_class=HTMLResponse)
def player_detail(request: Request, player_id: str, db: str | None = Query(default=None)) -> HTMLResponse:
    db_path = resolve_db_path(db)
    con = connect(db_path)
    try:
        payload = player_detail_payload(con, player_id=player_id)
        if not payload["player"]:
            raise HTTPException(status_code=404, detail=f"Player {player_id} not found")
        return templates.TemplateResponse(
            request,
            "player_detail.html",
            {
                "title": f"Player {payload['player'].get('player_name', player_id)}",
                "request": request,
                "db_path": db_path,
                **payload,
            },
        )
    finally:
        _close_quietly(con)
