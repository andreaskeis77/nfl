from __future__ import annotations


class FakeCursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeConnection:
    def __init__(
        self,
        *,
        table_rows=None,
        columns=None,
        runs=None,
        table_stats=None,
        season_rows=None,
        week_rows=None,
        game_rows=None,
        game_details=None,
        play_rows=None,
        pbp_season_rows=None,
        pbp_week_rows=None,
        pbp_game_rows=None,
        pbp_game_details=None,
    ):
        self.table_rows = table_rows or {}
        self.columns = columns or {}
        self.runs = runs or []
        self.table_stats = table_stats or []
        self.season_rows = season_rows or []
        self.week_rows = week_rows or {}
        self.game_rows = game_rows or {}
        self.game_details = game_details or {}
        self.play_rows = play_rows or {}
        self.pbp_season_rows = pbp_season_rows or []
        self.pbp_week_rows = pbp_week_rows or {}
        self.pbp_game_rows = pbp_game_rows or {}
        self.pbp_game_details = pbp_game_details or {}
        self.closed = False

    @property
    def tables(self):
        return set(self.table_rows.keys()) | set(self.columns.keys())

    def close(self):
        self.closed = True

    def execute(self, query, params=None):
        q = " ".join(query.lower().split())
        params = params or []

        if "from information_schema.tables" in q and "count(*)" in q:
            schema, table = params
            exists = (schema, table) in self.tables
            return FakeCursor([(1 if exists else 0,)])

        if "select table_schema, table_name" in q and "from information_schema.tables" in q:
            schema = params[0]
            rows = [(s, t) for (s, t) in sorted(self.tables) if s == schema]
            return FakeCursor(rows)

        if q.startswith('select count(*) from "'):
            fqn = q[len('select count(*) from ') :].rstrip(';')
            schema, table = [part.strip('"') for part in fqn.split('.', 1)]
            return FakeCursor([(self.table_rows.get((schema, table), 0),)])

        if "from information_schema.columns" in q:
            schema, table = params
            rows = [(col,) for col in self.columns.get((schema, table), [])]
            return FakeCursor(rows)

        if "from audit.ingest_run" in q and "order by started_at desc" in q:
            limit = int(params[0]) if params else len(self.runs)
            return FakeCursor(self.runs[:limit])

        if "from audit.ingest_table_stat as its" in q and "left join audit.ingest_run as ir" in q:
            limit = int(params[0]) if params else len(self.table_stats)
            return FakeCursor(self.table_stats[:limit])

        if "from audit.ingest_table_stat" in q and "row_number() over" in q:
            limit = int(params[0]) if params else len(self.table_stats)
            trimmed = [row[:6] for row in self.table_stats[:limit]]
            return FakeCursor(trimmed)

        if "from core.game" in q and "group by" in q and "count(distinct week)" in q:
            return FakeCursor(self.season_rows)

        if "from core.pbp" in q and "count(distinct week)" in q:
            return FakeCursor(self.pbp_season_rows)

        if "from core.game" in q and "group by week, season_type" in q:
            season = int(params[0])
            season_type = params[1] if len(params) > 1 else None
            return FakeCursor(self.week_rows.get((season, season_type), []))

        if "from core.game" in q and "group by week" in q and "null as season_type" in q:
            season = int(params[0])
            return FakeCursor(self.week_rows.get((season, None), []))

        if "from core.pbp" in q and "count(distinct game_id) as games" in q and "group by week" in q:
            season = int(params[0])
            return FakeCursor(self.pbp_week_rows.get((season, None), []))

        if "from core.game" in q and "null as plays" in q and "where" in q and "order by" in q:
            if len(params) == 3:
                season = int(params[0])
                week = int(params[1])
                season_type = params[2]
            else:
                season = int(params[0])
                week = int(params[1])
                season_type = None
            return FakeCursor(self.game_rows.get((season, week, season_type), []))

        if "from core.pbp" in q and "count(*) as plays" in q and "where season = ? and week = ?" in q:
            if len(params) == 3:
                season = int(params[0])
                week = int(params[1])
                season_type = params[2]
            else:
                season = int(params[0])
                week = int(params[1])
                season_type = None
            return FakeCursor(self.pbp_game_rows.get((season, week, season_type), []))

        if "from core.game" in q and "where game_id = ?" in q and "limit 1" in q:
            game_id = params[0]
            row = self.game_details.get(game_id)
            return FakeCursor([] if row is None else [row])

        if "from core.pbp" in q and "where game_id = ?" in q and "group by game_id" in q:
            game_id = params[0]
            row = self.pbp_game_details.get(game_id)
            return FakeCursor([] if row is None else [row])

        if "from core.pbp" in q and "where game_id = ?" in q and "order by" in q and "limit ?" in q:
            game_id = params[0]
            limit = int(params[1])
            return FakeCursor(self.play_rows.get(game_id, [])[:limit])

        raise AssertionError(f"Unhandled query: {query}")
