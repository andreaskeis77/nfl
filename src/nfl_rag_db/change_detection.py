from __future__ import annotations

from typing import Iterable

import duckdb


def _qident(name: str) -> str:
    """Quote an identifier for DuckDB/SQL."""
    return '"' + name.replace('"', '""') + '"'


def _concat_ws_expr(cols: Iterable[str], *, sep: str) -> str:
    """
    Build a concat_ws expression that is stable under NULLs.
    NULL is represented explicitly so NULL != '' in hash comparisons.
    """
    parts = [f"coalesce(cast({_qident(c)} as VARCHAR), '<NULL>')" for c in cols]
    return f"concat_ws('{sep}', {', '.join(parts)})"


def compute_change_counts(
    con: duckdb.DuckDBPyConnection,
    *,
    existing_fqn: str,
    incoming_fqn: str,
    key_cols: list[str],
    hash_cols: list[str],
) -> dict[str, int]:
    """
    Compare existing vs incoming by key and full-row hash.

    Returns:
      - incoming_rows
      - existing_rows
      - inserted  (new keys in incoming)
      - updated   (same key, different hash)
      - deleted   (keys missing from incoming)
    """
    if not key_cols:
        raise ValueError("key_cols must not be empty")
    if not hash_cols:
        raise ValueError("hash_cols must not be empty")

    incoming_rows = int(con.execute(f"SELECT COUNT(*) FROM {incoming_fqn};").fetchone()[0])

    try:
        con.execute(f"SELECT 1 FROM {existing_fqn} LIMIT 1;")
    except duckdb.CatalogException:
    # Existing table does not exist yet -> everything is new
        return {
            "incoming_rows": incoming_rows,
            "existing_rows": 0,
            "inserted": incoming_rows,
            "updated": 0,
            "deleted": 0,
    }

    key_expr = _concat_ws_expr(key_cols, sep="|")
    hash_expr = f"md5({_concat_ws_expr(hash_cols, sep='§')})"

    sql = f"""
    WITH incoming AS (
      SELECT {key_expr} AS k, {hash_expr} AS h
      FROM {incoming_fqn}
    ),
    existing AS (
      SELECT {key_expr} AS k, {hash_expr} AS h
      FROM {existing_fqn}
    )
    SELECT
      (SELECT COUNT(*) FROM incoming) AS incoming_rows,
      (SELECT COUNT(*) FROM existing) AS existing_rows,
      (SELECT COUNT(*)
         FROM incoming i
         LEFT JOIN existing e USING(k)
        WHERE e.k IS NULL
      ) AS inserted,
      (SELECT COUNT(*)
         FROM incoming i
         JOIN existing e USING(k)
        WHERE i.h <> e.h
      ) AS updated,
      (SELECT COUNT(*)
         FROM existing e
         LEFT JOIN incoming i USING(k)
        WHERE i.k IS NULL
      ) AS deleted
    ;
    """

    inc, ex, ins, upd, dele = [int(x) for x in con.execute(sql).fetchone()]
    return {
        "incoming_rows": inc,
        "existing_rows": ex,
        "inserted": ins,
        "updated": upd,
        "deleted": dele,
    }