# PROJECT_STATE

Stand: 2026-03-07  
Status: aktiv, frühe belastbare Plattformbasis vorhanden, aber noch keine fertige Produktoberfläche

## 1. Executive Summary

Das Projekt ist bereits klar als lokale NFL-Datenplattform erkennbar.  
Es hat die Phase eines reinen Scaffolds überschritten, ist aber noch nicht in einer Form, in der die Daten für den Nutzer sichtbar und komfortabel nutzbar sind.

Stärken des aktuellen Stands:

- klare Struktur unter `src/`, `tests/`, `tools/`, `docs/`
- DuckDB-first ist per ADR gesetzt
- Audit-/Run-Registry existiert
- Player-Stats-Ingestion ist bereits substanziell
- Snapshot-/Handoff-Tooling ist vorhanden
- erster Test-Satz schützt zentrale Basisfunktionen

Größte Lücken:

- README und Projektdokumentation waren bisher nicht auf Projektstand
- keine explizite Dataset Registry / Freshness-Sicht
- noch kein API- oder Web-Layer
- Daten sind noch nicht “sichtbar”
- Browse-Ziele (Season / Week / Game / Team / Player) sind noch nicht als Produkt realisiert

## 2. Verifizierter Repository-Stand

### 2.1 Top-Level-Struktur

Vorhanden:

- `docs/`
- `src/nfl_rag_db/`
- `tests/`
- `tools/`
- `README.md`
- `pyproject.toml`

### 2.2 Dokumentation

Vorhanden:

- `docs/reference/ENGINEERING_MANIFEST_v2.0.md`
- `docs/adr/ADR-0001-db-engine.md`
- `docs/concepts/nfl_rag_db_concept_v0_2.md`
- `docs/HANDOFF_MANIFEST.md`

### 2.3 Python-Paketstruktur

Beobachtete Kernmodule:

- `src/nfl_rag_db/db.py`
- `src/nfl_rag_db/audit_log.py`
- `src/nfl_rag_db/change_detection.py`
- `src/nfl_rag_db/http_download.py`
- `src/nfl_rag_db/run_registry.py`
- `src/nfl_rag_db/ingest_pbp.py`
- `src/nfl_rag_db/ingest_player_stats.py`
- `src/nfl_rag_db/ingest_nfldata.py`

Beobachtete Ingest-Unterpakete:

- `src/nfl_rag_db/ingest/nfldata.py`
- `src/nfl_rag_db/ingest/pbp.py`
- `src/nfl_rag_db/ingest/player_stats.py`

### 2.4 Tests

Beobachtet:

- `tests/test_audit_log.py`
- `tests/test_change_detection.py`
- `tests/test_pbp_url.py`
- `tests/test_player_stats_key_cols.py`
- `tests/test_run_registry.py`
- `tests/test_smoke.py`

### 2.5 Tooling

Beobachtet:

- `tools/db_snapshot_dump.py`
- `tools/file_catalog_dump.py`
- `tools/handoff_make.py`
- `tools/project_audit_dump.py`
- `tools/project_data_snapshot.py`
- `tools/project_tree_dump.py`
- `tools/show_last_run.py`
- `tools/source_bundle_dump.py`

## 3. Technische Basis

### 3.1 Datenbank-Engine

Entscheidung laut ADR:

- DuckDB first
- lokale, private Datenbank auf dem Laptop
- geeignet für analytische Nutzung und lokale Exploration

Aktueller Default-Pfad:

- `data/nfl.duckdb`

### 3.2 Audit und Observability

Beobachtete funktionale Basis:

- Run Registry mit `run_id`, Start/Ende, Outcome, Dauer, Params, Counts, Retry Count, Fehlerfeldern
- Audit-Logging für Source Files
- Audit-Logging für Tabellenstatistiken
- Snapshot-Dumps für Daten- und DB-Überblick

Das ist eine sehr gute Grundlage für spätere UI-Seiten wie:

- letzte Läufe
- Freshness
- Rows pro Tabelle
- fehlerhafte Läufe
- Quell-Dateien pro Dataset

### 3.3 Ingestion-Stand

#### PBP

Der Repository-Stand enthält einen CLI-Entrypoint für PBP:

- `python -m nfl_rag_db.ingest_pbp --season <YEAR>`

Die Business-Implementierung liegt unter:

- `src/nfl_rag_db/ingest/pbp.py`

#### Player Stats

Der Player-Stats-Ingest ist bereits am weitesten:

- Download eines Parquet-Snapshots
- Ablage in `data/raw/player_stats/...`
- Audit-Erfassung der Quelldatei
- Laden nach `stg.player_stats`
- Filter / incoming view
- Change Detection gegenüber `core.player_week_stats`
- Materialisierung von `core.player_week_stats`
- Duplicate-Key-Prüfung
- Finish Run mit Counts und Outcome

## 4. Aktueller Datenmodell-Stand

### 4.1 Nachweislich implementiert / belegt

- `audit.ingest_run`
- `audit.ingest_source_file`
- `audit.ingest_table_stat`
- `stg.player_stats`
- `core.player_week_stats`

### 4.2 In Tooling / Konzept klar vorgesehen

- `core.pbp` (im Snapshot-Tool explizit berücksichtigt)
- weitere `core.*`-Tabellen laut Konzept v0.2:
  - `game`
  - `game_result`
  - `play`
  - `scoring_event`
  - `team`
  - `venue`
  - `person`
  - `roster_membership`
  - weitere kategorisierte Player-/Game-Stats

## 5. Produktlücke

Die größte aktuelle Lücke ist nicht der reine Daten-Load, sondern die fehlende Produktoberfläche.

Im Moment existiert bereits viel interne Substanz, aber kaum Sichtbarkeit für den Nutzer:

- Wie viele Tabellen gibt es?
- Wie viele Rows enthält jede Tabelle?
- Welche Seasons / Weeks sind geladen?
- Welche Quellen wurden zuletzt aktualisiert?
- Welche Läufe waren erfolgreich oder fehlgeschlagen?
- Welche Datensätze fehlen noch?

Genau hier muss die nächste Phase ansetzen.

## 6. Priorisierte nächste Zielarchitektur

### 6.1 Kurzfristig

1. Projektdokumentation vollständig und ehrlich halten
2. Dataset Inventory und Freshness explizit modellieren
3. kleines read-only Webinterface bauen
4. Browse-Queries als stabile interne Query-Schicht definieren

### 6.2 Mittelfristig

1. `games`, `teams`, `players`, `rosters_weekly`, `team_week_stats` ergänzen
2. UI-orientierte Views / `mart.*`-Tabellen aufbauen
3. Audit / Freshness / DQ stärker systematisieren
4. CI und Hardening ergänzen

## 7. Empfohlener nächster Meilenstein

## M1 — “Data Visibility Foundation”

Ziel:
Die vorhandenen Daten und Audit-Informationen erstmals sichtbar machen.

Scope:

- API-/Web-Startpunkt
- Dashboard
- Dataset Inventory
- Latest Runs / Audit Summary
- Seasons / Weeks / Games Grundnavigation

Acceptance Criteria:

- Start der Web-App lokal möglich
- Dashboard zeigt:
  - DB-Pfad
  - Tabellen pro Schema
  - Rows pro Tabelle
  - letzte Ingest Runs
- Dataset-Seite listet alle aktuellen Tabellen mit Row Counts
- Season Browser zeigt geladene Seasons / Weeks / Games
- Tests decken mindestens die Query-Layer-Basis und einen UI-Smoke-Path ab

## 8. Offene Architekturfragen

Noch bewusst offen bzw. noch nicht finalisiert:

- Query-/UI-Layer: reine API oder API + server-rendered HTML
- `mart.*`-Tabellen vs. reine Views
- explizite Dataset Registry in `control.*`
- paralleles Lesen/Schreiben rund um DuckDB
- Orchestrierungsmodell für mehrere Ingestoren
- CI / Packaging / Lockfile / Typechecking-Strategie

## 9. Projektregeln für die nächsten Änderungen

Für alle kommenden Bolts gilt:

- kleine End-to-End-Schritte
- komplette Dateien liefern, keine halben Fragmente
- bei Code immer Tests mitliefern
- Project State nach Meilenstein aktualisieren
- keine stillen Architekturwechsel ohne ADR
