# PROJECT_STATE

Stand: 2026-03-09  
Status: aktiv, belastbare lokale Datenplattformbasis vorhanden; read-only Web-UI und Handoff-Analyse sind real, aber noch im Hardening

## 1. Executive Summary

Das Projekt ist klar über die reine Scaffold-Phase hinaus. Es besitzt eine funktionierende lokale DuckDB-Datenplattform mit Audit-/Run-Tracking, Ingestion-Bausteinen, Snapshot-/Handoff-Tooling sowie einer read-only Weboberfläche für Dateninventar, Freshness und Browse-Pfade.

Stärken des aktuellen Stands:

- klare Struktur unter `src/`, `tests/`, `tools/`, `docs/`
- DuckDB-first ist per ADR gesetzt
- Audit-/Run-Registry existiert
- Ingestion-Bausteine sind vorhanden und testseitig abgesichert
- Snapshot-/Handoff-Tooling ist vorhanden und wurde um einen umfassenderen Analyzer ergänzt
- Query-Schicht und read-only Web-UI sind real vorhanden
- Season-, Team- und Player-Browsing ist funktional angelegt

Größte aktuelle Lücken:

- UI-Hardening und visuelle Konsolidierung statt weiterer Rohfunktionen
- Dokumentation muss regelmäßig mit dem echten Stand synchron gehalten werden
- JSON-/API-Erweiterungen fehlen noch als Produktschicht neben HTML
- DQ-/Freshness-Modellierung ist noch eher implizit als explizit registriert
- CI / reproduzierbare Gates / Setup-Härtung sind noch ausbaufähig

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
- `docs/reference/PROJECT_STATE.md`
- `docs/reference/DATA_CATALOG.md`
- `docs/reference/ROADMAP.md`
- `docs/reference/UI_BACKLOG.md`
- `docs/adr/ADR-0001-db-engine.md`
- `docs/concepts/nfl_rag_db_concept_v0_2.md`
- `docs/HANDOFF_MANIFEST.md`
- Snapshot-/Handoff-Ausgaben unter `docs/_snapshot/...`

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

### 2.4 Web- und Query-Schicht

Vorhanden:

- `src/nfl_rag_db/webapp/app.py`
- `src/nfl_rag_db/webapp/queries.py`
- Jinja-Templates unter `src/nfl_rag_db/webapp/templates/`

Vorhandene Browse-/Audit-Seiten:

- Dashboard
- Datasets
- Freshness
- Runs
- Seasons
- Season Detail
- Week Detail
- Game Detail
- Teams
- Team Detail
- Players
- Player Detail

### 2.5 Tests

Beobachtet:

- Audit-/Run-/Change-Detection-Tests
- Smoke-Tests
- Web-Query-Tests
- Web-App-Smoke-Tests
- Handoff-Analyzer-Tests (falls der Analyzer-Bolt bereits eingespielt ist)

### 2.6 Tooling

Beobachtet:

- `tools/db_snapshot_dump.py`
- `tools/file_catalog_dump.py`
- `tools/handoff_make.py`
- `tools/project_audit_dump.py`
- `tools/project_data_snapshot.py`
- `tools/project_tree_dump.py`
- `tools/show_last_run.py`
- `tools/source_bundle_dump.py`
- `tools/handoff_analyze.py` als interpretierende Handoff-Erweiterung (falls eingespielt)

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
- Snapshot-Dumps für Daten-, DB- und Repo-Überblick
- erweiterte Handoff-Analyse für Chat-Umzüge und Zustandsanalyse

Diese Basis trägt bereits UI-Seiten wie:

- letzte Läufe
- Freshness
- Rows pro Tabelle
- Quelle / Source
- Snapshot- und Handoff-Analyse

### 3.3 Ingestion-Stand

Der Repository-Stand enthält Ingestion-Bausteine für:

- `nfldata`
- `pbp`
- `player_stats`

Der Player-Stats-Ingest ist besonders weit ausgebaut:

- Download eines Snapshots
- Ablage in `data/raw/...`
- Audit-Erfassung der Quelldatei
- Laden nach `stg.player_stats`
- Change Detection gegenüber `core.player_week_stats`
- Materialisierung von `core.player_week_stats`
- Duplicate-Key-Prüfung
- Finish Run mit Counts und Outcome

## 4. Aktueller Datenmodell- und Browse-Stand

### 4.1 Nachweislich im Plattformkern relevant

Audit:

- `audit.ingest_run`
- `audit.ingest_source_file`
- `audit.ingest_table_stat`

Logische Browse-Datasets / Query-Kandidaten:

- `core.game`
- `core.pbp`
- `core.team`
- `core.player_week_stats`
- `core.team_week_stats`
- `core.rosters` / `core.rosters_weekly`

Wichtig: Nicht jede lokale DB muss jederzeit alle dieser Tabellen enthalten. Die Query- und Browse-Schicht ist deshalb bewusst tolerant gegenüber Varianten und fehlenden Tabellen aufgebaut.

### 4.2 Read-only Web-Produktstand

Die Produktoberfläche ist vorhanden, aber noch nicht fertig ausdesignt. Sie kann heute schon reale Nutzerfragen beantworten wie:

- Welche Tabellen und logischen Datasets sind sichtbar?
- Welche Audit-Stats und Runs liegen vor?
- Welche Seasons / Weeks / Games sind browsebar?
- Welche Teams und Players sind sichtbar?

Das ist funktional ein großer Schritt über den alten Stand „Daten vorhanden, aber unsichtbar“ hinaus.

## 5. Hauptrisiken und Drift-Signale

Die größten Risiken liegen aktuell weniger im nackten Datenladen als in Konsistenz und Hardening:

- UI-Regressionsrisiko im Template-Layer
- Doku-Drift zwischen Repo-Text und echtem Code-Stand
- unterschiedliche reale DB-Stände vs. vereinfachte Test-Fixtures
- Gefahr, Query-/UI-Logik still gegen das Testschema statt gegen echte Audit-/Core-Tabellen zu optimieren
- Snapshot-/Handoff-Prozess muss konsequent genutzt werden, sonst geht Wissen beim Chat-Wechsel verloren

## 6. Priorisierte nächste Zielarchitektur

### 6.1 Kurzfristig

- UI-Schale stabilisieren und Regressionen verhindern
- Dashboard / Datasets / Freshness / Runs sauber rendern
- README und Projektdoku konsequent auf den echten Stand ziehen
- Handoff-Analyzer als festen Schritt vor neuen Chats etablieren

### 6.2 Mittelfristig

- Game-Detailseiten fachlich vertiefen
- Team-/Roster-/Season-Bezüge ausbauen
- JSON-API ergänzend zum HTML-UI anbieten
- Coverage-/Inventory-Metriken expliziter modellieren
- Dataset-Freshness und DQ-Checks stärker formalisieren

### 6.3 Danach

- weitere Datenquellen: Injuries, Coaches, Venues, Weather
- CI / Setup-Härtung / reproduzierbare Gates
- mart-/view-orientierte Schicht für Retrieval, RAG und APIs

## 7. Arbeitsmodus / Engineering Defaults

Für die nächsten Bolts gilt:

- kleine, vertikale, testbare Bolts
- keine großen Umbauten ohne grünes Gate
- Dateien komplett liefern, nicht als Diff-Fragmente
- bei Codeänderungen immer passende Tests
- Browser-/Smoke-Check nach Web-Änderungen
- Snapshot-/Handoff-Lauf vor Chat-Wechseln
- keine stillen Architekturwechsel ohne Doku-/ADR-Nachzug
