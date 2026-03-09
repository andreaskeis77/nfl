# nfl-rag-db

Lokale, auditierbare NFL-Datenplattform auf Basis von DuckDB — mit Fokus auf reproduzierbare Ingestion, nachvollziehbare Datenherkunft, schrittweise Modellierung und eine bereits vorhandene read-only Weboberfläche für Exploration, Browsing und Qualitätskontrolle.

## Zielbild

Dieses Repository baut keine isolierte Einmal-Pipeline, sondern eine belastbare lokale Datenplattform für NFL-Daten:

- historische Datensammlung + laufende Updates
- klarer Raw → Staging → Core → Audit-Fluss
- nachvollziehbare Runs, Quelldateien und Table-Stats
- reproduzierbare Datenbestände auf dem lokalen Rechner
- read-only Weboberfläche für:
  - Dateninventar und Freshness
  - Runs / Audit
  - Season → Week → Game Browsing
  - Team- und Spieleransichten
- Handoff-/Snapshot-Tooling für Analyse, Chat-Umzüge und Projektübergaben
- mittelfristig ergänzende JSON-/API- und RAG-Schicht auf Basis stabiler Core-Tabellen und dokumentierter Views

## Aktueller Stand

Der aktuelle Repository-Stand hat die reine Scaffold-Phase klar verlassen. Bereits vorhanden sind:

- Python-Paket unter `src/nfl_rag_db`
- zentrale DuckDB-Anbindung (`db.py`)
- Audit-/Run-Registry
- Audit-Tabellen für Quelldateien und Tabellenstatistiken
- Ingestion-Bausteine für `nfldata`, `pbp` und `player_stats`
- Query-Schicht und read-only FastAPI-/Jinja-Webapp unter `src/nfl_rag_db/webapp`
- browsebare Seiten für:
  - Dashboard
  - Datasets
  - Freshness
  - Runs
  - Seasons / Season Detail / Week Detail / Game Detail
  - Teams / Team Detail
  - Players / Player Detail
- Snapshot-/Handoff-Tooling unter `tools/`
- erweiterter Analyzer für umfassende Repo-/DB-/Docs-Handoffs
- Tests für Audit, Run Registry, Change Detection, Web-Queries und UI-Smoke

Noch nicht fertig bzw. bewusst noch nicht ausgebaut:

- umfassende CI-/Workflow-Härtung
- JSON-API zusätzlich zum HTML-UI
- tiefer ausgebaute Game-Detailseiten und Drilldowns
- explizite Dataset Registry / DQ-Historisierung als eigene Schicht
- zusätzliche Datenquellen wie Injuries, Coaches, Venues, Weather

## Architektur in Kurzform

### 1. Raw Landing Zone

Unveränderte Downloads werden unter `data/raw/...` abgelegt.

Ziele:

- immutable Snapshots
- Hash / Größe / Quelle nachvollziehbar
- Wiederholbarkeit und Debugbarkeit

### 2. Staging

Source-nahe Tabellen im DuckDB-Schema `stg`.

Ziele:

- Dateiformate einlesen
- Typisierung und technische Validierung
- noch keine fachliche Uminterpretation

### 3. Core

Kanonische, browsebare Tabellen im DuckDB-Schema `core`.

Je nach Ingest-Stand bzw. DB-Inhalt nutzt die Web- und Query-Schicht insbesondere logische Datasets wie:

- `core.game`
- `core.pbp`
- `core.team`
- `core.player_week_stats`
- `core.team_week_stats`
- `core.rosters` / `core.rosters_weekly`

### 4. Audit

Betriebs- und Nachvollziehbarkeitstabellen im Schema `audit`.

Der aktuelle Plattformkern umfasst:

- `audit.ingest_run`
- `audit.ingest_source_file`
- `audit.ingest_table_stat`

### 5. Read-only Weboberfläche

Die Web-App liegt unter `src/nfl_rag_db/webapp/` und baut auf einer kleinen Query-Schicht auf.

Produktziel der UI:

- Daten sichtbar machen, nicht verändern
- Audit/Freshness lesbar machen
- Browse-Pfade für Season, Week, Game, Team und Player bereitstellen
- leere oder partielle Daten robust behandeln

## Repository-Struktur

```text
docs/
  adr/
  concepts/
  reference/
  _snapshot/
  HANDOFF_MANIFEST.md

src/nfl_rag_db/
  ingest/
  webapp/
  audit_log.py
  change_detection.py
  db.py
  http_download.py
  ingest_nfldata.py
  ingest_pbp.py
  ingest_player_stats.py
  run_registry.py

tests/
tools/
pyproject.toml
README.md
```

## Entwicklungsprinzipien

Dieses Projekt folgt dem Engineering Manifest unter:

- `docs/reference/ENGINEERING_MANIFEST_v2.0.md`

Wesentliche Defaults:

- Correctness > Cleverness
- Small Batches statt großer Umbauten
- Zero-Trust gegenüber Daten und KI-Output
- Observability ist ein Feature
- Fail-Fast oder bewusst geplante Degradation
- Project State und Dokumentation sind Teil des Produkts

## Setup

### Voraussetzungen

- Python 3.10+
- lokales DuckDB-Dateisystem
- Windows 11 / lokale Entwicklung ist das Primärszenario

### Installation

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -e ".[dev]"
```

### Tests ausführen

```bash
pytest -q
```

### Web-App lokal starten

```bash
python -m uvicorn nfl_rag_db.webapp.app:app --reload --app-dir src
```

## Wichtige Konventionen

### Standard-DB-Pfad

Standardmäßig wird die DuckDB-Datei unter folgendem Pfad erzeugt bzw. erwartet:

```text
data/nfl.duckdb
```

### Beispiel: PBP-Ingestion per CLI

```bash
python -m nfl_rag_db.ingest_pbp --season 2024
```

### Beispiel: Player-Stats-Ingestion aus Python

```python
from nfl_rag_db.db import connect
from nfl_rag_db.ingest.player_stats import ingest_player_stats

con = connect()
run_id = ingest_player_stats(con, season_min=2011)
print(run_id)
```

### Beispiel: erweiterten Handoff-Analyzer starten

```bash
python .\tools\handoff_analyze.py --root . --db data/nfl.duckdb --max-bytes 1000000 --zip
```

## Dokumentation

Die wichtigsten projektinternen Dokumente sind:

- `docs/concepts/nfl_rag_db_concept_v0_2.md`
- `docs/adr/ADR-0001-db-engine.md`
- `docs/reference/ENGINEERING_MANIFEST_v2.0.md`
- `docs/reference/PROJECT_STATE.md`
- `docs/reference/DATA_CATALOG.md`
- `docs/reference/ROADMAP.md`
- `docs/reference/UI_BACKLOG.md`
- `docs/HANDOFF_MANIFEST.md`
- `HANDOFF_ANALYSIS.md` (falls im Repo vorhanden)
- `docs/_snapshot/latest/chat_handoff.md` nach einem Analyzer-Lauf

## Nächste priorisierte Schritte

1. UI-Schale und Web-Polish stabilisieren
2. Game-Detail und Drilldowns fachlich vertiefen
3. JSON-API ergänzend zum HTML-UI bereitstellen
4. Datenmodell gezielt erweitern:
   - `games`
   - `teams`
   - `players`
   - `rosters_weekly`
   - `team_week_stats`
   - `pbp`
   - weitere Source-Erweiterungen wie Injuries / Venues / Weather
5. Freshness / DQ expliziter modellieren
6. Hardening:
   - Tests ausbauen
   - CI ergänzen
   - klare Query-/UI-Schicht weiter festigen

## Arbeitsmodus für die nächsten Bolts

Der empfohlene Arbeitsmodus ist:

- kleine, vertikale End-to-End-Bolts
- jede Änderung mit kompletter Datei, nicht als Diff-Fragment
- bei Code immer begleitende Tests
- nach jedem signifikanten Schritt Snapshot/Handoff aktualisieren
- vor Chat-Wechseln den Analyzer laufen lassen und `docs/_snapshot/latest/chat_handoff.md` als Startdokument verwenden
