# nfl-rag-db

Local, auditierbare NFL-Datenplattform auf Basis von DuckDB — mit Fokus auf reproduzierbare Ingestion, nachvollziehbare Datenherkunft, schrittweise Modellierung und einem späteren read-only Webinterface für Exploration, Browsing und Qualitätskontrolle.

## Zielbild

Dieses Repository baut keine isolierte Einmal-Pipeline, sondern eine belastbare lokale Datenplattform für NFL-Daten:

- historische Datensammlung + laufende Updates
- klarer Raw → Staging → Core → Audit-Fluss
- nachvollziehbare Runs, Quelldateien und Table-Stats
- reproduzierbare Datenbestände auf dem lokalen Rechner
- späteres Webinterface für:
  - Dateninventar und Freshness
  - Season / Week / Game Browsing
  - Team- und Spieleransichten
  - Audit- und Ingestion-Überblick
- mittelfristig RAG-/Chat-Bridge auf Basis stabiler Core-Tabellen und dokumentierter Views

## Aktueller Stand

Der aktuelle Repository-Stand zeigt bereits eine belastbare Basis, ist aber noch kein fertiges Produkt.

Bereits vorhanden:

- Python-Paket unter `src/nfl_rag_db`
- zentrale DuckDB-Anbindung (`db.py`)
- Audit-/Run-Registry
- Audit-Tabellen für Quelldateien und Tabellenstatistiken
- PBP-CLI-Entrypoint
- Player-Stats-Ingestion mit Raw-Snapshot, Staging, Core-Load und Change Detection
- Snapshot-/Handoff-Tooling unter `tools/`
- erster Test-Satz für Audit, Run Registry, PBP-URL, Player-Stats-Keying und Smoke

Noch nicht vorhanden bzw. noch nicht produktreif:

- vollständige README / aktuelle Projektdokumentation
- expliziter Data Catalog für aktuelle Tabellen
- Dataset Registry / Freshness Registry
- API-Layer / Web-UI
- browsebare Team-/Spieler-/Game-Seiten
- CI/Workflow-Härtung
- klar definierte `mart.*`- oder UI-orientierte Views

## Architektur in Kurzform

### 1. Raw Landing Zone

Unveränderte Downloads werden unter `data/raw/...` abgelegt.  
Beispielhaft sichtbar ist dies bereits für Player Stats.

Ziele:

- immutable Snapshots
- Hash / Größe / Quelle nachvollziehbar
- Wiederholbarkeit und Debugbarkeit

### 2. Staging

Source-nahe Tabellen im DuckDB-Schema `stg`.

Ziele:

- Dateiformate einlesen
- Typisierung und technische Validierung
- noch keine fachliche “Uminterpretation”

### 3. Core

Kanonische, browsebare Tabellen im DuckDB-Schema `core`.

Der aktuelle belegte Kern ist:

- `core.player_week_stats`

Mittelfristig sollen hier insbesondere `games`, `teams`, `players`, `rosters`, `pbp`, `team_week_stats` und abgeleitete Browse-/Mart-Views liegen.

### 4. Audit

Betriebs- und Nachvollziehbarkeitstabellen im Schema `audit`.

Der aktuelle Stand umfasst bereits:

- Ingest Runs
- Source Files
- Table Stats

## Repository-Struktur

```text
docs/
  adr/
  concepts/
  reference/
  HANDOFF_MANIFEST.md

src/nfl_rag_db/
  ingest/
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

## Dokumentation

Die wichtigsten projektinternen Dokumente sind:

- `docs/concepts/nfl_rag_db_concept_v0_2.md`
- `docs/adr/ADR-0001-db-engine.md`
- `docs/reference/ENGINEERING_MANIFEST_v2.0.md`
- `docs/reference/PROJECT_STATE.md`
- `docs/reference/DATA_CATALOG.md`
- `docs/reference/ROADMAP.md`
- `docs/reference/UI_BACKLOG.md`

## Nächste priorisierte Schritte

1. Dokumentation auf aktuellen Stand bringen
2. Dataset Inventory + Freshness explizit modellieren
3. erste read-only Weboberfläche bauen
4. Browse-Achsen stabilisieren:
   - Season → Week → Game
   - Team
   - Player
5. Datenmodell gezielt erweitern:
   - `games`
   - `teams`
   - `players`
   - `rosters_weekly`
   - `team_week_stats`
   - `pbp`
6. Hardening:
   - Tests ausbauen
   - CI ergänzen
   - klare Query-/UI-Schicht einziehen

## Arbeitsmodus für die nächsten Bolts

Der empfohlene Arbeitsmodus ist:

- kleine, vertikale End-to-End-Bolts
- jede Änderung mit kompletter Datei, nicht als Diff-Fragment
- bei Code immer begleitende Tests
- nach jedem Meilenstein `PROJECT_STATE.md` aktualisieren
- bei Architekturentscheidungen ADR nachziehen
