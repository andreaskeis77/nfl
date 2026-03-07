# DATA_CATALOG

Stand: 2026-03-07  
Zweck: Aktueller Datenkatalog des Projekts — getrennt nach **nachweislich implementiert**, **im Tooling referenziert** und **konzeptionell geplant**.

---

## 1. Schema-Überblick

Aktuell relevant:

- `audit` — Betriebs- und Nachvollziehbarkeitstabellen
- `stg` — source-nahe technische Landing-/Staging-Tabellen
- `core` — kanonische, browsebare Daten
- `mart` — noch nicht beobachtet, aber perspektivisch empfohlen
- `control` — noch nicht beobachtet, perspektivisch empfohlen für Dataset Registry / Freshness / Konfiguration

---

## 2. Nachweislich implementierte Tabellen

## 2.1 `audit.ingest_run`

**Status:** implementiert  
**Grain:** ein Datensatz pro Ingest-/Job-Run  
**Zweck:** Run Registry für technische Nachvollziehbarkeit

### Beobachtete Kernfelder

- `run_id`
- `component`
- `source`
- `started_at`
- `ended_at`
- `outcome`
- `duration_ms`
- `params_json`
- `counts_json`
- `retry_count`
- `error_class`
- `error_code`
- `error_message`

### Hinweise

- zentrale Basis für spätere Dashboard-/Audit-Seiten
- aktueller Outcome-Standard:
  - `ok`
  - `partial`
  - `fail`

---

## 2.2 `audit.ingest_source_file`

**Status:** implementiert  
**Grain:** eine Zeile pro verarbeiteter Quelldatei  
**Zweck:** Provenance und technische Dokumentation von Raw-Dateien

### Nachweisbare Felder aus Test / Nutzung

- `file_id`
- `run_id`
- `source`
- `dataset`
- `url`
- `local_path`
- `sha256`
- `size_bytes`

### Hinweise

- ideal für spätere Freshness- und Provenance-Sichten
- sollte mittelfristig zusätzlich `retrieved_at` / `content_type` / ggf. `http_status` sauber tragen

---

## 2.3 `audit.ingest_table_stat`

**Status:** implementiert  
**Grain:** eine Zeile pro Tabellenstatistik-Ereignis innerhalb eines Runs  
**Zweck:** technische und mengenbezogene Beobachtung einzelner Tabellenladungen

### Nachweisbare Felder aus Test / Nutzung

- `stat_id`
- `run_id`
- `table_fqn`
- `row_count`
- `previous_row_count`
- `delta_row_count`
- `note`

### Hinweise

- für UI später sehr wertvoll:
  - Row Count Trends
  - Delta pro Run
  - Erkennung leerer / unerwartet kleiner Loads

---

## 2.4 `stg.player_stats`

**Status:** implementiert  
**Grain:** source-nahe geladene Player-Stats-Zeilen aus Parquet  
**Zweck:** technisches Staging der nflverse-Player-Stats

### Herkunft

- Rohdatei aus nflverse Release `player_stats.parquet`

### Hinweise

- Quelle wird vollständig nach Staging geladen
- Staging ist bewusst source-nah
- genaue Spaltenliste hängt von der Upstream-Datei ab

---

## 2.5 `core.player_week_stats`

**Status:** implementiert  
**Grain:** aktuell wöchentliche oder spielnahe Player-Stats je nach vorhandenen Upstream-Schlüsseln  
**Zweck:** kanonische Materialisierung der Player-Stats für das Projekt

### Nachweisbare Logik

- basiert auf `stg.player_stats`
- Filter `season >= season_min`
- Change Detection vor Ersetzung
- Duplicate-Key-Check aktiv

### beobachtete Key-Strategie

Die Implementierung leitet einen stabilen Schlüssel aus den verfügbaren Spalten ab:

- Pflicht:
  - `season`
  - `player_id`
- wenn vorhanden zusätzlich:
  - `season_type` oder `game_type`
  - `week` oder alternativ `game_id`

### Hinweise

- dies ist aktuell die am weitesten ausgebaute Core-Tabelle
- gute Basis für spätere Player Pages und Player Season/Week Views

---

## 3. Im Tooling explizit referenziert

## 3.1 `core.pbp`

**Status:** im Tooling referenziert  
**Grain:** Play-by-Play-Events  
**Zweck:** zentrale Faktentabelle für Spielverlauf

### Evidenz

- Snapshot-Tool enthält eine gesonderte Ausgabe `core.pbp rows by season`
- PBP-CLI und Ingest-Modul existieren

### Bemerkung

Diese Tabelle ist im Repository klar vorgesehen und vermutlich in realen lokalen DB-Ständen bereits relevant, aber ihre vollständige aktuelle Definition ist im Repo-Check nicht so belastbar nachgewiesen wie bei `core.player_week_stats`.

---

## 4. Konzeptionell geplantes Zielmodell (noch nicht als “sicher implementiert” behandeln)

Die folgenden Tabellen stammen aus dem Konzept `nfl_rag_db_concept_v0_2.md` und bilden das Zielbild.  
Sie sind **nicht automatisch als bereits implementiert zu verstehen**.

## 4.1 Dimensionen

### `core.season`
- eine Zeile pro Season

### `core.week`
- eine Zeile pro Season/Woche/Season Type

### `core.team`
- Team-Dimension

### `core.venue`
- Venue-Dimension

### `core.person`
- Person-Dimension für Spieler, Coaches, Officials

### `core.person_role`
- Rollen-Historisierung pro Person

### `core.roster_membership`
- Teamzugehörigkeit über Zeit

### `core.entity`
- interne kanonische Entitäten

### `core.entity_external_id`
- Mapping von externen IDs auf interne Entitäten

---

## 4.2 Games / Verlauf / Scoring

### `core.game`
- eine Zeile pro Spiel

### `core.game_result`
- Ergebnisdaten eines finalen Spiels

### `core.play`
- zentrale PBP-Faktentabelle

### `core.drive`
- Drive-Ebene, aus Plays ableitbar

### `core.scoring_event`
- scoring timeline / Punktabfolge pro Spiel

---

## 4.3 Weitere Statistik-Tabellen

Das Konzept nennt kategoriale `core.player_game_*`-Tabellen bzw. ähnliche kanonische Player-/Game-Stats-Strukturen.

Empfohlen für das Projekt:

- klar benannte faktische Tabellen statt später schwer interpretierbarer Mischcontainer
- zusätzlich UI-orientierte Views / Marts

Beispiel-Zieltabellen:

- `core.player_game_passing`
- `core.player_game_rushing`
- `core.player_game_receiving`
- `core.player_game_defense`
- `core.player_game_kicking`
- `core.team_week_stats`

---

## 5. Datenquellen

## 5.1 Nachweislich aktuell genutzt

### nflverse Player Stats Release

- Release-Artefakt: `player_stats.parquet`
- Rolle:
  - Raw Download
  - Staging
  - Core-Materialisierung

## 5.2 Im Projekt klar vorgesehen

### nflverse / nfldata / nflfastR / nflreadr / nflreadpy

Rolle:

- schedules
- teams
- players
- rosters
- pbp
- stats

### ESPN

Rolle laut Konzept:

- preseason Ergänzungen
- injuries
- venues / coaches / Metadaten

### NOAA

Rolle laut Konzept:

- optionales Weather Enrichment

---

## 6. Empfohlene Ergänzungen für den Katalog

Die aktuelle Projektbasis sollte um folgende Metastrukturen ergänzt werden:

## 6.1 `control.dataset_registry`

Empfohlene Felder:

- `dataset_name`
- `schema_name`
- `table_name`
- `grain`
- `source_system`
- `owner_component`
- `freshness_expectation`
- `status`
- `notes`

## 6.2 `audit.dataset_freshness`

Empfohlene Felder:

- `dataset_name`
- `latest_successful_run_id`
- `latest_successful_at`
- `freshness_seconds`
- `freshness_status`

## 6.3 `audit.data_quality_check`

Empfohlene Felder:

- `run_id`
- `dataset_name`
- `check_name`
- `outcome`
- `severity`
- `detail`

---

## 7. Offene Kataloglücken

Aktuell noch nicht explizit dokumentiert, aber wichtig:

- vollständige Spaltenkataloge je Tabelle
- Business Keys / technische Keys je Tabelle
- erwartete Update-Cadence pro Dataset
- bekannte Nullability-/Constraint-Regeln
- Abgrenzung `core` vs. zukünftiges `mart`
- genaue Browse-geeignete Views für UI und RAG
