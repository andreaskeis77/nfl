# ROADMAP

Stand: 2026-03-07  
Planungsprinzip: kleine, vertikale, testbare Bolts

## Leitlinien

- zuerst Sichtbarkeit, dann Breite
- keine große UI ohne stabile Query-Schicht
- keine neue Datenquelle ohne Audit / Freshness / DQ-Überlegung
- jede Phase muss einen sichtbaren Nutzwert liefern
- Dokumentation läuft parallel zum Code

---

## Phase 1 — Dokumentation auf Projektstand

### Ziel

Das Repository soll den realen Projektstand korrekt wiedergeben und als belastbare Arbeitsbasis dienen.

### Scope

- `README.md`
- `PROJECT_STATE.md`
- `DATA_CATALOG.md`
- `ROADMAP.md`
- `UI_BACKLOG.md`

### Ergebnis

- der Projektstatus ist für Mensch und KI klar
- der nächste Code-Schritt ist sauber eingerahmt
- das Repo wirkt nicht mehr wie ein Minimal-Scaffold

---

## Phase 2 — Data Visibility Foundation

### Ziel

Vorhandene Daten und Audit-Informationen erstmals sichtbar machen.

### Scope

- Query-Schicht für:
  - Tabelleninventar
  - Row Counts
  - letzte Runs
  - Seasons / Weeks / Games
- kleines read-only Webinterface
- erster UI-Smoke-Test
- Health-/Status-Endpunkte

### Deliverables

- `src/nfl_rag_db/webapp/...`
- Templates für Dashboard / Dataset Inventory
- Tests für Query-Layer und UI-Smoke

### Acceptance Criteria

- App startet lokal
- Dashboard zeigt Tabellen und Rows
- letzte Runs werden sichtbar
- Season Browser funktioniert für vorhandene Daten
- Tests grün

---

## Phase 3 — Browsebare Kernobjekte

### Ziel

Von Dateninventar zu inhaltlicher Navigation.

### Scope

- Team-Ansicht
- Player-Ansicht
- Game-Detail
- Drilldown Season → Week → Game
- erste UI-orientierte Views / Query-Optimierungen

### Acceptance Criteria

- Teams können mit Stats / Schedule angezeigt werden
- Player können mit saisonalen oder wöchentlichen Stats angezeigt werden
- Games zeigen Grunddaten und PBP-/Scoring-Überblick
- keine direkte SQL-Logik in Templates

---

## Phase 4 — Datenmodell verbreitern

### Ziel

Die wichtigsten Browse-Achsen fachlich vollständig machen.

### Priorisierte Datasets

1. `games`
2. `teams`
3. `players`
4. `rosters_weekly`
5. `team_week_stats`
6. `pbp`
7. zusätzliche Player-/Game-Stats
8. Snap Counts / Next Gen Stats
9. Injuries / Preseason / Weather nur nach Bedarf

### Acceptance Criteria

- jede neue Quelle hat:
  - dokumentierte Herkunft
  - Audit
  - Row Count Beobachtung
  - klare Zieltabellen
  - mindestens einen Test

---

## Phase 5 — Freshness, Registry, DQ

### Ziel

Die Plattform weiß explizit, was sie hat, wie frisch es ist und ob es plausibel ist.

### Scope

- `control.dataset_registry`
- `audit.dataset_freshness`
- `audit.data_quality_check`
- UI-Ansicht für Freshness / DQ
- Warnindikatoren für leere oder schrumpfende Tabellen

### Acceptance Criteria

- pro Dataset ist Freshness sichtbar
- fehlgeschlagene / veraltete Datasets sind erkennbar
- DQ-Checks sind historisiert

---

## Phase 6 — Hardening

### Ziel

Aus einer guten lokalen Plattform eine robuste lokale Plattform machen.

### Scope

- CI / Workflows
- optional Typechecking
- Lockfile / Dependency-Hygiene
- bessere Fehlerklassifikation
- Retry-/Timeout-Konfiguration zentralisieren
- Test- und Smoke-Strategie ausbauen

### Acceptance Criteria

- wiederholbare lokale Setups
- definierte Gates
- weniger manuelle Wissensabhängigkeit

---

## Phase 7 — RAG- und Query-Produktschicht

### Ziel

Strukturierte Daten und spätere Chat-/Retrieval-Szenarien sauber verbinden.

### Scope

- dokumentierte browsebare Views
- `mart.*` oder dedizierte Dokument-Views
- Game Summary / Player Season Views
- API-Endpunkte für externe Query-Clients

### Acceptance Criteria

- Datenmodell bleibt System of Record
- RAG/Chat wird aus Views gespeist, nicht aus adhoc-Text
- Herkunft und Reproduzierbarkeit bleiben erhalten

---

## Unmittelbar empfohlener nächster Bolt

## Bolt 01 — Read-only Web-Foundation

### Was gebaut werden soll

- FastAPI-App
- Dashboard
- Dataset Inventory
- Latest Runs
- Season Browser Skeleton

### Warum genau jetzt

Weil bereits genug Daten- und Audit-Struktur vorhanden ist, um daraus sofort sichtbaren Produktwert zu machen.  
Mehr Daten ohne Sichtbarkeit würden das Projekt im Blindflug vergrößern.

### Was noch nicht Teil dieses Bolts ist

- komplexe React-Frontends
- Auth
- Write-Operationen
- voll ausgebaute Team-/Player-Profile
- RAG-Integration
