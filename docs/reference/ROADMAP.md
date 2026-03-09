# ROADMAP

Stand: 2026-03-09  
Planungsprinzip: kleine, vertikale, testbare Bolts

## Leitlinien

- zuerst Sichtbarkeit und Stabilität, dann neue Breite
- keine neue Datenquelle ohne Audit-/Freshness-/DQ-Überlegung
- Web-UI bleibt read-only, bis die Daten- und Query-Schicht stabil genug ist
- Dokumentation und Handoff sind Teil des Produkts
- vor Chat-Wechseln wird ein Snapshot/Handoff erzeugt

---

## Phase 1 — Plattformbasis und Sichtbarkeit

### Status

**Im Kern erreicht.**

### Ergebnis

Bereits real vorhanden:

- DuckDB-Plattformbasis
- Audit-/Run-Tracking
- erste Ingestion-Bausteine
- Snapshot-/Handoff-Tooling
- read-only Web-Foundation
- browsebare Kernseiten für Audit, Seasons, Teams und Players

### Restarbeiten innerhalb dieser Phase

- visuelle UI-Schale stabilisieren
- Dokumentation auf den echten Stand ziehen
- Smoke-Checks gegen UI-Regressionen schärfen

---

## Phase 2 — Web-Hardening und Produktkonsistenz

### Ziel

Aus der vorhandenen Web-Foundation eine stabile, gut lesbare Produktoberfläche machen.

### Scope

- saubere HTML-/Template-Schale
- konsistente Navigation und Tabellenlayouts
- Regression-Tests für UI-Grundlayout
- README / PROJECT_STATE / UI-Doku synchronisieren
- Handoff-/Analyzer-Prozess im Alltag verankern

### Acceptance Criteria

- Dashboard / Datasets / Freshness / Runs rendern sauber
- keine Markdown-/Rohtext-Regressionsfehler in Templates
- Doku behauptet nicht mehr das Gegenteil des echten Repostands
- Chat-Handoffs können über `docs/_snapshot/latest/chat_handoff.md` starten

---

## Phase 3 — Browsebare Kernobjekte vertiefen

### Ziel

Von „sichtbar“ zu „inhaltlich nützlich“ kommen.

### Scope

- bessere Game-Detailseiten
- Team-/Roster-/Season-Bezüge ausbauen
- Week-/Game-Listen weiter verfeinern
- Such-/Filterpfade verbessern
- UI-orientierte Views oder Query-Optimierungen ergänzen

### Acceptance Criteria

- Games sind fachlich verständlicher browsbar
- Team- und Player-Seiten liefern belastbare Drilldowns
- keine direkte SQL-Logik in Templates

---

## Phase 4 — Datenmodell verbreitern

### Ziel

Die wichtigsten Browse-Achsen fachlich vollständiger machen.

### Priorisierte Datasets

1. `games`
2. `teams`
3. `players`
4. `rosters_weekly`
5. `team_week_stats`
6. `pbp`
7. Injuries / Coaches / Venues
8. Weather Enrichment
9. weitere Player-/Game-Stats nach Nutzwert

### Acceptance Criteria

- jede neue Quelle hat:
  - dokumentierte Herkunft
  - Audit
  - Row-Count-/Freshness-Beobachtung
  - klare Zieltabellen
  - mindestens einen Test

---

## Phase 5 — Registry, Freshness, DQ

### Ziel

Die Plattform weiß explizit, was sie hat, wie frisch es ist und welche Warnsignale bestehen.

### Scope

- explizitere Dataset Registry
- Freshness-Sicht je Dataset
- DQ-Check-Historie
- Warnindikatoren für leere oder schrumpfende Tabellen
- UI-Ansichten für Freshness / DQ / Coverage

### Acceptance Criteria

- pro Dataset ist Freshness sichtbar
- fehlgeschlagene / veraltete Datasets sind klar erkennbar
- DQ-Signale sind historisiert oder zumindest nachvollziehbar abgelegt

---

## Phase 6 — API- und Produktschicht

### Ziel

Neben der HTML-Oberfläche eine saubere maschinenlesbare Produktschicht aufbauen.

### Scope

- JSON-API für Dashboard / Datasets / Freshness / Browse-Achsen
- stabile query-orientierte Endpunkte
- dokumentierte Response-Formate
- Vorbereitung für Retrieval-/RAG-/Agent-Anbindung

### Acceptance Criteria

- HTML und JSON teilen sich dieselbe belastbare Query-Schicht
- keine doppelte Fachlogik in zwei separaten Implementierungen
- Herkunft und Reproduzierbarkeit bleiben erhalten

---

## Phase 7 — Hardening

### Ziel

Aus einer guten lokalen Plattform eine robuste lokale Plattform machen.

### Scope

- CI / Workflows
- Lockfile / Dependency-Hygiene
- bessere Fehlerklassifikation
- Retry-/Timeout-Konfiguration zentralisieren
- mehr Gates für Query-/UI-/Snapshot-Regressionen

### Acceptance Criteria

- wiederholbare lokale Setups
- definierte Gates
- geringere Wissensabhängigkeit von einzelnen Chats

---

## Unmittelbar empfohlener nächster Bolt

## Bolt — UI-Schale reparieren + Doku synchronisieren

### Was gebaut werden soll

- `base.html` als echte HTML-Schale
- `freshness.html` und `runs.html` ohne Markdown-/Rohtext-Ausgabe
- Smoke-Test gegen diese Regression
- Synchronisierung von `README.md`, `PROJECT_STATE.md`, `ROADMAP.md`, `UI_BACKLOG.md`

### Warum genau jetzt

Weil der Plattformkern und das Handoff-Tooling bereits stehen, aber die sichtbare Oberfläche und Teile der Doku noch uneinheitlich sind. Mehr Feature-Breite würde auf instabiler Grundlage aufsetzen.
