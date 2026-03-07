# UI_BACKLOG

Stand: 2026-03-07  
Ziel: erste nutzbare read-only Weboberfläche für die lokale NFL-Datenplattform

## Produktprinzipien

- read-only zuerst
- lokale Nutzung zuerst
- Übersicht vor Schönheit
- schnelle Drilldowns statt schwerer Monolith-Seiten
- jede UI-Seite muss direkt auf reale Daten- oder Audit-Fragen antworten

---

## Release 0 — Foundation

### 1. Dashboard

**Nutzen:** Sofort sehen, ob die Plattform “lebt”.

#### Inhalte

- DB-Pfad
- DB-Dateigröße
- Anzahl Tabellen pro Schema
- Row Counts der wichtigsten `core`-Tabellen
- letzte erfolgreichen und fehlgeschlagenen Runs
- sichtbare Freshness-Indikatoren
- ggf. schnelle KPI-Karten:
  - Tabellenzahl
  - letzte erfolgreiche Ingestion
  - Anzahl auditierter Runs
  - Anzahl Raw-Dateien

#### Acceptance Criteria

- Seite lädt auch bei wenig Daten robust
- keine Exception bei fehlenden Tabellen
- “leere Datenbank” wird als gültiger Zustand verständlich angezeigt

---

### 2. Dataset Inventory

**Nutzen:** Welche Daten habe ich überhaupt?

#### Inhalte

- Schema
- Tabellenname
- Row Count
- letzte Änderung / letzte erfolgreiche Ingestion
- Quelle
- kurze Beschreibung / Grain
- Status:
  - implemented
  - partial
  - planned

#### Acceptance Criteria

- Inventar basiert nicht auf hardcodierter Liste allein
- Tabellen aus `information_schema` können angezeigt werden
- Metadaten aus Registry können später ergänzt werden

---

### 3. Latest Runs / Audit

**Nutzen:** Welche Ingests liefen wann und mit welchem Ergebnis?

#### Inhalte

- `started_at`
- `component`
- `source`
- `outcome`
- `duration_ms`
- Counts
- Fehlerklasse / Fehlertext
- Drilldown zu verarbeiteten Source Files

#### Acceptance Criteria

- auch leere Audit-Tabelle wird sauber behandelt
- fehlgeschlagene Läufe sind visuell erkennbar
- Counts werden lesbar formatiert

---

## Release 1 — Browsebare Saison-Navigation

### 4. Season Browser

**Nutzen:** Einstieg in die Daten aus Nutzersicht.

#### Inhalte

- geladene Seasons
- Weeks pro Season
- Anzahl Games pro Week
- Filter auf Season Type, falls verfügbar

#### Acceptance Criteria

- funktioniert mit minimalen `games`-/PBP-Grunddaten
- Navigation bleibt auch bei fehlenden Weeks stabil

---

### 5. Week / Game List

**Nutzen:** Spiele einer Woche direkt sehen.

#### Inhalte

- Home / Away
- Datum / Kickoff
- Status / Final Score
- Link auf Game Detail

#### Acceptance Criteria

- Sortierung nach Kickoff
- kein Template-SQL
- fehlende Scores bei nicht finalen Spielen sauber dargestellt

---

### 6. Game Detail

**Nutzen:** Ein einzelnes Spiel verstehen.

#### Inhalte

- Grunddaten
- Ergebnis
- Scoring Timeline
- wichtige Plays / PBP-Auszug
- Basisinfos zu Teams

#### Acceptance Criteria

- funktioniert mit teilweise verfügbaren Daten
- PBP und Score-Timeline sind klar getrennt
- keine riesige unformatierte Datentabelle als erste Version

---

## Release 2 — Team- und Spieleransichten

### 7. Team Page

**Nutzen:** Teamfokussierte Exploration.

#### Inhalte

- Stammdaten
- Season Schedule
- Saisonbilanz
- Team Week Stats
- Roster Snapshot

#### Acceptance Criteria

- URL stabil über Team-Abkürzung oder kanonische Team-ID
- Stats und Schedule sind trennbar darstellbar

---

### 8. Player Page

**Nutzen:** Spielerstats browsebar machen.

#### Inhalte

- Name / Team / Position
- verfügbare Seasons
- wöchentliche oder spielnahe Stats
- Quick Links zu Team / Games

#### Acceptance Criteria

- große Tabellen paginierbar oder begrenzt
- Filter nach Season vorhanden
- leere oder partielle Datensätze werden robust gehandhabt

---

## Release 3 — Betriebsoberfläche

### 9. Freshness View

**Nutzen:** Sehen, welche Datasets aktuell sind.

#### Inhalte

- Dataset
- erwartete Update-Cadence
- letzte erfolgreiche Aktualisierung
- Freshness Status

### 10. DQ / Warnings

**Nutzen:** Datenprobleme sichtbar machen.

#### Inhalte

- leere Tabellen
- plötzliche Row-Count-Einbrüche
- DQ-Check-Historie
- bekannte Warnungen

---

## Technischer Ansatz

Empfohlen für die erste Iteration:

- FastAPI als Backend
- server-rendered HTML mit Templates
- kleine, saubere Query-Schicht zwischen DB und UI
- bewusst read-only

### Warum so starten?

Weil das Projekt zuerst Sichtbarkeit und robuste Browse-Pfade braucht, nicht sofort ein schweres Frontend-Framework.  
Eine kleine serverseitige Oberfläche ist schneller, testbarer und näher an den Daten.

---

## Nicht-Ziele der ersten UI

Noch nicht Teil der ersten Stufe:

- Login / Benutzerverwaltung
- Write-Flows
- komplexe Inline-Edits
- aggressive Client-State-Architektur
- Diagramm-Overkill vor stabilen Datenmodellen
- “perfektes Design” vor robuster Daten-Navigation

---

## UI-Bolt-Reihenfolge

1. Dashboard
2. Dataset Inventory
3. Latest Runs / Audit
4. Season Browser
5. Week / Game List
6. Game Detail
7. Team Page
8. Player Page
9. Freshness / DQ
