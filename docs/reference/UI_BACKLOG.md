# UI_BACKLOG

Stand: 2026-03-09  
Ziel: die vorhandene read-only Weboberfläche schrittweise zu einer stabilen, nützlichen Explorationsoberfläche ausbauen

## Produktprinzipien

- read-only zuerst
- lokale Nutzung zuerst
- Sichtbarkeit vor Overengineering
- robuste Drilldowns statt schwerer Monolith-Seiten
- jede UI-Seite muss auf reale Daten-, Audit- oder Freshness-Fragen antworten
- Template-/Layout-Qualität ist Teil der Funktionalität

---

## Bereits vorhandene Flächen

Heute bereits angelegt bzw. vorhanden:

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

Die UI ist damit funktional über die Foundation-Phase hinaus, aber noch nicht überall produktreif ausformuliert.

---

## Release 0 — UI-Basis stabilisieren

### 1. HTML-Schale / Navigation / Layout

**Nutzen:** Die Oberfläche soll jederzeit als echte Webanwendung lesbar bleiben und nicht in Rohtext/Markdown zurückfallen.

#### Inhalte

- echtes HTML-Layout in `base.html`
- konsistente Navigation
- Basis-Styling für Karten, Tabellen, Tags, Suchformulare
- lesbare Anzeige von DB-Pfad und Seitentitel

#### Acceptance Criteria

- kein Rohtext-Rendering von Markdown-artigen Links
- Dashboard / Datasets / Freshness / Runs rendern stabil
- Smoke-Test schützt gegen diesen Regressionsfall

---

### 2. Dashboard

**Status:** vorhanden, weiter zu härten

**Nutzen:** Sofort sehen, ob die Plattform „lebt“.

#### Inhalte

- Anzahl Tabellen / Schemas
- logische Coverage
- letzte erfolgreiche Ingestion
- letzte Audit Table Stats
- letzte Runs
- Season / Week Überblick

#### Nächste Verbesserungen

- bessere KPI-Gruppierung
- klickbare Deep Links auf relevante Detailseiten
- evtl. zusätzliche Health-/Warning-Signale

---

### 3. Datasets

**Status:** vorhanden, weiter zu härten

**Nutzen:** Welche Daten habe ich überhaupt?

#### Inhalte

- logische Coverage
- physische Tabellen
- Row Counts
- Last Seen / Audit Rows

#### Nächste Verbesserungen

- Dataset-Beschreibungen / Grain
- Quelle / Herkunft direkter sichtbar
- evtl. Verlinkung zu Freshness / DQ

---

### 4. Runs / Audit

**Status:** vorhanden, weiter zu härten

**Nutzen:** Welche Ingests liefen wann und mit welchem Ergebnis?

#### Inhalte

- `started_at`
- `component`
- `source`
- `outcome`
- `duration_ms`
- Counts
- Fehlerklasse / Fehlertext

#### Nächste Verbesserungen

- Drilldown zu verarbeiteten Source Files
- visuelle Gruppierung erfolgreicher vs. fehlerhafter Läufe
- evtl. Filter auf Component / Zeitraum

---

### 5. Freshness

**Status:** vorhanden, aktuell Fokusbereich

**Nutzen:** Sehen, welche Tabellen und logischen Datasets zuletzt sichtbar waren.

#### Inhalte

- letzter Audit-Stand je Tabelle
- Delta Row Count
- Component / Source / Note
- Coverage-Zusammenfassung
- fehlende logische Datasets

#### Nächste Verbesserungen

- Dataset-spezifische Freshness-Cadence
- explizite Freshness-Statuslogik
- Verknüpfung zu DQ-Signalen

---

## Release 1 — Browsebare Saison-Navigation vertiefen

### 6. Seasons / Weeks / Games

**Status:** vorhanden, fachlich ausbaufähig

**Nutzen:** Einstieg in die Daten aus Nutzersicht.

#### Nächste Verbesserungen

- Kickoff-/Datumsspalten sauberer integrieren
- Status / Final Score robuster darstellen
- Week-Listen und Game-Detail stärker verbinden

### 7. Game Detail

**Status:** vorhanden, aber noch roh

**Nutzen:** Ein einzelnes Spiel verständlich machen.

#### Nächste Verbesserungen

- klarere Zusammenfassung oben
- Scoring-/PBP-/Player-Preview sauberer strukturieren
- keine „zu breite Rohdatentabelle“ als alleinige Darstellungsform

---

## Release 2 — Team- und Spieleransichten

### 8. Team Page

**Status:** vorhanden, ausbaufähig

**Nutzen:** Teamfokussierte Exploration.

#### Nächste Verbesserungen

- Schedule vs. Stats klarer trennen
- Team-/Season-Bezüge stärker herausarbeiten
- Roster-/Leader-Module verbessern

### 9. Player Page

**Status:** vorhanden, ausbaufähig

**Nutzen:** Spielerstats browsebar machen.

#### Nächste Verbesserungen

- Filter nach Season
- bessere Aggregationen / Leaders / Splits
- Pagination oder Limits für große Tabellen

---

## Release 3 — Betriebsoberfläche und API

### 10. JSON-API ergänzend zum HTML-UI

**Nutzen:** dieselben Daten auch maschinenlesbar nutzbar machen.

#### Inhalte

- Dashboard-JSON
- Dataset-Inventory-JSON
- Freshness-JSON
- Browse-JSON für Seasons / Games / Teams / Players

### 11. DQ / Warnings

**Nutzen:** Datenprobleme sichtbar machen.

#### Inhalte

- leere Tabellen
- plötzliche Row-Count-Einbrüche
- bekannte Warnungen
- ggf. DQ-Check-Historie

---

## Nicht-Ziele der aktuellen UI-Stufe

Noch nicht Teil der aktuellen Stufe:

- Login / Benutzerverwaltung
- Write-Flows
- komplexe Inline-Edits
- schweres Frontend-Framework ohne klaren Bedarf
- Diagramm-Overkill vor stabilen Datenmodellen
- Schönheitsarbeit ohne fachlichen Nutzen

---

## Konkrete UI-Bolt-Reihenfolge

1. UI-Schale / Layout-Regression absichern
2. Dashboard polishen
3. Datasets / Freshness / Runs konsolidieren
4. Game Detail vertiefen
5. Team- und Player-Drilldowns verbessern
6. JSON-API ergänzen
7. DQ-/Warning-Oberfläche ergänzen
