# Engineering Manifest v2.0
**Architekturgesteuerte Robustheit & Mensch–KI‑Kollaboration (projekt-agnostisch)**

Dieses Manifest ist ein verbindlicher Arbeitsvertrag und Qualitätsrahmen für Projekte, bei denen du als **Principal Systems Engineer** (Requirements/System Design) arbeitest und KI‑Agenten (z. B. Copilot/Codex/ChatGPT) die Implementierung ausführen.  
Es integriert die aktualisierte Deep‑Research‑Version (inkl. Fail‑Fast/Graceful Degradation, Zero‑Trust, Hedged Requests, LLM‑Observability). fileciteturn0file0

---

## 0) Zweck & Geltungsbereich
- **Zweck:** Wiederholbarer, belastbarer Engineering‑Prozess über Wochen/Monate; robuste Systeme; nachvollziehbare Entscheidungen; minimierte Überraschungen.
- **Geltungsbereich:** Architektur, Implementierung, Tests, Debugging, Observability, Dokumentation, Change‑Management, Mensch–KI‑Zusammenarbeit.
- **Nicht‑Ziel:** Stack/Framework festzuschreiben. Das Manifest ist **technologie‑agnostisch**.

---

## 1) Prinzipien (Non‑Negotiables, außer per ADR)
1. **Correctness > Cleverness**  
2. **Small Batches / Inkrementell** (kurze „Bolts“ statt großer Sprints) fileciteturn0file0  
3. **Reproduzierbarkeit by Default** (Build/Tests/Run‑Artefakte deterministisch)  
4. **Security & Data Hygiene by Default**  
5. **Observability ist ein Feature** (kein nachträgliches Add‑on) fileciteturn0file0  
6. **Fail‑Fast oder Graceful Degradation – keine naiven Fallbacks** fileciteturn0file0  
7. **Zero‑Trust gegenüber KI‑Output** (Beweise via Tests/Checks, nicht via Text) fileciteturn0file0  
8. **Explizite Entscheidungen** (ADR/Decision Log, inkl. Tradeoffs)

**MUST darf gebrochen werden**, aber nur:
- nach kurzer Diskussion,
- mit dokumentierter Entscheidung (**ADR**),
- inkl. Konsequenzen und Rückfallplan.

---

## 2) Rollen & Umgangsvertrag (Human‑in‑the‑Loop, Zero‑Trust)

### 2.1 Rollen
- **Du (Principal Systems Engineer / Reviewer / Product Owner):**  
  Anforderungen, Prioritäten, Architektur‑Entscheidungen, Risikoabwägung, Freigabe am Quality Gate.
- **KI (Implementer / Co‑Architect / Challenger):**  
  Vorschläge, Implementierung, Tests/Checks, kritisches Hinterfragen von Design‑Schwächen; stoppt bei Regelbruch.

> **Challenge aus Research:** Architekturhoheit liegt beim Menschen; KI liefert Entwürfe + Umsetzung, aber nicht die finale Architekturverantwortung. fileciteturn0file0

### 2.2 Kommunikationsregeln
- **Requirements‑first:** Ziel → Constraints → Acceptance Criteria (ACs) → Design → Code.
- **Transparenzpflicht:** Annahmen, Risiken, Alternativen, Nebenwirkungen.
- **Stop‑&‑Discuss Trigger:** MUST‑Abweichung, kritischer Pfad betroffen, Datenintegritäts‑Risiko, Security‑Risiko.
- **Jeder Bugfix**: Repro/Run‑ID + Root‑Cause‑Hypothese + Fix + Regressionstest.

### 2.3 Change‑Regeln
- Keine „Refactor‑Sweeps“ ohne funktionierende Zwischenstände. fileciteturn0file0  
- Jede Änderung liefert:
  - Zweck/Scope,
  - betroffene Komponenten,
  - ACs,
  - Tests/Checks,
  - Observability‑Impact,
  - Migration/Kompatibilität (falls Contract‑Change).

---

## 3) Kritische Pfade (höchste Strenge)
**Kritische Pfade** sind mindestens:
1) **Datenintegrität** (Schema, Constraints, Historisierung, Provenance)  
2) **Auth/Security** (Secrets, Rechte, Zugriffe)  
3) **Ingestion Core** (Fetch → Validate → Normalize → Persist → Audit)

Für kritische Pfade gilt:
- strengere Tests,
- defensive Error Handling,
- lückenlose Observability,
- klarer Rollback/Quarantäne‑Mechanismus.

---

## 4) Architektur‑Grundsätze (stack‑neutral, robust by design)

### 4.1 Clean / Layered Architecture (Default)
- **Domain** (Regeln/Modelle, pure, testbar)
- **Application** (Use‑Cases/Workflows)
- **Infrastructure** (DB/HTTP/FS/External)
- **Interfaces** (API/CLI/UI/Jobs)

**Dependency Rule:** Abhängigkeiten zeigen nach innen. fileciteturn0file0

### 4.2 Separation of Concerns & Single Responsibility
- Jede Komponente hat **eine** primäre Verantwortlichkeit.
- Schnittstellen sind klein, stabil, versionierbar.

### 4.3 Contracts & Versionierung
- DTOs/Schemas/Events: versionierbar.
- Breaking Changes nur mit:
  - Migration,
  - Backward‑Compatibility Strategie,
  - Tests,
  - ADR.

### 4.4 Design by Contract (DbC) für kritische Pfade
- Preconditions/Postconditions (oder äquivalente Validierungen) sind Pflicht.
- Contract‑Violation → **Fail‑Fast** mit definiertem Fehlercode, ohne Systemkorruption. fileciteturn0file0

---

## 5) Reliability & Resilience (Fehler sind Normalzustand)

### 5.1 Failure Taxonomy (Pflicht)
- **Transient:** Retry/Backoff möglich
- **Permanent:** Fail‑Fast
- **Partial:** Degradation / Partial Result + Status

### 5.2 Resilience Patterns (Pflicht für kritische Pfade)
- **Timeouts** überall bei I/O
- **Retry** mit Exponential Backoff + Jitter
- **Circuit Breaker** gegen kaskadierende Fehler
- **Bulkheads** (Ressourcen‑Isolation)
- **Idempotency** (safe re-run)
- **Load Shedding / Throttling** (planvoll, kontrolliert)

### 5.3 Fallback‑Regel (wichtig!)
- **Naive Fallbacks sind verboten**, weil sie in der Praxis latente Bugs + Kaskadenausfälle begünstigen. fileciteturn0file0  
Erlaubt sind nur:
- **Fail‑Fast** (definiert, schnell, ressourcenschonend) oder
- **Graceful Degradation** (vorab geplant, getestet, klar begrenzt). fileciteturn0file0

### 5.4 Redundanz‑Patterns (wenn relevant)
- **Endpoint Redirection** (Failover in Infrastruktur delegieren) fileciteturn0file0  
- **Hedged Requests** (Micro‑Timeout → parallele Anfrage an redundantes Backend; Tail‑Latency reduzieren) fileciteturn0file0  
- **CQRS / Read Replicas** (Skalierung & Read‑Redundanz), wenn Architekturtreiber das erfordern. fileciteturn0file0

---

## 6) Observability Standard (MELT + Run Registry)

### 6.1 Logs (Structured, maschinenlesbar)
Unstrukturierte Print‑Logs sind verboten. fileciteturn0file0

**Pflichtfelder (wo anwendbar):**
- `timestamp, level, component, event, run_id/correlation_id, outcome(ok|partial|fail), duration_ms`
- Fehler zusätzlich: `error_class(transient|permanent|partial), error_code, retry_count`

**Regeln:**
- Keine Secrets/PII in Logs.
- Exceptions mit Kontext, nicht mit Datenmüll.

### 6.2 Run Registry (Pipeline/Jobs)
Jeder Run bekommt:
- Run‑ID, Start/Ende, Outcome
- Quell‑Systeme
- Counts (ingested/validated/failed/quarantined)
- Retry‑Statistiken
- Fehler‑Synopsis

### 6.3 LLM / Agent Observability (falls Agenten im System oder Build‑Prozess)
Bei KI‑gestützten Workflows müssen zusätzlich nachvollziehbar sein:  
- Prompt/System‑Instruktionen (oder deren Hash/Version),
- Tool‑Calls,
- Guardrail‑Entscheidungen,
- Token/Kosten‑Metriken,
- Modell/Parameter. fileciteturn0file0

---

## 7) Testing & Qualitätssicherung (Beweise statt Behauptungen)

### 7.1 Testpyramide
- **Unit:** Domain/Application
- **Integration:** DB/HTTP/FS/External
- **E2E:** wenige, kritische Flows

### 7.2 Negativtests (Pflicht für kritische Pfade)
- invalid input / schema violations
- transient failure path (retry)
- permanent failure path (fail‑fast)
- partial degradation path (partial result + status)

### 7.3 Regression Standard
Jeder Bugfix enthält mindestens **einen Regressionstest**.

### 7.4 Quality Gates (stufenweise)
- **Stage 1:** format + lint + unit tests
- **Stage 2:** + integration tests + security hygiene
- **Stage 3:** + coverage targets (moderat) + static analysis + perf smoke

Gates werden nur per ADR geändert.

---

## 8) Engineering Health & Source‑Analyse
- Formatter/Linter/Typechecks (wo sinnvoll)
- Komplexitätslimits & Lesbarkeit priorisieren
- Dependency‑Hygiene (Lockfiles, pinned versions)
- Regelmäßige „Health Reports“: offene Tech‑Debt, test gaps, risk hotspots

---

## 9) Dokumentation & Langzeit‑Kontinuität

### 9.1 Dokumentationspflicht
Docs sind Teil des Produkts.

### 9.2 Project State Pflicht (nach jedem Meilenstein)
**Project State Update ist Pflicht** nach jedem Meilenstein (nicht nach jeder Session).

**Meilenstein‑Trigger:**
- neues Feature end‑to‑end
- Contract‑Change (Schema/API/Event)
- neue Pipeline‑Stage / neue Datenquelle
- Runtime/Deploy‑Konzept geändert
- signifikanter Refactor abgeschlossen
- Hardening/Performance‑Stufe erreicht

### 9.3 ADR / Decision Log Pflicht
Für relevante Entscheidungen:
- Kontext
- Entscheidung
- Alternativen
- Konsequenzen
- Rollback/Exit‑Strategy

---

## 10) Security Baseline
- Secrets nie im Repo, nie in Logs
- Least privilege, klare Auth‑Flows
- Dependency Updates geplant, nicht zufällig
- Lizenz/Terms‑Risiko ist Engineering‑Risiko: Provenance dokumentieren

---

## 11) Reliability KPIs (Baseline)
Wir messen Reliability objektiv (Run Registry + Logs reichen initial).  
**Start‑Set:**
1. **Run Success Rate (RSR)** = successful_runs / total_runs  
2. **Partial Run Rate (PRR)** = partial_runs / total_runs  
3. **MTTR‑Dev** = Zeit von Fail → wieder Success  
4. **Retry Pressure Index (RPI)** = total_retries / total_runs (pro Quelle/Job)  
5. **Freshness / Data Latency** = now − latest_successful_ingest_time (pro Domäne)  
6. **Data Quality Pass Rate (DQPR)** = passed_checks / total_checks (pro Stage)

Optional später:
- Constraint violations, reconciliation error rate, change failure rate, incident mix.

**SLO‑Handling:** SLOs definieren wir pro kritischem Pfad (z. B. wöchentlich), Abweichungen erzeugen Hardening‑Work.

---

## 12) Manifest‑Änderungen
- Das Manifest ist lebendig.
- Änderungen nur bewusst (ADR), danach neuer Default.

---

## 13) Kurz‑Checkliste (für jede Implementierung durch KI)
- [ ] ACs verstanden und dokumentiert
- [ ] Kritischer Pfad? → erhöhte Strenge
- [ ] Timeouts/Retry/CircuitBreaker/Bulkhead (wo relevant)
- [ ] Structured logs + run_id/correlation_id
- [ ] Tests: Unit/Integration + Negative + Regression (bei Bugfix)
- [ ] Docs/Project State/ADR aktualisiert, falls Meilenstein oder MUST‑Abweichung
