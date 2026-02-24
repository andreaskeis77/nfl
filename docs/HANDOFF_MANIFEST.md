# Handoff Bundle Manifest (NFL RAG DB)

Dieses Dokument beschreibt, was `tools/handoff_make.py` erzeugt.

## Output
- `docs/_snapshot/handoff_YYYYMMDD-HHMMSS/` (neuer Snapshot)
- `docs/_snapshot/latest/` (Kopie des letzten Snapshots)
- optional `docs/_snapshot/handoff_YYYYMMDD-HHMMSS.zip`

## Enthaltene Artefakte (Snapshot)
- project_tree.txt
- file_catalog.md
- source_bundle.md
- project_audit_dump.md (safe defaults: redaction, excludes)
- data_snapshot.md
- db_snapshot.md
- runtime_state.md
- handoff_summary.md
- Kopien von docs/: adr, concepts, reference (inkl. Engineering Manifest)

## Hinweis
Der Snapshot ist zum Teilen/Upload gedacht und sollte i. d. R. nicht committed werden.