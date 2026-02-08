# FeedsBarServer v2

Cloud-based ingestion and background processing for FeedsBar.

## Architecture (v2)

- Ingestion is handled by **Google Cloud Run**
- Worker service: `feeds-worker`
- Triggered every 5 minutes via **Cloud Scheduler**
- Runs authenticated via OIDC
- Netlify Functions for ingestion are deprecated and disabled

## Key Endpoints

- `POST /jobs/ingest` – run ingestion batch
- `GET /health` – liveness check

## Source of Truth

- Repo: `FeedsBarServer_v2`
- Baseline tag: `ingestion-v2-baseline`
- Production tag: `ingestion-v2-prod`

## Notes

- Do NOT re-enable Netlify ingestion functions
- Ingestion is designed to be idempotent
- Duplicate items are handled via DB constraints

## Orb Pipeline v1
- Velocity now computed from most recent prior window
- Time-normalized per-hour velocity added
- Snapshot velocity capped for UI stability
- Label cadence tied to orb_labels.generated_at
- Label promotion semantics hardened

