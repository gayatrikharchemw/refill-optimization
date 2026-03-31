# Refill Optimization

Internal tooling for tracking and reporting on Humana refill reminder outreach campaigns. Pulls data from CMAPP, MongoDB, NICE CXone, and the Outreach platform to generate weekly/daily reports and send automated email alerts.

---

## Project Structure

```
refill-optimization/
├── config/
│   └── humana_refill_etl_prod_config.yml   # Production config (paths, credentials, email recipients)
├── scripts/
│   ├── refill_reminder_result_summary.py   # Daily YTD summary + email alerts
│   ├── pharmacy_request_report.py          # Pharmacy request analysis + clerk performance
│   ├── generate_weekly_report.py           # Weekly refill metrics by contract/star rating
│   ├── get_nice_data.py                    # NICE CXone skill metrics
│   └── get_never_answered.py               # Pickup rate analysis for phone numbers
├── utils/
│   ├── config_utils.py                     # Config loading and client factories
│   ├── email_utils.py                      # Email alert rendering and sending
│   ├── file_utils.py                       # CSV/S3 I/O, phone number normalization
│   ├── date_util.py                        # PST timezone helpers
│   ├── nice_utils.py                       # NICE CXone API queries
│   ├── fields.py                           # Disposition mappings and constants
│   └── reporting/
│       ├── daily.py                        # CMAPP report downloads
│       ├── refill_summary.py               # Refill metric calculations + agent flagging
│       └── weekly.py                       # Weekly lead and disposition processing
└── pyproject.toml
```

---

## Scripts

### `refill_reminder_result_summary.py`

Downloads the YTD Humana Refill Report from CMAPP and generates four CSV reports along with automated email alerts.

```bash
uv run scripts/refill_reminder_result_summary.py config/humana_refill_etl_prod_config.yml
uv run scripts/refill_reminder_result_summary.py config/humana_refill_etl_prod_config.yml --etl-dir s3://bucket/path/
```

**Outputs (written to `paths.reports` in config):**
- `result_summary.csv` — weekly accepted/declined/not-completed lead counts
- `decline_reason_counts.csv` — breakdown of decline reasons by week
- `submission_result_counts.csv` — refill submission disposition breakdown with ratios
- `agent_report.csv` — YTD agent performance with performance flags

---

### `pharmacy_request_report.py`

Downloads the YTD Refill Pharmacy Request Report and produces a summary plus a daily clerk-level performance report. Filters data from March 1, 2026 onwards.

```bash
uv run scripts/pharmacy_request_report.py config/humana_refill_etl_prod_config.yml
uv run scripts/pharmacy_request_report.py config/humana_refill_etl_prod_config.yml --date 2026-03-15
```

**Outputs:**
- `pharmacy_request_summary.csv` — YTD disposition breakdown (AI vs. agent, completion %)
- `pharmacy_clerk_daily.csv` — per-clerk daily performance for the given date (defaults to yesterday)

---

### `generate_weekly_report.py`

Combines original lead files, ETL-processed lead files, and CMAPP call dispositions into a weekly report split by contract and star rating.

```bash
uv run scripts/generate_weekly_report.py config/humana_refill_etl_prod_config.yml
uv run scripts/generate_weekly_report.py config/humana_refill_etl_prod_config.yml --date 2026-03-21
uv run scripts/generate_weekly_report.py config/humana_refill_etl_prod_config.yml --ytd
uv run scripts/generate_weekly_report.py config/humana_refill_etl_prod_config.yml --contract_splits S5884:Stars S5552:Both
```

**Outputs:** CSV files under `reports/weekly/<date>/` organized by contract and star split.

**Metrics calculated:**
1. Leads Received
2. Leads Available for Outreach (post-scrub)
3. Attempted Leads + Attempt Rate
4. Available for 2nd Attempts + 2nd Attempt Rate
5. Reached Leads + Reach Rate
6. Refill Submitted
7. Reminded – Refilled on Own
8. Refill Request
9. Scrub Rate
10. Refill rates as % of Attempted/Reached

---

### `get_nice_data.py`

Retrieves NICE CXone skill performance metrics for a given date range.

```bash
uv run scripts/get_nice_data.py config/humana_refill_etl_prod_config.yml \
  --skill "SKILL-NAME" --start_datetime "2026-01-01" --end_datetime "2026-01-09"
```

**Metrics returned:** pickup rate, conversion rate, completion rate, abandon rate.

---

### `get_never_answered.py`

Identifies phone numbers that were never answered across 2025 and 2026 outreach data.

```bash
uv run scripts/get_never_answered.py config/humana_refill_etl_prod_config.yml \
  --input s3://bucket/numbers.csv --output results.csv
```

Input CSV must have a `number` column. Output is sorted by pickup rate ascending.

---

## Notifications & Alerts

All alerts are sent via email. Recipients and thresholds are configured in the config YAML under `email:`.

| Alert | Trigger Condition | Recipients Config Key |
|---|---|---|
| Weekly Metric Summary | Runs every time `refill_reminder_result_summary.py` runs | `email.to` |
| No Post Completion Workflow | Count > 10 in current week | `email.no_post_completion_to` |
| Member Does Not Want Refill | Rate > 25% in current week | `email.to` |
| Agent Performance Review | Flagged agents with 100+ completed cases and acceptance rate < 84% | `email.agent_performance_to` |
| Weekly Case Completion Summary | Runs every time `refill_reminder_result_summary.py` runs | `email.to` |

### Weekly Metric Summary

Compares the current week vs. prior week for three KPIs and shows the delta:
- Refill Submission Rate (%)
- Reminded – Refilled on Own Rate (%)
- Ratio of (Refilled on Own + No Post Completion) : (Refill Submitted + Out of Refill)

### Agent Performance Flags

Agents with 100+ completed cases are evaluated YTD. Flagged agents receive one of three flags:

| Flag | Condition | Recommendation |
|---|---|---|
| A | High acceptance + low refill submissions | Improve refill submission rate during calls |
| B | Low acceptance + low refill submissions | Highlight for performance improvement/training |
| C | Low acceptance + high "Member Does Not Want Refill" | Improve objection handling & member communication |

Acceptance rate threshold: **84%**

---

## Configuration

The config YAML (`config/humana_refill_etl_prod_config.yml`) controls:

| Section | Purpose |
|---|---|
| `fs` / `paths` | Storage backend (`s3` or `local`) and output directories |
| `cmapp` | CMAPP host, client name, vault path for credentials |
| `mongodb` | MongoDB URI and vault path for read-only credentials |
| `nice` | Vault path for NICE CXone API credentials |
| `outreach` | Skill IDs (English/Spanish), API host, PostgreSQL connection |
| `notifications.email` | ETL alert from/to addresses |
| `email` | Per-alert recipient lists and sender/recipient display names |

Credentials are fetched at runtime from **MedWatchers Vault** — no secrets are stored in the repo.

---

## Setup

```bash
# Install dependencies
uv sync

# Run any script
uv run scripts/<script_name>.py config/humana_refill_etl_prod_config.yml
```

**Requirements:** Python >= 3.13, `uv`, AWS credentials configured, Vault access.

**Key dependencies:**
- `pandas` — data processing
- `cmappclient` — CMAPP report downloads
- `cmappmongo` — MongoDB + CMAPP integration
- `niceclient` — NICE CXone API
- `outreach-api-client` — Outreach platform API
- `mw-vault` — secrets management
- `mwemailer` — email sending
- `s3path`, `boto3` — S3 storage
- `psycopg2-binary`, `sqlalchemy` — PostgreSQL (Outreach DB)

---

## Integrations

| System | Purpose |
|---|---|
| CMAPP | Source of refill reminder and pharmacy request reports |
| MongoDB | Case registry and member data |
| PostgreSQL (Outreach) | Call center contact logs and pickup rate data |
| NICE CXone | Contact center skill performance metrics |
| AWS S3 | Remote file storage for ETL files and report output |
| MedWatchers Vault | Runtime credential retrieval |
