#!/usr/bin/env python3
"""Generate synthetic landing-zone CSVs for local ADLS upload or DBFS testing."""

from __future__ import annotations

import csv
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

THERAPIES = ["MAGN-101", "MAGN-204", "MAGN-330"]
OUT = Path(__file__).resolve().parent.parent / "local-data" / "landing"


def _patient_ids(n: int) -> list[str]:
    return [f"PAT-{i:06d}" for i in range(1, n + 1)]


def write_claims(patients: list[str], days: int = 120) -> None:
    path = OUT / "claims"
    path.mkdir(parents=True, exist_ok=True)
    start = date.today() - timedelta(days=days)
    rows = []
    for pid in patients:
        for _ in range(random.randint(2, 8)):
            fill = start + timedelta(days=random.randint(0, days))
            rows.append(
                {
                    "claim_id": str(uuid.uuid4()),
                    "patient_id": pid,
                    "provider_npi": f"{random.randint(1000000000, 1999999999)}",
                    "ndc": f"00000-{random.randint(1000, 9999)}",
                    "fill_date": fill.isoformat(),
                    "days_supply": random.choice([28, 30, 90]),
                    "paid_amount": round(random.uniform(500, 8500), 2),
                }
            )
    _write_csv(path / f"claims_{datetime.now(UTC):%Y%m%d_%H%M%S}.csv", rows)


def write_specialty_rx(patients: list[str]) -> None:
    path = OUT / "specialty_rx"
    path.mkdir(parents=True, exist_ok=True)
    rows = []
    for pid in patients:
        therapy = random.choice(THERAPIES)
        for _ in range(random.randint(1, 5)):
            rows.append(
                {
                    "rx_id": str(uuid.uuid4()),
                    "patient_id": pid,
                    "therapy_code": therapy,
                    "ship_date": (date.today() - timedelta(days=random.randint(1, 90))).isoformat(),
                    "quantity": random.randint(1, 3),
                    "hub_status": random.choice(["SHIPPED", "DELAYED", "CANCELLED"]),
                }
            )
    _write_csv(path / f"specialty_rx_{datetime.now(UTC):%Y%m%d_%H%M%S}.csv", rows)


def write_prior_auth(patients: list[str]) -> None:
    path = OUT / "prior_auth"
    path.mkdir(parents=True, exist_ok=True)
    rows = []
    for pid in random.sample(patients, k=len(patients) // 3):
        submitted = datetime.now(UTC) - timedelta(days=random.randint(1, 180))
        rows.append(
            {
                "auth_id": str(uuid.uuid4()),
                "patient_id": pid,
                "therapy_code": random.choice(THERAPIES),
                "status": random.choice(["APPROVED", "DENIED", "PENDING"]),
                "submitted_at": submitted.isoformat(),
                "decided_at": (submitted + timedelta(days=random.randint(1, 14))).isoformat(),
            }
        )
    _write_csv(path / f"prior_auth_{datetime.now(UTC):%Y%m%d_%H%M%S}.csv", rows)


def write_crm(patients: list[str]) -> None:
    path = OUT / "crm"
    path.mkdir(parents=True, exist_ok=True)
    rows = []
    for pid in random.sample(patients, k=len(patients) // 2):
        rows.append(
            {
                "interaction_id": str(uuid.uuid4()),
                "patient_id": pid,
                "channel": random.choice(["PHONE", "EMAIL", "SMS"]),
                "outcome": random.choice(["REACHED", "NO_ANSWER", "SCHEDULED"]),
                "interaction_at": (datetime.now(UTC) - timedelta(days=random.randint(0, 45))).isoformat(),
            }
        )
    _write_csv(path / f"crm_{datetime.now(UTC):%Y%m%d_%H%M%S}.csv", rows)


def write_inventory() -> None:
    path = OUT / "inventory"
    path.mkdir(parents=True, exist_ok=True)
    rows = []
    for therapy in THERAPIES:
        rows.append(
            {
                "sku": f"SKU-{therapy}",
                "therapy_code": therapy,
                "site_id": random.choice(["DFW-01", "ATL-02", "PHX-03"]),
                "on_hand": random.randint(50, 500),
                "as_of_date": date.today().isoformat(),
            }
        )
    _write_csv(path / f"inventory_{datetime.now(UTC):%Y%m%d_%H%M%S}.csv", rows)


def _write_csv(file_path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {file_path}")


def main() -> None:
    random.seed(42)
    patients = _patient_ids(500)
    write_claims(patients)
    write_specialty_rx(patients)
    write_prior_auth(patients)
    write_crm(patients)
    write_inventory()
    print(f"Landing files under {OUT}")


if __name__ == "__main__":
    main()
