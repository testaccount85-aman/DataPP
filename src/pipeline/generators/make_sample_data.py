"""Generate synthetic sample data for the pipeline."""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT / "data"
SEED_DIR = DATA_DIR / "seed"

USERS = [f"user_{i:03d}" for i in range(1, 51)]
CAMPAIGNS = [f"campaign_{i:02d}" for i in range(1, 9)]
MESSAGES = [
    "Loved the latest offers!",
    "Interested in travel deals",
    "Looking for eco-friendly products",
    "Flash sale emails are great",
    "Interested in loyalty rewards",
    "New arrivals caught my eye",
    "Remind me about expiring coupons",
    "Personalize tech recommendations",
]


def _random_timestamp(start: datetime, end: datetime) -> str:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    ts = start + timedelta(seconds=seconds)
    return ts.replace(microsecond=0).isoformat() + "Z"


def generate_conversations(records: int = 300) -> list[dict[str, str]]:
    now = datetime.utcnow()
    start = now - timedelta(days=30)
    data: list[dict[str, str]] = []
    for idx in range(1, records + 1):
        user = random.choice(USERS)
        campaign = random.choice(CAMPAIGNS)
        message = random.choice(MESSAGES)
        timestamp = _random_timestamp(start, now)
        data.append(
            {
                "user_id": user,
                "campaign_id": campaign,
                "message": message,
                "timestamp": timestamp,
                "message_id": f"msg_{idx:05d}",
            }
        )
    return data


def write_csv(path: Path, rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows_list = list(rows)
    if not rows_list:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows_list[0].keys())
        writer.writeheader()
        writer.writerows(rows_list)


def main() -> None:
    random.seed(42)

    conversations = generate_conversations()
    write_csv(DATA_DIR / "conversations.csv", conversations)

    write_csv(SEED_DIR / "users.csv", [{"user_id": user} for user in USERS])
    write_csv(SEED_DIR / "campaigns.csv", [{"campaign_id": campaign} for campaign in CAMPAIGNS])
    print(f"Generated {len(conversations)} conversations at {DATA_DIR / 'conversations.csv'}")


if __name__ == "__main__":
    main()
