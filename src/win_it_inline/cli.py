from __future__ import annotations

import argparse

from .pipeline import run_pipeline
from .settings import DATA_LOOKBACK_DAYS
from .settings import DEFAULT_DATASOURCE
from .settings import DEFAULT_OUTPUT


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the CDT/GTO EPD inline analysis pipeline.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="CSV output path.")
    parser.add_argument("--datasource", default=DEFAULT_DATASOURCE, help="PyUber datasource name.")
    parser.add_argument("--lookback-days", type=int, default=DATA_LOOKBACK_DAYS, help="Number of days to query.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    destination = run_pipeline(
        output_path=args.output,
        datasource=args.datasource,
        lookback_days=args.lookback_days,
    )
    print(f"Wrote report to {destination}")
    return 0