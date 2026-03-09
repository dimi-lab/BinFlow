#!/usr/bin/env python3
import argparse
import sys

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract only a single label column from a TSV file."
    )
    parser.add_argument("input_table", help="Input TSV table")
    parser.add_argument("output_table", help="Output TSV table with only the label column")
    parser.add_argument("label_column", help="Label column to retain")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        df = pd.read_csv(args.input_table, sep="\t", usecols=[args.label_column], low_memory=True)
    except ValueError as exc:
        print(
            f"Error: column '{args.label_column}' was not found in {args.input_table}: {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:
        print(f"Error reading {args.input_table}: {exc}", file=sys.stderr)
        return 1

    df.to_csv(args.output_table, sep="\t", index=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
