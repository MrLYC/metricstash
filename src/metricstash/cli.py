"""Command-line entry point for Metricstash."""

from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level command parser."""
    parser = argparse.ArgumentParser(prog="metricstash")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("validate")
    subparsers.add_parser("collect")
    subparsers.add_parser("query")
    inspect_parser = subparsers.add_parser("inspect")
    inspect_subparsers = inspect_parser.add_subparsers(dest="inspect_command")
    inspect_subparsers.add_parser("histogram")
    inspect_subparsers.add_parser("summary")
    subparsers.add_parser("prune")
    db_parser = subparsers.add_parser("db")
    db_parser.add_subparsers(dest="db_command").add_parser("migrate")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse command-line arguments.

    Later implementation tasks bind concrete command handlers to this parser.
    """
    build_parser().parse_args(argv)
    return 0
