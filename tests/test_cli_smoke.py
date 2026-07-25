from metricstash.cli import build_parser


def test_cli_exposes_top_level_commands() -> None:
    parser = build_parser()
    commands = parser._subparsers._group_actions[0].choices
    assert {"validate", "collect", "query", "inspect", "prune", "db"} <= commands.keys()
