"""The CLI wires every subcommand to its entry point."""

import pytest

from wslive import cli


@pytest.mark.parametrize("command", ["serve", "feed", "bench", "sweep", "report"])
def test_every_subcommand_has_an_entry_point(command: str) -> None:
    parser = cli.build_parser()
    action = next(a for a in parser._actions if a.dest == "command")
    args = action.choices[command].parse_args([])
    assert callable(args.func)


def test_unknown_command_exits_with_usage() -> None:
    with pytest.raises(SystemExit) as exc:
        cli.main(["nope"])
    assert exc.value.code == 2


def test_report_refuses_without_a_sweep(tmp_path) -> None:
    code = cli.main(
        ["report", "--sweep", str(tmp_path / "missing.jsonl"), "--out", str(tmp_path / "r.md")]
    )
    assert code != 0
