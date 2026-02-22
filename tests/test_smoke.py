import pytest
from inga_quant.cli import main


def test_main_no_command_exits(capsys):
    """CLI with no subcommand should print help and exit non-zero."""
    with pytest.raises(SystemExit) as exc_info:
        main(argv=[])
    assert exc_info.value.code != 0
