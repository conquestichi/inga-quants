from inga_quants.cli import main

def test_main(capsys):
    main()
    assert "inga-quants ok" in capsys.readouterr().out
