from pathlib import Path


def test_interpreter_receives_scoped_openai_credential_from_compose_env() -> None:
    compose = (Path(__file__).parents[2] / "compose.yaml").read_text()
    interpreter = compose.split("  interpreter:\n", maxsplit=1)[1]
    assert "VUZOL_OPENAI_INTERPRETER_API_KEY: ${VUZOL_OPENAI_INTERPRETER_API_KEY:-}" in interpreter
    assert (
        "VUZOL_OPENAI_TRANSCRIPTION_API_KEY: ${VUZOL_OPENAI_TRANSCRIPTION_API_KEY:-}" in interpreter
    )
    assert 'user: "${VUZOL_RUNTIME_UID:-1000}:10001"' in interpreter
    assert (
        "chmod 0770 /srv/vuzol/artifacts" in (Path(__file__).parents[2] / "Dockerfile").read_text()
    )
