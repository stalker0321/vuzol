from vuzol.telegram.topic_line import account_alias, line_revision, model_aliases


def test_short_names_add_a_version_only_when_a_family_has_two() -> None:
    one = model_aliases(["deepseek-v4.1-flash", "muse-spark-1.3-contributor", "gpt-6.1-sol"])
    assert one == {
        "deepseek-v4.1-flash": "ds",
        "muse-spark-1.3-contributor": "muse",
        "gpt-6.1-sol": "sol",
    }
    two = model_aliases(["deepseek-v4.1-flash", "deepseek-v4.2"])
    assert two == {"deepseek-v4.1-flash": "ds-4.1f", "deepseek-v4.2": "ds-4.2"}


def test_accounts_shrink_to_provider_and_letter() -> None:
    assert account_alias("pi-opencode-go-c") == "oc-c"
    assert account_alias("pi-opencode-go-c-muse") == "oc-c"
    assert account_alias("codex-a") == "cx-a"


def test_the_phone_sees_the_whole_line() -> None:
    # About 30 characters fit in the pinned preview on a phone.
    assert len("🧭 Лид думает · muse·oc-c") <= 30
    assert line_revision("💤 Свободен") == line_revision("💤 Свободен") > 0
