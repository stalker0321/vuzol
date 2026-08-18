from vuzol.projects.artifact_contracts import expected_artifacts


def test_expected_artifacts_are_typed_for_web_android_and_mcp() -> None:
    artifacts = expected_artifacts(
        {
            "components": {
                "android": {
                    "kind": "android_app",
                    "label": "Phone app",
                    "artifact_patterns": ["**/*.apk"],
                },
                "api": {"kind": "web_service", "label": "API"},
                "tools": {"kind": "mcp_server", "label": "Tools"},
            }
        }
    )

    assert [(artifact.component_key, artifact.kind) for artifact in artifacts] == [
        ("android", "android_apk"),
        ("api", "web_preview"),
        ("tools", "mcp_conformance_report"),
    ]
    assert artifacts[0].patterns == ("**/*.apk",)


def test_unknown_component_uses_validation_report() -> None:
    artifact = expected_artifacts(
        {"components": {"device": {"kind": "future_device", "label": "Device"}}}
    )[0]

    assert artifact.kind == "validation_report"


def test_artifact_contract_ignores_malformed_components_and_defaults_labels() -> None:
    assert expected_artifacts({}) == ()
    assert expected_artifacts({"components": []}) == ()

    artifacts = expected_artifacts(
        {
            "components": {
                "bad": [],
                "cli": {"kind": "cli", "artifact_patterns": ["dist/*", 7]},
                "library": {"kind": "library", "artifact_patterns": "dist/*"},
            }
        }
    )

    assert [(item.label, item.kind, item.patterns) for item in artifacts] == [
        ("cli", "cli_transcript", ("dist/*",)),
        ("library", "package_archive", ()),
    ]
