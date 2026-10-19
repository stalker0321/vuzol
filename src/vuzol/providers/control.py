"""What a provider adapter can actually constrain.

``max_input_tokens`` and a reserved estimate are not a ceiling on a whole
CLI session. Subscription tokens are not priced here as API money.
"""

from vuzol.config.models import LaunchMode, ProviderProfileConfig


def ceiling_is_admission_only(profile: ProviderProfileConfig) -> bool:
    """True when the process can outspend the reservation after it starts."""

    return profile.launch_mode in {LaunchMode.CLI, LaunchMode.TOOL}


def ceiling_statement(profile: ProviderProfileConfig) -> str:
    """Owner-facing limit of the adapter, checked before launch."""

    if ceiling_is_admission_only(profile):
        return (
            "This adapter can be refused before launch and its process can be "
            "stopped. It cannot cap tokens of a running session. An explicit "
            "money ceiling checked at admission is not a promise to the cent."
        )
    return (
        "This adapter admits one inference. Per-call context and output limits "
        "apply to that request. A cumulative session total is not a property "
        "of the call."
    )
