"""What a provider adapter can actually constrain.

``max_input_tokens`` and a reserved estimate are not a ceiling on a whole
CLI session. Subscription tokens are not priced here as API money.
"""

from vuzol.config.models import LaunchMode, ProviderProfileConfig


def ceiling_is_admission_only(profile: ProviderProfileConfig) -> bool:
    """True when the process can outspend the reservation after it starts."""

    return profile.launch_mode in {LaunchMode.CLI, LaunchMode.TOOL}


