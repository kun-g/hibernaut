from __future__ import annotations

import os
from collections.abc import Mapping

from switchboard.config import SwitchboardConfig
from switchboard.credentials import EnvCredentialBroker, feishu_app_credentials, github_app_credentials

_CREDENTIAL_PAIRS = (
    ("SWITCHBOARD_GITHUB_APP_ID", "SWITCHBOARD_GITHUB_APP_PRIVATE_KEY"),
    ("SWITCHBOARD_FEISHU_APP_ID", "SWITCHBOARD_FEISHU_APP_SECRET"),
)


# What parse_config lets through but --check must not: settings startup silently ignores because the one they
# depend on is missing, and a relay port that cannot be bound next to [http].
def config_problems(config: SwitchboardConfig, environ: Mapping[str, str] | None = None) -> list[str]:
    env = os.environ if environ is None else environ
    delivery, outbound, relay = config.delivery, config.outbound, config.relay
    problems: list[str] = []

    def both(first: object, first_name: str, second: object, second_name: str) -> None:
        if bool(first) != bool(second):
            present, missing = (first_name, second_name) if first else (second_name, first_name)
            problems.append(f"{missing} is missing but {present} is set")

    both(
        delivery.dead_letter_alert_channel,
        "delivery.dead_letter_alert_channel",
        delivery.dead_letter_alert_target,
        "delivery.dead_letter_alert_target",
    )
    if outbound.notify_channel and not outbound.notify_target:
        problems.append("outbound.notify_target is missing but outbound.notify_channel is set")
    if relay.listen_host and relay.listen_port is None:
        problems.append("relay.listen_port is missing but relay.listen_host is set")
    if (
        relay.enabled
        and relay.listen_port
        and relay.listen_port == config.http.port
        and (relay.listen_host or config.http.host) == config.http.host
    ):
        problems.append(f"relay.listen_port {relay.listen_port} is already http.port on {config.http.host}")
    for first_name, second_name in _CREDENTIAL_PAIRS:
        both(env.get(first_name, "").strip(), first_name, env.get(second_name, "").strip(), second_name)
    return problems


def config_summary(config: SwitchboardConfig, environ: Mapping[str, str] | None = None) -> list[str]:
    http, routing, relay = config.http, config.routing, config.relay
    rules = sum(
        len(table)
        for table in (routing.github_repos, routing.github_owners, routing.feishu_chats, routing.event_consumers)
    )
    broker = EnvCredentialBroker(environ)
    inbound = [name for name in ("github", "feishu", "mcp") if broker.get_webhook_secret(name)]
    outbound = [
        name
        for name, app in (("github", github_app_credentials(environ)), ("feishu", feishu_app_credentials(environ)))
        if app is not None or broker.issue_ephemeral(name)
    ]
    channels = [f"inbound {', '.join(inbound) or 'none'}", f"outbound {', '.join(outbound) or 'none'}"]
    if relay.enabled:
        secret = "" if broker.get_webhook_secret("relay") else ", no credentials"
        channels.append(f"relay ({relay.transport}{secret})")
    listen = f"{http.host}:{http.port}{http.base_path}"
    if relay.enabled and relay.listen_port is not None:
        listen += f", relay {relay.listen_host or http.host}:{relay.listen_port}"
    return [
        f"listen: {listen}",
        f"routing rules: {rules} (unmatched_policy={routing.unmatched_policy})",
        f"channels: {'; '.join(channels)}",
    ]
