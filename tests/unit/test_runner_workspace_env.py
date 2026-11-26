from __future__ import annotations

import pytest

from runner.supervisor import build_child_env
from tests.support.runner_workspace_env import MARK


_PROXIES = [
    ("HTTPS_PROXY", f"http://user:{MARK}@proxy.invalid:8080"),
    ("all_proxy", f"user:{MARK}@proxy.invalid:8080"),
    # Unescaped reserved characters end a URL's authority for urlsplit, not for every proxy client.
    ("HTTP_PROXY", f"http://user:{MARK}#x@proxy.invalid:8080"),
    ("http_proxy", f"user:{MARK}/x@proxy.invalid:8080"),
    ("https_proxy", f"http://user:{MARK}?x@proxy.invalid:8080"),
    ("ALL_PROXY", f"socks5://{MARK}@proxy.invalid:1080"),
]


@pytest.mark.parametrize(("name", "value"), _PROXIES)
def test_child_env_refuses_a_proxy_with_credentials_naming_only_the_variable(name: str, value: str) -> None:
    with pytest.raises(PermissionError) as err:
        build_child_env({"PATH": "/usr/bin:/bin", name: value})
    assert name in str(err.value) and MARK not in str(err.value)
    with pytest.raises(PermissionError) as err:
        build_child_env({"PATH": "/usr/bin:/bin"}, extra_env={name: value})
    assert name in str(err.value) and MARK not in str(err.value)


def test_child_env_keeps_a_proxy_without_credentials() -> None:
    env = build_child_env({"PATH": "/usr/bin", "HTTPS_PROXY": "http://127.0.0.1:7890", "NO_PROXY": "localhost,.example"})
    assert env["HTTPS_PROXY"] == "http://127.0.0.1:7890" and env["NO_PROXY"] == "localhost,.example"
