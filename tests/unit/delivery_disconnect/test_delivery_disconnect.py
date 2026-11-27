import subprocess

import pytest

from agent_runtime.adapters.delivery.pr_delivery import _remote_problem, _TransientRemoteError


@pytest.mark.parametrize(("stderr", "retry"), [
    ('Post "https://api.github.com/graphql": EOF\n', True),
    ("HTTP request failed: EOF", True),
    ("fatal: unexpected EOF", True),
    ("ssh: Connection closed by remote host", True),
    ("write: broken pipe", True),
    ("GraphQL: Something went wrong while executing your query on 2026-11-27T11:24:45Z. "
     "Please include `REQUEST-ID` when reporting this issue.", True),
    ("fatal: unable to access 'https://github.com/example/repo.git/': "
     "LibreSSL SSL_connect: SSL_ERROR_SYSCALL in connection to github.com:443", True),
    ("GraphQL: Resource not accessible by integration", False),
    ("GraphQL: Field 'checks' doesn't exist on type 'Query'", False),
    ("SSL certificate problem: unable to get local issuer certificate", False),
    ("HTTP 403: Resource not accessible by integration", False),
    ("invalid configuration: EOF marker missing", False),
    ("failed to read local configuration: EOF", False),
])
def test_remote_error_is_eligible_for_automatic_retry(stderr, retry):
    result = subprocess.CompletedProcess(["gh", "pr", "checks"], 1, stderr=stderr)
    assert isinstance(_remote_problem("gh pr checks failed", result), _TransientRemoteError) is retry
