from __future__ import annotations

import json
import unittest

from switchboard.credentials import feishu_app_credentials
from switchboard.feishu_auth import FeishuAuthError, FeishuTenantTokenSource
from switchboard.http_client import HttpResponse
from switchboard.outbound.feishu_adapter import FeishuOutboundAdapter
from switchboard.ports import OutboundMessage
from tests.support.switchboard_support import FakeHttpClient, StaticCredentials

TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"


def _granted(token: str, expire: int = 7200) -> HttpResponse:
    return HttpResponse(200, json.dumps({"code": 0, "tenant_access_token": token, "expire": expire}).encode())


def _sent() -> HttpResponse:
    return HttpResponse(200, json.dumps({"code": 0, "data": {"message_id": "om_1"}}).encode())


def _message() -> OutboundMessage:
    return OutboundMessage(
        msg_id="m1",
        idempotency_key="k1",
        channel="feishu",
        kind="reply",
        target={"chat_id": "oc_1"},
        correlation_key=None,
        title="",
        body_md="hi",
    )


class FeishuTenantTokenSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = 1_800_000_000.0
        self.http = FakeHttpClient()
        self.source = FeishuTenantTokenSource(
            self.http, app_id="cli_1", app_secret="s3cret", now=lambda: self.clock
        )

    def test_mints_with_app_credentials_and_reuses_until_ten_minutes_before_expiry(self) -> None:
        self.http.responses.append(_granted("t-one"))
        self.assertEqual(self.source.token(), "t-one")
        self.assertEqual(self.http.calls[0]["url"], TOKEN_URL)
        self.assertEqual(self.http.calls[0]["json"], {"app_id": "cli_1", "app_secret": "s3cret"})
        self.clock += 6500
        self.assertEqual(self.source.token(), "t-one")
        self.clock += 200
        self.http.responses.append(_granted("t-two"))
        self.assertEqual(self.source.token(), "t-two")
        self.assertEqual(len(self.http.calls), 2)

    def test_wrong_secret_is_not_retryable_and_5xx_is(self) -> None:
        self.http.responses.append(HttpResponse(200, b'{"code":10014,"msg":"app secret invalid"}'))
        with self.assertRaises(FeishuAuthError) as wrong:
            self.source.token()
        self.assertFalse(wrong.exception.retryable)
        self.http.responses.append(HttpResponse(503, b"busy"))
        with self.assertRaises(FeishuAuthError) as flaky:
            self.source.token()
        self.assertTrue(flaky.exception.retryable)

    def test_rate_limit_sent_as_http_400_is_retryable_and_other_400s_are_not(self) -> None:
        self.http.responses.append(HttpResponse(400, b'{"code":99991400,"msg":"request trigger frequency limit"}'))
        with self.assertRaises(FeishuAuthError) as limited:
            self.source.token()
        self.assertTrue(limited.exception.retryable)
        for body in (b'{"code":10003,"msg":"invalid param"}', b'{"code":["odd"]}'):
            self.http.responses.append(HttpResponse(400, body))
            with self.assertRaises(FeishuAuthError) as wrong:
                self.source.token()
            self.assertFalse(wrong.exception.retryable)


class FeishuOutboundWithAppTokenTests(unittest.TestCase):
    def setUp(self) -> None:
        self.http = FakeHttpClient()
        self.source = FeishuTenantTokenSource(self.http, app_id="cli_1", app_secret="s3cret")
        self.adapter = FeishuOutboundAdapter(self.http, StaticCredentials(), tokens=self.source)

    def test_sends_with_the_minted_token(self) -> None:
        self.http.responses += [_granted("t-one"), _sent()]
        result = self.adapter.send(_message())
        self.assertTrue(result.ok)
        self.assertEqual(self.http.calls[-1]["headers"]["Authorization"], "Bearer t-one")

    def test_a_rejected_token_is_dropped_and_the_retry_mints_a_new_one(self) -> None:
        self.http.responses += [
            _granted("t-old"),
            HttpResponse(400, b'{"code":99991663,"msg":"Invalid access token"}'),
            _granted("t-new"),
            _sent(),
        ]
        first = self.adapter.send(_message())
        self.assertFalse(first.ok)
        self.assertTrue(first.retryable)
        self.assertTrue(self.adapter.send(_message()).ok)
        self.assertEqual(self.http.calls[-1]["headers"]["Authorization"], "Bearer t-new")

    def test_auth_failure_becomes_a_send_result(self) -> None:
        self.http.responses.append(HttpResponse(200, b'{"code":10014,"msg":"app secret invalid"}'))
        result = self.adapter.send(_message())
        self.assertFalse(result.ok)
        self.assertFalse(result.retryable)


class FeishuCredentialTests(unittest.TestCase):
    def test_needs_both_id_and_secret(self) -> None:
        env = {"SWITCHBOARD_FEISHU_APP_ID": " cli_1 ", "SWITCHBOARD_FEISHU_APP_SECRET": "s3cret\n"}
        self.assertEqual(feishu_app_credentials(env), ("cli_1", "s3cret"))
        self.assertIsNone(feishu_app_credentials({"SWITCHBOARD_FEISHU_APP_ID": "cli_1"}))


if __name__ == "__main__":
    unittest.main()
