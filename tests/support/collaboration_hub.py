from contextlib import AbstractContextManager
from functools import partialmethod
from unittest.mock import patch

from collaboration.hub_server import _HubHTTPServer


def fast_hub_shutdown() -> AbstractContextManager:
    # shutdown() waits for the real server loop's next poll before it can join the thread.
    return patch.object(_HubHTTPServer, "serve_forever", partialmethod(_HubHTTPServer.serve_forever, poll_interval=0.01))
