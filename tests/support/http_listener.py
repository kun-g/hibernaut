from socketserver import BaseServer


def serve_with_short_poll(server: BaseServer, poll_interval: float) -> None:
    # shutdown() wakes only on the next accept poll, even when all requests have finished.
    BaseServer.serve_forever(server, poll_interval=min(0.001, poll_interval))
