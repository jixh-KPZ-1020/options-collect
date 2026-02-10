"""Graceful shutdown handler for long-running pipelines.

First Ctrl+C sets a cooperative flag — the main loop finishes the
current file and exits cleanly. Second Ctrl+C raises KeyboardInterrupt
for immediate termination.
"""

from __future__ import annotations

import signal
import threading
from types import FrameType


class GracefulShutdown:
    """Cooperative shutdown manager.

    Usage::

        shutdown = GracefulShutdown()
        shutdown.install()

        for file in files:
            if shutdown.requested:
                print("Shutting down after current file...")
                break
            process(file)

        shutdown.uninstall()
    """

    def __init__(self) -> None:
        self._requested = threading.Event()
        self._original_sigint: signal.Handlers | None = None
        self._original_sigterm: signal.Handlers | None = None

    def install(self) -> None:
        """Install signal handlers. Call once at startup."""
        self._original_sigint = signal.getsignal(signal.SIGINT)
        self._original_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def uninstall(self) -> None:
        """Restore original signal handlers."""
        if self._original_sigint is not None:
            signal.signal(signal.SIGINT, self._original_sigint)
        if self._original_sigterm is not None:
            signal.signal(signal.SIGTERM, self._original_sigterm)

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        """Signal handler — sets the flag on first call.

        On second signal, restores the original handler so the default
        behavior (KeyboardInterrupt / termination) takes over.
        """
        if not self._requested.is_set():
            self._requested.set()
            print(
                "\nShutdown requested. Finishing current file... "
                "(press Ctrl+C again to force quit)"
            )
            # On next signal, use original handler (force quit)
            signal.signal(signum, self._original_sigint or signal.SIG_DFL)
        else:
            # Already requested — let default behavior through
            original = self._original_sigint or signal.SIG_DFL
            if callable(original):
                original(signum, frame)

    @property
    def requested(self) -> bool:
        return self._requested.is_set()
