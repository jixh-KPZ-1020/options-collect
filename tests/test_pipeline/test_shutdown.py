"""Tests for graceful shutdown signal handling."""

from __future__ import annotations

import os
import signal
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from _options_pipeline.shutdown import GracefulShutdown


class TestGracefulShutdown:
    """Test signal handler installation and behavior."""

    def test_not_requested_initially(self):
        shutdown = GracefulShutdown()
        shutdown.install()
        try:
            assert not shutdown.requested
        finally:
            shutdown.uninstall()

    def test_sigint_sets_requested(self):
        shutdown = GracefulShutdown()
        shutdown.install()
        try:
            os.kill(os.getpid(), signal.SIGINT)
            assert shutdown.requested
        finally:
            shutdown.uninstall()

    def test_second_sigint_raises_keyboard_interrupt(self):
        shutdown = GracefulShutdown()
        shutdown.install()
        try:
            # First signal sets flag
            os.kill(os.getpid(), signal.SIGINT)
            assert shutdown.requested

            # Second signal should raise KeyboardInterrupt
            with pytest.raises(KeyboardInterrupt):
                os.kill(os.getpid(), signal.SIGINT)
        finally:
            shutdown.uninstall()

    def test_uninstall_restores_handlers(self):
        original_handler = signal.getsignal(signal.SIGINT)

        shutdown = GracefulShutdown()
        shutdown.install()

        # Handler should be different now
        assert signal.getsignal(signal.SIGINT) != original_handler

        shutdown.uninstall()

        # Original should be restored
        assert signal.getsignal(signal.SIGINT) is original_handler
