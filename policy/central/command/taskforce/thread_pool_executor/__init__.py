"""Concurrent-future wrapper used by the async-thread-pool backend.

This package no longer ships a thread-pool taskforce class — the
async-thread-pool taskforce is the unified backend. Only
``ConcurrentPackageFuture`` remains here, used as the future type for
async-runner-completed tasks.
"""

from .future import ConcurrentPackageFuture
