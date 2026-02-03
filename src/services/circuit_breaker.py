"""
Task 11: Circuit Breaker service.

Implement a Circuit Breaker pattern to protect against failures of the external
notification API. Do not use external libraries such as `pybreaker`.

State transition rules:
- CLOSED → OPEN: at least 5 failures in the last 10 requests.
- OPEN → HALF_OPEN: after 30 seconds.
- HALF_OPEN → CLOSED: 1 successful call.
- HALF_OPEN → OPEN: 1 failed call.
"""

import asyncio
import time
from enum import Enum
from typing import Callable, Any, Optional
from collections import deque
from dataclasses import dataclass


class CircuitState(Enum):
    CLOSED = "closed"       # Normal state - requests are allowed
    OPEN = "open"           # Open state - requests fail immediately
    HALF_OPEN = "half_open" # Half-open state - limited probing requests allowed


@dataclass
class CircuitBreakerConfig:
    """Circuit Breaker configuration."""
    failure_threshold: int = 5        # Failure count before transitioning to OPEN
    success_threshold: int = 1        # Successes required to transition to CLOSED
    window_size: int = 10             # Window size for failure rate calculation
    timeout_seconds: float = 30.0     # Time to stay OPEN before trying HALF_OPEN
    half_open_max_calls: int = 1      # Max concurrent calls allowed in HALF_OPEN


class CircuitBreakerOpenError(Exception):
    """Raised when a call is attempted while the circuit is OPEN."""
    def __init__(self, retry_after: float):
        self.retry_after = retry_after
        super().__init__(f"Circuit is OPEN. Retry after {retry_after:.1f} seconds")


class CircuitBreaker:
    """
    Circuit Breaker implementation.

    Usage example:
    ```python
    cb = CircuitBreaker("notify_api")

    try:
        result = await cb.call(notify_function, ticket_id, tenant_id)
    except CircuitBreakerOpenError as e:
        print(f"Circuit open, retry after {e.retry_after}s")
    ```
    """

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._opened_at: Optional[float] = None

        # Track recent call results (True = success, False = failure)
        self._recent_results: deque = deque(maxlen=self.config.window_size)

        self._lock = asyncio.Lock()
        self._half_open_calls = 0

    @property
    def state(self) -> CircuitState:
        """Return the current state, applying automatic transitions as needed."""
        # When in OPEN, transition to HALF_OPEN after the timeout elapses.
        if self._state == CircuitState.OPEN:
            if self._opened_at and time.time() - self._opened_at >= self.config.timeout_seconds:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
        return self._state

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Call an async function through the Circuit Breaker.

        Args:
            func: Async function to call.
            *args, **kwargs: Function arguments.

        Returns:
            The function's return value.

        Raises:
            CircuitBreakerOpenError: When the circuit is OPEN.
        """
        current_state = self.state  # Check state and apply transitions
        
        # OPEN state: fail fast
        if current_state == CircuitState.OPEN:
            retry_after = self.config.timeout_seconds - (time.time() - self._opened_at) if self._opened_at else 0
            raise CircuitBreakerOpenError(retry_after=max(0, retry_after))
        
        # HALF_OPEN state: limit concurrent calls
        if current_state == CircuitState.HALF_OPEN:
            async with self._lock:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    raise CircuitBreakerOpenError(retry_after=1.0)
                self._half_open_calls += 1
        
        # Execute the function
        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure()
            raise
        finally:
            if current_state == CircuitState.HALF_OPEN:
                async with self._lock:
                    self._half_open_calls = max(0, self._half_open_calls - 1)

    async def _on_success(self) -> None:
        """
        Handler invoked on successful calls.
        """
        async with self._lock:
            self._success_count += 1
            self._recent_results.append(True)
            
            # Prevent unbounded counter growth (memory leak fix)
            # Reset counters when they exceed a threshold
            if self._success_count > 100000:
                self._success_count = 0
            
            # HALF_OPEN → CLOSED on success
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._opened_at = None

    async def _on_failure(self) -> None:
        """
        Handler invoked on failed calls.
        """
        async with self._lock:
            self._failure_count += 1
            self._recent_results.append(False)
            self._last_failure_time = time.time()
            
            # Prevent unbounded counter growth (memory leak fix)
            if self._failure_count > 100000:
                self._failure_count = len([r for r in self._recent_results if not r])
            
            # HALF_OPEN → OPEN immediately on failure
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._opened_at = time.time()
            # CLOSED → OPEN if threshold exceeded
            elif self._state == CircuitState.CLOSED and self._should_open():
                self._state = CircuitState.OPEN
                self._opened_at = time.time()

    def _should_open(self) -> bool:
        """
        Determine whether to transition to the OPEN state.

        Returns:
            True if the circuit should transition to OPEN.
        """
        if len(self._recent_results) < self.config.window_size:
            return False
        
        # Count failures in the window
        failures = sum(1 for r in self._recent_results if not r)
        return failures >= self.config.failure_threshold

    def get_status(self) -> dict:
        """
        Return the current Circuit Breaker status.

        Returns:
            {
                "name": str,
                "state": str,
                "failure_count": int,
                "success_count": int,
                "recent_failure_rate": float,
                "opened_at": Optional[str],
                "retry_after": Optional[float]
            }
        """
        state = self.state  # triggers automatic state transition checks

        failure_rate = 0.0
        if self._recent_results:
            failures = sum(1 for r in self._recent_results if not r)
            failure_rate = failures / len(self._recent_results)

        retry_after = None
        if state == CircuitState.OPEN and self._opened_at:
            remaining = self.config.timeout_seconds - (time.time() - self._opened_at)
            retry_after = max(0, remaining)

        return {
            "name": self.name,
            "state": state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "recent_failure_rate": failure_rate,
            "opened_at": self._opened_at,
            "retry_after": retry_after
        }

    def reset(self) -> None:
        """Reset Circuit Breaker state and counters."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._opened_at = None
        self._recent_results.clear()
        self._half_open_calls = 0


# Global Circuit Breaker instances registry
_circuit_breakers: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(name: str) -> CircuitBreaker:
    """
    Get a Circuit Breaker instance by name (or create one if missing).
    """
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(name)
    return _circuit_breakers[name]
