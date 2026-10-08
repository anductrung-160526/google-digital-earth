"""Shared request pacing and bounded retries; never log signed URLs or response bodies."""
import logging
import random
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

log = logging.getLogger('vngis')


def retry_after(value, now=None):
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except (ValueError, TypeError):
        try:
            date = parsedate_to_datetime(str(value))
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return max(0.0, (date - (now or datetime.now(timezone.utc))).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return None


def status_code(exc):
    response = getattr(exc, 'response', None)
    code = getattr(response, 'status_code', None) or getattr(exc, 'status_code', None)
    if code is None:
        code = getattr(getattr(exc, 'resp', None), 'status', None)
    if code is not None:
        return int(code)
    # EEException often wraps its HTTP error as text, without a response attribute.
    import re
    match = re.search(r'\b(429|500|502|503|504|401|403)\b', str(exc))
    if match:
        return int(match[1])
    if 'too many requests' in str(exc).lower():
        return 429
    return None


def retry_delay(attempt, header=None):
    explicit = retry_after(header)
    if explicit is not None:
        return explicit
    base = min(120.0, 5.0 * 2 ** attempt)
    return min(120.0, base + random.uniform(0, base * .25))


class RequestGate:
    """One concurrency budget for EE computations and image HTTP downloads.

    Service clocks pace request starts; a 429 pauses all workers using this gate.
    No semaphore is held while waiting for pacing, backoff or cooldown.
    """
    def __init__(self, concurrency, qps, check_stop, stop_event, clock=time.monotonic):
        if concurrency < 1 or qps <= 0:
            raise ValueError('Concurrency và QPS phải > 0')
        self.semaphore = threading.BoundedSemaphore(concurrency)
        self.qps = qps
        self.check_stop = check_stop
        self.stop_event = stop_event
        self.clock = clock
        self.lock = threading.Lock()
        self.next_start = {}
        self.cooldown = 0.0
        self.throttles = {}

    def wait(self, seconds, allow_stopped=False):
        if allow_stopped:
            time.sleep(seconds)
        else:
            self.check_stop()
            if self.stop_event.wait(seconds):
                self.check_stop()

    def defer(self, service, seconds, throttled=False):
        with self.lock:
            self.cooldown = max(self.cooldown, self.clock() + seconds)
            if throttled:
                self.throttles[service] = self.throttles.get(service, 0) + 1
                count = self.throttles[service]
            else:
                count = None
        if throttled:
            log.warning('%s: HTTP 429 #%s; cooldown chung %.1fs', service, count, seconds)

    @contextmanager
    def slot(self, service, allow_stopped=False):
        bucket = 'Drive' if service == 'Google Drive' else 'EE'
        while True:
            if not allow_stopped:
                self.check_stop()
            with self.lock:
                delay = max(self.cooldown, self.next_start.get(bucket, 0)) - self.clock()
            if delay > 0:
                self.wait(min(delay, 1), allow_stopped)
                continue
            if not self.semaphore.acquire(timeout=.1):
                continue
            with self.lock:
                now = self.clock()
                ready = max(self.cooldown, self.next_start.get(bucket, 0)) <= now
                if ready:
                    self.next_start[bucket] = now + 1.0 / self.qps
            if ready:
                break
            self.semaphore.release()
        try:
            yield
        finally:
            self.semaphore.release()

    def call(self, service, operation, attempts=6):
        for attempt in range(attempts):
            try:
                with self.slot(service):
                    return operation()
            except Exception as exc:
                code = status_code(exc)
                if code not in {429, 500, 502, 503, 504}:
                    raise
                if attempt == attempts - 1:
                    if code == 429:
                        self.defer(service, 0, True)
                    # Omit exception text: it can contain authenticated URLs.
                    raise RuntimeError(f'{service}: HTTP {code or "error"}; '
                                       f'{attempt + 1}/{attempts} lần thử') from None
                headers = getattr(getattr(exc, 'response', None), 'headers', {}) or getattr(exc, 'resp', {}) or {}
                delay = retry_delay(attempt, headers.get('Retry-After', headers.get('retry-after')))
                self.defer(service, delay, code == 429)
