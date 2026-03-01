"""
Critical Ops ID Scanner Engine
- Single ID per request (batching broken — 500 Error 53 on any invalid ID)
- Real API: https://api-cops.criticalforce.fi/api/public/profile?ids=<id>
- 200 = valid player, save it
- 500 = invalid/not found, skip
- 403 = rate limited, pause 3 mins, resume from SAME ID
- 429 = short backoff, retry
"""

import asyncio
import aiohttp
import json
import os
import time
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("scanner")

API_URL = "https://api-cops.criticalforce.fi/api/public/profile"
HEADERS = {"content-type": "application/json"}


@dataclass
class ScannerState:
    running: bool = False
    paused: bool = False
    current_id: int = 1
    target_id: int = 250_000_000
    valid_count: int = 0
    scanned_count: int = 0
    start_time: float = field(default_factory=time.time)
    pause_until: float = 0.0
    total_403s: int = 0
    total_errors: int = 0
    speed: float = 0.0
    forbidden_pause_seconds: int = 180


class CopsScanner:
    def __init__(self, config: dict):
        self.config = config
        self.state = ScannerState()
        self.state.target_id = config.get("target_id", 250_000_000)
        self.state.current_id = config.get("start_id", 1)
        self.state.forbidden_pause_seconds = config.get("forbidden_pause_seconds", 180)

        self.concurrency = config.get("concurrency", 500)
        self.timeout = config.get("request_timeout", 10)
        self.retry_limit = config.get("retry_limit", 3)

        self.valid_ids_file = config.get("valid_ids_file", "data/valid_ids.jsonl")
        self.checkpoint_file = config.get("checkpoint_file", "data/checkpoint.json")

        os.makedirs(os.path.dirname(self.valid_ids_file), exist_ok=True)

        self._task: Optional[asyncio.Task] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._file_lock = asyncio.Lock()

        # 403 signalling — shared across all concurrent workers
        self._403_event = asyncio.Event()
        self._403_id: Optional[int] = None

    # ------------------------------------------------------------------ #
    #  Checkpoint                                                          #
    # ------------------------------------------------------------------ #

    def load_checkpoint(self) -> bool:
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file) as f:
                    data = json.load(f)
                self.state.current_id    = data.get("current_id",    self.config.get("start_id", 1))
                self.state.valid_count   = data.get("valid_count",   0)
                self.state.scanned_count = data.get("scanned_count", 0)
                self.state.total_403s    = data.get("total_403s",    0)
                self.state.total_errors  = data.get("total_errors",  0)
                logger.info(f"Checkpoint loaded — resuming from ID {self.state.current_id:,}")
                return True
            except Exception as e:
                logger.error(f"Checkpoint load failed: {e}")
        return False

    def save_checkpoint(self):
        try:
            os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
            with open(self.checkpoint_file, "w") as f:
                json.dump({
                    "current_id":    self.state.current_id,
                    "valid_count":   self.state.valid_count,
                    "scanned_count": self.state.scanned_count,
                    "total_403s":    self.state.total_403s,
                    "total_errors":  self.state.total_errors,
                    "target_id":     self.state.target_id,
                    "timestamp":     time.time(),
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Checkpoint save failed: {e}")

    # ------------------------------------------------------------------ #
    #  File writing                                                        #
    # ------------------------------------------------------------------ #

    async def _write_valid(self, player_id: int, raw: dict):
        """Write a confirmed valid player to the JSONL output file."""
        async with self._file_lock:
            with open(self.valid_ids_file, "a", encoding="utf-8") as f:
                basic = raw.get("basicInfo", {})
                record = {
                    "id":    player_id,
                    "name":  basic.get("name", ""),
                    "level": basic.get("playerLevel", {}).get("level", 0),
                }
                f.write(json.dumps(record) + "\n")

    # ------------------------------------------------------------------ #
    #  Single-ID fetch                                                     #
    # ------------------------------------------------------------------ #

    async def _fetch_id(self, session: aiohttp.ClientSession, player_id: int) -> str:
        """
        Fetch one player ID.
        Returns: "valid" | "invalid" | "403" | "error"
        """
        url = f"{API_URL}?ids={player_id}"

        for attempt in range(self.retry_limit):
            try:
                async with session.get(
                    url,
                    headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as resp:

                    if resp.status == 200:
                        try:
                            data = await resp.json(content_type=None)
                            # API returns a list — grab first element
                            player = data[0] if isinstance(data, list) and data else \
                                     data   if isinstance(data, dict) and data else None
                            if player:
                                await self._write_valid(player_id, player)
                                return "valid"
                        except Exception:
                            pass
                        return "invalid"

                    elif resp.status == 500:
                        # "Error 53" — player ID does not exist
                        return "invalid"

                    elif resp.status == 403:
                        return "403"

                    elif resp.status == 429:
                        await asyncio.sleep(2 ** attempt)
                        continue

                    else:
                        await asyncio.sleep(0.5)
                        continue

            except asyncio.TimeoutError:
                if attempt < self.retry_limit - 1:
                    await asyncio.sleep(1)
            except aiohttp.ClientError:
                if attempt < self.retry_limit - 1:
                    await asyncio.sleep(0.5)

        return "error"

    # ------------------------------------------------------------------ #
    #  Main scan loop                                                      #
    # ------------------------------------------------------------------ #

    async def _scan_loop(self):
        connector = aiohttp.TCPConnector(
            limit=self.concurrency + 100,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            self._semaphore = asyncio.Semaphore(self.concurrency)
            self._403_event.clear()
            self._403_id = None

            current_id         = self.state.current_id
            target_id          = self.state.target_id
            checkpoint_counter = 0
            speed_counter      = 0
            speed_timer        = time.time()
            pending: set[asyncio.Task] = set()
            MAX_PENDING = self.concurrency * 3

            async def worker(pid: int):
                nonlocal checkpoint_counter, speed_counter, speed_timer

                async with self._semaphore:
                    # Another worker already triggered 403 — bail
                    if self._403_event.is_set():
                        return

                    result = await self._fetch_id(session, pid)

                    if result == "valid":
                        self.state.valid_count += 1

                    elif result == "403":
                        self.state.total_403s += 1
                        # Only set once — first worker to hit 403 wins
                        if not self._403_event.is_set():
                            self._403_id = pid
                            self._403_event.set()
                        return

                    elif result == "error":
                        self.state.total_errors += 1

                    # "invalid" — just skip, no action needed

                    self.state.scanned_count += 1
                    speed_counter            += 1
                    checkpoint_counter       += 1

                    # Recalculate speed every 2000 IDs
                    if speed_counter >= 2000:
                        elapsed = time.time() - speed_timer
                        self.state.speed = speed_counter / elapsed if elapsed > 0 else 0
                        speed_counter = 0
                        speed_timer   = time.time()

                    # Save checkpoint every 10,000 IDs
                    if checkpoint_counter >= 10_000:
                        self.state.current_id = pid
                        self.save_checkpoint()
                        checkpoint_counter = 0

            # ── dispatch loop ─────────────────────────────────────────────
            while current_id <= target_id and self.state.running:

                # ── 403 hit: drain workers, pause, then resume same ID ────
                if self._403_event.is_set():
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                        pending.clear()

                    resume_from            = self._403_id
                    self.state.current_id  = resume_from
                    self.state.paused      = True
                    self.state.running     = False
                    self.save_checkpoint()

                    wait = self.state.forbidden_pause_seconds
                    self.state.pause_until = time.time() + wait
                    logger.warning(f"403 at ID {resume_from:,} — pausing {wait}s")
                    await asyncio.sleep(wait)

                    logger.info(f"Resuming from ID {resume_from:,}")
                    current_id             = resume_from
                    self._403_event.clear()
                    self._403_id           = None
                    self.state.paused      = False
                    self.state.running     = True
                    continue

                # ── manual pause ──────────────────────────────────────────
                if self.state.paused:
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                        pending.clear()
                    self.state.current_id = current_id
                    self.save_checkpoint()
                    while self.state.paused and not self.state.running:
                        await asyncio.sleep(0.5)
                    continue

                # ── throttle: don't let pending grow too large ────────────
                if len(pending) >= MAX_PENDING:
                    done, pending = await asyncio.wait(
                        pending, return_when=asyncio.FIRST_COMPLETED
                    )

                # ── dispatch one worker per ID ────────────────────────────
                task = asyncio.create_task(worker(current_id))
                pending.add(task)
                task.add_done_callback(pending.discard)
                current_id += 1

            # ── drain remaining on exit ───────────────────────────────────
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        self.state.current_id = current_id
        self.save_checkpoint()

        if current_id > target_id:
            self.state.running = False
            logger.info(
                f"✅ Scan complete! {self.state.valid_count:,} valid IDs "
                f"out of {self.state.scanned_count:,} scanned."
            )

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def start(self) -> bool:
        if self.state.running:
            return False
        self.state.running    = True
        self.state.paused     = False
        self.state.start_time = time.time()
        self._task = asyncio.create_task(self._scan_loop())
        return True

    def stop(self):
        self.state.running = False
        self.state.paused  = False
        if self._task and not self._task.done():
            self._task.cancel()
        self.save_checkpoint()

    def pause(self):
        if self.state.running:
            self.state.paused  = True
            self.state.running = False

    def resume(self) -> bool:
        if self.state.paused:
            self.state.paused  = False
            self.state.running = True
            self._task = asyncio.create_task(self._scan_loop())
            return True
        return False

    def reset(self):
        self.stop()
        self.state = ScannerState()
        self.state.target_id              = self.config.get("target_id", 250_000_000)
        self.state.current_id             = self.config.get("start_id", 1)
        self.state.forbidden_pause_seconds = self.config.get("forbidden_pause_seconds", 180)
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)

    def get_status(self) -> dict:
        elapsed   = time.time() - self.state.start_time
        start     = self.config.get("start_id", 1)
        total     = self.state.target_id - start + 1
        pct       = (self.state.scanned_count / total * 100) if total > 0 else 0
        remaining = max(0, self.state.target_id - self.state.current_id)
        eta       = (remaining / self.state.speed) if self.state.speed > 0 else None

        if self.state.running and not self.state.paused:
            status = "🟢 Running"
        elif self.state.paused:
            status = "⏸️ Paused (403)"
        else:
            status = "🔴 Stopped"

        return {
            "status":       status,
            "current_id":   self.state.current_id,
            "target_id":    self.state.target_id,
            "scanned":      self.state.scanned_count,
            "valid_count":  self.state.valid_count,
            "progress_pct": round(pct, 2),
            "speed":        round(self.state.speed, 1),
            "elapsed_secs": round(elapsed, 1),
            "eta_secs":     round(eta) if eta else None,
            "total_403s":   self.state.total_403s,
            "total_errors": self.state.total_errors,
            "pause_until":  self.state.pause_until if self.state.paused else None,
        }
