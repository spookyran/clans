"""
Critical Ops Clan Tag Fetcher Engine

Phase 2: Reads valid_ids.jsonl and fetches full profile data for each ID.
Since all IDs here are KNOWN valid, we attempt batching first (comma-separated).
If the API returns 500 on a batch, we fall back to single-ID mode automatically.

API: https://api-cops.criticalforce.fi/api/public/profile?ids=<id1>,<id2>,...
"""

import asyncio
import aiohttp
import json
import os
import time
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("clantags")

API_URL = "https://api-cops.criticalforce.fi/api/public/profile"
HEADERS = {"content-type": "application/json"}


@dataclass
class ClanTagState:
    running: bool = False
    paused: bool = False
    processed: int = 0
    total: int = 0
    found_tags: int = 0
    speed: float = 0.0
    start_time: float = field(default_factory=time.time)
    pause_until: float = 0.0
    total_403s: int = 0
    current_index: int = 0
    forbidden_pause_seconds: int = 180
    batch_mode: bool = True          # flipped to False if API rejects batches


class ClanTagFetcher:
    def __init__(self, config: dict):
        self.config = config
        self.state = ClanTagState()
        self.state.forbidden_pause_seconds = config.get("forbidden_pause_seconds", 180)

        self.concurrency = config.get("concurrency", 500)
        self.batch_size  = config.get("clan_tag_batch_size", 10)  # smaller default — all valid
        self.timeout     = config.get("request_timeout", 10)
        self.retry_limit = config.get("retry_limit", 3)

        self.valid_ids_file   = config.get("valid_ids_file",          "data/valid_ids.jsonl")
        self.clan_tags_file   = config.get("clan_tags_file",          "data/clan_tags.jsonl")
        self.checkpoint_file  = config.get("clantag_checkpoint_file", "data/clantag_checkpoint.json")

        os.makedirs(os.path.dirname(self.clan_tags_file), exist_ok=True)

        self._task: Optional[asyncio.Task] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._file_lock = asyncio.Lock()
        self._403_event = asyncio.Event()
        self._403_index: int = 0
        self._ids: list[int] = []

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _load_valid_ids(self) -> list[int]:
        ids = []
        if not os.path.exists(self.valid_ids_file):
            logger.error(f"valid_ids.jsonl not found at {self.valid_ids_file}")
            return ids
        with open(self.valid_ids_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ids.append(int(json.loads(line)["id"]))
                except Exception:
                    continue
        return ids

    def load_checkpoint(self) -> bool:
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file) as f:
                    d = json.load(f)
                self.state.current_index = d.get("current_index", 0)
                self.state.processed     = d.get("processed",     0)
                self.state.found_tags    = d.get("found_tags",    0)
                self.state.total_403s    = d.get("total_403s",    0)
                self.state.batch_mode    = d.get("batch_mode",    True)
                return True
            except Exception as e:
                logger.error(f"Checkpoint load error: {e}")
        return False

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, "w") as f:
                json.dump({
                    "current_index": self.state.current_index,
                    "processed":     self.state.processed,
                    "found_tags":    self.state.found_tags,
                    "total_403s":    self.state.total_403s,
                    "batch_mode":    self.state.batch_mode,
                    "timestamp":     time.time(),
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Checkpoint save error: {e}")

    async def _write_results(self, records: list[dict]):
        async with self._file_lock:
            with open(self.clan_tags_file, "a", encoding="utf-8") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")

    # ------------------------------------------------------------------ #
    #  API fetch — tries batch, falls back to single if needed            #
    # ------------------------------------------------------------------ #

    def _extract_clan_tag(self, player: dict) -> str:
        """Extract clan tag from a player object — tries all known field paths."""
        # Direct fields
        for field in ("clanTag", "clan_tag", "clanName", "clanAbbreviation"):
            val = player.get(field)
            if val:
                return str(val)
        # Nested clan object
        clan = player.get("clan")
        if isinstance(clan, dict):
            for f in ("tag", "abbreviation", "name", "clanTag"):
                val = clan.get(f)
                if val:
                    return str(val)
        return ""

    def _parse_player(self, player: dict, expected_id: int = None) -> dict:
        basic   = player.get("basicInfo", {})
        pid     = basic.get("userID") or player.get("id") or player.get("userID") or expected_id
        name    = basic.get("name", "") or player.get("name", "")
        level   = basic.get("playerLevel", {}).get("level", 0)
        tag     = self._extract_clan_tag(player)
        return {"id": pid, "name": name, "level": level, "clan_tag": tag}

    async def _fetch_batch(
        self,
        session: aiohttp.ClientSession,
        ids: list[int],
    ) -> tuple[str, list[dict]]:
        """
        Fetch a batch of (known valid) IDs.
        Returns (status, records) where status is "ok" | "fallback" | "403" | "error"
        "fallback" means the batch returned 500 — caller should retry one-by-one
        """
        id_str = ",".join(str(i) for i in ids)
        url    = f"{API_URL}?ids={id_str}"

        for attempt in range(self.retry_limit):
            try:
                async with session.get(
                    url,
                    headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as resp:

                    if resp.status == 200:
                        try:
                            data    = await resp.json(content_type=None)
                            players = data if isinstance(data, list) else [data]
                            records = [self._parse_player(p) for p in players if isinstance(p, dict)]
                            return "ok", records
                        except Exception:
                            return "error", []

                    elif resp.status == 500:
                        # Batch got rejected — signal caller to use single-ID mode
                        return "fallback", []

                    elif resp.status == 403:
                        return "403", []

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

        return "error", []

    async def _fetch_single(
        self,
        session: aiohttp.ClientSession,
        player_id: int,
    ) -> tuple[str, list[dict]]:
        """Fetch a single ID. Returns (status, records)."""
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
                            data   = await resp.json(content_type=None)
                            player = data[0] if isinstance(data, list) and data else \
                                     data   if isinstance(data, dict) else None
                            if player:
                                return "ok", [self._parse_player(player, player_id)]
                        except Exception:
                            pass
                        return "ok", []

                    elif resp.status == 500:
                        return "ok", []    # shouldn't happen for known valid IDs, skip

                    elif resp.status == 403:
                        return "403", []

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

        return "error", []

    # ------------------------------------------------------------------ #
    #  Main fetch loop                                                     #
    # ------------------------------------------------------------------ #

    async def _fetch_loop(self):
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

            ids               = self._ids
            idx               = self.state.current_index
            speed_counter     = 0
            speed_timer       = time.time()
            checkpoint_counter = 0
            pending: set[asyncio.Task] = set()
            MAX_PENDING = self.concurrency * 3

            async def process(batch_ids: list[int], batch_start_idx: int):
                nonlocal speed_counter, speed_timer, checkpoint_counter

                async with self._semaphore:
                    if self._403_event.is_set():
                        return

                    # ── Try batch first (if batch_mode still on) ──────────
                    if self.state.batch_mode and len(batch_ids) > 1:
                        status, records = await self._fetch_batch(session, batch_ids)

                        if status == "fallback":
                            # Batch rejected — switch permanently to single-ID mode
                            logger.warning(
                                "Batch returned 500 — switching to single-ID mode for clan tags"
                            )
                            self.state.batch_mode = False
                            # Re-fetch all IDs in this batch individually
                            records = []
                            for pid in batch_ids:
                                s2, r2 = await self._fetch_single(session, pid)
                                if s2 == "403":
                                    status = "403"
                                    break
                                records.extend(r2)

                    else:
                        # Single-ID mode
                        records = []
                        status  = "ok"
                        for pid in batch_ids:
                            s, r = await self._fetch_single(session, pid)
                            if s == "403":
                                status = "403"
                                break
                            records.extend(r)

                    # ── Handle 403 ────────────────────────────────────────
                    if status == "403":
                        self.state.total_403s += 1
                        if not self._403_event.is_set():
                            self._403_index = batch_start_idx
                            self._403_event.set()
                        return

                    # ── Write results ─────────────────────────────────────
                    if records:
                        await self._write_results(records)
                        self.state.found_tags += sum(1 for r in records if r.get("clan_tag"))

                    count = len(batch_ids)
                    self.state.processed  += count
                    speed_counter         += count
                    checkpoint_counter    += count

                    if speed_counter >= 2000:
                        elapsed = time.time() - speed_timer
                        self.state.speed = speed_counter / elapsed if elapsed > 0 else 0
                        speed_counter = 0
                        speed_timer   = time.time()

                    if checkpoint_counter >= 5_000:
                        self.state.current_index = batch_start_idx
                        self.save_checkpoint()
                        checkpoint_counter = 0

            # ── dispatch loop ─────────────────────────────────────────────
            while idx < len(ids) and self.state.running:

                # ── 403 handling ──────────────────────────────────────────
                if self._403_event.is_set():
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                        pending.clear()

                    self.state.current_index = self._403_index
                    self.state.paused        = True
                    self.state.running       = False
                    self.save_checkpoint()

                    wait = self.state.forbidden_pause_seconds
                    self.state.pause_until = time.time() + wait
                    logger.warning(f"403 on clan tags — pausing {wait}s")
                    await asyncio.sleep(wait)

                    logger.info("Resuming clan tag fetch...")
                    idx                  = self._403_index
                    self._403_event.clear()
                    self.state.paused    = False
                    self.state.running   = True
                    continue

                # ── manual pause ──────────────────────────────────────────
                if self.state.paused:
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                        pending.clear()
                    self.state.current_index = idx
                    self.save_checkpoint()
                    while self.state.paused and not self.state.running:
                        await asyncio.sleep(0.5)
                    continue

                # ── throttle ──────────────────────────────────────────────
                if len(pending) >= MAX_PENDING:
                    done, pending = await asyncio.wait(
                        pending, return_when=asyncio.FIRST_COMPLETED
                    )

                # ── build next batch and dispatch ─────────────────────────
                bs    = self.batch_size if self.state.batch_mode else 1
                batch = ids[idx: idx + bs]
                start = idx
                idx  += len(batch)

                task = asyncio.create_task(process(batch, start))
                pending.add(task)
                task.add_done_callback(pending.discard)

            # ── drain ─────────────────────────────────────────────────────
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        self.state.current_index = idx
        self.save_checkpoint()

        if idx >= len(ids):
            self.state.running = False
            logger.info(
                f"✅ Clan tag fetch complete! "
                f"{self.state.found_tags:,} tags found from {self.state.processed:,} IDs."
            )

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def start(self) -> tuple[bool, str]:
        if self.state.running:
            return False, "Already running."
        self._ids = self._load_valid_ids()
        if not self._ids:
            return False, "No valid IDs found — run `/scan start` first."
        self.state.total      = len(self._ids)
        self.state.running    = True
        self.state.paused     = False
        self.state.start_time = time.time()
        self._task = asyncio.create_task(self._fetch_loop())
        mode = "batch" if self.state.batch_mode else "single-ID"
        return True, f"Started fetching {self.state.total:,} IDs in **{mode}** mode."

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
            self._task = asyncio.create_task(self._fetch_loop())
            return True
        return False

    def reset(self):
        self.stop()
        self.state = ClanTagState()
        self.state.forbidden_pause_seconds = self.config.get("forbidden_pause_seconds", 180)
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)

    def get_status(self) -> dict:
        elapsed   = time.time() - self.state.start_time
        total     = self.state.total
        processed = self.state.processed
        pct       = (processed / total * 100) if total > 0 else 0
        remaining = total - processed
        eta       = (remaining / self.state.speed) if self.state.speed > 0 else None

        if self.state.running and not self.state.paused:
            status = "🟢 Running"
        elif self.state.paused:
            status = "⏸️ Paused (403)"
        else:
            status = "🔴 Stopped"

        return {
            "status":       status,
            "processed":    processed,
            "total":        total,
            "progress_pct": round(pct, 2),
            "found_tags":   self.state.found_tags,
            "speed":        round(self.state.speed, 1),
            "elapsed_secs": round(elapsed, 1),
            "eta_secs":     round(eta) if eta else None,
            "total_403s":   self.state.total_403s,
            "batch_mode":   self.state.batch_mode,
            "pause_until":  self.state.pause_until if self.state.paused else None,
        }
