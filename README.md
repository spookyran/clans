# 🎮 Critical Ops Scanner Bot

High-speed Discord bot for scanning all Critical Ops player IDs and fetching clan tags.

---

## 🔑 API Facts (confirmed by testing)

| Endpoint | `https://api-cops.criticalforce.fi/api/public/profile?ids=<id>` |
|---|---|
| Valid ID | `200` → JSON array with player object |
| Invalid ID | `500 Error 53` → skip |
| Rate limited | `403` → pause 3 min, resume same ID |
| Batch with any invalid | `500 Error 53` → **batching is broken for Phase 1** |

---

## 📁 File Structure

```
cops_scanner_bot/
├── bot.py                  # Entry point
├── config.json             # All settings
├── scanner_engine.py       # Phase 1 — ID scanner core
├── clantag_engine.py       # Phase 2 — clan tag fetcher core
├── requirements.txt
├── cogs/
│   ├── scanner.py          # /scan commands
│   ├── clantags.py         # /clantags commands
│   └── info.py             # /help /config /datainfo
└── data/                   # Created automatically
    ├── valid_ids.jsonl
    ├── clan_tags.jsonl
    ├── checkpoint.json
    └── clantag_checkpoint.json
```

---

## ⚡ Setup

```bash
pip install -r requirements.txt
```

Edit `config.json`:
```json
{
  "discord_token": "YOUR_TOKEN_HERE",
  "target_id": 250000000,
  "concurrency": 500
}
```

```bash
python bot.py
```

---

## 🎮 Commands

### Phase 1 — `/scan`
| Command | Description |
|---|---|
| `/scan start` | Start or resume from checkpoint |
| `/scan stop` | Stop and save progress |
| `/scan pause` / `/scan resume` | Manual pause |
| `/scan status` | Progress embed |
| `/scan live` | Auto-refresh embed (2 min) |
| `/scan reset` | ⚠️ Wipe all progress |
| `/scan setconfig` | Change target ID or concurrency |

### Phase 2 — `/clantags`
| Command | Description |
|---|---|
| `/clantags start` | Fetch clan tags for all valid IDs |
| `/clantags stop` | Stop |
| `/clantags pause` / `/clantags resume` | Manual pause |
| `/clantags status` | Progress embed |
| `/clantags live` | Auto-refresh embed |
| `/clantags preview [N]` | Show first N results |
| `/clantags reset` | Reset fetcher |

### Utility
`/help` `/config` `/datainfo`

---

## 📊 Output Format

### `valid_ids.jsonl`
```json
{"id": 176409706, "name": "Fluff", "level": 597}
```

### `clan_tags.jsonl`
```json
{"id": 176409706, "name": "Fluff", "level": 597, "clan_tag": "ABC"}
```

---

## ⚡ Performance

- **Concurrency 500** = 500 simultaneous requests
- At ~100ms average API latency → ~5,000 IDs/sec
- 250M IDs ÷ 5,000/sec = **~14 hours**
- Checkpoint saved every 10,000 IDs — safe to restart anytime

### Tuning
- Raise `concurrency` in config for more speed (try 750, 1000)
- If you get lots of 403s, reduce it
- `forbidden_pause_seconds` controls how long to wait on 403 (default 3 min)

---

## 🛡️ 403 Handling

1. Any worker hits 403 → sets a shared event flag
2. All other workers see the flag and stop immediately
3. Exact ID that triggered it is saved to checkpoint
4. Bot waits 3 minutes
5. Resumes from that **exact same ID** — nothing is lost

---

## 🏷️ Clan Tag Phase Notes

Since all IDs in Phase 2 are confirmed valid, the fetcher **tries batch mode first** (10 IDs per request). If the API still returns 500 on a batch of all-valid IDs, it automatically and permanently switches to single-ID mode for the rest of the run. This is logged and shown in the status embed.
