"""
/scan commands — ID scanner Discord cog
"""

import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import json
import time

from scanner_engine import CopsScanner

with open("config.json") as f:
    config = json.load(f)

scanner = CopsScanner(config)


def fmt_num(n) -> str:
    return f"{n:,}"


def fmt_eta(secs) -> str:
    if secs is None:
        return "calculating..."
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    elif secs < 3600:
        return f"{secs // 60}m {secs % 60}s"
    else:
        h = secs // 3600
        m = (secs % 3600) // 60
        return f"{h}h {m}m"


def build_embed(s: dict) -> discord.Embed:
    color = discord.Color.green()  if "Running" in s["status"] else \
            discord.Color.orange() if "Paused"  in s["status"] else \
            discord.Color.red()

    embed = discord.Embed(title="🔍 ID Scanner", color=color)
    embed.add_field(name="Status", value=s["status"], inline=False)

    pct    = s["progress_pct"]
    filled = int(pct / 5)
    bar    = "█" * filled + "░" * (20 - filled)
    embed.add_field(
        name=f"Progress  {pct:.2f}%",
        value=f"`[{bar}]`\n{fmt_num(s['scanned'])} / {fmt_num(s['target_id'])} IDs scanned",
        inline=False,
    )

    embed.add_field(name="✅ Valid IDs",   value=fmt_num(s["valid_count"]),          inline=True)
    embed.add_field(name="⚡ Speed",       value=f"{fmt_num(int(s['speed']))} /sec", inline=True)
    embed.add_field(name="⏱️ ETA",         value=fmt_eta(s["eta_secs"]),             inline=True)
    embed.add_field(name="🕐 Elapsed",     value=fmt_eta(int(s["elapsed_secs"])),    inline=True)
    embed.add_field(name="⛔ 403s",        value=fmt_num(s["total_403s"]),           inline=True)
    embed.add_field(name="🎯 Current ID",  value=fmt_num(s["current_id"]),           inline=True)

    if s.get("pause_until"):
        remaining = max(0, s["pause_until"] - time.time())
        embed.add_field(
            name="⏸️ Auto-resume in",
            value=f"{remaining:.0f}s",
            inline=False,
        )

    embed.set_footer(text="500=invalid (skip) • 403=pause 3min then resume same ID")
    return embed


class ScannerCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    scan = app_commands.Group(name="scan", description="ID scanner commands")

    @scan.command(name="start", description="Start or resume the ID scanner")
    async def scan_start(self, interaction: discord.Interaction):
        await interaction.response.defer()
        resumed = scanner.load_checkpoint()
        ok = scanner.start()

        if not ok:
            await interaction.followup.send("❌ Scanner is already running.", ephemeral=True)
            return

        s = scanner.get_status()
        msg = (
            f"{'▶️ Resumed' if resumed else '🚀 Started'} scanning!\n"
            f"Range: **{fmt_num(s['current_id'])}** → **{fmt_num(s['target_id'])}**\n"
            f"Concurrency: **{scanner.concurrency}** simultaneous requests\n"
            f"API: `api-cops.criticalforce.fi` • Single-ID mode (batching disabled)"
        )
        await interaction.followup.send(msg, embed=build_embed(s))

    @scan.command(name="stop", description="Stop scanner and save progress")
    async def scan_stop(self, interaction: discord.Interaction):
        scanner.stop()
        await interaction.response.send_message(
            "🛑 Scanner stopped. Progress saved.",
            embed=build_embed(scanner.get_status()),
        )

    @scan.command(name="pause", description="Manually pause the scanner")
    async def scan_pause(self, interaction: discord.Interaction):
        if not scanner.state.running:
            await interaction.response.send_message("❌ Scanner isn't running.", ephemeral=True)
            return
        scanner.pause()
        await interaction.response.send_message("⏸️ Scanner paused.")

    @scan.command(name="resume", description="Resume a manually paused scanner")
    async def scan_resume(self, interaction: discord.Interaction):
        ok = scanner.resume()
        if not ok:
            await interaction.response.send_message("❌ Scanner isn't paused.", ephemeral=True)
            return
        await interaction.response.send_message("▶️ Scanner resumed!")

    @scan.command(name="status", description="Show current scanner stats")
    async def scan_status(self, interaction: discord.Interaction):
        await interaction.response.send_message(embed=build_embed(scanner.get_status()))

    @scan.command(name="live", description="Live updating status (refreshes every 10s for 2 min)")
    async def scan_live(self, interaction: discord.Interaction):
        await interaction.response.defer()
        msg = await interaction.followup.send(embed=build_embed(scanner.get_status()))
        for _ in range(12):
            await asyncio.sleep(10)
            try:
                await msg.edit(embed=build_embed(scanner.get_status()))
            except Exception:
                break
            if not scanner.state.running and not scanner.state.paused:
                break

    @scan.command(name="reset", description="⚠️ Reset scanner to ID 1 and clear all progress")
    async def scan_reset(self, interaction: discord.Interaction):
        scanner.reset()
        await interaction.response.send_message(
            "♻️ Scanner reset to start. Use `/scan start` to begin fresh."
        )

    @scan.command(name="setconfig", description="Change scanner settings on the fly")
    @app_commands.describe(
        target_id="Highest ID to scan up to",
        concurrency="Max simultaneous requests (careful — too high may trigger 403s)",
    )
    async def scan_setconfig(
        self,
        interaction: discord.Interaction,
        target_id: int = None,
        concurrency: int = None,
    ):
        changes = []
        if target_id is not None:
            scanner.state.target_id  = target_id
            config["target_id"]      = target_id
            changes.append(f"Target ID → `{fmt_num(target_id)}`")
        if concurrency is not None:
            scanner.concurrency    = max(1, min(concurrency, 2000))
            config["concurrency"]  = scanner.concurrency
            changes.append(f"Concurrency → `{scanner.concurrency}`")

        if not changes:
            await interaction.response.send_message("Nothing changed.", ephemeral=True)
            return

        with open("config.json", "w") as f:
            json.dump(config, f, indent=2)

        await interaction.response.send_message(
            "✅ Config updated:\n" + "\n".join(f"• {c}" for c in changes)
        )


async def setup(bot):
    cog = ScannerCog(bot)
    bot.tree.add_command(cog.scan)
    await bot.add_cog(cog)
