"""
/clantags commands — clan tag fetcher Discord cog
"""

import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import json
import os
import time

from clantag_engine import ClanTagFetcher

with open("config.json") as f:
    config = json.load(f)

fetcher = ClanTagFetcher(config)


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
    color = discord.Color.blue()   if "Running" in s["status"] else \
            discord.Color.orange() if "Paused"  in s["status"] else \
            discord.Color.greyple()

    embed = discord.Embed(title="🏷️ Clan Tag Fetcher", color=color)
    embed.add_field(name="Status", value=s["status"], inline=False)

    pct    = s["progress_pct"]
    filled = int(pct / 5)
    bar    = "█" * filled + "░" * (20 - filled)
    embed.add_field(
        name=f"Progress  {pct:.2f}%",
        value=f"`[{bar}]`\n{s['processed']:,} / {s['total']:,} IDs processed",
        inline=False,
    )

    embed.add_field(name="🏷️ Tags Found",  value=f"{s['found_tags']:,}",            inline=True)
    embed.add_field(name="⚡ Speed",        value=f"{int(s['speed']):,} /sec",       inline=True)
    embed.add_field(name="⏱️ ETA",          value=fmt_eta(s["eta_secs"]),            inline=True)
    embed.add_field(name="🕐 Elapsed",      value=fmt_eta(int(s["elapsed_secs"])),   inline=True)
    embed.add_field(name="⛔ 403s",         value=f"{s['total_403s']:,}",            inline=True)
    embed.add_field(name="📦 Mode",         value="Batch" if s["batch_mode"] else "Single-ID", inline=True)

    if s.get("pause_until"):
        remaining = max(0, s["pause_until"] - time.time())
        embed.add_field(name="⏸️ Auto-resume in", value=f"{remaining:.0f}s", inline=False)

    embed.set_footer(text="Batch mode auto-disables if API rejects grouped requests")
    return embed


class ClanTagCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    ct = app_commands.Group(name="clantags", description="Clan tag fetcher commands")

    @ct.command(name="start", description="Fetch clan tags for all valid IDs")
    async def ct_start(self, interaction: discord.Interaction):
        await interaction.response.defer()
        fetcher.load_checkpoint()
        ok, msg = fetcher.start()
        if not ok:
            await interaction.followup.send(f"❌ {msg}", ephemeral=True)
            return
        await interaction.followup.send(
            f"▶️ {msg}\nConcurrency: **{fetcher.concurrency}**",
            embed=build_embed(fetcher.get_status()),
        )

    @ct.command(name="stop", description="Stop the clan tag fetcher")
    async def ct_stop(self, interaction: discord.Interaction):
        fetcher.stop()
        await interaction.response.send_message(
            "🛑 Fetcher stopped.", embed=build_embed(fetcher.get_status())
        )

    @ct.command(name="pause", description="Manually pause the fetcher")
    async def ct_pause(self, interaction: discord.Interaction):
        if not fetcher.state.running:
            await interaction.response.send_message("❌ Fetcher isn't running.", ephemeral=True)
            return
        fetcher.pause()
        await interaction.response.send_message("⏸️ Fetcher paused.")

    @ct.command(name="resume", description="Resume a paused fetcher")
    async def ct_resume(self, interaction: discord.Interaction):
        ok = fetcher.resume()
        if not ok:
            await interaction.response.send_message("❌ Fetcher isn't paused.", ephemeral=True)
            return
        await interaction.response.send_message("▶️ Fetcher resumed!")

    @ct.command(name="status", description="Show clan tag fetcher progress")
    async def ct_status(self, interaction: discord.Interaction):
        await interaction.response.send_message(embed=build_embed(fetcher.get_status()))

    @ct.command(name="live", description="Live updating status (refreshes every 10s for 2 min)")
    async def ct_live(self, interaction: discord.Interaction):
        await interaction.response.defer()
        msg = await interaction.followup.send(embed=build_embed(fetcher.get_status()))
        for _ in range(12):
            await asyncio.sleep(10)
            try:
                await msg.edit(embed=build_embed(fetcher.get_status()))
            except Exception:
                break
            if not fetcher.state.running and not fetcher.state.paused:
                break

    @ct.command(name="reset", description="Reset the clan tag fetcher")
    async def ct_reset(self, interaction: discord.Interaction):
        fetcher.reset()
        await interaction.response.send_message("♻️ Clan tag fetcher reset.")

    @ct.command(name="preview", description="Preview first N results from clan_tags.jsonl")
    @app_commands.describe(count="How many entries to show (max 20)")
    async def ct_preview(self, interaction: discord.Interaction, count: int = 10):
        path = config.get("clan_tags_file", "data/clan_tags.jsonl")
        if not os.path.exists(path):
            await interaction.response.send_message("❌ No clan tag data yet.", ephemeral=True)
            return

        count = min(count, 20)
        lines = []
        with open(path, encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= count:
                    break
                try:
                    d   = json.loads(line)
                    tag = d.get("clan_tag") or "—"
                    lines.append(
                        f"`{d.get('id','?')}` **{d.get('name','?')}** "
                        f"Lv.{d.get('level','?')} | Tag: `{tag}`"
                    )
                except Exception:
                    continue

        if not lines:
            await interaction.response.send_message("No results found.", ephemeral=True)
            return

        embed = discord.Embed(
            title=f"🏷️ Clan Tags Preview (first {len(lines)})",
            description="\n".join(lines),
            color=discord.Color.blue(),
        )
        await interaction.response.send_message(embed=embed)


async def setup(bot):
    cog = ClanTagCog(bot)
    bot.tree.add_command(cog.ct)
    await bot.add_cog(cog)
