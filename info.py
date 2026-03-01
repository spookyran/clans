"""
/help, /config, /datainfo — utility commands
"""

import discord
from discord.ext import commands
from discord import app_commands
import json
import os

with open("config.json") as f:
    config = json.load(f)


class InfoCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="help", description="Show all bot commands")
    async def help_cmd(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="🎮 Critical Ops Scanner Bot",
            description=(
                "Scans the Critical Ops public API to find all valid player IDs, "
                "then fetches clan tags for every valid player."
            ),
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="🔍 Phase 1 — ID Scanner (`/scan`)",
            value=(
                "`/scan start` — Start or resume from checkpoint\n"
                "`/scan stop` — Stop and save progress\n"
                "`/scan pause` / `/scan resume` — Manual pause control\n"
                "`/scan status` — Progress embed\n"
                "`/scan live` — Auto-refreshing status (2 min)\n"
                "`/scan reset` — ⚠️ Wipe progress and start over\n"
                "`/scan setconfig` — Change target ID or concurrency"
            ),
            inline=False,
        )
        embed.add_field(
            name="🏷️ Phase 2 — Clan Tags (`/clantags`)",
            value=(
                "`/clantags start` — Fetch clan tags for all valid IDs\n"
                "`/clantags stop` — Stop fetcher\n"
                "`/clantags pause` / `/clantags resume`\n"
                "`/clantags status` — Progress embed\n"
                "`/clantags live` — Auto-refreshing status\n"
                "`/clantags preview [N]` — Show first N results\n"
                "`/clantags reset` — Reset fetcher"
            ),
            inline=False,
        )
        embed.add_field(
            name="ℹ️ Utility",
            value="`/help` `/config` `/datainfo`",
            inline=False,
        )
        embed.add_field(
            name="⚡ Key Behaviours",
            value=(
                "• **500** response = ID doesn't exist → skip silently\n"
                "• **403** response = pause immediately, wait 3 min, resume from SAME ID\n"
                "• Progress checkpointed every 10,000 IDs — crash-safe\n"
                "• Clan tag fetcher tries batch mode first, auto-falls back to single-ID"
            ),
            inline=False,
        )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="config", description="Show current configuration")
    async def config_cmd(self, interaction: discord.Interaction):
        with open("config.json") as f:
            cfg = json.load(f)
        embed = discord.Embed(title="⚙️ Configuration", color=discord.Color.gold())
        embed.add_field(name="Target ID",           value=f"`{cfg.get('target_id', 250_000_000):,}`",    inline=True)
        embed.add_field(name="Start ID",            value=f"`{cfg.get('start_id', 1):,}`",               inline=True)
        embed.add_field(name="Concurrency",         value=f"`{cfg.get('concurrency', 500)}`",            inline=True)
        embed.add_field(name="Timeout",             value=f"`{cfg.get('request_timeout', 10)}s`",        inline=True)
        embed.add_field(name="Retry Limit",         value=f"`{cfg.get('retry_limit', 3)}`",              inline=True)
        embed.add_field(name="403 Pause",           value=f"`{cfg.get('forbidden_pause_seconds', 180)}s`", inline=True)
        embed.add_field(name="CT Batch Size",       value=f"`{cfg.get('clan_tag_batch_size', 10)}`",     inline=True)
        embed.add_field(name="Valid IDs File",      value=f"`{cfg.get('valid_ids_file')}`",              inline=False)
        embed.add_field(name="Clan Tags File",      value=f"`{cfg.get('clan_tags_file')}`",              inline=False)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="datainfo", description="Show output file sizes and record counts")
    async def datainfo_cmd(self, interaction: discord.Interaction):
        embed = discord.Embed(title="📁 Data Files", color=discord.Color.teal())
        files = [
            (config.get("valid_ids_file",          "data/valid_ids.jsonl"),        "Valid IDs"),
            (config.get("clan_tags_file",           "data/clan_tags.jsonl"),        "Clan Tags"),
            (config.get("checkpoint_file",          "data/checkpoint.json"),        "Scanner Checkpoint"),
            (config.get("clantag_checkpoint_file",  "data/clantag_checkpoint.json"),"Clan Tag Checkpoint"),
        ]
        for path, label in files:
            if os.path.exists(path):
                size     = os.path.getsize(path)
                size_str = f"{size/1024/1024:.2f} MB" if size > 1_048_576 else f"{size/1024:.1f} KB"
                extra    = ""
                if path.endswith(".jsonl"):
                    with open(path) as f:
                        lc = sum(1 for _ in f)
                    extra = f" | **{lc:,}** records"
                embed.add_field(name=label, value=f"`{path}`\n{size_str}{extra}", inline=False)
            else:
                embed.add_field(name=label, value=f"`{path}` — *not created yet*", inline=False)
        await interaction.response.send_message(embed=embed)


async def setup(bot):
    cog = InfoCog(bot)
    bot.tree.add_command(cog.help_cmd)
    bot.tree.add_command(cog.config_cmd)
    bot.tree.add_command(cog.datainfo_cmd)
    await bot.add_cog(cog)
