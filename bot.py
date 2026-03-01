"""
Critical Ops Scanner Bot — entry point
"""

import discord
from discord.ext import commands
import asyncio
import json
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

with open("config.json") as f:
    config = json.load(f)

TOKEN = config.get("discord_token", "")
if not TOKEN or TOKEN == "YOUR_DISCORD_BOT_TOKEN_HERE":
    print("ERROR: Set your discord_token in config.json")
    sys.exit(1)

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    await bot.load_extension("cogs.scanner")
    await bot.load_extension("cogs.clantags")
    await bot.load_extension("cogs.info")
    synced = await bot.tree.sync()
    print(f"✅ Synced {len(synced)} slash commands")


async def main():
    async with bot:
        await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
