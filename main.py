import asyncio
import discord
from discord.ext import commands

from config.settings import settings
from utils.logger import get_logger


logger = get_logger(__name__)


def create_bot() -> commands.Bot:
    intents = discord.Intents.default()
    bot = commands.Bot(command_prefix="!", intents=intents)

    @bot.event
    async def on_ready():
        logger.info("Bot connected as %s (ID: %s)", bot.user, bot.user and bot.user.id)
        try:
            # Ensure application commands are registered for the guilds the bot is in.
            await bot.tree.sync()
            logger.info("Slash commands synced")
        except Exception:
            logger.exception("Failed to sync application commands")

    @bot.tree.command(name="ping", description="Placeholder command that replies with pong.")
    async def ping_command(interaction: discord.Interaction):
        await interaction.response.send_message("Pong!", ephemeral=True)

    return bot


async def main():
    bot = create_bot()

    try:
        await bot.load_extension("cogs.tracker")
        logger.info("Loaded extension: cogs.tracker")
    except Exception:
        logger.exception("Failed to load extension: cogs.tracker")

    if not settings.is_configured:
        logger.error("DISCORD_TOKEN is not set; update your .env file.")
        return

    await bot.start(settings.DISCORD_TOKEN)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shutdown requested by user")
