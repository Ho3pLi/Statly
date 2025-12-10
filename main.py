import asyncio
import discord
from discord.ext import commands

from config.settings import appSettings
from utils.logger import getLogger


appLogger = getLogger(__name__)


def createBot() -> commands.Bot:
    botIntents = discord.Intents.default()
    botClient = commands.Bot(command_prefix="!", intents=botIntents)

    @botClient.event
    async def on_ready():
        appLogger.info("Bot connected as %s (ID: %s)", botClient.user, botClient.user and botClient.user.id)
        try:
            await botClient.tree.sync()
            appLogger.info("Slash commands synced")
        except Exception:
            appLogger.exception("Failed to sync application commands")

    @botClient.tree.command(name="ping", description="Placeholder command that replies with pong.")
    async def pingCommand(interaction: discord.Interaction):
        await interaction.response.send_message("Pong!", ephemeral=True)

    return botClient


async def runBot():
    botClient = createBot()

    for extension in ["cogs.tracker", "cogs.lolReport"]:
        try:
            await botClient.load_extension(extension)
            appLogger.info("Loaded extension: %s", extension)
        except Exception:
            appLogger.exception("Failed to load extension: %s", extension)

    if not appSettings.isConfigured:
        appLogger.error("DISCORD_TOKEN is not set; update your .env file.")
        return

    await botClient.start(appSettings.discordToken)


if __name__ == "__main__":
    try:
        asyncio.run(runBot())
    except KeyboardInterrupt:
        appLogger.info("Bot shutdown requested by user")
