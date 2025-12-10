import discord
from discord import app_commands
from discord.ext import commands

from config.settings import appSettings
from utils.database import DatabaseClient
from utils.logger import getLogger
from utils.riotApi import RiotAPI


trackerLogger = getLogger(__name__)


class Tracker(commands.Cog):
    """Game tracking placeholder cog with user registration support (SQLite-backed)."""

    def __init__(self, botClient: commands.Bot):
        self.botClient = botClient
        self.riotApi = RiotAPI(appSettings.riotRegion)
        self.dbClient = DatabaseClient(appSettings.databasePath)

    @app_commands.command(name="register", description="Link your Riot ID to the bot.")
    @app_commands.rename(gameName="gamename", tagLine="tagline")
    async def registerCommand(self, interaction: discord.Interaction, gameName: str, tagLine: str):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        if not appSettings.riotApiKey:
            await interaction.response.send_message("RIOT_API_KEY is not configured. Please contact an admin.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        account = self.riotApi.getAccountByRiotId(gameName, tagLine)
        if not account:
            await interaction.followup.send("Could not find that account. Check the Riot ID (name#tag).", ephemeral=True)
            return

        puuid = account.get("puuid")
        if not puuid:
            await interaction.followup.send("Account found but missing PUUID; please try again later.", ephemeral=True)
            return

        gameId = self.dbClient.getOrCreateGame("LOL", "League of Legends")
        guildId = self.dbClient.getOrCreateGuild(str(interaction.guild_id), getattr(interaction.guild, "name", None))
        userId = self.dbClient.getOrCreateUser(
            str(interaction.user.id),
            getattr(interaction.user, "name", None),
            getattr(interaction.user, "discriminator", None),
        )
        externalAccountId = self.dbClient.getOrCreateExternalAccount(
            gameId=gameId,
            externalId=puuid,
            displayName=gameName,
            tagLine=tagLine,
            region=appSettings.riotRegion,
        )

        linkOk = self.dbClient.linkGuildMemberAccount(guildId, userId, externalAccountId, forcePrimary=False)
        if not linkOk:
            await interaction.followup.send("Error while saving the account; please try again later.", ephemeral=True)
            return

        await interaction.followup.send(
            f"Linked Riot ID {gameName}#{tagLine}. PUUID stored for this server.", ephemeral=True
        )


async def setup(botClient: commands.Bot):
    await botClient.add_cog(Tracker(botClient))
