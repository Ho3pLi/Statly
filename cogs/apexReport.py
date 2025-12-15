from datetime import datetime
from typing import Dict, Optional

import discord
from discord import app_commands
from discord.ext import commands

from config.settings import appSettings
from services.apex_api import getApexRankSummary
from utils.database import DatabaseClient
from utils.logger import getLogger


apexReportLogger = getLogger(__name__)


class ApexReport(commands.Cog):
    def __init__(self, botClient: commands.Bot):
        self.botClient = botClient
        self.dbClient = DatabaseClient(appSettings.databasePath)

    @app_commands.command(name="apexreport", description="Show your Apex ranked status.")
    async def apexReportCommand(self, interaction: discord.Interaction):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        if not appSettings.apexApiKey:
            await interaction.response.send_message("APEX_API_KEY is not configured. Please contact an admin.", ephemeral=True)
            return

        externalAccount = self.getPrimaryApexAccount(interaction.user.id, interaction.guild_id)
        if not externalAccount:
            await interaction.response.send_message(
                "No linked Apex account found for you in this server. Use /registerapex first.", ephemeral=True
            )
            return

        playerName = externalAccount["displayName"]
        platform = externalAccount["platform"]

        await interaction.response.defer(ephemeral=True)
        rankData = getApexRankSummary(playerName, platform)
        if not rankData:
            await interaction.followup.send("Could not fetch Apex rank data. Please try again later.", ephemeral=True)
            return

        embed = self.buildRankEmbed(interaction.user, rankData)
        await interaction.followup.send(embed=embed, ephemeral=True)

    def buildRankEmbed(self, user: discord.abc.User, rankData: Dict) -> discord.Embed:
        rankName = rankData.get("rankName", "Unknown")
        rankDiv = rankData.get("rankDiv")
        rankScore = rankData.get("rankScore")
        ladderPos = rankData.get("ladderPosPlatform")
        season = rankData.get("rankedSeason") or "N/A"
        platform = rankData.get("platform") or "N/A"
        playerName = rankData.get("playerName") or "N/A"

        titleRank = f"{rankName} Div {rankDiv}" if rankDiv is not None else rankName
        description = f"Player: **{playerName}** ({platform})\nSeason: **{season}**"
        embed = discord.Embed(
            title="Apex Ranked Status",
            description=description,
            color=discord.Color.dark_gold(),
            timestamp=datetime.utcnow(),
        )
        embed.set_author(name=str(user))
        embed.add_field(name="Rank", value=titleRank, inline=True)
        embed.add_field(name="RP", value=str(rankScore) if rankScore is not None else "N/A", inline=True)
        embed.add_field(name="Ladder Pos (platform)", value=str(ladderPos) if ladderPos is not None else "N/A", inline=True)
        if rankData.get("rankImg"):
            embed.set_thumbnail(url=rankData["rankImg"])
        return embed

    def getPrimaryApexAccount(self, discordUserId: int, guildId: int) -> Optional[Dict]:
        row = self.dbClient.connection.execute(
            """
            SELECT ea.id, ea.displayName, ea.tagLine, ea.region
            FROM guildMemberAccount gma
            JOIN externalAccount ea ON ea.id = gma.externalAccountId
            JOIN game g ON g.id = ea.gameId
            WHERE gma.guildId = (SELECT id FROM guild WHERE discordGuildId = ?)
              AND gma.userId = (SELECT id FROM user WHERE discordUserId = ?)
              AND g.code = 'APEX'
              AND gma.isPrimary = 1
            LIMIT 1
            """,
            (str(guildId), str(discordUserId)),
        ).fetchone()
        if not row:
            row = self.dbClient.connection.execute(
                """
                SELECT ea.id, ea.displayName, ea.tagLine, ea.region
                FROM guildMemberAccount gma
                JOIN externalAccount ea ON ea.id = gma.externalAccountId
                JOIN game g ON g.id = ea.gameId
                WHERE gma.guildId = (SELECT id FROM guild WHERE discordGuildId = ?)
                  AND gma.userId = (SELECT id FROM user WHERE discordUserId = ?)
                  AND g.code = 'APEX'
                LIMIT 1
                """,
                (str(guildId), str(discordUserId)),
            ).fetchone()
        if not row:
            return None
        return {
            "externalAccountId": row["id"],
            "displayName": row["displayName"],
            "platform": row["tagLine"],  # stored as platform
            "region": row["region"],
        }


async def setup(botClient: commands.Bot):
    await botClient.add_cog(ApexReport(botClient))
