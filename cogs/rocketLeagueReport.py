from datetime import datetime
from typing import Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands

from config.settings import appSettings
from services.rocket_api import fetchRocketLeagueRanks
from utils.database import DatabaseClient
from utils.logger import getLogger


rocketReportLogger = getLogger(__name__)


class RocketLeagueReport(commands.Cog):
    def __init__(self, botClient: commands.Bot):
        self.botClient = botClient
        self.dbClient = DatabaseClient(appSettings.databasePath)

    @app_commands.command(name="rocketleagueranks", description="Show your Rocket League ranks for all playlists.")
    async def rocketLeagueRanksCommand(self, interaction: discord.Interaction):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        if not appSettings.rocketLeagueApiKey:
            await interaction.response.send_message(
                "ROCKET_LEAGUE_API_KEY is not configured. Please contact an admin.", ephemeral=True
            )
            return

        externalAccount = self.getPrimaryRocketLeagueAccount(interaction.user.id, interaction.guild_id)
        if not externalAccount:
            await interaction.response.send_message(
                "No linked Rocket League account found for you in this server. Use /registerrocketleague first.",
                ephemeral=True,
            )
            return

        epicId = externalAccount["externalId"]
        await interaction.response.defer(ephemeral=True)

        ranks = fetchRocketLeagueRanks(epicId)
        if not ranks:
            await interaction.followup.send("Could not fetch Rocket League ranks. Please try again later.", ephemeral=True)
            return

        filteredRanks = self.filterRanks(ranks)
        if not filteredRanks:
            await interaction.followup.send(
                "No ranked playlists to display (after filtering non-standard modes).", ephemeral=True
            )
            return

        embed = self.buildRanksEmbed(interaction.user, epicId, filteredRanks)
        await interaction.followup.send(embed=embed, ephemeral=True)

    def buildRanksEmbed(self, user: discord.abc.User, epicId: str, ranks: List[Dict]) -> discord.Embed:
        embed = discord.Embed(
            title="Rocket League Ranks",
            description=f"Epic ID: **{epicId}**",
            color=discord.Color.dark_teal(),
            timestamp=datetime.utcnow(),
        )
        embed.set_author(name=str(user))

        for rank in ranks:
            playlist = rank.get("playlist", "Playlist")
            rankName = rank.get("rank", "Unknown")
            division = rank.get("division", "N/A")
            mmr = rank.get("mmr", "N/A")
            streak = rank.get("streak", "N/A")
            embed.add_field(
                name=playlist,
                value=f"Rank: {rankName} (Div {division})\nMMR: {mmr}\nStreak: {streak}",
                inline=False,
            )
        return embed

    def filterRanks(self, ranks: List[Dict]) -> List[Dict]:
        excluded = {"hoops", "rumble", "dropshot", "snow day", "snowday"}
        filtered: List[Dict] = []
        for rank in ranks:
            playlist = (rank.get("playlist") or "").lower()
            if any(ex in playlist for ex in excluded):
                continue
            filtered.append(rank)
        return filtered

    def getPrimaryRocketLeagueAccount(self, discordUserId: int, guildId: int) -> Optional[Dict]:
        row = self.dbClient.connection.execute(
            """
            SELECT ea.id, ea.displayName, ea.externalId
            FROM guildMemberAccount gma
            JOIN externalAccount ea ON ea.id = gma.externalAccountId
            JOIN game g ON g.id = ea.gameId
            WHERE gma.guildId = (SELECT id FROM guild WHERE discordGuildId = ?)
              AND gma.userId = (SELECT id FROM user WHERE discordUserId = ?)
              AND g.code = 'RL'
              AND gma.isPrimary = 1
            LIMIT 1
            """,
            (str(guildId), str(discordUserId)),
        ).fetchone()
        if not row:
            row = self.dbClient.connection.execute(
                """
                SELECT ea.id, ea.displayName, ea.externalId
                FROM guildMemberAccount gma
                JOIN externalAccount ea ON ea.id = gma.externalAccountId
                JOIN game g ON g.id = ea.gameId
                WHERE gma.guildId = (SELECT id FROM guild WHERE discordGuildId = ?)
                  AND gma.userId = (SELECT id FROM user WHERE discordUserId = ?)
                  AND g.code = 'RL'
                LIMIT 1
                """,
                (str(guildId), str(discordUserId)),
            ).fetchone()
        if not row:
            return None
        return {"externalAccountId": row["id"], "displayName": row["displayName"], "externalId": row["externalId"]}


async def setup(botClient: commands.Bot):
    await botClient.add_cog(RocketLeagueReport(botClient))
