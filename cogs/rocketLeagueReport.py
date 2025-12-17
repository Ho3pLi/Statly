import asyncio
from datetime import datetime
from typing import Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

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

        ranks = await asyncio.to_thread(fetchRocketLeagueRanks, epicId)
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

    @app_commands.command(name="reportaddrl", description="Add or update your daily Rocket League report schedule.")
    @app_commands.rename(schedule="schedule")
    @app_commands.describe(schedule="Time in HH:MM UTC (minute capped)")
    async def reportAddRocketLeagueCommand(self, interaction: discord.Interaction, schedule: str):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        externalAccount = self.getPrimaryRocketLeagueAccount(interaction.user.id, interaction.guild_id)
        if not externalAccount:
            await interaction.response.send_message(
                "No linked Rocket League account found for you in this server. Use /registerrocketleague first.",
                ephemeral=True,
            )
            return

        if not self.isValidSchedule(schedule):
            await interaction.response.send_message("Schedule must be in HH:MM format (UTC).", ephemeral=True)
            return

        guildId = self.dbClient.getOrCreateGuild(str(interaction.guild_id), getattr(interaction.guild, "name", None))
        userId = self.dbClient.getOrCreateUser(
            str(interaction.user.id),
            getattr(interaction.user, "name", None),
            getattr(interaction.user, "discriminator", None),
        )
        maxPerMinute = appSettings.reportSlotsPerMinute
        created = self.dbClient.upsertReportPreference(
            guildId=guildId,
            userId=userId,
            externalAccountId=externalAccount["externalAccountId"],
            queueType="ALL_PLAYLISTS",
            schedule=schedule,
            maxPerMinute=maxPerMinute,
        )
        if not created:
            await interaction.response.send_message(
                f"Schedule {schedule} is full ({maxPerMinute} users). Please choose a different minute.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            f"Rocket League daily report scheduled at {schedule} UTC.", ephemeral=True
        )

    @app_commands.command(name="reportdisable_rl", description="Disable your Rocket League daily report.")
    async def reportDisableRocketLeagueCommand(self, interaction: discord.Interaction):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        externalAccount = self.getPrimaryRocketLeagueAccount(interaction.user.id, interaction.guild_id)
        if not externalAccount:
            await interaction.response.send_message(
                "No linked Rocket League account found for you in this server.", ephemeral=True
            )
            return

        guildId = self.dbClient.getOrCreateGuild(str(interaction.guild_id), getattr(interaction.guild, "name", None))
        userId = self.dbClient.getOrCreateUser(
            str(interaction.user.id),
            getattr(interaction.user, "name", None),
            getattr(interaction.user, "discriminator", None),
        )
        self.dbClient.disableReportPreference(guildId, userId, externalAccount["externalAccountId"], "ALL_PLAYLISTS")
        await interaction.response.send_message("Rocket League daily report disabled.", ephemeral=True)

    @app_commands.command(name="reportlist_rl", description="List your Rocket League daily report schedules.")
    async def reportListRocketLeagueCommand(self, interaction: discord.Interaction):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        rows = self.dbClient.connection.execute(
            """
            SELECT rp.queueType, rp.schedule, rp.enabled, ea.displayName
            FROM reportPreference rp
            JOIN user u ON u.id = rp.userId
            JOIN guild g ON g.id = rp.guildId
            JOIN externalAccount ea ON ea.id = rp.externalAccountId
            JOIN game gm ON gm.id = ea.gameId
            WHERE g.discordGuildId = ? AND u.discordUserId = ? AND gm.code = 'RL'
            """,
            (str(interaction.guild_id), str(interaction.user.id)),
        ).fetchall()

        if not rows:
            await interaction.response.send_message("No Rocket League daily reports configured in this server.", ephemeral=True)
            return

        lines = []
        for row in rows:
            status = "enabled" if row["enabled"] else "disabled"
            lines.append(f"{row['schedule']} UTC | {row['queueType']} | {row['displayName']} | {status}")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

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

    def getUsersWithDailyReports(self) -> List[Dict]:
        rows = self.dbClient.connection.execute(
            """
            SELECT
                rp.externalAccountId,
                rp.queueType,
                rp.schedule,
                u.discordUserId
            FROM reportPreference rp
            JOIN user u ON u.id = rp.userId
            JOIN externalAccount ea ON ea.id = rp.externalAccountId
            JOIN game g ON g.id = ea.gameId
            WHERE rp.enabled = 1 AND g.code = 'RL'
            """,
        ).fetchall()
        return [dict(row) for row in rows]

    @tasks.loop(minutes=1)
    async def reportLoop(self):
        nowUtc = datetime.utcnow().strftime("%H:%M")
        usersToNotify = self.getUsersWithDailyReports()
        for pref in usersToNotify:
            if pref.get("schedule") != nowUtc:
                continue
            externalAccountId = pref.get("externalAccountId")
            userId = pref.get("discordUserId")
            if not externalAccountId or not userId:
                continue
            user = self.botClient.get_user(int(userId))
            if not user:
                try:
                    user = await self.botClient.fetch_user(int(userId))
                except Exception:
                    rocketReportLogger.exception("Failed to fetch user %s for Rocket League daily report", userId)
                    continue
            accountRow = self.dbClient.connection.execute(
                "SELECT externalId FROM externalAccount WHERE id = ?", (externalAccountId,)
            ).fetchone()
            if not accountRow:
                continue
            epicId = accountRow["externalId"]
            ranks = await asyncio.to_thread(fetchRocketLeagueRanks, epicId)
            if not ranks:
                continue
            filteredRanks = self.filterRanks(ranks)
            if not filteredRanks:
                continue
            embed = self.buildRanksEmbed(user, epicId, filteredRanks)
            try:
                await user.send(embed=embed)
            except Exception:
                rocketReportLogger.exception("Failed to send Rocket League daily report to user %s", userId)

    @reportLoop.before_loop
    async def beforeReportLoop(self):
        await self.botClient.wait_until_ready()

    @commands.Cog.listener("on_ready")
    async def onReady(self):
        if not self.reportLoop.is_running():
            self.reportLoop.start()

    def isValidSchedule(self, schedule: str) -> bool:
        try:
            datetime.strptime(schedule, "%H:%M")
            return True
        except ValueError:
            return False


async def setup(botClient: commands.Bot):
    await botClient.add_cog(RocketLeagueReport(botClient))
