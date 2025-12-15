from datetime import datetime
from typing import Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

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

    @app_commands.command(name="reportaddapex", description="Add or update your daily Apex report schedule.")
    @app_commands.rename(schedule="schedule")
    @app_commands.describe(schedule="Time in HH:MM UTC (minute capped)")
    async def reportAddApexCommand(self, interaction: discord.Interaction, schedule: str):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        externalAccount = self.getPrimaryApexAccount(interaction.user.id, interaction.guild_id)
        if not externalAccount:
            await interaction.response.send_message(
                "No linked Apex account found for you in this server. Use /registerapex first.", ephemeral=True
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
            queueType="RANKED_BR",
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
            f"Apex daily report scheduled at {schedule} UTC for your Apex account.", ephemeral=True
        )

    @app_commands.command(name="reportdisableapex", description="Disable your Apex daily report.")
    async def reportDisableApexCommand(self, interaction: discord.Interaction):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        externalAccount = self.getPrimaryApexAccount(interaction.user.id, interaction.guild_id)
        if not externalAccount:
            await interaction.response.send_message(
                "No linked Apex account found for you in this server.", ephemeral=True
            )
            return

        guildId = self.dbClient.getOrCreateGuild(str(interaction.guild_id), getattr(interaction.guild, "name", None))
        userId = self.dbClient.getOrCreateUser(
            str(interaction.user.id),
            getattr(interaction.user, "name", None),
            getattr(interaction.user, "discriminator", None),
        )
        self.dbClient.disableReportPreference(guildId, userId, externalAccount["externalAccountId"], "RANKED_BR")
        await interaction.response.send_message("Apex daily report disabled.", ephemeral=True)

    @app_commands.command(name="reportlistapex", description="List your Apex daily report schedules.")
    async def reportListApexCommand(self, interaction: discord.Interaction):
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
            WHERE g.discordGuildId = ? AND u.discordUserId = ? AND gm.code = 'APEX'
            """,
            (str(interaction.guild_id), str(interaction.user.id)),
        ).fetchall()

        if not rows:
            await interaction.response.send_message("No Apex daily reports configured in this server.", ephemeral=True)
            return

        lines = []
        for row in rows:
            status = "enabled" if row["enabled"] else "disabled"
            lines.append(f"{row['schedule']} UTC | {row['queueType']} | {row['displayName']} | {status}")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

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
            WHERE rp.enabled = 1 AND g.code = 'APEX'
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
                    apexReportLogger.exception("Failed to fetch user %s for Apex daily report", userId)
                    continue
            accountRow = self.dbClient.connection.execute(
                "SELECT externalId, displayName, tagLine FROM externalAccount WHERE id = ?", (externalAccountId,)
            ).fetchone()
            if not accountRow:
                continue
            playerName = accountRow["externalId"]
            platform = accountRow["tagLine"] or "PC"
            rankData = getApexRankSummary(playerName, platform)
            if not rankData:
                continue
            embed = self.buildRankEmbed(user, rankData)
            try:
                await user.send(embed=embed)
            except Exception:
                apexReportLogger.exception("Failed to send Apex daily report to user %s", userId)

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
    await botClient.add_cog(ApexReport(botClient))
