from datetime import datetime
from typing import Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

from config.settings import appSettings
from services.lolTracking import generateDailyReport
from utils.database import DatabaseClient
from utils.logger import getLogger


lolReportLogger = getLogger(__name__)


class LolReport(commands.Cog):
    def __init__(self, botClient: commands.Bot):
        self.botClient = botClient
        self.dbClient = DatabaseClient(appSettings.databasePath)

    @app_commands.command(name="dailyreport", description="Send your daily League ranked report.")
    @app_commands.rename(queueType="queuetype")
    @app_commands.describe(queueType="Queue type, e.g., RANKED_SOLO_5x5")
    async def dailyReportCommand(self, interaction: discord.Interaction, queueType: str):
        externalAccountId = self.getPrimaryLolAccountId(interaction.user.id, interaction.guild_id)
        if not externalAccountId:
            await interaction.response.send_message(
                "No linked League account found for you in this server.", ephemeral=True
            )
            return

        todayStr = datetime.utcnow().strftime("%Y-%m-%d")
        reportData = generateDailyReport(self.dbClient, externalAccountId, queueType, todayStr)

        accountInfo = self.getExternalAccountInfo(externalAccountId)
        embed = self.buildReportEmbed(interaction.user, queueType, reportData, accountInfo)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    def buildReportEmbed(
        self, user: discord.abc.User, queueType: str, reportData: Dict, accountInfo: Optional[Dict]
    ) -> discord.Embed:
        baseline = reportData.get("baseline") or {}
        current = reportData.get("current") or {}
        diff = reportData.get("diff") or {}

        accountLabel = "Unknown account"
        if accountInfo:
            tag = f"#{accountInfo.get('tagLine')}" if accountInfo.get("tagLine") else ""
            region = f" ({accountInfo.get('region')})" if accountInfo.get("region") else ""
            accountLabel = f"{accountInfo.get('displayName', 'Unknown')}{tag}{region}"

        embed = discord.Embed(
            title="Daily Ranked Report",
            description=f"Queue: **{queueType}**\nAccount: **{accountLabel}**",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow(),
        )
        embed.set_author(name=str(user))

        baselineText = (
            f"{baseline.get('tier', 'N/A')} {baseline.get('division', '')} "
            f"({baseline.get('lp', 0)} LP) | W {baseline.get('wins', 0)} / L {baseline.get('losses', 0)}"
        )
        currentText = (
            f"{current.get('tier', 'N/A')} {current.get('division', '')} "
            f"({current.get('lp', 0)} LP) | W {current.get('wins', 0)} / L {current.get('losses', 0)}"
        )
        diffTextParts: List[str] = []
        lpDiff = diff.get("lpDiff", 0)
        diffTextParts.append(f"LP diff: {lpDiff:+}")
        tierChange = diff.get("tierChange")
        if tierChange:
            diffTextParts.append(f"Rank change: {tierChange}")
        else:
            if diff.get("rankUp"):
                diffTextParts.append("Rank movement: up")
            elif diff.get("rankDown"):
                diffTextParts.append("Rank movement: down")
            else:
                diffTextParts.append("Rank movement: none")

        embed.add_field(name="Baseline", value=baselineText, inline=False)
        embed.add_field(name="Current", value=currentText, inline=False)
        embed.add_field(name="Diff", value="\n".join(diffTextParts), inline=False)

        return embed

    def getPrimaryLolAccountId(self, discordUserId: int, guildId: Optional[int]) -> Optional[int]:
        if guildId is None:
            return None
        row = self.dbClient.connection.execute(
            """
            SELECT gma.externalAccountId
            FROM guildMemberAccount gma
            JOIN externalAccount ea ON ea.id = gma.externalAccountId
            JOIN game g ON g.id = ea.gameId
            WHERE gma.guildId = (SELECT id FROM guild WHERE discordGuildId = ?)
              AND gma.userId = (SELECT id FROM user WHERE discordUserId = ?)
              AND g.code = 'LOL'
              AND gma.isPrimary = 1
            LIMIT 1
            """,
            (str(guildId), str(discordUserId)),
        ).fetchone()
        if row:
            return int(row["externalAccountId"])
        fallback = self.dbClient.connection.execute(
            """
            SELECT gma.externalAccountId
            FROM guildMemberAccount gma
            JOIN externalAccount ea ON ea.id = gma.externalAccountId
            JOIN game g ON g.id = ea.gameId
            WHERE gma.guildId = (SELECT id FROM guild WHERE discordGuildId = ?)
              AND gma.userId = (SELECT id FROM user WHERE discordUserId = ?)
              AND g.code = 'LOL'
            LIMIT 1
            """,
            (str(guildId), str(discordUserId)),
        ).fetchone()
        return int(fallback["externalAccountId"]) if fallback else None

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
            WHERE rp.enabled = 1
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
            queueType = pref.get("queueType", "RANKED_SOLO_5x5")
            userId = pref.get("discordUserId")
            if not externalAccountId or not userId:
                continue
            user = self.botClient.get_user(int(userId))
            if not user:
                try:
                    user = await self.botClient.fetch_user(int(userId))
                except Exception:
                    lolReportLogger.exception("Failed to fetch user %s for daily report", userId)
                    continue
            reportData = generateDailyReport(
                self.dbClient, externalAccountId, queueType, datetime.utcnow().strftime("%Y-%m-%d")
            )
            accountInfo = self.getExternalAccountInfo(externalAccountId)
            embed = self.buildReportEmbed(user, queueType, reportData, accountInfo)
            try:
                await user.send(embed=embed)
            except Exception:
                lolReportLogger.exception("Failed to send daily report to user %s", userId)

    @reportLoop.before_loop
    async def beforeReportLoop(self):
        await self.botClient.wait_until_ready()

    @commands.Cog.listener("on_ready")
    async def onReady(self):
        if not self.reportLoop.is_running():
            self.reportLoop.start()

    @app_commands.command(name="reportadd", description="Add or update your daily report schedule.")
    @app_commands.rename(queueType="queuetype", schedule="schedule")
    @app_commands.describe(
        queueType="Queue type, e.g., RANKED_SOLO_5x5",
        schedule="Time in HH:MM UTC (minute capped at 25 users)",
    )
    async def reportAddCommand(self, interaction: discord.Interaction, queueType: str, schedule: str):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        externalAccountId = self.getPrimaryLolAccountId(interaction.user.id, interaction.guild_id)
        if not externalAccountId:
            await interaction.response.send_message(
                "No linked League account found for you in this server.", ephemeral=True
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
            externalAccountId=externalAccountId,
            queueType=queueType,
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
            f"Daily report scheduled at {schedule} UTC for queue {queueType}.", ephemeral=True
        )

    @app_commands.command(name="reportlist", description="List your daily report schedules.")
    async def reportListCommand(self, interaction: discord.Interaction):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        rows = self.dbClient.connection.execute(
            """
            SELECT rp.queueType, rp.schedule, rp.enabled, ea.displayName, ea.tagLine, ea.region
            FROM reportPreference rp
            JOIN user u ON u.id = rp.userId
            JOIN guild g ON g.id = rp.guildId
            JOIN externalAccount ea ON ea.id = rp.externalAccountId
            JOIN game gm ON gm.id = ea.gameId
            WHERE g.discordGuildId = ? AND u.discordUserId = ? AND gm.code = 'LOL'
            """,
            (str(interaction.guild_id), str(interaction.user.id)),
        ).fetchall()

        if not rows:
            await interaction.response.send_message("No daily reports configured for you in this server.", ephemeral=True)
            return

        lines = []
        for row in rows:
            tag = f"#{row['tagLine']}" if row["tagLine"] else ""
            region = f" ({row['region']})" if row["region"] else ""
            status = "enabled" if row["enabled"] else "disabled"
            lines.append(
                f"{row['schedule']} UTC | {row['queueType']} | {row['displayName']}{tag}{region} | {status}"
            )

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @app_commands.command(name="reportdisable", description="Disable your daily report for a queue.")
    @app_commands.rename(queueType="queuetype")
    @app_commands.describe(queueType="Queue type to disable, e.g., RANKED_SOLO_5x5")
    async def reportDisableCommand(self, interaction: discord.Interaction, queueType: str):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        externalAccountId = self.getPrimaryLolAccountId(interaction.user.id, interaction.guild_id)
        if not externalAccountId:
            await interaction.response.send_message(
                "No linked League account found for you in this server.", ephemeral=True
            )
            return

        guildId = self.dbClient.getOrCreateGuild(str(interaction.guild_id), getattr(interaction.guild, "name", None))
        userId = self.dbClient.getOrCreateUser(
            str(interaction.user.id),
            getattr(interaction.user, "name", None),
            getattr(interaction.user, "discriminator", None),
        )

        self.dbClient.disableReportPreference(guildId, userId, externalAccountId, queueType)
        await interaction.response.send_message(
            f"Daily report disabled for queue {queueType} (primary account).", ephemeral=True
        )

    def isValidSchedule(self, schedule: str) -> bool:
        try:
            datetime.strptime(schedule, "%H:%M")
            return True
        except ValueError:
            return False

    def getExternalAccountInfo(self, externalAccountId: int) -> Optional[Dict]:
        row = self.dbClient.connection.execute(
            """
            SELECT externalId, displayName, tagLine, region
            FROM externalAccount
            WHERE id = ?
            """,
            (externalAccountId,),
        ).fetchone()
        return dict(row) if row else None


async def setup(botClient: commands.Bot):
    await botClient.add_cog(LolReport(botClient))
