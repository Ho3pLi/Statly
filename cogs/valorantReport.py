from datetime import datetime
from typing import Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

from config.settings import appSettings
from services.valorantTracking import generateDailyReport
from utils.database import DatabaseClient
from utils.logger import getLogger


valorantReportLogger = getLogger(__name__)


class ValorantReport(commands.Cog):
    def __init__(self, botClient: commands.Bot):
        self.botClient = botClient
        self.dbClient = DatabaseClient(appSettings.databasePath)

    @app_commands.command(name="valorantreport", description="Send your daily Valorant ranked report (mock data).")
    async def valorantReportCommand(self, interaction: discord.Interaction):
        queueType = "COMPETITIVE"
        externalAccountId = self.getPrimaryValorantAccountId(interaction.user.id, interaction.guild_id)
        if not externalAccountId:
            await interaction.response.send_message(
                "No linked Valorant account found for you in this server.", ephemeral=True
            )
            return

        todayStr = datetime.utcnow().strftime("%Y-%m-%d")
        reportData = await generateDailyReport(self.dbClient, externalAccountId, queueType, todayStr)

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
            title="Daily Valorant Ranked Report",
            description=f"Queue: **{queueType}**\nAccount: **{accountLabel}**",
            color=discord.Color.red(),
            timestamp=datetime.utcnow(),
        )
        embed.set_author(name=str(user))

        baselineText = (
            f"{baseline.get('tier', 'N/A')} {baseline.get('division', '')} "
            f"({baseline.get('lp', 0)} RR) | W {baseline.get('wins', 0)} / L {baseline.get('losses', 0)}"
        )
        currentText = (
            f"{current.get('tier', 'N/A')} {current.get('division', '')} "
            f"({current.get('lp', 0)} RR) | W {current.get('wins', 0)} / L {current.get('losses', 0)}"
        )
        diffTextParts: List[str] = []
        lpDiff = diff.get("lpDiff", 0)
        diffTextParts.append(f"RR diff: {lpDiff:+}")
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

    def getPrimaryValorantAccountId(self, discordUserId: int, guildId: Optional[int]) -> Optional[int]:
        return self.getPrimaryAccountId(discordUserId, guildId, "VAL")

    def getPrimaryAccountId(self, discordUserId: int, guildId: Optional[int], gameCode: str) -> Optional[int]:
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
              AND g.code = ?
              AND gma.isPrimary = 1
            LIMIT 1
            """,
            (str(guildId), str(discordUserId), gameCode),
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
              AND g.code = ?
            LIMIT 1
            """,
            (str(guildId), str(discordUserId), gameCode),
        ).fetchone()
        return int(fallback["externalAccountId"]) if fallback else None

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
            WHERE rp.enabled = 1 AND g.code = 'VAL'
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
            queueType = pref.get("queueType", "COMPETITIVE")
            userId = pref.get("discordUserId")
            if not externalAccountId or not userId:
                continue
            user = self.botClient.get_user(int(userId))
            if not user:
                continue
            reportData = await generateDailyReport(
                self.dbClient, externalAccountId, queueType, datetime.utcnow().strftime("%Y-%m-%d")
            )
            accountInfo = self.getExternalAccountInfo(externalAccountId)
            embed = self.buildReportEmbed(user, queueType, reportData, accountInfo)
            try:
                await user.send(embed=embed)
            except Exception:
                valorantReportLogger.exception("Failed to send daily Valorant report to user %s", userId)

    @reportLoop.before_loop
    async def beforeReportLoop(self):
        await self.botClient.wait_until_ready()

    @commands.Cog.listener("on_ready")
    async def onReady(self):
        if not self.reportLoop.is_running():
            self.reportLoop.start()


async def setup(botClient: commands.Bot):
    await botClient.add_cog(ValorantReport(botClient))
