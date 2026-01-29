import asyncio
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands, tasks

from config.settings import appSettings
from services.riot_api import (
    fetchValorantCurrentRankByNameTag,
    fetchValorantDailySnapshotByNameTag,
    resolveValorantRegion,
)
from services.valorantTracking import VALORANT_TIER_ORDER, generateDailyReport
from utils.database import DatabaseClient
from utils.logger import getLogger


valorantReportLogger = getLogger(__name__)


class ValorantReport(commands.Cog):
    def __init__(self, botClient: commands.Bot):
        self.botClient = botClient
        self.dbClient = DatabaseClient(appSettings.databasePath)

    @app_commands.command(name="valorantreport", description="Send your daily Valorant ranked report.")
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

    @app_commands.command(name="registergroup", description="Register a Valorant group of Riot IDs.")
    @app_commands.rename(groupName="groupname", members="members", region="region")
    @app_commands.describe(
        groupName="Group name (unique per server)",
        members="Comma or newline separated Riot IDs (name#tag)",
        region="Valorant region (e.g., eu, na, ap). Defaults to server Riot region.",
    )
    async def registerGroupCommand(
        self,
        interaction: discord.Interaction,
        groupName: str,
        members: str,
        region: Optional[str] = None,
    ):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.")
            return
        groupName = groupName.strip()
        if not groupName:
            await interaction.response.send_message("Group name cannot be empty.")
            return

        parsed_members, invalid = self.parseRiotIdList(members)
        if not parsed_members:
            await interaction.response.send_message(
                "No valid Riot IDs found. Provide one or more `name#tag` entries, separated by commas or newlines.",
            )
            return
        if len(parsed_members) > 25:
            await interaction.response.send_message(
                "Too many members (max 25). Split the group into smaller chunks.",
            )
            return

        regionValue = resolveValorantRegion(region or appSettings.riotRegion)
        for member in parsed_members:
            member["region"] = regionValue

        guildId = self.dbClient.getOrCreateGuild(str(interaction.guild_id), getattr(interaction.guild, "name", None))
        userId = self.dbClient.getOrCreateUser(
            str(interaction.user.id),
            getattr(interaction.user, "name", None),
            getattr(interaction.user, "discriminator", None),
        )
        groupId = self.dbClient.getOrCreateValorantGroup(guildId, groupName, userId)
        self.dbClient.replaceValorantGroupMembers(groupId, parsed_members)

        invalid_note = f" Invalid entries skipped: {', '.join(invalid)}" if invalid else ""
        await interaction.response.send_message(
            f"Group **{groupName}** saved with {len(parsed_members)} members.{invalid_note}"
        )

    @app_commands.command(name="groupreport", description="Show a Valorant rank report for a saved group.")
    @app_commands.rename(groupName="groupname")
    @app_commands.describe(groupName="Group name registered with /registergroup")
    async def groupReportCommand(self, interaction: discord.Interaction, groupName: str):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.")
            return
        groupName = groupName.strip()
        if not groupName:
            await interaction.response.send_message("Group name cannot be empty.")
            return
        if not appSettings.valorantApiKey:
            await interaction.response.send_message(
                "VALORANT_API_KEY is not configured. Please contact an admin.",
            )
            return

        await interaction.response.defer()

        guildId = self.dbClient.getOrCreateGuild(str(interaction.guild_id), getattr(interaction.guild, "name", None))
        groupRow = self.dbClient.getValorantGroup(guildId, groupName)
        if not groupRow:
            available = self.dbClient.listValorantGroups(guildId)
            if available:
                await interaction.followup.send(
                    f"Group **{groupName}** not found. Available groups: {', '.join(available)}",
                )
            else:
                await interaction.followup.send(
                    f"Group **{groupName}** not found. Create one with /registergroup.",
                )
            return

        members = self.dbClient.getValorantGroupMembers(int(groupRow["id"]))
        if not members:
            await interaction.followup.send(
                f"Group **{groupName}** has no members. Re-register it with /registergroup.",
            )
            return

        results = []
        failures = []
        todayStr = datetime.utcnow().strftime("%Y-%m-%d")
        for member in members:
            displayName = member["displayName"]
            tagLine = member["tagLine"]
            region = resolveValorantRegion(member.get("region") or appSettings.riotRegion)
            snapshot = await fetchValorantDailySnapshotByNameTag(
                appSettings.valorantApiKey,
                region,
                "pc",
                displayName,
                tagLine,
                todayStr,
            )
            rankData = None
            if snapshot:
                current = snapshot.get("current") or {}
                rankData = {
                    "tier": current.get("tier"),
                    "division": current.get("division"),
                    "lp": current.get("lp"),
                    "lpDiff": snapshot.get("lpDiff"),
                }
            if not rankData:
                rankData = await asyncio.to_thread(
                    fetchValorantCurrentRankByNameTag,
                    appSettings.valorantApiKey,
                    region,
                    "pc",
                    displayName,
                    tagLine,
                )
            if not rankData:
                failures.append(f"{displayName}#{tagLine}")
                continue
            rankData["displayName"] = displayName
            rankData["tagLine"] = tagLine
            results.append(rankData)

        results.sort(key=self.valorantRankKey, reverse=True)
        embed = self.buildGroupReportEmbed(groupName, results, failures)
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="groupdelete", description="Delete a saved Valorant group.")
    @app_commands.rename(groupName="groupname")
    @app_commands.describe(groupName="Group name to delete")
    async def groupDeleteCommand(self, interaction: discord.Interaction, groupName: str):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.")
            return
        groupName = groupName.strip()
        if not groupName:
            await interaction.response.send_message("Group name cannot be empty.")
            return
        guildId = self.dbClient.getOrCreateGuild(str(interaction.guild_id), getattr(interaction.guild, "name", None))
        groupRow = self.dbClient.getValorantGroup(guildId, groupName)
        if not groupRow:
            await interaction.response.send_message(
                f"Group **{groupName}** not found."
            )
            return
        self.dbClient.deleteValorantGroup(int(groupRow["id"]))
        await interaction.response.send_message(f"Group **{groupRow['name']}** deleted.")

    @app_commands.command(name="groupaddmembers", description="Add Riot IDs to an existing Valorant group.")
    @app_commands.rename(groupName="groupname", members="members", region="region")
    @app_commands.describe(
        groupName="Group name to update",
        members="Comma or newline separated Riot IDs (name#tag)",
        region="Valorant region for added members (e.g., eu, na, ap). Defaults to server Riot region.",
    )
    async def groupAddMembersCommand(
        self,
        interaction: discord.Interaction,
        groupName: str,
        members: str,
        region: Optional[str] = None,
    ):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.")
            return
        groupName = groupName.strip()
        if not groupName:
            await interaction.response.send_message("Group name cannot be empty.")
            return
        parsed_members, invalid = self.parseRiotIdList(members)
        if not parsed_members:
            await interaction.response.send_message(
                "No valid Riot IDs found. Provide one or more `name#tag` entries, separated by commas or newlines.",
            )
            return

        guildId = self.dbClient.getOrCreateGuild(str(interaction.guild_id), getattr(interaction.guild, "name", None))
        groupRow = self.dbClient.getValorantGroup(guildId, groupName)
        if not groupRow:
            await interaction.response.send_message(
                f"Group **{groupName}** not found. Create one with /registergroup.",
            )
            return

        regionValue = resolveValorantRegion(region or appSettings.riotRegion)
        for member in parsed_members:
            member["region"] = regionValue

        existing_members = self.dbClient.getValorantGroupMembers(int(groupRow["id"]))
        existing_keys = {
            (m["displayName"].lower(), m["tagLine"].lower()) for m in existing_members
        }
        to_add = [
            member
            for member in parsed_members
            if (member["displayName"].lower(), member["tagLine"].lower()) not in existing_keys
        ]
        self.dbClient.addValorantGroupMembers(int(groupRow["id"]), to_add)

        skipped = len(parsed_members) - len(to_add)
        invalid_note = f" Invalid entries skipped: {', '.join(invalid)}" if invalid else ""
        await interaction.response.send_message(
            f"Group **{groupRow['name']}** updated. Added {len(to_add)} members, skipped {skipped}.{invalid_note}"
        )

    @app_commands.command(name="groupremovemembers", description="Remove Riot IDs from a Valorant group.")
    @app_commands.rename(groupName="groupname", members="members")
    @app_commands.describe(
        groupName="Group name to update",
        members="Comma or newline separated Riot IDs (name#tag)",
    )
    async def groupRemoveMembersCommand(
        self,
        interaction: discord.Interaction,
        groupName: str,
        members: str,
    ):
        if not interaction.guild_id:
            await interaction.response.send_message("This command must be used in a server.")
            return
        groupName = groupName.strip()
        if not groupName:
            await interaction.response.send_message("Group name cannot be empty.")
            return
        parsed_members, invalid = self.parseRiotIdList(members)
        if not parsed_members:
            await interaction.response.send_message(
                "No valid Riot IDs found. Provide one or more `name#tag` entries, separated by commas or newlines.",
            )
            return

        guildId = self.dbClient.getOrCreateGuild(str(interaction.guild_id), getattr(interaction.guild, "name", None))
        groupRow = self.dbClient.getValorantGroup(guildId, groupName)
        if not groupRow:
            await interaction.response.send_message(
                f"Group **{groupName}** not found."
            )
            return

        existing_members = self.dbClient.getValorantGroupMembers(int(groupRow["id"]))
        existing_keys = {
            (m["displayName"].lower(), m["tagLine"].lower()) for m in existing_members
        }
        to_remove = [
            member
            for member in parsed_members
            if (member["displayName"].lower(), member["tagLine"].lower()) in existing_keys
        ]
        self.dbClient.removeValorantGroupMembers(int(groupRow["id"]), to_remove)

        skipped = len(parsed_members) - len(to_remove)
        invalid_note = f" Invalid entries skipped: {', '.join(invalid)}" if invalid else ""
        await interaction.response.send_message(
            f"Group **{groupRow['name']}** updated. Removed {len(to_remove)} members, skipped {skipped}.{invalid_note}"
        )

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

    def parseRiotIdList(self, raw: str) -> Tuple[List[Dict], List[str]]:
        parts = re.split(r"[,\n;]+", raw)
        valid: List[Dict] = []
        invalid: List[str] = []
        for part in parts:
            candidate = part.strip()
            if not candidate:
                continue
            if "#" not in candidate:
                invalid.append(candidate)
                continue
            name, tag = candidate.split("#", 1)
            name = name.strip()
            tag = tag.strip()
            if not name or not tag:
                invalid.append(candidate)
                continue
            valid.append({"displayName": name, "tagLine": tag})
        return valid, invalid

    def valorantRankKey(self, entry: Dict) -> Tuple[int, int, int]:
        tier = entry.get("tier")
        tierIndex = VALORANT_TIER_ORDER.index(tier) if tier in VALORANT_TIER_ORDER else -1
        division = entry.get("division")
        divOrder = {"1": 0, "2": 1, "3": 2}
        divisionValue = divOrder.get(str(division), -1)
        rrValue = entry.get("lp")
        try:
            rrValue = int(rrValue)
        except (TypeError, ValueError):
            rrValue = -1
        return tierIndex, divisionValue, rrValue

    def buildGroupReportEmbed(self, groupName: str, results: List[Dict], failures: List[str]) -> discord.Embed:
        totalMembers = len(results) + len(failures)
        embed = discord.Embed(
            title="Valorant Group Report",
            description=f"Group: **{groupName}**\nMembers: **{totalMembers}**",
            color=discord.Color.red(),
            timestamp=datetime.utcnow(),
        )

        if results:
            lines = []
            for idx, entry in enumerate(results, start=1):
                name = f"{entry.get('displayName')}#{entry.get('tagLine')}"
                tier = entry.get("tier") or "UNRANKED"
                division = entry.get("division")
                rr = entry.get("lp")
                if division:
                    rankLabel = f"{tier} {division}"
                else:
                    rankLabel = tier
                rrLabel = f"{rr} RR" if rr is not None else "RR N/A"
                lpDiff = entry.get("lpDiff")
                if isinstance(lpDiff, int):
                    diffLabel = f" | Today {lpDiff:+} RR"
                else:
                    diffLabel = ""
                lines.append(f"{idx}. {name} — {rankLabel} ({rrLabel}){diffLabel}")
            embed.add_field(name="Rankings", value="\n".join(lines), inline=False)

        if failures:
            embed.add_field(
                name="Missing data",
                value="No data for: " + ", ".join(failures),
                inline=False,
            )

        return embed

    def getUsersWithDailyReports(self) -> List[Dict]:
        rows = self.dbClient.connection.execute(
            """
            SELECT
                rp.externalAccountId,
                rp.queueType,
                rp.schedule,
                rp.channelId,
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
            channelId = pref.get("channelId")
            if not externalAccountId or not userId:
                continue
            user = self.botClient.get_user(int(userId))
            if not user:
                try:
                    user = await self.botClient.fetch_user(int(userId))
                except Exception:
                    valorantReportLogger.exception("Failed to fetch user %s for daily Valorant report", userId)
                    continue
            reportData = await generateDailyReport(
                self.dbClient, externalAccountId, queueType, datetime.utcnow().strftime("%Y-%m-%d")
            )
            accountInfo = self.getExternalAccountInfo(externalAccountId)
            embed = self.buildReportEmbed(user, queueType, reportData, accountInfo)
            channel = await self.resolveReportChannel(channelId)
            if not channel:
                valorantReportLogger.warning("Missing report channel for daily Valorant report (user %s).", userId)
                continue
            try:
                await channel.send(embed=embed)
            except Exception:
                valorantReportLogger.exception("Failed to send daily Valorant report to channel %s", channelId)

    @reportLoop.before_loop
    async def beforeReportLoop(self):
        await self.botClient.wait_until_ready()

    @commands.Cog.listener("on_ready")
    async def onReady(self):
        if not self.reportLoop.is_running():
            self.reportLoop.start()

    async def resolveReportChannel(self, channelId: Optional[str]) -> Optional[discord.abc.Messageable]:
        if not channelId:
            return None
        channel = self.botClient.get_channel(int(channelId))
        if channel:
            return channel
        try:
            return await self.botClient.fetch_channel(int(channelId))
        except Exception:
            valorantReportLogger.exception("Failed to fetch channel %s for daily Valorant report", channelId)
            return None


async def setup(botClient: commands.Bot):
    await botClient.add_cog(ValorantReport(botClient))
