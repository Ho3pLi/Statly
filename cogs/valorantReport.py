import asyncio
import os
import re
from io import BytesIO
from datetime import datetime, timedelta, timezone
from pathlib import Path
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
from services.valorantTracking import (
    VALORANT_TIER_ORDER,
    generateDailyReport,
    generatePeriodReport,
    getPeriodBounds,
)
from utils.database import DatabaseClient
from utils.logger import getLogger


valorantReportLogger = getLogger(__name__)

VALORANT_ROLE_TIERS = ["iron", "bronze", "silver", "gold", "platinum", "diamond", "ascendant", "immortal", "radiant"]
VALORANT_DEFAULT_ROLE_IDS = {
    "radiant": 1502632604070055936,
    "immortal": 1502632539376975953,
    "ascendant": 1502632469935951963,
    "diamond": 1502632293011951750,
    "platinum": 1502632148451066017,
    "gold": 1502631975222120488,
    "silver": 1502631902698278952,
    "bronze": 1502443610921238650,
    "iron": 1502631794326110348,
}


class ValorantReport(commands.Cog):
    def __init__(self, botClient: commands.Bot):
        self.botClient = botClient
        self.dbClient = DatabaseClient(appSettings.databasePath)

    @app_commands.command(
        name="valorantreport",
        description="Send your Valorant RR report for day, week, or a custom range.",
    )
    @app_commands.rename(period="period", start="start", end="end")
    @app_commands.describe(
        period="Report window",
        start="Custom start in UTC: YYYY-MM-DD or YYYY-MM-DD HH:MM",
        end="Custom end in UTC: YYYY-MM-DD or YYYY-MM-DD HH:MM",
    )
    @app_commands.choices(
        period=[
            app_commands.Choice(name="Day", value="day"),
            app_commands.Choice(name="Week (Mon-Sun)", value="week"),
            app_commands.Choice(name="Custom", value="custom"),
        ]
    )
    async def valorantReportCommand(
        self,
        interaction: discord.Interaction,
        period: str = "day",
        start: Optional[str] = None,
        end: Optional[str] = None,
    ):
        if not appSettings.valorantApiKey:
            await interaction.response.send_message(
                "VALORANT_API_KEY is not configured. Please contact an admin.", ephemeral=True
            )
            return

        externalAccountId = self.getPrimaryValorantAccountId(interaction.user.id, interaction.guild_id)
        if not externalAccountId:
            await interaction.response.send_message(
                "No linked Valorant account found for you in this server.", ephemeral=True
            )
            return

        try:
            startAt, endAt, title = self.resolveReportRange(period, start, end)
        except ValueError as error:
            await interaction.response.send_message(str(error), ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        reportData = await generatePeriodReport(self.dbClient, externalAccountId, startAt, endAt, title)

        accountInfo = self.getExternalAccountInfo(externalAccountId)
        embed = self.buildReportEmbed(interaction.user, reportData, accountInfo)
        chartFile = self.buildChartFile(reportData)
        if chartFile:
            embed.set_image(url=f"attachment://{chartFile.filename}")
            await interaction.followup.send(embed=embed, file=chartFile, ephemeral=True)
            return
        await interaction.followup.send(embed=embed, ephemeral=True)

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
        valorantReportLogger.info(
            "groupreport requested by discordUserId=%s discordGuildId=%s internalGuildId=%s groupName='%s'",
            interaction.user.id,
            interaction.guild_id,
            guildId,
            groupName,
        )
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
            await interaction.response.send_message(f"Group **{groupName}** not found.")
            return
        self.dbClient.deleteValorantGroup(int(groupRow["id"]))
        await interaction.response.send_message(f"Group **{groupRow['name']}** deleted.")

    @app_commands.command(name="getrank", description="Fetch your current Valorant rank and update your rank role.")
    async def getRankCommand(self, interaction: discord.Interaction):
        if not interaction.guild_id or not interaction.guild:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return
        if not appSettings.valorantApiKey:
            await interaction.response.send_message(
                "VALORANT_API_KEY is not configured. Please contact an admin.", ephemeral=True
            )
            return
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Could not resolve your server member profile.", ephemeral=True)
            return
        me = interaction.guild.me
        if not me:
            await interaction.response.send_message("Bot member not available in this guild.", ephemeral=True)
            return
        if not me.guild_permissions.manage_roles:
            await interaction.response.send_message(
                "I need the `Manage Roles` permission to update rank roles.", ephemeral=True
            )
            return

        externalAccountId = self.getPrimaryValorantAccountId(interaction.user.id, interaction.guild_id)
        if not externalAccountId:
            await interaction.response.send_message(
                "No linked Valorant account found for you in this server.", ephemeral=True
            )
            return
        accountInfo = self.getExternalAccountInfo(externalAccountId)
        if not accountInfo:
            await interaction.response.send_message("Linked account data is missing. Re-run /register.", ephemeral=True)
            return

        displayName = accountInfo.get("displayName")
        tagLine = accountInfo.get("tagLine")
        if not displayName or not tagLine:
            await interaction.response.send_message(
                "Linked Valorant account is incomplete (missing Riot ID). Re-run /register.", ephemeral=True
            )
            return
        region = resolveValorantRegion(accountInfo.get("region") or appSettings.riotRegion)

        await interaction.response.defer(ephemeral=True)
        rankData = await asyncio.to_thread(
            fetchValorantCurrentRankByNameTag,
            appSettings.valorantApiKey,
            region,
            "pc",
            displayName,
            tagLine,
        )
        if not rankData:
            await interaction.followup.send("Could not fetch your Valorant rank right now. Please try again later.", ephemeral=True)
            return
        resolvedRegion = rankData.get("_resolved_region")
        if resolvedRegion and resolvedRegion != accountInfo.get("region"):
            self.dbClient.connection.execute(
                "UPDATE externalAccount SET region = ? WHERE id = ?",
                (resolvedRegion, externalAccountId),
            )
            self.dbClient.connection.commit()

        tierRaw = rankData.get("tier")
        tierKey = str(tierRaw).strip().lower() if tierRaw else "unranked"
        targetRole = self.resolveValorantRole(interaction.guild, tierKey)
        rankRoles = self.getValorantRankRoles(interaction.guild)

        if not targetRole and tierKey != "unranked":
            await interaction.followup.send(
                f"Your rank is **{self.formatRank(rankData.get('tier'), rankData.get('division'))}**, "
                "but I could not find the matching role in this server.",
                ephemeral=True,
            )
            return

        rolesToRemove = [role for role in rankRoles if role in interaction.user.roles and role != targetRole]
        if any(role >= me.top_role for role in rolesToRemove):
            await interaction.followup.send(
                "I can't remove one or more rank roles because they are above my highest role.", ephemeral=True
            )
            return
        if targetRole and targetRole >= me.top_role:
            await interaction.followup.send(
                f"I can't assign **{targetRole.name}** because it is above my highest role.", ephemeral=True
            )
            return

        try:
            if rolesToRemove:
                await interaction.user.remove_roles(*rolesToRemove, reason="Valorant /getrank role sync")
            if targetRole and targetRole not in interaction.user.roles:
                await interaction.user.add_roles(targetRole, reason="Valorant /getrank role sync")
        except discord.Forbidden:
            await interaction.followup.send("I don't have permission to update your roles.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.followup.send("Discord error while updating roles. Please try again.", ephemeral=True)
            return

        rankLabel = self.formatRank(rankData.get("tier"), rankData.get("division"))
        rr = rankData.get("lp")
        rrLabel = f"{rr} RR" if rr is not None else "RR N/A"
        if targetRole:
            await interaction.followup.send(
                f"Rank synced: **{rankLabel}** ({rrLabel}). Assigned role: **{targetRole.name}**.",
                ephemeral=True,
            )
            return
        await interaction.followup.send(
            f"Rank synced: **{rankLabel}** ({rrLabel}). No rank role assigned for this tier.",
            ephemeral=True,
        )

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
        valorantReportLogger.info(
            "groupaddmembers requested by discordUserId=%s discordGuildId=%s internalGuildId=%s groupName='%s'",
            interaction.user.id,
            interaction.guild_id,
            guildId,
            groupName,
        )
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
            await interaction.response.send_message(f"Group **{groupName}** not found.")
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
        self, user: discord.abc.User, reportData: Dict, accountInfo: Optional[Dict]
    ) -> discord.Embed:
        baseline = reportData.get("baseline") or {}
        current = reportData.get("current") or {}
        diff = reportData.get("diff") or {}
        summary = reportData.get("summary") or {}

        accountLabel = "Unknown account"
        displayName = summary.get("displayName") or (accountInfo.get("displayName") if accountInfo else None)
        tagLine = summary.get("tagLine") or (accountInfo.get("tagLine") if accountInfo else None)
        regionValue = summary.get("region") or (accountInfo.get("region") if accountInfo else None)
        if displayName or tagLine or regionValue:
            tag = f"#{tagLine}" if tagLine else ""
            region = f" ({regionValue})" if regionValue else ""
            accountLabel = f"{displayName or 'Unknown'}{tag}{region}"

        periodLabel = self.formatPeriodLabel(summary.get("startAt"), summary.get("endAt"))
        embed = discord.Embed(
            title=reportData.get("title") or "Valorant RR Report",
            description=f"Account: **{accountLabel}**\nWindow: **{periodLabel}**",
            color=discord.Color.red(),
            timestamp=datetime.utcnow(),
        )
        embed.set_author(name=str(user))

        matchCount = summary.get("matches", 0)
        if matchCount == 0:
            embed.add_field(
                name="Summary",
                value="No stored matches found in this range.",
                inline=False,
            )
            return embed

        baselineText = self.formatSnapshot(baseline, defaultLabel="Start")
        currentText = self.formatSnapshot(current, defaultLabel="End")

        diffTextParts: List[str] = [
            f"Matches: {matchCount}",
            f"RR diff: {diff.get('lpDiff', 0):+}",
        ]
        tierChange = diff.get("tierChange")
        if tierChange:
            diffTextParts.append(f"Rank change: {tierChange}")
        elif diff.get("rankUp"):
            diffTextParts.append("Rank movement: up")
        elif diff.get("rankDown"):
            diffTextParts.append("Rank movement: down")
        else:
            diffTextParts.append("Rank movement: none")

        topMaps = summary.get("maps") or []
        if topMaps:
            mapLabel = ", ".join(f"{entry['name']} {entry['lpDiff']:+}" for entry in topMaps)
            diffTextParts.append(f"Top maps: {mapLabel}")

        embed.add_field(name="Baseline", value=baselineText, inline=False)
        embed.add_field(name="Current", value=currentText, inline=False)
        embed.add_field(name="Diff", value="\n".join(diffTextParts), inline=False)

        return embed

    def formatSnapshot(self, snapshot: Dict, defaultLabel: str) -> str:
        if not snapshot:
            return f"{defaultLabel}: N/A"
        rankLabel = self.formatRank(snapshot.get("tier"), snapshot.get("division"))
        rrLabel = f"{snapshot.get('lp')} RR" if snapshot.get("lp") is not None else "RR N/A"
        timeLabel = self.formatTimestamp(snapshot.get("capturedAt"))
        return f"{rankLabel} ({rrLabel})\n{timeLabel}"

    def formatRank(self, tier: Optional[str], division: Optional[str]) -> str:
        if not tier:
            return "UNRANKED"
        return f"{tier} {division}" if division else tier

    def formatTimestamp(self, raw: Optional[str]) -> str:
        if not raw:
            return "Time: N/A"
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
            return parsed.strftime("UTC %Y-%m-%d %H:%M")
        except ValueError:
            return raw

    def formatPeriodLabel(self, startRaw: Optional[str], endRaw: Optional[str]) -> str:
        if not startRaw or not endRaw:
            return "N/A"
        try:
            startAt = datetime.fromisoformat(startRaw.replace("Z", "+00:00")).astimezone(timezone.utc)
            endAt = datetime.fromisoformat(endRaw.replace("Z", "+00:00")).astimezone(timezone.utc)
            displayEnd = endAt - timedelta(seconds=1)
            return f"{startAt.strftime('%Y-%m-%d %H:%M')} -> {displayEnd.strftime('%Y-%m-%d %H:%M')} UTC"
        except ValueError:
            return f"{startRaw} -> {endRaw}"

    def resolveReportRange(
        self,
        period: str,
        start: Optional[str],
        end: Optional[str],
    ) -> tuple[datetime, datetime, str]:
        if period == "custom":
            if not start or not end:
                raise ValueError("Custom report requires both `start` and `end` in UTC.")
            startAt = self.parseUtcInput(start, endOfDay=False)
            endAt = self.parseUtcInput(end, endOfDay=True)
            if not startAt or not endAt:
                raise ValueError("Use UTC dates in `YYYY-MM-DD` or `YYYY-MM-DD HH:MM` format.")
            if endAt <= startAt:
                raise ValueError("Custom end must be after start.")
            return startAt, endAt, "Custom Valorant RR Report"

        return getPeriodBounds(period)

    def parseUtcInput(self, raw: str, endOfDay: bool) -> Optional[datetime]:
        value = raw.strip()
        formats = [
            ("%Y-%m-%d %H:%M", False),
            ("%Y-%m-%dT%H:%M", False),
            ("%Y-%m-%d", True),
        ]
        for fmt, isDateOnly in formats:
            try:
                parsed = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
                if isDateOnly and endOfDay:
                    return parsed + timedelta(days=1)
                return parsed
            except ValueError:
                continue
        return None

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

    def resolveValorantRole(self, guild: discord.Guild, tierKey: str) -> Optional[discord.Role]:
        roleId = VALORANT_DEFAULT_ROLE_IDS.get(tierKey)
        if roleId:
            role = guild.get_role(roleId)
            if role:
                return role
        return discord.utils.find(lambda r: r.name.strip().lower() == tierKey, guild.roles)

    def getValorantRankRoles(self, guild: discord.Guild) -> List[discord.Role]:
        roles: List[discord.Role] = []
        for tier in VALORANT_ROLE_TIERS:
            role = self.resolveValorantRole(guild, tier)
            if role and role not in roles:
                roles.append(role)
        return roles

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
                rankLabel = f"{tier} {division}" if division else tier
                rrLabel = f"{rr} RR" if rr is not None else "RR N/A"
                lpDiff = entry.get("lpDiff")
                diffLabel = f" | Today {lpDiff:+} RR" if isinstance(lpDiff, int) else ""
                lines.append(f"{idx}. {name} - {rankLabel} ({rrLabel}){diffLabel}")
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
                self.dbClient,
                externalAccountId,
                pref.get("queueType", "COMPETITIVE"),
                datetime.utcnow().strftime("%Y-%m-%d"),
            )
            accountInfo = self.getExternalAccountInfo(externalAccountId)
            embed = self.buildReportEmbed(user, reportData, accountInfo)
            chartFile = self.buildChartFile(reportData)
            channel = await self.resolveReportChannel(channelId)
            if not channel:
                valorantReportLogger.warning("Missing report channel for daily Valorant report (user %s).", userId)
                continue
            try:
                if chartFile:
                    embed.set_image(url=f"attachment://{chartFile.filename}")
                    await channel.send(embed=embed, file=chartFile)
                else:
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

    def buildChartFile(self, reportData: Dict) -> Optional[discord.File]:
        entries = reportData.get("entries") or []
        if not entries:
            return None

        self.ensurePlotlyBrowser()

        try:
            import plotly.graph_objects as go
        except ImportError:
            valorantReportLogger.warning("Plotly is not installed; skipping Valorant chart rendering.")
            return None

        xValues: List[datetime] = []
        yValues: List[int] = []
        hoverText: List[str] = []

        baseline = reportData.get("baseline") or {}
        baselineTimeRaw = baseline.get("capturedAt")
        baselineLp = baseline.get("lp")
        if baselineTimeRaw and baselineLp is not None:
            try:
                baselineTime = datetime.fromisoformat(baselineTimeRaw.replace("Z", "+00:00")).astimezone(timezone.utc)
                xValues.append(baselineTime)
                yValues.append(int(baselineLp))
                hoverText.append(f"Start: {self.formatRank(baseline.get('tier'), baseline.get('division'))} ({baselineLp} RR)")
            except (ValueError, TypeError):
                pass

        for entry in entries:
            capturedAtRaw = entry.get("capturedAtIso") or entry.get("capturedAt")
            lp = entry.get("lp")
            if not capturedAtRaw or lp is None:
                continue
            try:
                capturedAt = datetime.fromisoformat(capturedAtRaw.replace("Z", "+00:00")).astimezone(timezone.utc)
                xValues.append(capturedAt)
                yValues.append(int(lp))
                lpChange = entry.get("lpChange") or 0
                hoverText.append(
                    f"{self.formatRank(entry.get('tier'), entry.get('division'))} ({int(lp)} RR)<br>"
                    f"Delta: {lpChange:+} RR<br>"
                    f"Map: {entry.get('map') or 'Unknown'}"
                )
            except (ValueError, TypeError):
                continue

        if len(xValues) < 2:
            return None

        figure = go.Figure()
        figure.add_trace(
            go.Scatter(
                x=xValues,
                y=yValues,
                mode="lines+markers",
                line={"color": "#ff4655", "width": 3},
                marker={"size": 7, "color": "#111827"},
                hovertemplate="%{text}<br>%{x|%Y-%m-%d %H:%M UTC}<extra></extra>",
                text=hoverText,
            )
        )
        figure.update_layout(
            title=reportData.get("title") or "Valorant RR Trend",
            template="plotly_white",
            paper_bgcolor="#ffffff",
            plot_bgcolor="#f8fafc",
            margin={"l": 48, "r": 24, "t": 56, "b": 48},
            xaxis={"title": "Time (UTC)", "gridcolor": "#e5e7eb"},
            yaxis={"title": "RR", "gridcolor": "#e5e7eb", "zerolinecolor": "#d1d5db"},
            font={"family": "Arial", "color": "#111827"},
        )

        try:
            imageBytes = figure.to_image(format="png", width=1100, height=550, scale=2)
        except Exception:
            valorantReportLogger.exception("Failed to render Valorant chart image.")
            return None

        return discord.File(BytesIO(imageBytes), filename="valorant_rr_chart.png")

    def ensurePlotlyBrowser(self) -> None:
        if os.getenv("BROWSER_PATH"):
            return
        chromePath = Path(".plotly-chrome/chrome-linux64/chrome").resolve()
        if chromePath.exists():
            os.environ["BROWSER_PATH"] = str(chromePath)


async def setup(botClient: commands.Bot):
    await botClient.add_cog(ValorantReport(botClient))
