import asyncio
from typing import Dict, Optional

import requests
from urllib.parse import quote
from datetime import datetime, timezone

from config.settings import appSettings
from utils.database import DatabaseClient
from utils.logger import getLogger
from utils.riotApi import RiotAPI


riotApiLogger = getLogger(__name__)


async def fetchCurrentLolRank(externalAccountId: int, queueType: str) -> Optional[Dict]:
    """
    Fetch current ranked data for a League of Legends account by externalAccountId and queueType.
    Resolves the puuid from the database, calls Riot API, and returns rank data dict or None.
    """
    dbClient = DatabaseClient(appSettings.databasePath)
    accountRow = dbClient.connection.execute(
        "SELECT externalId, region FROM externalAccount WHERE id = ?", (externalAccountId,)
    ).fetchone()
    if not accountRow:
        riotApiLogger.error("No external account found for id=%s", externalAccountId)
        return None

    puuid = accountRow["externalId"]
    region = accountRow["region"] or appSettings.riotRegion

    riotClient = RiotAPI(region)
    entries = await asyncio.to_thread(riotClient.getLolRankedEntriesByPuuid, puuid)
    for entry in entries:
        if entry.get("queueType") == queueType:
            return {
                "queueType": entry.get("queueType"),
                "tier": entry.get("tier"),
                "division": entry.get("rank"),
                "lp": entry.get("leaguePoints"),
                "wins": entry.get("wins"),
                "losses": entry.get("losses"),
            }
    return None


async def fetchValorantDailySnapshot(externalAccountId: int, todayDateStr: str) -> Optional[Dict]:
    """
    Fetch Valorant MMR history (v2) once and derive baseline/current snapshots for today.
    """
    dbClient = DatabaseClient(appSettings.databasePath)
    accountRow = dbClient.connection.execute(
        "SELECT externalId, displayName, tagLine, region FROM externalAccount WHERE id = ?",
        (externalAccountId,),
    ).fetchone()
    if not accountRow:
        riotApiLogger.error("No external account found for id=%s", externalAccountId)
        return None

    apiKey = appSettings.valorantApiKey
    if not apiKey:
        riotApiLogger.error("VALORANT_API_KEY is not configured.")
        return None

    region = resolveValorantRegion(accountRow["region"] or appSettings.riotRegion)
    displayName = accountRow["displayName"]
    tagLine = accountRow["tagLine"]
    platform = "pc"

    payload = None
    if displayName and tagLine:
        riotApiLogger.info(
            "Fetching Valorant MMR history by name/tag (region=%s, platform=%s, name=%s, tag=%s)",
            region,
            platform,
            displayName,
            tagLine,
        )
        payload = await asyncio.to_thread(
            fetchValorantMmrHistoryByNameTag, apiKey, region, platform, displayName, tagLine
        )
    if payload is None:
        riotApiLogger.info(
            "Falling back to Valorant MMR history by PUUID (region=%s, platform=%s, externalAccountId=%s)",
            region,
            platform,
            externalAccountId,
        )
        puuid = accountRow["externalId"]
        payload = await asyncio.to_thread(fetchValorantMmrHistoryByPuuid, apiKey, region, platform, puuid)

    if not payload:
        return None

    history = payload.get("history") if isinstance(payload, dict) else None
    if not isinstance(history, list) or not history:
        return None

    latest = history[0]
    latest_tier_name = (latest.get("tier") or {}).get("name")
    current_tier, current_division = parseValorantTier(latest_tier_name)
    current_rr = latest.get("rr")

    today_entries = []
    for entry in history:
        entry_date = parseValorantDate(entry.get("date"))
        if entry_date == todayDateStr:
            today_entries.append(entry)

    lp_diff = 0
    for entry in today_entries:
        try:
            lp_diff += int(entry.get("last_change") or 0)
        except (TypeError, ValueError):
            continue

    baseline_entry = None
    for entry in history:
        entry_date = parseValorantDate(entry.get("date"))
        if entry_date and entry_date < todayDateStr:
            baseline_entry = entry
            break

    if baseline_entry:
        baseline_tier_name = (baseline_entry.get("tier") or {}).get("name")
        baseline_tier, baseline_division = parseValorantTier(baseline_tier_name)
        baseline_rr = baseline_entry.get("rr")
    else:
        baseline_tier, baseline_division = current_tier, current_division
        baseline_rr = None
        if current_rr is not None:
            baseline_rr = current_rr - lp_diff

    return {
        "baseline": {
            "tier": baseline_tier,
            "division": baseline_division,
            "lp": baseline_rr,
        },
        "current": {
            "tier": current_tier,
            "division": current_division,
            "lp": current_rr,
        },
    }


async def fetchValorantDailySnapshotByNameTag(
    apiKey: str, region: str, platform: str, gameName: str, tagLine: str, todayDateStr: str
) -> Optional[Dict]:
    payload = await asyncio.to_thread(
        fetchValorantMmrHistoryByNameTag, apiKey, region, platform, gameName, tagLine
    )
    return buildValorantDailySnapshotFromHistory(payload, todayDateStr)


def resolveValorantRegion(region: str) -> str:
    regionLower = (region or "").lower()
    if regionLower in {"eu", "na", "latam", "br", "ap", "kr"}:
        return regionLower
    if regionLower in {"euw1", "eun1", "tr1", "ru"}:
        return "eu"
    if regionLower in {"na1", "oc1"}:
        return "na"
    if regionLower in {"la1", "la2"}:
        return "latam"
    if regionLower == "br1":
        return "br"
    if regionLower == "kr":
        return "kr"
    if regionLower in {"jp1"}:
        return "ap"
    return "eu"


def fetchValorantMmrHistoryByNameTag(
    apiKey: str, region: str, platform: str, gameName: str, tagLine: str
) -> Optional[Dict]:
    safeName = quote(gameName, safe="")
    safeTag = quote(tagLine, safe="")
    url = f"https://api.henrikdev.xyz/valorant/v2/mmr-history/{region}/{platform}/{safeName}/{safeTag}"
    headers = {"Authorization": apiKey}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            payload = response.json()
            data = payload.get("data")
            if isinstance(data, dict):
                return data
            riotApiLogger.warning(
                "Unexpected Valorant MMR history payload (name/tag): data=%s status=%s errors=%s",
                type(data).__name__,
                payload.get("status"),
                payload.get("errors"),
            )
            riotApiLogger.warning("Full payload: %s", payload)
            return None
        riotApiLogger.error(
            "Failed to fetch Valorant MMR history (name/tag): %s %s", response.status_code, response.text
        )
    except requests.RequestException as error:
        riotApiLogger.error("Request error in fetchValorantMmrHistoryByNameTag: %s", error)
    return None


def fetchValorantMmrHistoryByPuuid(
    apiKey: str, region: str, platform: str, puuid: str
) -> Optional[Dict]:
    safePuuid = quote(puuid, safe="")
    url = f"https://api.henrikdev.xyz/valorant/v2/mmr-history/{region}/{platform}/by-puuid/{safePuuid}"
    headers = {"Authorization": apiKey}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            payload = response.json()
            data = payload.get("data")
            return data if isinstance(data, dict) else None
        riotApiLogger.error(
            "Failed to fetch Valorant MMR history (puuid): %s %s", response.status_code, response.text
        )
    except requests.RequestException as error:
        riotApiLogger.error("Request error in fetchValorantMmrHistoryByPuuid: %s", error)
    return None


def buildValorantDailySnapshotFromHistory(data: Optional[Dict], todayDateStr: str) -> Optional[Dict]:
    if not data:
        return None
    history = data.get("history") if isinstance(data, dict) else None
    if not isinstance(history, list) or not history:
        return None

    latest = history[0]
    latest_tier_name = (latest.get("tier") or {}).get("name")
    current_tier, current_division = parseValorantTier(latest_tier_name)
    current_rr = latest.get("rr")

    today_entries = []
    for entry in history:
        entry_date = parseValorantDate(entry.get("date"))
        if entry_date == todayDateStr:
            today_entries.append(entry)

    lp_diff = 0
    for entry in today_entries:
        try:
            lp_diff += int(entry.get("last_change") or 0)
        except (TypeError, ValueError):
            continue

    baseline_entry = None
    for entry in history:
        entry_date = parseValorantDate(entry.get("date"))
        if entry_date and entry_date < todayDateStr:
            baseline_entry = entry
            break

    if baseline_entry:
        baseline_tier_name = (baseline_entry.get("tier") or {}).get("name")
        baseline_tier, baseline_division = parseValorantTier(baseline_tier_name)
        baseline_rr = baseline_entry.get("rr")
    else:
        baseline_tier, baseline_division = current_tier, current_division
        baseline_rr = None
        if current_rr is not None:
            baseline_rr = current_rr - lp_diff

    return {
        "baseline": {
            "tier": baseline_tier,
            "division": baseline_division,
            "lp": baseline_rr,
        },
        "current": {
            "tier": current_tier,
            "division": current_division,
            "lp": current_rr,
        },
        "lpDiff": lp_diff,
    }


def fetchValorantCurrentRankByNameTag(
    apiKey: str, region: str, platform: str, gameName: str, tagLine: str
) -> Optional[Dict]:
    data = fetchValorantMmrHistoryByNameTag(apiKey, region, platform, gameName, tagLine)
    return extractValorantCurrentRank(data)


def extractValorantCurrentRank(data: Optional[Dict]) -> Optional[Dict]:
    if not isinstance(data, dict):
        return None
    history = data.get("history")
    if not isinstance(history, list) or not history:
        return None
    latest = history[0]
    tier_name = (latest.get("tier") or {}).get("name")
    tier, division = parseValorantTier(tier_name)
    return {
        "tier": tier,
        "division": division,
        "lp": latest.get("rr"),
    }


def parseValorantTier(tierName: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not tierName:
        return None, None
    parts = tierName.strip().split()
    if not parts:
        return None, None
    division = None
    if parts[-1].isdigit():
        division = parts[-1]
        tier = " ".join(parts[:-1]).upper()
    else:
        tier = " ".join(parts).upper()
    return tier or None, division


def parseValorantDate(dateStr: Optional[str]) -> Optional[str]:
    if not dateStr:
        return None
    try:
        normalized = dateStr.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).astimezone(timezone.utc).date().isoformat()
    except ValueError:
        return None
