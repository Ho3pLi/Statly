from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from services.riot_api import (
    fetchValorantStoredMmrHistoryByPuuid,
    parseValorantDatetime,
    parseValorantTier,
    resolveValorantRegion,
)
from utils.database import DatabaseClient

VALORANT_TIER_ORDER = [
    "IRON",
    "BRONZE",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "DIAMOND",
    "ASCENDANT",
    "IMMORTAL",
    "RADIANT",
]


def to_int(value) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def normalizeHistoryEntry(raw: Dict) -> Optional[Dict]:
    parsedAt = parseValorantDatetime(raw.get("date"))
    if parsedAt is None:
        return None
    tierName = (raw.get("tier") or {}).get("name")
    tier, division = parseValorantTier(tierName)
    return {
        "matchId": raw.get("match_id"),
        "tier": tier,
        "division": division,
        "lp": to_int(raw.get("elo")),
        "lpChange": to_int(raw.get("last_mmr_change")) or 0,
        "map": (raw.get("map") or {}).get("name"),
        "season": (raw.get("season") or {}).get("short"),
        "capturedAt": parsedAt,
        "capturedAtIso": parsedAt.isoformat(),
    }


async def fetchStoredHistory(dbClient: DatabaseClient, externalAccountId: int) -> Optional[Dict]:
    accountRow = dbClient.connection.execute(
        "SELECT externalId, region, displayName, tagLine FROM externalAccount WHERE id = ?",
        (externalAccountId,),
    ).fetchone()
    if not accountRow:
        return None

    payload = await fetchValorantStoredMmrHistoryByPuuid(
        resolveValorantRegion(accountRow["region"]),
        accountRow["externalId"],
    )
    if not payload:
        return None

    return {
        "payload": payload,
        "displayName": accountRow["displayName"],
        "tagLine": accountRow["tagLine"],
        "region": resolveValorantRegion(accountRow["region"]),
    }


def getPeriodBounds(period: str, now: Optional[datetime] = None) -> tuple[datetime, datetime, str]:
    current = now or datetime.now(timezone.utc)
    current = current.astimezone(timezone.utc)
    dayStart = current.replace(hour=0, minute=0, second=0, microsecond=0)

    if period == "week":
        weekStart = dayStart - timedelta(days=dayStart.weekday())
        return weekStart, weekStart + timedelta(days=7), "Weekly Valorant RR Report"

    return dayStart, dayStart + timedelta(days=1), "Daily Valorant RR Report"


def buildBaseline(previousEntry: Optional[Dict], periodEntries: List[Dict], periodStart: datetime) -> Dict:
    if previousEntry:
        return {
            "tier": previousEntry.get("tier"),
            "division": previousEntry.get("division"),
            "lp": previousEntry.get("lp"),
            "capturedAt": previousEntry.get("capturedAtIso"),
        }

    if not periodEntries:
        return {}

    firstEntry = periodEntries[0]
    openingLp = None
    if firstEntry.get("lp") is not None:
        openingLp = firstEntry.get("lp") - (firstEntry.get("lpChange") or 0)

    return {
        "tier": firstEntry.get("tier"),
        "division": firstEntry.get("division"),
        "lp": openingLp,
        "capturedAt": periodStart.isoformat(),
    }


def buildCurrent(periodEntries: List[Dict]) -> Dict:
    if not periodEntries:
        return {}
    latestEntry = periodEntries[-1]
    return {
        "tier": latestEntry.get("tier"),
        "division": latestEntry.get("division"),
        "lp": latestEntry.get("lp"),
        "capturedAt": latestEntry.get("capturedAtIso"),
    }


def computeRankDiff(baseline: Dict, current: Dict, lpDiff: int) -> Dict:
    if not baseline or not current:
        return {"lpDiff": lpDiff, "rankUp": False, "rankDown": False, "tierChange": None}

    def tierIndex(tier: Optional[str]) -> int:
        if tier and tier.upper() in VALORANT_TIER_ORDER:
            return VALORANT_TIER_ORDER.index(tier.upper())
        return -1

    def divisionValue(division: Optional[str]) -> int:
        order = {"1": 0, "2": 1, "3": 2}
        return order.get(str(division), -1)

    baselineTierIdx = tierIndex(baseline.get("tier"))
    currentTierIdx = tierIndex(current.get("tier"))
    baselineDivVal = divisionValue(baseline.get("division"))
    currentDivVal = divisionValue(current.get("division"))

    rankUp = False
    rankDown = False
    tierChange = None

    if currentTierIdx > baselineTierIdx:
        rankUp = True
        tierChange = f"{baseline.get('tier')} {baseline.get('division')} -> {current.get('tier')} {current.get('division')}"
    elif currentTierIdx < baselineTierIdx:
        rankDown = True
        tierChange = f"{baseline.get('tier')} {baseline.get('division')} -> {current.get('tier')} {current.get('division')}"
    else:
        if currentDivVal > baselineDivVal:
            rankUp = True
        elif currentDivVal < baselineDivVal:
            rankDown = True

    return {
        "lpDiff": lpDiff,
        "rankUp": rankUp,
        "rankDown": rankDown,
        "tierChange": tierChange,
    }


async def generatePeriodReport(
    dbClient: DatabaseClient,
    externalAccountId: int,
    startAt: datetime,
    endAt: datetime,
    title: str,
) -> Dict:
    historyData = await fetchStoredHistory(dbClient, externalAccountId)
    if not historyData:
        return {
            "title": title,
            "baseline": {},
            "current": {},
            "diff": {"lpDiff": 0, "rankUp": False, "rankDown": False, "tierChange": None},
            "summary": {"matches": 0, "maps": [], "startAt": startAt.isoformat(), "endAt": endAt.isoformat()},
            "entries": [],
        }

    payload = historyData.get("payload") or {}
    rawEntries = payload.get("data") or []
    normalizedEntries = [entry for entry in (normalizeHistoryEntry(raw) for raw in rawEntries) if entry]
    normalizedEntries.sort(key=lambda entry: entry["capturedAt"])

    periodEntries = [
        entry for entry in normalizedEntries
        if startAt <= entry["capturedAt"] < endAt
    ]
    previousEntry = None
    for entry in normalizedEntries:
        if entry["capturedAt"] < startAt:
            previousEntry = entry
        else:
            break

    baseline = buildBaseline(previousEntry, periodEntries, startAt)
    current = buildCurrent(periodEntries)
    lpDiff = sum(entry.get("lpChange") or 0 for entry in periodEntries)
    diff = computeRankDiff(baseline, current, lpDiff)

    mapTotals: Dict[str, int] = {}
    for entry in periodEntries:
        mapName = entry.get("map") or "Unknown"
        mapTotals[mapName] = mapTotals.get(mapName, 0) + (entry.get("lpChange") or 0)

    topMaps = sorted(mapTotals.items(), key=lambda item: (-abs(item[1]), item[0]))[:3]

    return {
        "title": title,
        "baseline": baseline,
        "current": current,
        "diff": diff,
        "summary": {
            "matches": len(periodEntries),
            "maps": [{"name": name, "lpDiff": value} for name, value in topMaps],
            "startAt": startAt.isoformat(),
            "endAt": endAt.isoformat(),
            "displayName": historyData.get("displayName") or payload.get("name"),
            "tagLine": historyData.get("tagLine") or payload.get("tag"),
            "region": historyData.get("region"),
        },
        "entries": periodEntries,
    }


async def generateDailyReport(
    dbClient: DatabaseClient, externalAccountId: int, queueType: str, todayDateStr: str
) -> Dict:
    periodStart = datetime.fromisoformat(f"{todayDateStr}T00:00:00+00:00")
    periodEnd = periodStart + timedelta(days=1)
    return await generatePeriodReport(dbClient, externalAccountId, periodStart, periodEnd, "Daily Valorant RR Report")
