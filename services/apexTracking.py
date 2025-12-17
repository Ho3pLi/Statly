import asyncio
from datetime import datetime
from typing import Dict, Optional

from services.apex_api import getApexRankSummary
from utils.database import DatabaseClient
from utils.logger import getLogger


apexTrackingLogger = getLogger(__name__)


def rowToDict(row) -> Dict:
    return {key: row[key] for key in row.keys()}


async def fetchCurrentApexRank(playerName: str, platform: str) -> Optional[Dict]:
    return await asyncio.to_thread(getApexRankSummary, playerName, platform)


async def getOrCreateDailyBaseline(
    dbClient: DatabaseClient, externalAccountId: int, playerName: str, platform: str, todayDateStr: str
) -> Optional[Dict]:
    baselineRow = dbClient.connection.execute(
        """
        SELECT * FROM apexRankSnapshot
        WHERE externalAccountId = ? AND date(capturedAt) = ?
        ORDER BY capturedAt ASC
        LIMIT 1
        """,
        (externalAccountId, todayDateStr),
    ).fetchone()

    if baselineRow:
        return rowToDict(baselineRow)

    current = await fetchCurrentApexRank(playerName, platform)
    if not current:
        return None

    nowStr = datetime.utcnow().isoformat()
    dbClient.connection.execute(
        """
        INSERT INTO apexRankSnapshot (externalAccountId, rankName, rankDiv, rankScore, ladderPosPlatform, rankedSeason, capturedAt)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            externalAccountId,
            current.get("rankName"),
            current.get("rankDiv"),
            current.get("rankScore"),
            current.get("ladderPosPlatform"),
            current.get("rankedSeason"),
            nowStr,
        ),
    )
    dbClient.connection.commit()

    return {
        "id": dbClient.connection.execute("SELECT last_insert_rowid() as id").fetchone()["id"],
        "externalAccountId": externalAccountId,
        "rankName": current.get("rankName"),
        "rankDiv": current.get("rankDiv"),
        "rankScore": current.get("rankScore"),
        "ladderPosPlatform": current.get("ladderPosPlatform"),
        "rankedSeason": current.get("rankedSeason"),
        "capturedAt": nowStr,
    }


async def getCurrentState(playerName: str, platform: str) -> Optional[Dict]:
    current = await fetchCurrentApexRank(playerName, platform)
    if not current:
        return None
    return {
        "rankName": current.get("rankName"),
        "rankDiv": current.get("rankDiv"),
        "rankScore": current.get("rankScore"),
        "ladderPosPlatform": current.get("ladderPosPlatform"),
        "rankedSeason": current.get("rankedSeason"),
        "platform": current.get("platform"),
        "playerName": current.get("playerName"),
        "rankImg": current.get("rankImg"),
    }


def computeRankDiff(baseline: Dict, current: Dict) -> Dict:
    if not baseline or not current:
        return {"scoreDiff": 0, "ladderDiff": None, "rankChange": None}

    baseRank = f"{baseline.get('rankName', 'N/A')} {baseline.get('rankDiv') or ''}".strip()
    currRank = f"{current.get('rankName', 'N/A')} {current.get('rankDiv') or ''}".strip()
    rankChange = None
    if baseRank != currRank:
        rankChange = f"{baseRank} -> {currRank}"

    ladderDiff = None
    if baseline.get("ladderPosPlatform") is not None and current.get("ladderPosPlatform") is not None:
        ladderDiff = int(current.get("ladderPosPlatform")) - int(baseline.get("ladderPosPlatform"))

    scoreDiff = (current.get("rankScore") or 0) - (baseline.get("rankScore") or 0)

    return {
        "scoreDiff": scoreDiff,
        "ladderDiff": ladderDiff,
        "rankChange": rankChange,
    }


async def generateDailyReport(
    dbClient: DatabaseClient, externalAccountId: int, playerName: str, platform: str, todayDateStr: str
) -> Dict:
    baseline = await getOrCreateDailyBaseline(dbClient, externalAccountId, playerName, platform, todayDateStr)
    current = await getCurrentState(playerName, platform)
    diff = computeRankDiff(baseline or {}, current or {})

    return {"baseline": baseline, "current": current, "diff": diff}
