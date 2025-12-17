import asyncio
from datetime import datetime
from typing import Dict, List, Optional

from services.rocket_api import fetchRocketLeagueRanks
from utils.database import DatabaseClient
from utils.logger import getLogger


rocketTrackingLogger = getLogger(__name__)


def rowToDict(row) -> Dict:
    return {key: row[key] for key in row.keys()}


def filterRanks(ranks: List[Dict]) -> List[Dict]:
    excluded = {"hoops", "rumble", "dropshot", "snow day", "snowday"}
    filtered: List[Dict] = []
    for rank in ranks:
        playlist = (rank.get("playlist") or "").lower()
        if any(ex in playlist for ex in excluded):
            continue
        filtered.append(rank)
    return filtered


def normalizeRankEntry(rank: Dict) -> Dict:
    mmr = rank.get("mmr")
    try:
        mmr = int(mmr) if mmr is not None else None
    except (TypeError, ValueError):
        mmr = None
    return {
        "playlist": rank.get("playlist"),
        "rank": rank.get("rank"),
        "division": str(rank.get("division")) if rank.get("division") is not None else None,
        "mmr": mmr,
        "streak": rank.get("streak"),
    }


async def fetchCurrentRanks(epicId: str) -> Optional[List[Dict]]:
    return await asyncio.to_thread(fetchRocketLeagueRanks, epicId)


async def getOrCreateDailyBaseline(
    dbClient: DatabaseClient, externalAccountId: int, epicId: str, todayDateStr: str
) -> Optional[List[Dict]]:
    baselineTimeRow = dbClient.connection.execute(
        """
        SELECT MIN(capturedAt) as capturedAt
        FROM rocketLeagueRankSnapshot
        WHERE externalAccountId = ? AND date(capturedAt) = ?
        """,
        (externalAccountId, todayDateStr),
    ).fetchone()
    if baselineTimeRow and baselineTimeRow["capturedAt"]:
        rows = dbClient.connection.execute(
            """
            SELECT * FROM rocketLeagueRankSnapshot
            WHERE externalAccountId = ? AND capturedAt = ?
            ORDER BY playlist ASC
            """,
            (externalAccountId, baselineTimeRow["capturedAt"]),
        ).fetchall()
        return [rowToDict(row) for row in rows]

    ranks = await fetchCurrentRanks(epicId)
    if not ranks:
        return None

    filteredRanks = filterRanks(ranks)
    if not filteredRanks:
        return []

    nowStr = datetime.utcnow().isoformat()
    normalizedRanks = [normalizeRankEntry(rank) for rank in filteredRanks]
    for normalized in normalizedRanks:
        dbClient.connection.execute(
            """
            INSERT INTO rocketLeagueRankSnapshot (externalAccountId, playlist, rank, division, mmr, streak, capturedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                externalAccountId,
                normalized.get("playlist"),
                normalized.get("rank"),
                normalized.get("division"),
                normalized.get("mmr"),
                normalized.get("streak"),
                nowStr,
            ),
        )
    dbClient.connection.commit()

    return [
        {
            "externalAccountId": externalAccountId,
            "playlist": normalized.get("playlist"),
            "rank": normalized.get("rank"),
            "division": normalized.get("division"),
            "mmr": normalized.get("mmr"),
            "streak": normalized.get("streak"),
            "capturedAt": nowStr,
        }
        for normalized in normalizedRanks
    ]


async def getCurrentState(epicId: str) -> Optional[List[Dict]]:
    ranks = await fetchCurrentRanks(epicId)
    if not ranks:
        return None
    filteredRanks = filterRanks(ranks)
    return [normalizeRankEntry(rank) for rank in filteredRanks]


def computeRankDiff(baseline: List[Dict], current: List[Dict]) -> List[Dict]:
    baselineByPlaylist = {row.get("playlist"): row for row in baseline or []}
    diffs: List[Dict] = []
    for entry in current or []:
        playlist = entry.get("playlist")
        base = baselineByPlaylist.get(playlist) or {}
        mmrDiff = None
        if entry.get("mmr") is not None and base.get("mmr") is not None:
            mmrDiff = entry.get("mmr") - base.get("mmr")
        baseRank = f"{base.get('rank', 'N/A')} {base.get('division') or ''}".strip()
        currRank = f"{entry.get('rank', 'N/A')} {entry.get('division') or ''}".strip()
        rankChange = None
        if base and baseRank != currRank:
            rankChange = f"{baseRank} -> {currRank}"
        diffs.append(
            {
                "playlist": playlist,
                "mmrDiff": mmrDiff,
                "rankChange": rankChange,
                "baselineMissing": not bool(base),
            }
        )
    return diffs


async def generateDailyReport(
    dbClient: DatabaseClient, externalAccountId: int, epicId: str, todayDateStr: str
) -> Dict:
    baseline = await getOrCreateDailyBaseline(dbClient, externalAccountId, epicId, todayDateStr)
    current = await getCurrentState(epicId)
    diff = computeRankDiff(baseline or [], current or [])
    return {"baseline": baseline, "current": current, "diff": diff}
