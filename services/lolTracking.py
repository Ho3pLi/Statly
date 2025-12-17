from datetime import datetime
from typing import Dict, Optional

from services.riot_api import fetchCurrentLolRank
from utils.database import DatabaseClient
from utils.logger import getLogger


lolTrackingLogger = getLogger(__name__)

TIER_ORDER = [
    "IRON",
    "BRONZE",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "EMERALD",
    "DIAMOND",
    "MASTER",
    "GRANDMASTER",
    "CHALLENGER",
]


def rowToDict(row) -> Dict:
    return {key: row[key] for key in row.keys()}


async def getOrCreateDailyBaseline(
    dbClient: DatabaseClient, externalAccountId: int, queueType: str, todayDateStr: str
) -> Optional[Dict]:
    baselineRow = dbClient.connection.execute(
        """
        SELECT * FROM lolRankSnapshot
        WHERE externalAccountId = ? AND queueType = ? AND date(capturedAt) = ?
        ORDER BY capturedAt ASC
        LIMIT 1
        """,
        (externalAccountId, queueType, todayDateStr),
    ).fetchone()

    if baselineRow:
        return rowToDict(baselineRow)

    current = await fetchCurrentLolRank(externalAccountId, queueType)
    if not current:
        return None

    nowStr = datetime.utcnow().isoformat()
    dbClient.connection.execute(
        """
        INSERT INTO lolRankSnapshot (externalAccountId, queueType, tier, division, lp, wins, losses, capturedAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            externalAccountId,
            queueType,
            current.get("tier"),
            current.get("division"),
            current.get("lp"),
            current.get("wins"),
            current.get("losses"),
            nowStr,
        ),
    )
    dbClient.connection.commit()

    return {
        "id": dbClient.connection.execute("SELECT last_insert_rowid() as id").fetchone()["id"],
        "externalAccountId": externalAccountId,
        "queueType": queueType,
        "tier": current.get("tier"),
        "division": current.get("division"),
        "lp": current.get("lp"),
        "wins": current.get("wins"),
        "losses": current.get("losses"),
        "capturedAt": nowStr,
    }


async def getCurrentState(dbClient: DatabaseClient, externalAccountId: int, queueType: str) -> Optional[Dict]:
    current = await fetchCurrentLolRank(externalAccountId, queueType)
    if not current:
        return None
    return {
        "externalAccountId": externalAccountId,
        "queueType": queueType,
        "tier": current.get("tier"),
        "division": current.get("division"),
        "lp": current.get("lp"),
        "wins": current.get("wins"),
        "losses": current.get("losses"),
    }


def computeRankDiff(baseline: Dict, current: Dict) -> Dict:
    if not baseline or not current:
        return {"lpDiff": 0, "rankUp": False, "rankDown": False, "tierChange": None}

    def tierIndex(tier: Optional[str]) -> int:
        if tier and tier.upper() in TIER_ORDER:
            return TIER_ORDER.index(tier.upper())
        return -1

    def divisionValue(division: Optional[str]) -> int:
        order = {"I": 3, "II": 2, "III": 1, "IV": 0}
        return order.get(division, -1)

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

    lpDiff = (current.get("lp") or 0) - (baseline.get("lp") or 0)

    return {
        "lpDiff": lpDiff,
        "rankUp": rankUp,
        "rankDown": rankDown,
        "tierChange": tierChange,
    }


async def generateDailyReport(
    dbClient: DatabaseClient, externalAccountId: int, queueType: str, todayDateStr: str
) -> Dict:
    baseline = await getOrCreateDailyBaseline(dbClient, externalAccountId, queueType, todayDateStr)
    current = await getCurrentState(dbClient, externalAccountId, queueType)
    diff = computeRankDiff(baseline or {}, current or {})

    return {"baseline": baseline, "current": current, "diff": diff}
