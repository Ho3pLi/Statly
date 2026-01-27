from typing import Dict, Optional

from services.riot_api import fetchValorantDailySnapshot
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


async def getDailySnapshot(
    externalAccountId: int, queueType: str, todayDateStr: str
) -> Optional[Dict]:
    snapshot = await fetchValorantDailySnapshot(externalAccountId, todayDateStr)
    if not snapshot:
        return None
    baseline = snapshot.get("baseline") or {}
    current = snapshot.get("current") or {}
    baseline["queueType"] = queueType
    current["queueType"] = queueType
    return {"baseline": baseline, "current": current}


def computeRankDiff(baseline: Dict, current: Dict) -> Dict:
    if not baseline or not current:
        return {"lpDiff": 0, "rankUp": False, "rankDown": False, "tierChange": None}

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
    snapshot = await getDailySnapshot(externalAccountId, queueType, todayDateStr)
    baseline = (snapshot or {}).get("baseline") or {}
    current = (snapshot or {}).get("current") or {}
    diff = computeRankDiff(baseline or {}, current or {})
    return {"baseline": baseline, "current": current, "diff": diff}
