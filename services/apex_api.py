from typing import Dict, Optional

import requests

from config.settings import appSettings
from utils.logger import getLogger


apexApiLogger = getLogger(__name__)


def fetchApexStats(playerName: str, platform: str) -> Optional[Dict]:
    """
    Fetch Apex Legends stats using the Mozambique API bridge endpoint.
    """
    apiKey = appSettings.apexApiKey
    if not apiKey:
        apexApiLogger.error("APEX_API_KEY is not configured.")
        return None

    url = f"https://api.mozambiquehe.re/bridge?player={playerName}&platform={platform}"
    headers = {"Authorization": apiKey}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        apexApiLogger.error("Failed to fetch Apex stats: %s %s", response.status_code, response.text)
    except requests.RequestException as error:
        apexApiLogger.error("Request error in fetchApexStats: %s", error)
    return None


def getApexRankSummary(playerName: str, platform: str) -> Optional[Dict]:
    """
    Return rank-related fields from the Apex Legends stats response.
    """
    data = fetchApexStats(playerName, platform)
    if not data:
        return None

    rankInfo = (data.get("global") or {}).get("rank") or {}
    return {
        "rankName": rankInfo.get("rankName"),
        "rankDiv": rankInfo.get("rankDiv"),
        "rankScore": rankInfo.get("rankScore"),
        "ladderPosPlatform": rankInfo.get("ladderPosPlatform"),
        "rankImg": rankInfo.get("rankImg"),
        "rankedSeason": rankInfo.get("rankedSeason"),
        "platform": (data.get("global") or {}).get("platform"),
        "playerName": (data.get("global") or {}).get("name") or playerName,
    }
