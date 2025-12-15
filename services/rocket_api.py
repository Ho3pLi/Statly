from typing import Dict, List, Optional

import requests

from config.settings import appSettings
from utils.logger import getLogger


rocketApiLogger = getLogger(__name__)


def fetchRocketLeagueRanks(epicId: str) -> Optional[List[Dict]]:
    """
    Fetch Rocket League ranks for the given Epic ID.
    """
    apiKey = appSettings.rocketLeagueApiKey
    if not apiKey:
        rocketApiLogger.error("ROCKET_LEAGUE_API_KEY is not configured.")
        return None

    url = f"https://rocket-league1.p.rapidapi.com/ranks/{epicId}"
    headers = {
        "x-rapidapi-key": apiKey,
        "x-rapidapi-host": "rocket-league1.p.rapidapi.com",
        "User-Agent": "RapidAPI Playground",
        "Accept-Encoding": "identity",
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            ranks = data.get("ranks")
            return ranks if isinstance(ranks, list) else None
        rocketApiLogger.error("Failed to fetch Rocket League ranks: %s %s", response.status_code, response.text)
    except requests.RequestException as error:
        rocketApiLogger.error("Request error in fetchRocketLeagueRanks: %s", error)
    return None
