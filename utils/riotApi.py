import os
from typing import Dict, List, Optional

import requests

from config.settings import appSettings
from utils.logger import getLogger


riotLogger = getLogger(__name__)


class RiotAPI:
    """Lightweight wrapper around Riot Games endpoints for account and ranked data."""

    def __init__(self, region: str, apiKey: Optional[str] = None):
        self.region: str = region
        self.apiKey: str = apiKey or appSettings.riotApiKey or os.getenv("RIOT_API_KEY", "")
        self.platformBaseUrl: str = f"https://{self.region}.api.riotgames.com"
        self.accountBaseUrl: str = f"https://{self.region}.api.riotgames.com"
        self.headers: Dict[str, str] = {"X-Riot-Token": self.apiKey}

    def getAccountByRiotId(self, gameName: str, tagLine: str) -> Optional[Dict]:
        """Fetch account details by Riot ID (gameName + tagLine)."""
        url = f"{self.accountBaseUrl}/riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return response.json()
            riotLogger.error("Failed to fetch account: %s %s", response.status_code, response.text)
        except requests.RequestException as error:
            riotLogger.error("Request error in getAccountByRiotId: %s", error)
        return None

    def getLolRankedEntriesByPuuid(self, puuid: str) -> List[Dict]:
        """Fetch all ranked queue entries for a League of Legends player by PUUID."""
        url = f"{self.platformBaseUrl}/lol/league/v4/entries/by-puuid/{puuid}"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data if isinstance(data, list) else []
            riotLogger.error("Failed to fetch ranked entries: %s %s", response.status_code, response.text)
        except requests.RequestException as error:
            riotLogger.error("Request error in getLolRankedEntriesByPuuid: %s", error)
        return []

    def getLolSoloQueueRank(self, puuid: str) -> Optional[Dict]:
        """Return solo queue rank summary for a League of Legends player or None if unavailable."""
        entries = self.getLolRankedEntriesByPuuid(puuid)
        for entry in entries:
            if entry.get("queueType") == "RANKED_SOLO_5x5":
                return {
                    "tier": entry.get("tier"),
                    "division": entry.get("rank"),
                    "lp": entry.get("leaguePoints"),
                    "wins": entry.get("wins"),
                    "losses": entry.get("losses"),
                }
        return None


if __name__ == "__main__":
    if not appSettings.riotApiKey:
        print("Set RIOT_API_KEY in your environment before running this example.")
    else:
        region = "europe"
        riotApi = RiotAPI(region)
        gameName = "exampleName"
        tagLine = "EUW"
        account = riotApi.getAccountByRiotId(gameName, tagLine)
        if account:
            puuid = account.get("puuid", "")
            soloRank = riotApi.getLolSoloQueueRank(puuid)
            print(f"Solo queue rank for {gameName}#{tagLine}: {soloRank}")
        else:
            print("Account not found; unable to fetch rank.")
