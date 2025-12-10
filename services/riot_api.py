from typing import Dict, Optional

from config.settings import appSettings
from utils.database import DatabaseClient
from utils.logger import getLogger
from utils.riotApi import RiotAPI


riotApiLogger = getLogger(__name__)


def fetchCurrentLolRank(externalAccountId: int, queueType: str) -> Optional[Dict]:
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
    entries = riotClient.getLolRankedEntriesByPuuid(puuid)
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
