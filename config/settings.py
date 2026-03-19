import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def resolveDatabasePath(rawPath: str) -> str:
    path = Path(rawPath)
    if path.is_absolute():
        return str(path)
    return str((PROJECT_ROOT / path).resolve())


@dataclass
class Settings:
    discordToken: str = os.getenv("DISCORD_TOKEN", "")
    riotApiKey: str = os.getenv("RIOT_API_KEY", "")
    riotRegion: str = os.getenv("RIOT_REGION", "euw1")
    databasePath: str = resolveDatabasePath(os.getenv("DATABASE_PATH", "data/statly.db"))
    apexApiKey: str = os.getenv("APEX_API_KEY", "")
    rocketLeagueApiKey: str = os.getenv("ROCKET_LEAGUE_API_KEY", "")
    valorantApiKey: str = os.getenv("VALORANT_API_KEY", "")
    reportMaxRequestsPerMinute: int = int(os.getenv("REPORT_MAX_REQUESTS_PER_MINUTE", "100"))
    reportCallsPerDelivery: int = int(os.getenv("REPORT_CALLS_PER_DELIVERY", "2"))

    @property
    def isConfigured(self) -> bool:
        return bool(self.discordToken)

    @property
    def reportSlotsPerMinute(self) -> int:
        perDelivery = max(self.reportCallsPerDelivery, 1)
        slots = self.reportMaxRequestsPerMinute // perDelivery
        return max(slots, 1)


appSettings = Settings()
