import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass
class Settings:
    discordToken: str = os.getenv("DISCORD_TOKEN", "")
    riotApiKey: str = os.getenv("RIOT_API_KEY", "")
    riotRegion: str = os.getenv("RIOT_REGION", "euw1")
    databasePath: str = os.getenv("DATABASE_PATH", "data/statly.db")
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
