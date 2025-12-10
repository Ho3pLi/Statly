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

    @property
    def isConfigured(self) -> bool:
        return bool(self.discordToken)


appSettings = Settings()
