import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass
class Settings:
    DISCORD_TOKEN: str = os.getenv("DISCORD_TOKEN", "")

    @property
    def is_configured(self) -> bool:
        return bool(self.DISCORD_TOKEN)


settings = Settings()
