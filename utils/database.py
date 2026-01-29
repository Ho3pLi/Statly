import re
import sqlite3
from pathlib import Path
from typing import Optional

from utils.logger import getLogger


dbLogger = getLogger(__name__)


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS game (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user (
    id INTEGER PRIMARY KEY,
    discordUserId TEXT NOT NULL UNIQUE,
    username TEXT,
    discriminator TEXT,
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS externalAccount (
    id INTEGER PRIMARY KEY,
    gameId INTEGER NOT NULL REFERENCES game(id) ON DELETE CASCADE,
    externalId TEXT NOT NULL,
    displayName TEXT,
    tagLine TEXT,
    region TEXT,
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (gameId, externalId)
);

CREATE TABLE IF NOT EXISTS guild (
    id INTEGER PRIMARY KEY,
    discordGuildId TEXT NOT NULL UNIQUE,
    name TEXT,
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS guildMemberAccount (
    id INTEGER PRIMARY KEY,
    guildId INTEGER NOT NULL REFERENCES guild(id) ON DELETE CASCADE,
    userId INTEGER NOT NULL REFERENCES user(id) ON DELETE CASCADE,
    externalAccountId INTEGER NOT NULL REFERENCES externalAccount(id) ON DELETE CASCADE,
    isPrimary INTEGER NOT NULL DEFAULT 0 CHECK (isPrimary IN (0, 1)),
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (guildId, userId, externalAccountId)
);

CREATE TABLE IF NOT EXISTS lolProfile (
    id INTEGER PRIMARY KEY,
    externalAccountId INTEGER NOT NULL UNIQUE REFERENCES externalAccount(id) ON DELETE CASCADE,
    summonerLevel INTEGER,
    iconId INTEGER,
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lolRankSnapshot (
    id INTEGER PRIMARY KEY,
    externalAccountId INTEGER NOT NULL REFERENCES externalAccount(id) ON DELETE CASCADE,
    queueType TEXT,
    tier TEXT,
    division TEXT,
    lp INTEGER,
    wins INTEGER,
    losses INTEGER,
    capturedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS valorantProfile (
    id INTEGER PRIMARY KEY,
    externalAccountId INTEGER NOT NULL UNIQUE REFERENCES externalAccount(id) ON DELETE CASCADE,
    accountLevel INTEGER,
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS valorantRankSnapshot (
    id INTEGER PRIMARY KEY,
    externalAccountId INTEGER NOT NULL REFERENCES externalAccount(id) ON DELETE CASCADE,
    queueType TEXT,
    tier TEXT,
    division TEXT,
    lp INTEGER,
    wins INTEGER,
    losses INTEGER,
    capturedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS valorantGroup (
    id INTEGER PRIMARY KEY,
    guildId INTEGER NOT NULL REFERENCES guild(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    createdByUserId INTEGER REFERENCES user(id) ON DELETE SET NULL,
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (guildId, name)
);

CREATE TABLE IF NOT EXISTS valorantGroupMember (
    id INTEGER PRIMARY KEY,
    groupId INTEGER NOT NULL REFERENCES valorantGroup(id) ON DELETE CASCADE,
    displayName TEXT NOT NULL,
    tagLine TEXT NOT NULL,
    region TEXT,
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (groupId, displayName, tagLine)
);

CREATE TABLE IF NOT EXISTS csgoProfile (
    id INTEGER PRIMARY KEY,
    externalAccountId INTEGER NOT NULL UNIQUE REFERENCES externalAccount(id) ON DELETE CASCADE,
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS csgoRankSnapshot (
    id INTEGER PRIMARY KEY,
    externalAccountId INTEGER NOT NULL REFERENCES externalAccount(id) ON DELETE CASCADE,
    rank TEXT,
    wins INTEGER,
    losses INTEGER,
    capturedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS apexRankSnapshot (
    id INTEGER PRIMARY KEY,
    externalAccountId INTEGER NOT NULL REFERENCES externalAccount(id) ON DELETE CASCADE,
    rankName TEXT,
    rankDiv INTEGER,
    rankScore INTEGER,
    ladderPosPlatform INTEGER,
    rankedSeason TEXT,
    capturedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rocketLeagueRankSnapshot (
    id INTEGER PRIMARY KEY,
    externalAccountId INTEGER NOT NULL REFERENCES externalAccount(id) ON DELETE CASCADE,
    playlist TEXT NOT NULL,
    rank TEXT,
    division TEXT,
    mmr INTEGER,
    streak TEXT,
    capturedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_externalAccount_game_externalId ON externalAccount (gameId, externalId);
CREATE INDEX IF NOT EXISTS idx_guildMemberAccount_guildId ON guildMemberAccount (guildId);
CREATE INDEX IF NOT EXISTS idx_guildMemberAccount_userId ON guildMemberAccount (userId);
CREATE INDEX IF NOT EXISTS idx_guildMemberAccount_externalAccountId ON guildMemberAccount (externalAccountId);
CREATE INDEX IF NOT EXISTS idx_lolRankSnapshot_externalAccountId ON lolRankSnapshot (externalAccountId);
CREATE INDEX IF NOT EXISTS idx_valorantRankSnapshot_externalAccountId ON valorantRankSnapshot (externalAccountId);
CREATE INDEX IF NOT EXISTS idx_valorantGroup_guildId ON valorantGroup (guildId);
CREATE INDEX IF NOT EXISTS idx_valorantGroupMember_groupId ON valorantGroupMember (groupId);
CREATE INDEX IF NOT EXISTS idx_csgoRankSnapshot_externalAccountId ON csgoRankSnapshot (externalAccountId);
CREATE INDEX IF NOT EXISTS idx_apexRankSnapshot_externalAccountId ON apexRankSnapshot (externalAccountId);
CREATE INDEX IF NOT EXISTS idx_rocketLeagueRankSnapshot_externalAccountId ON rocketLeagueRankSnapshot (externalAccountId);

CREATE TABLE IF NOT EXISTS reportPreference (
    id INTEGER PRIMARY KEY,
    guildId INTEGER NOT NULL REFERENCES guild(id) ON DELETE CASCADE,
    userId INTEGER NOT NULL REFERENCES user(id) ON DELETE CASCADE,
    externalAccountId INTEGER NOT NULL REFERENCES externalAccount(id) ON DELETE CASCADE,
    queueType TEXT NOT NULL DEFAULT 'RANKED_SOLO_5x5',
    schedule TEXT NOT NULL, -- HH:MM in UTC
    channelId TEXT,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    createdAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (guildId, userId, externalAccountId, queueType)
);

CREATE INDEX IF NOT EXISTS idx_reportPreference_schedule ON reportPreference (schedule);
CREATE INDEX IF NOT EXISTS idx_reportPreference_enabled ON reportPreference (enabled);
"""


class DatabaseClient:
    def normalizeGroupName(self, name: str) -> str:
        cleaned = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", name or "")
        return re.sub(r"\s+", "", cleaned).strip().lower()

    def __init__(self, dbPath: str):
        self.dbPath = Path(dbPath)
        self.dbPath.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.dbPath)
        self.connection.row_factory = sqlite3.Row
        self.ensureSchema()

    def ensureSchema(self) -> None:
        self.connection.executescript(SCHEMA_SQL)
        self.ensureColumn("valorantRankSnapshot", "queueType", "TEXT")
        self.ensureColumn("valorantRankSnapshot", "tier", "TEXT")
        self.ensureColumn("valorantRankSnapshot", "division", "TEXT")
        self.ensureColumn("valorantRankSnapshot", "lp", "INTEGER")
        self.ensureColumn("valorantRankSnapshot", "wins", "INTEGER")
        self.ensureColumn("valorantRankSnapshot", "losses", "INTEGER")
        self.ensureColumn("reportPreference", "channelId", "TEXT")
        self.connection.commit()

    def ensureColumn(self, tableName: str, columnName: str, columnType: str) -> None:
        columns = self.connection.execute(f"PRAGMA table_info({tableName})").fetchall()
        if not any(col["name"] == columnName for col in columns):
            self.connection.execute(f"ALTER TABLE {tableName} ADD COLUMN {columnName} {columnType}")

    def getOrCreateGame(self, code: str, name: str) -> int:
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO game (code, name) VALUES (?, ?)", (code, name)
        )
        if cursor.lastrowid:
            self.connection.commit()
            return cursor.lastrowid
        existing = self.connection.execute("SELECT id FROM game WHERE code = ?", (code,)).fetchone()
        return int(existing["id"])

    def getOrCreateGuild(self, discordGuildId: str, name: Optional[str]) -> int:
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO guild (discordGuildId, name) VALUES (?, ?)",
            (discordGuildId, name),
        )
        if cursor.lastrowid:
            self.connection.commit()
            return cursor.lastrowid
        self.connection.execute(
            "UPDATE guild SET name = COALESCE(?, name) WHERE discordGuildId = ?",
            (name, discordGuildId),
        )
        self.connection.commit()
        existing = self.connection.execute(
            "SELECT id FROM guild WHERE discordGuildId = ?", (discordGuildId,)
        ).fetchone()
        return int(existing["id"])

    def getOrCreateUser(self, discordUserId: str, username: Optional[str], discriminator: Optional[str]) -> int:
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO user (discordUserId, username, discriminator) VALUES (?, ?, ?)",
            (discordUserId, username, discriminator),
        )
        if cursor.lastrowid:
            self.connection.commit()
            return cursor.lastrowid
        self.connection.execute(
            "UPDATE user SET username = COALESCE(?, username), discriminator = COALESCE(?, discriminator) WHERE discordUserId = ?",
            (username, discriminator, discordUserId),
        )
        self.connection.commit()
        existing = self.connection.execute(
            "SELECT id FROM user WHERE discordUserId = ?", (discordUserId,)
        ).fetchone()
        return int(existing["id"])

    def getOrCreateExternalAccount(
        self,
        gameId: int,
        externalId: str,
        displayName: Optional[str],
        tagLine: Optional[str],
        region: Optional[str],
    ) -> int:
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO externalAccount (gameId, externalId, displayName, tagLine, region) VALUES (?, ?, ?, ?, ?)",
            (gameId, externalId, displayName, tagLine, region),
        )
        if cursor.lastrowid:
            self.connection.commit()
            return cursor.lastrowid
        self.connection.execute(
            "UPDATE externalAccount SET displayName = COALESCE(?, displayName), tagLine = COALESCE(?, tagLine), region = COALESCE(?, region) WHERE gameId = ? AND externalId = ?",
            (displayName, tagLine, region, gameId, externalId),
        )
        self.connection.commit()
        existing = self.connection.execute(
            "SELECT id FROM externalAccount WHERE gameId = ? AND externalId = ?", (gameId, externalId)
        ).fetchone()
        return int(existing["id"])

    def getGameIdForExternalAccount(self, externalAccountId: int) -> Optional[int]:
        row = self.connection.execute(
            "SELECT gameId FROM externalAccount WHERE id = ?", (externalAccountId,)
        ).fetchone()
        return int(row["gameId"]) if row else None

    def hasPrimaryForGame(self, guildId: int, userId: int, gameId: int) -> bool:
        row = self.connection.execute(
            """
            SELECT 1
            FROM guildMemberAccount gma
            JOIN externalAccount ea ON ea.id = gma.externalAccountId
            WHERE gma.guildId = ? AND gma.userId = ? AND ea.gameId = ? AND gma.isPrimary = 1
            LIMIT 1
            """,
            (guildId, userId, gameId),
        ).fetchone()
        return bool(row)

    def setPrimaryForGame(self, guildId: int, userId: int, gameId: int, externalAccountId: int) -> None:
        self.connection.execute(
            """
            UPDATE guildMemberAccount
            SET isPrimary = 0
            WHERE guildId = ? AND userId = ? AND externalAccountId IN (
                SELECT id FROM externalAccount WHERE gameId = ?
            )
            """,
            (guildId, userId, gameId),
        )
        self.connection.execute(
            """
            UPDATE guildMemberAccount
            SET isPrimary = 1
            WHERE guildId = ? AND userId = ? AND externalAccountId = ?
            """,
            (guildId, userId, externalAccountId),
        )
        self.connection.commit()

    def linkGuildMemberAccount(self, guildId: int, userId: int, externalAccountId: int, forcePrimary: bool = False) -> bool:
        gameId = self.getGameIdForExternalAccount(externalAccountId)
        if gameId is None:
            dbLogger.error("Cannot link guild member: externalAccountId %s missing gameId", externalAccountId)
            return False

        shouldBePrimary = forcePrimary or not self.hasPrimaryForGame(guildId, userId, gameId)

        try:
            self.connection.execute(
                """
                INSERT INTO guildMemberAccount (guildId, userId, externalAccountId, isPrimary)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(guildId, userId, externalAccountId) DO UPDATE SET
                    isPrimary = excluded.isPrimary
                """,
                (guildId, userId, externalAccountId, 1 if shouldBePrimary else 0),
            )
            self.connection.commit()
        except sqlite3.IntegrityError as error:
            dbLogger.error(
                "Failed to link guild member (guildId=%s, userId=%s, externalAccountId=%s): %s",
                guildId,
                userId,
                externalAccountId,
                error,
            )
            return False

        if shouldBePrimary:
            self.setPrimaryForGame(guildId, userId, gameId, externalAccountId)
        return True

    def getReportPreference(self, guildId: int, userId: int, externalAccountId: int, queueType: str):
        return self.connection.execute(
            """
            SELECT id, schedule, enabled
            FROM reportPreference
            WHERE guildId = ? AND userId = ? AND externalAccountId = ? AND queueType = ?
            """,
            (guildId, userId, externalAccountId, queueType),
        ).fetchone()

    def countEnabledReportsForSchedule(self, guildId: int, schedule: str) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(*) as cnt
            FROM reportPreference
            WHERE enabled = 1 AND schedule = ? AND guildId = ?
            """,
            (schedule, guildId),
        ).fetchone()
        return int(row["cnt"]) if row else 0

    def upsertReportPreference(
        self,
        guildId: int,
        userId: int,
        externalAccountId: int,
        queueType: str,
        schedule: str,
        channelId: Optional[str],
        maxPerMinute: int = 25,
    ) -> bool:
        existing = self.getReportPreference(guildId, userId, externalAccountId, queueType)
        targetSchedule = schedule
        if existing and existing["schedule"] == targetSchedule and existing["enabled"]:
            return True

        currentCount = self.countEnabledReportsForSchedule(guildId, targetSchedule)
        if currentCount >= maxPerMinute:
            return False

        self.connection.execute(
            """
            INSERT INTO reportPreference (guildId, userId, externalAccountId, queueType, schedule, channelId, enabled)
            VALUES (?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(guildId, userId, externalAccountId, queueType) DO UPDATE SET
                schedule = excluded.schedule,
                channelId = excluded.channelId,
                enabled = 1
            """,
            (guildId, userId, externalAccountId, queueType, targetSchedule, channelId),
        )
        self.connection.commit()
        return True

    def disableReportPreference(self, guildId: int, userId: int, externalAccountId: int, queueType: str) -> bool:
        self.connection.execute(
            """
            UPDATE reportPreference
            SET enabled = 0
            WHERE guildId = ? AND userId = ? AND externalAccountId = ? AND queueType = ?
            """,
            (guildId, userId, externalAccountId, queueType),
        )
        self.connection.commit()
        return True

    def getOrCreateValorantGroup(self, guildId: int, name: str, createdByUserId: Optional[int]) -> int:
        normalized = self.normalizeGroupName(name)
        existing = self.connection.execute(
            "SELECT id FROM valorantGroup WHERE guildId = ? AND lower(trim(name)) = lower(trim(?))",
            (guildId, name),
        ).fetchone()
        if existing:
            trimmed = name.strip()
            self.connection.execute(
                """
                UPDATE valorantGroup
                SET name = ?,
                    createdByUserId = COALESCE(?, createdByUserId)
                WHERE id = ?
                """,
                (trimmed, createdByUserId, int(existing["id"])),
            )
            self.connection.commit()
            return int(existing["id"])
        rows = self.connection.execute(
            "SELECT id, name FROM valorantGroup WHERE guildId = ?",
            (guildId,),
        ).fetchall()
        for row in rows:
            if self.normalizeGroupName(row["name"]) == normalized:
                display_name = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", name or "").strip()
                self.connection.execute(
                    """
                    UPDATE valorantGroup
                    SET name = ?,
                        createdByUserId = COALESCE(?, createdByUserId)
                    WHERE id = ?
                    """,
                    (display_name, createdByUserId, int(row["id"])),
                )
                self.connection.commit()
                return int(row["id"])
        trimmed = name.strip()
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO valorantGroup (guildId, name, createdByUserId) VALUES (?, ?, ?)",
            (guildId, trimmed, createdByUserId),
        )
        if cursor.lastrowid:
            self.connection.commit()
            return cursor.lastrowid
        self.connection.execute(
            """
            UPDATE valorantGroup
            SET name = COALESCE(?, name),
                createdByUserId = COALESCE(?, createdByUserId)
            WHERE guildId = ? AND lower(trim(name)) = lower(trim(?))
            """,
            (trimmed, createdByUserId, guildId, name),
        )
        self.connection.commit()
        existing = self.connection.execute(
            "SELECT id FROM valorantGroup WHERE guildId = ? AND lower(trim(name)) = lower(trim(?))",
            (guildId, name),
        ).fetchone()
        return int(existing["id"])

    def getValorantGroup(self, guildId: int, name: str) -> Optional[sqlite3.Row]:
        row = self.connection.execute(
            "SELECT id, name FROM valorantGroup WHERE guildId = ? AND lower(trim(name)) = lower(trim(?))",
            (guildId, name),
        ).fetchone()
        if row:
            return row
        normalized = self.normalizeGroupName(name)
        rows = self.connection.execute(
            "SELECT id, name FROM valorantGroup WHERE guildId = ?",
            (guildId,),
        ).fetchall()
        for candidate in rows:
            if self.normalizeGroupName(candidate["name"]) == normalized:
                return candidate
        return None

    def listValorantGroups(self, guildId: int) -> list[str]:
        rows = self.connection.execute(
            "SELECT name FROM valorantGroup WHERE guildId = ? ORDER BY name ASC",
            (guildId,),
        ).fetchall()
        return [row["name"] for row in rows]

    def replaceValorantGroupMembers(
        self, groupId: int, members: list[dict]
    ) -> None:
        self.connection.execute(
            "DELETE FROM valorantGroupMember WHERE groupId = ?",
            (groupId,),
        )
        self.connection.executemany(
            """
            INSERT OR IGNORE INTO valorantGroupMember (groupId, displayName, tagLine, region)
            VALUES (?, ?, ?, ?)
            """,
            [
                (groupId, member["displayName"], member["tagLine"], member.get("region"))
                for member in members
            ],
        )
        self.connection.commit()

    def getValorantGroupMembers(self, groupId: int) -> list[dict]:
        rows = self.connection.execute(
            """
            SELECT displayName, tagLine, region
            FROM valorantGroupMember
            WHERE groupId = ?
            ORDER BY displayName ASC, tagLine ASC
            """,
            (groupId,),
        ).fetchall()
        return [dict(row) for row in rows]

    def addValorantGroupMembers(self, groupId: int, members: list[dict]) -> None:
        if not members:
            return
        self.connection.executemany(
            """
            INSERT OR IGNORE INTO valorantGroupMember (groupId, displayName, tagLine, region)
            VALUES (?, ?, ?, ?)
            """,
            [
                (groupId, member["displayName"], member["tagLine"], member.get("region"))
                for member in members
            ],
        )
        self.connection.commit()

    def removeValorantGroupMembers(self, groupId: int, members: list[dict]) -> int:
        if not members:
            return 0
        cursor = self.connection.executemany(
            """
            DELETE FROM valorantGroupMember
            WHERE groupId = ? AND lower(displayName) = lower(?) AND lower(tagLine) = lower(?)
            """,
            [
                (groupId, member["displayName"], member["tagLine"])
                for member in members
            ],
        )
        self.connection.commit()
        return cursor.rowcount or 0

    def deleteValorantGroup(self, groupId: int) -> None:
        self.connection.execute("DELETE FROM valorantGroup WHERE id = ?", (groupId,))
        self.connection.commit()
