"""
aicq.db — SQLite 本地存储模块

管理智能体、好友、群组、会话密钥和聊天记录的持久化存储。
使用标准库 sqlite3 同步接口，数据库默认位于 ~/.aicq-sdk/data.db。
"""

from __future__ import annotations

import os
import sqlite3
import json
import time
from pathlib import Path
from typing import Optional, List, Dict, Any


DEFAULT_DB_PATH = "~/.aicq-sdk/data.db"


class Database:
    """AICQ SDK 本地数据库。"""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        expanded = os.path.expanduser(db_path)
        db_dir = os.path.dirname(expanded)
        os.makedirs(db_dir, exist_ok=True)
        self.db_path = expanded
        self._conn: Optional[sqlite3.Connection] = None
        self._connect()
        self._init_tables()

    # ─── 内部连接管理 ────────────────────────────────────────────

    def _connect(self):
        """建立数据库连接。"""
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def _init_tables(self):
        """初始化所有数据表。"""
        cur = self._conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS agents (
                id              TEXT PRIMARY KEY,
                account_id      TEXT,
                name            TEXT NOT NULL,
                type            TEXT NOT NULL DEFAULT 'my',
                signing_pub     TEXT NOT NULL,
                signing_sec     TEXT,
                exchange_pub    TEXT,
                exchange_sec    TEXT,
                is_current      INTEGER NOT NULL DEFAULT 0,
                created_at      REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS friends (
                agent_id        TEXT NOT NULL,
                friend_id       TEXT NOT NULL,
                name            TEXT DEFAULT '',
                type            TEXT DEFAULT 'ai',
                avatar          TEXT DEFAULT '',
                public_key      TEXT DEFAULT '',
                owner_id        TEXT DEFAULT '',
                PRIMARY KEY (agent_id, friend_id)
            );

            CREATE TABLE IF NOT EXISTS groups (
                agent_id        TEXT NOT NULL,
                group_id        TEXT NOT NULL,
                name            TEXT DEFAULT '',
                owner_id        TEXT DEFAULT '',
                is_ephemeral    INTEGER NOT NULL DEFAULT 0,
                expires_at      REAL,
                PRIMARY KEY (agent_id, group_id)
            );

            CREATE TABLE IF NOT EXISTS sessions (
                agent_id        TEXT NOT NULL,
                friend_id       TEXT NOT NULL,
                session_key     TEXT NOT NULL,
                updated_at      REAL NOT NULL,
                PRIMARY KEY (agent_id, friend_id)
            );

            CREATE TABLE IF NOT EXISTS chat_history (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id        TEXT NOT NULL,
                chat_id         TEXT NOT NULL,
                is_group        INTEGER NOT NULL DEFAULT 0,
                from_id         TEXT NOT NULL,
                content         TEXT NOT NULL DEFAULT '',
                msg_type        TEXT NOT NULL DEFAULT 'text',
                timestamp       REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_chat_agent_chat
                ON chat_history(agent_id, chat_id, timestamp);
        """)
        self._conn.commit()

    def close(self):
        """关闭数据库连接。"""
        if self._conn:
            self._conn.close()
            self._conn = None

    # ─── 智能体 (agents) ────────────────────────────────────────

    def save_agent(
        self,
        account_id: str,
        name: str,
        agent_type: str,
        signing_pub: str,
        signing_sec: Optional[str] = None,
        exchange_pub: Optional[str] = None,
        exchange_sec: Optional[str] = None,
    ) -> str:
        """保存智能体信息。

        Args:
            account_id: 服务器返回的账户 ID
            name: 智能体名称
            agent_type: 类型 'my' 或 'friend'
            signing_pub: Ed25519 签名公钥
            signing_sec: Ed25519 签名私钥（好友智能体为 None）
            exchange_pub: X25519 交换公钥
            exchange_sec: X25519 交换私钥（好友智能体为 None）

        Returns:
            agent_id（等于 account_id）
        """
        agent_id = account_id
        now = time.time()

        # 如果是第一个智能体，自动设为当前
        existing = self._conn.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
        is_current = 1 if existing == 0 else 0

        self._conn.execute(
            """INSERT OR REPLACE INTO agents
               (id, account_id, name, type, signing_pub, signing_sec,
                exchange_pub, exchange_sec, is_current, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                agent_id, account_id, name, agent_type, signing_pub, signing_sec,
                exchange_pub, exchange_sec, is_current, now,
            ),
        )
        self._conn.commit()
        return agent_id

    def get_agent(self, agent_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """获取智能体信息。

        Args:
            agent_id: 智能体 ID，None 表示获取当前智能体

        Returns:
            智能体信息字典，不存在则返回 None
        """
        if agent_id is None:
            row = self._conn.execute(
                "SELECT * FROM agents WHERE is_current = 1 LIMIT 1"
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT * FROM agents WHERE id = ?", (agent_id,)
            ).fetchone()

        if row is None:
            return None
        return dict(row)

    def list_agents(self) -> List[Dict[str, Any]]:
        """列出所有智能体。"""
        rows = self._conn.execute(
            "SELECT * FROM agents ORDER BY created_at"
        ).fetchall()
        return [dict(r) for r in rows]

    def set_current(self, agent_id: str) -> bool:
        """设置当前智能体。

        Args:
            agent_id: 要设为当前的智能体 ID

        Returns:
            是否成功
        """
        row = self._conn.execute(
            "SELECT id FROM agents WHERE id = ?", (agent_id,)
        ).fetchone()
        if row is None:
            return False
        self._conn.execute("UPDATE agents SET is_current = 0")
        self._conn.execute(
            "UPDATE agents SET is_current = 1 WHERE id = ?", (agent_id,)
        )
        self._conn.commit()
        return True

    def delete_agent(self, agent_id: str) -> bool:
        """删除智能体。"""
        self._conn.execute("DELETE FROM agents WHERE id = ?", (agent_id,))
        self._conn.commit()
        return self._conn.total_changes > 0

    # ─── 好友 (friends) ─────────────────────────────────────────

    def sync_friends(self, agent_id: str, friends_list: List[Dict[str, Any]]):
        """同步好友列表（全量替换）。

        Args:
            agent_id: 所属智能体 ID
            friends_list: 好友信息列表，每项包含 friend_id, name, type, avatar, public_key, owner_id
        """
        self._conn.execute(
            "DELETE FROM friends WHERE agent_id = ?", (agent_id,)
        )
        for f in friends_list:
            self._conn.execute(
                """INSERT OR REPLACE INTO friends
                   (agent_id, friend_id, name, type, avatar, public_key, owner_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    agent_id,
                    f.get("friend_id") or f.get("accountId") or f.get("id", ""),
                    f.get("name", ""),
                    f.get("type", "ai"),
                    f.get("avatar", ""),
                    f.get("public_key") or f.get("publicKey", ""),
                    f.get("owner_id") or f.get("ownerId", ""),
                ),
            )
        self._conn.commit()

    def get_friends(self, agent_id: str) -> List[Dict[str, Any]]:
        """获取好友列表。"""
        rows = self._conn.execute(
            "SELECT * FROM friends WHERE agent_id = ?", (agent_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ─── 群组 (groups) ──────────────────────────────────────────

    def sync_groups(self, agent_id: str, groups_list: List[Dict[str, Any]]):
        """同步群组列表（全量替换）。

        Args:
            agent_id: 所属智能体 ID
            groups_list: 群组信息列表
        """
        self._conn.execute(
            "DELETE FROM groups WHERE agent_id = ?", (agent_id,)
        )
        for g in groups_list:
            self._conn.execute(
                """INSERT OR REPLACE INTO groups
                   (agent_id, group_id, name, owner_id, is_ephemeral, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    agent_id,
                    g.get("group_id") or g.get("groupId") or g.get("id", ""),
                    g.get("name", ""),
                    g.get("owner_id") or g.get("ownerId", ""),
                    1 if g.get("is_ephemeral") or g.get("isEphemeral") else 0,
                    g.get("expires_at") or g.get("expiresAt"),
                ),
            )
        self._conn.commit()

    def get_groups(self, agent_id: str) -> List[Dict[str, Any]]:
        """获取群组列表。"""
        rows = self._conn.execute(
            "SELECT * FROM groups WHERE agent_id = ?", (agent_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ─── 会话密钥 (sessions) ────────────────────────────────────

    def save_session_key(self, agent_id: str, friend_id: str, session_key: str):
        """保存会话密钥。"""
        now = time.time()
        self._conn.execute(
            """INSERT OR REPLACE INTO sessions (agent_id, friend_id, session_key, updated_at)
               VALUES (?, ?, ?, ?)""",
            (agent_id, friend_id, session_key, now),
        )
        self._conn.commit()

    def get_session_key(self, agent_id: str, friend_id: str) -> Optional[str]:
        """获取会话密钥。"""
        row = self._conn.execute(
            "SELECT session_key FROM sessions WHERE agent_id = ? AND friend_id = ?",
            (agent_id, friend_id),
        ).fetchone()
        return row["session_key"] if row else None

    # ─── 聊天记录 (chat_history) ────────────────────────────────

    def save_message(
        self,
        agent_id: str,
        chat_id: str,
        is_group: bool,
        from_id: str,
        content: str,
        msg_type: str = "text",
    ) -> int:
        """保存聊天消息。

        Args:
            agent_id: 所属智能体 ID
            chat_id: 对话 ID（好友 ID 或群组 ID）
            is_group: 是否为群组消息
            from_id: 发送者 ID
            content: 消息内容
            msg_type: 消息类型（text, image, file 等）

        Returns:
            消息记录 ID
        """
        now = time.time()
        cur = self._conn.execute(
            """INSERT INTO chat_history
               (agent_id, chat_id, is_group, from_id, content, msg_type, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (agent_id, chat_id, 1 if is_group else 0, from_id, content, msg_type, now),
        )
        self._conn.commit()
        return cur.lastrowid

    def get_history(
        self, agent_id: str, chat_id: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """获取聊天记录。

        Args:
            agent_id: 所属智能体 ID
            chat_id: 对话 ID
            limit: 返回条数上限

        Returns:
            消息列表，按时间倒序
        """
        rows = self._conn.execute(
            """SELECT * FROM chat_history
               WHERE agent_id = ? AND chat_id = ?
               ORDER BY timestamp DESC LIMIT ?""",
            (agent_id, chat_id, limit),
        ).fetchall()
        return [dict(r) for r in reversed(rows)]
