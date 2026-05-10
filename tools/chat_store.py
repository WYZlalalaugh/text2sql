"""Persistence helpers for auth, conversations, and chat history."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

from config import config
from tools.auth_utils import hash_password, verify_password

import pymysql  # type: ignore[import-not-found]


PROJECT_DATABASE_NAME = "test_number"


@dataclass(frozen=True)
class AuthenticatedUser:
    id: int
    username: str
    status: str


def _connect() -> Any:
    return pymysql.connect(
        host=config.database.host,
        port=config.database.port,
        user=config.database.user,
        password=config.database.password,
        database=config.database.database,
        charset=config.database.charset,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def _ensure_project_database(cursor: Any) -> None:
    cursor.execute("SELECT DATABASE() AS db")
    current_db = (cursor.fetchone() or {}).get("db")
    configured_db = config.database.database
    if configured_db != PROJECT_DATABASE_NAME:
        raise RuntimeError(
            f"Refusing to operate on database '{configured_db}'. Expected '{PROJECT_DATABASE_NAME}'."
        )
    if current_db != PROJECT_DATABASE_NAME:
        raise RuntimeError(
            f"Connected to database '{current_db}'. Expected '{PROJECT_DATABASE_NAME}'."
        )


def _table_exists(cursor: Any, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*) AS count
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (config.database.database, table_name),
    )
    return bool((cursor.fetchone() or {}).get("count"))


def _get_columns(cursor: Any, table_name: str) -> dict[str, dict[str, Any]]:
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    return {row["Field"]: row for row in cursor.fetchall()}


def _column_exists(cursor: Any, table_name: str, column_name: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*) AS count
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """,
        (config.database.database, table_name, column_name),
    )
    return bool((cursor.fetchone() or {}).get("count"))


def _has_primary_key(cursor: Any, table_name: str) -> bool:
    cursor.execute(f"SHOW KEYS FROM `{table_name}` WHERE Key_name = 'PRIMARY'")
    return bool(cursor.fetchall())


def _table_has_rows(cursor: Any, table_name: str) -> bool:
    cursor.execute(f"SELECT 1 AS present FROM `{table_name}` LIMIT 1")
    return cursor.fetchone() is not None


def _supports_json_type(cursor: Any) -> bool:
    cursor.execute("SELECT VERSION() AS version")
    version = str((cursor.fetchone() or {}).get("version") or "").lower()
    if "mariadb" in version:
        return True
    digits = version.split("-", 1)[0].split(".")
    try:
        major = int(digits[0])
        minor = int(digits[1]) if len(digits) > 1 else 0
    except (TypeError, ValueError):
        return False
    return major > 5 or (major == 5 and minor >= 7)


def _json_type(cursor: Any) -> str:
    return "JSON" if _supports_json_type(cursor) else "LONGTEXT"


def _resolve_user_columns(cursor: Any) -> tuple[str, str]:
    columns = _get_columns(cursor, "user")
    id_candidates = ("id", "user_id")
    username_candidates = ("username", "user_name", "name", "account", "email")

    id_column = next((name for name in id_candidates if name in columns), None)
    username_column = next((name for name in username_candidates if name in columns), None)
    if id_column is None or username_column is None:
        raise RuntimeError(
            "The `user` table must expose an id column (`id` or `user_id`) and a username column "
            "(`username`, `user_name`, `name`, `account`, or `email`)."
        )
    return id_column, username_column


def ensure_chat_schema_initialized() -> None:
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_project_database(cursor)
            json_type = _json_type(cursor)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS `user` (
                    `id` BIGINT NOT NULL AUTO_INCREMENT,
                    `username` VARCHAR(255) NOT NULL,
                    `password_hash` VARCHAR(255) NULL,
                    `status` VARCHAR(32) NOT NULL DEFAULT 'active',
                    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (`id`),
                    UNIQUE KEY `uk_user_username` (`username`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `user_chat_history` (
                    `id` BIGINT NOT NULL AUTO_INCREMENT,
                    `user_id` BIGINT NULL,
                    `conversation_id` VARCHAR(64) NULL,
                    `role` VARCHAR(32) NULL,
                    `content` LONGTEXT NULL,
                    `steps_json` {json_type} NULL,
                    `generated_sql` LONGTEXT NULL,
                    `python_code` LONGTEXT NULL,
                    `need_clarification` BOOLEAN NOT NULL DEFAULT FALSE,
                    `clarification_sections_json` {json_type} NULL,
                    `reflection` LONGTEXT NULL,
                    `reasoning` LONGTEXT NULL,
                    `chart_reasoning` LONGTEXT NULL,
                    `chart_spec_json` {json_type} NULL,
                    `sql_result_json` {json_type} NULL,
                    `total_count` INT NULL,
                    `is_truncated` BOOLEAN NOT NULL DEFAULT FALSE,
                    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (`id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )

            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `conversation` (
                    `id` VARCHAR(64) NOT NULL,
                    `user_id` BIGINT NOT NULL,
                    `session_id` VARCHAR(128) NULL,
                    `workspace_id` VARCHAR(128) NULL,
                    `title` VARCHAR(255) NOT NULL DEFAULT '',
                    `suggested_questions_json` {json_type} NULL,
                    `enable_suggestions` BOOLEAN NOT NULL DEFAULT FALSE,
                    `last_message_preview` TEXT NULL,
                    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (`id`),
                    KEY `idx_conversation_user_updated` (`user_id`, `updated_at`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )

            _ensure_missing_columns(
                cursor,
                "user",
                {
                    "password_hash": "VARCHAR(255) NULL",
                    "status": "VARCHAR(32) NOT NULL DEFAULT 'active'",
                    "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
                    "updated_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
                },
            )

            history_has_rows = _table_has_rows(cursor, "user_chat_history")
            required_column_type = "NULL" if history_has_rows else "NOT NULL"
            if not _column_exists(cursor, "user_chat_history", "id"):
                if _has_primary_key(cursor, "user_chat_history"):
                    cursor.execute("ALTER TABLE `user_chat_history` ADD COLUMN `id` BIGINT NULL")
                else:
                    cursor.execute(
                        "ALTER TABLE `user_chat_history` "
                        "ADD COLUMN `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST"
                    )

            _ensure_missing_columns(
                cursor,
                "user_chat_history",
                {
                    "user_id": f"BIGINT {required_column_type}",
                    "conversation_id": f"VARCHAR(64) {required_column_type}",
                    "role": f"VARCHAR(32) {required_column_type}",
                    "content": f"LONGTEXT {required_column_type}",
                    "steps_json": f"{json_type} NULL",
                    "generated_sql": "LONGTEXT NULL",
                    "python_code": "LONGTEXT NULL",
                    "need_clarification": "BOOLEAN NOT NULL DEFAULT FALSE",
                    "clarification_sections_json": f"{json_type} NULL",
                    "reflection": "LONGTEXT NULL",
                    "reasoning": "LONGTEXT NULL",
                    "chart_reasoning": "LONGTEXT NULL",
                    "chart_spec_json": f"{json_type} NULL",
                    "sql_result_json": f"{json_type} NULL",
                    "total_count": "INT NULL",
                    "is_truncated": "BOOLEAN NOT NULL DEFAULT FALSE",
                    "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
                },
            )

            _ensure_index(
                cursor,
                "user_chat_history",
                "idx_chat_history_conversation_created",
                ["conversation_id", "created_at"],
            )
            _ensure_index(
                cursor,
                "user_chat_history",
                "idx_chat_history_user_conversation_created",
                ["user_id", "conversation_id", "created_at"],
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _ensure_missing_columns(cursor: Any, table_name: str, definitions: dict[str, str]) -> None:
    columns = _get_columns(cursor, table_name)
    for name, ddl in definitions.items():
        if name not in columns:
            cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{name}` {ddl}")


def _ensure_index(cursor: Any, table_name: str, index_name: str, columns: list[str]) -> None:
    cursor.execute(
        """
        SELECT COUNT(*) AS count
        FROM information_schema.statistics
        WHERE table_schema = %s AND table_name = %s AND index_name = %s
        """,
        (config.database.database, table_name, index_name),
    )
    if (cursor.fetchone() or {}).get("count"):
        return

    quoted_columns = ", ".join(f"`{column}`" for column in columns)
    cursor.execute(f"CREATE INDEX `{index_name}` ON `{table_name}` ({quoted_columns})")


def bootstrap_admin_user(username: str, password: str) -> None:
    if not username or not password:
        return

    ensure_chat_schema_initialized()
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_project_database(cursor)
            id_column, username_column = _resolve_user_columns(cursor)
            cursor.execute(
                f"SELECT `{id_column}` AS id FROM `user` WHERE `{username_column}` = %s LIMIT 1",
                (username,),
            )
            if cursor.fetchone():
                conn.commit()
                return

            password_hash = hash_password(password)
            cursor.execute(
                f"""
                INSERT INTO `user` (`{username_column}`, `password_hash`, `status`)
                VALUES (%s, %s, 'active')
                """,
                (username, password_hash),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def authenticate_user(username: str, password: str) -> AuthenticatedUser:
    ensure_chat_schema_initialized()
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_project_database(cursor)
            id_column, username_column = _resolve_user_columns(cursor)
            cursor.execute(
                f"""
                SELECT `{id_column}` AS id, `{username_column}` AS username, `password_hash`, `status`
                FROM `user`
                WHERE `{username_column}` = %s
                LIMIT 1
                """,
                (username,),
            )
            row = cursor.fetchone()
            if not row or not verify_password(password, row.get("password_hash") or ""):
                raise ValueError("Invalid username or password")

            status = str(row.get("status") or "active")
            if status != "active":
                raise ValueError("User is not active")
            return AuthenticatedUser(id=int(row["id"]), username=str(row["username"]), status=status)
    finally:
        conn.close()


def create_conversation(
    *,
    user_id: int,
    workspace_id: str | None = None,
    title: str | None = None,
    enable_suggestions: bool = False,
    conversation_id: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    ensure_chat_schema_initialized()
    conversation_id = conversation_id or uuid.uuid4().hex
    session_id = session_id or f"session_{uuid.uuid4().hex[:12]}"
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_project_database(cursor)
            cursor.execute(
                """
                INSERT INTO `conversation`
                (`id`, `user_id`, `session_id`, `workspace_id`, `title`,
                 `suggested_questions_json`, `enable_suggestions`, `last_message_preview`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    conversation_id,
                    user_id,
                    session_id,
                    workspace_id,
                    title or "",
                    json.dumps([], ensure_ascii=False),
                    bool(enable_suggestions),
                    None,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return get_conversation(user_id=user_id, conversation_id=conversation_id)


def ensure_conversation(
    *,
    user_id: int,
    conversation_id: str,
    workspace_id: str | None = None,
    session_id: str | None = None,
    title: str | None = None,
    enable_suggestions: bool = False,
) -> dict[str, Any]:
    ensure_chat_schema_initialized()
    try:
        conversation = get_conversation(user_id=user_id, conversation_id=conversation_id)
    except KeyError:
        conversation = create_conversation(
            user_id=user_id,
            workspace_id=workspace_id,
            title=title,
            enable_suggestions=enable_suggestions,
            conversation_id=conversation_id,
            session_id=session_id,
        )

    update_conversation_after_message(
        user_id=user_id,
        conversation_id=conversation_id,
        title=title if title is not None else conversation.get("title") or "",
        enable_suggestions=enable_suggestions,
        last_message_preview=conversation.get("last_message_preview") or "",
    )
    return get_conversation(user_id=user_id, conversation_id=conversation_id)


def list_conversations(*, user_id: int) -> list[dict[str, Any]]:
    ensure_chat_schema_initialized()
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_project_database(cursor)
            cursor.execute(
                """
                SELECT `id`, `session_id`, `workspace_id`, `title`, `suggested_questions_json`,
                       `enable_suggestions`, `last_message_preview`, `created_at`, `updated_at`
                FROM `conversation`
                WHERE `user_id` = %s
                ORDER BY `updated_at` DESC
                """,
                (user_id,),
            )
            rows = cursor.fetchall()
            return [_serialize_conversation_row(row) for row in rows]
    finally:
        conn.close()


def get_conversation(*, user_id: int, conversation_id: str) -> dict[str, Any]:
    ensure_chat_schema_initialized()
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_project_database(cursor)
            cursor.execute(
                """
                SELECT `id`, `session_id`, `workspace_id`, `title`, `suggested_questions_json`,
                       `enable_suggestions`, `last_message_preview`, `created_at`, `updated_at`
                FROM `conversation`
                WHERE `id` = %s AND `user_id` = %s
                LIMIT 1
                """,
                (conversation_id, user_id),
            )
            row = cursor.fetchone()
            if not row:
                raise KeyError("Conversation not found")
            return _serialize_conversation_row(row)
    finally:
        conn.close()


def get_conversation_messages(*, user_id: int, conversation_id: str) -> list[dict[str, Any]]:
    ensure_chat_schema_initialized()
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_project_database(cursor)
            cursor.execute(
                """
                SELECT `id`, `role`, `content`, `steps_json`, `generated_sql`, `python_code`,
                       `need_clarification`, `clarification_sections_json`, `reflection`, `reasoning`,
                       `chart_reasoning`, `chart_spec_json`, `sql_result_json`, `total_count`,
                       `is_truncated`, `created_at`
                FROM `user_chat_history`
                WHERE `user_id` = %s AND `conversation_id` = %s
                ORDER BY `created_at` ASC, `id` ASC
                """,
                (user_id, conversation_id),
            )
            return [_serialize_message_row(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def append_chat_message(
    *,
    user_id: int,
    conversation_id: str,
    role: str,
    content: str,
    steps: list[dict[str, Any]] | None = None,
    generated_sql: str | None = None,
    python_code: str | None = None,
    need_clarification: bool = False,
    clarification_sections: list[str] | None = None,
    reflection: str | None = None,
    reasoning: str | None = None,
    chart_reasoning: str | None = None,
    chart_spec: dict[str, Any] | None = None,
    sql_result: Any = None,
    total_count: int | None = None,
    is_truncated: bool = False,
) -> int:
    ensure_chat_schema_initialized()
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_project_database(cursor)
            cursor.execute(
                """
                INSERT INTO `user_chat_history`
                (`user_id`, `conversation_id`, `role`, `content`, `steps_json`,
                 `generated_sql`, `python_code`, `need_clarification`,
                 `clarification_sections_json`, `reflection`, `reasoning`,
                 `chart_reasoning`, `chart_spec_json`, `sql_result_json`,
                 `total_count`, `is_truncated`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    user_id,
                    conversation_id,
                    role,
                    content,
                    _dump_json(steps),
                    generated_sql,
                    python_code,
                    bool(need_clarification),
                    _dump_json(clarification_sections),
                    reflection,
                    reasoning,
                    chart_reasoning,
                    _dump_json(chart_spec),
                    _dump_json(sql_result),
                    total_count,
                    bool(is_truncated),
                ),
            )
            message_id = int(cursor.lastrowid or 0)
        conn.commit()
        return message_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_conversation_after_message(
    *,
    user_id: int,
    conversation_id: str,
    title: str | None = None,
    suggested_questions: list[str] | None = None,
    enable_suggestions: bool | None = None,
    last_message_preview: str | None = None,
) -> None:
    ensure_chat_schema_initialized()
    updates: list[str] = ["`updated_at` = CURRENT_TIMESTAMP"]
    params: list[Any] = []

    if title is not None:
        updates.append("`title` = %s")
        params.append(title)
    if suggested_questions is not None:
        updates.append("`suggested_questions_json` = %s")
        params.append(_dump_json(suggested_questions))
    if enable_suggestions is not None:
        updates.append("`enable_suggestions` = %s")
        params.append(bool(enable_suggestions))
    if last_message_preview is not None:
        updates.append("`last_message_preview` = %s")
        params.append(last_message_preview)

    params.extend([conversation_id, user_id])
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_project_database(cursor)
            cursor.execute(
                f"""
                UPDATE `conversation`
                SET {", ".join(updates)}
                WHERE `id` = %s AND `user_id` = %s
                """,
                tuple(params),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reset_conversation_session(*, user_id: int, conversation_id: str) -> str:
    ensure_chat_schema_initialized()
    new_session_id = f"session_{uuid.uuid4().hex[:12]}"
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            _ensure_project_database(cursor)
            cursor.execute(
                """
                UPDATE `conversation`
                SET `session_id` = %s, `updated_at` = CURRENT_TIMESTAMP
                WHERE `id` = %s AND `user_id` = %s
                """,
                (new_session_id, conversation_id, user_id),
            )
        conn.commit()
        return new_session_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _dump_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def _load_json(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return fallback


def _serialize_conversation_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "session_id": row.get("session_id") or "",
        "workspace_id": row.get("workspace_id") or "",
        "title": row.get("title") or "",
        "suggested_questions": _load_json(row.get("suggested_questions_json"), []),
        "enable_suggestions": bool(row.get("enable_suggestions")),
        "last_message_preview": row.get("last_message_preview") or "",
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
    }


def _serialize_message_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "role": row.get("role") or "assistant",
        "content": row.get("content") or "",
        "steps": _load_json(row.get("steps_json"), []),
        "sql": row.get("generated_sql"),
        "pythonCode": row.get("python_code"),
        "needClarification": bool(row.get("need_clarification")),
        "clarificationSections": _load_json(row.get("clarification_sections_json"), []),
        "reflection": row.get("reflection") or "",
        "reasoning": row.get("reasoning") or "",
        "chartReasoning": row.get("chart_reasoning") or "",
        "chartSpec": _load_json(row.get("chart_spec_json"), None),
        "sqlResult": _load_json(row.get("sql_result_json"), None),
        "totalCount": row.get("total_count"),
        "isTruncated": bool(row.get("is_truncated")),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
    }


def make_thread_title(messages: list[dict[str, Any]], fallback: str = "") -> str:
    for message in messages:
        if message.get("role") == "user":
            content = str(message.get("content") or "").strip()
            if content:
                return content[:18] + ("..." if len(content) > 18 else "")
    return fallback


def make_message_preview(content: str, limit: int = 100) -> str:
    content = (content or "").strip()
    if len(content) <= limit:
        return content
    return content[: limit - 3] + "..."
