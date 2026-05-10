"""
Schema provider abstraction for static cache and MySQL metadata.
"""
# pyright: reportUnannotatedClassAttribute=false, reportImplicitOverride=false, reportExplicitAny=false, reportAny=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportUnnecessaryIsInstance=false
import logging
try:
    import mysql.connector as mysql_connector
except ImportError:
    mysql_connector = None
from abc import ABC, abstractmethod
from typing import Any, cast

# Simple no-op override decorator to mark overrides for static checkers that understand it.
# Avoid importing typing.override to prevent signature mismatches in older typing stubs.
def override(func: Any) -> Any:
    return func
try:
    from config import config, DatabaseConfig
except ImportError:
    from ..config import config, DatabaseConfig

try:
    # workspace_context lives alongside this module in tools/
    from .workspace_context import get_workspace_context, WorkspaceContext
except ImportError:
    # fallback import path for different package layouts
    from tools.workspace_context import get_workspace_context, WorkspaceContext

logger = logging.getLogger(__name__)


class SchemaProvider(ABC):
    @abstractmethod
    def get_schema_text(self) -> str:
        """Return schema context for planner prompts."""

    @abstractmethod
    @override
    def get_metrics_text(self) -> str:
        """Return metrics context for planner prompts."""


class StaticSchemaProvider(SchemaProvider):
    """Delegate to the existing static schema cache."""

    workspace_id: str
    workspace_context: WorkspaceContext

    def __init__(self, workspace_id: str | None = None) -> None:
        # preserve the public workspace_id attribute for compatibility
        self.workspace_id = workspace_id or "default"
        # derive a WorkspaceContext to expose semantic/logging boundaries
        self.workspace_context = get_workspace_context(self.workspace_id)

    def get_schema_text(self) -> str:
        try:
            from tools.schema_cache import get_schema_text
        except ImportError:
            from .schema_cache import get_schema_text

        logger.debug(
            "Loading static schema for workspace=%s tag=%s",
            self.workspace_context.workspace_id,
            self.workspace_context.log_tag,
        )
        return get_schema_text()

    def get_metrics_text(self) -> str:
        try:
            from tools.schema_cache import get_metrics_text
        except ImportError:
            from .schema_cache import get_metrics_text

        logger.debug(
            "Loading static metrics for workspace=%s tag=%s",
            self.workspace_context.workspace_id,
            self.workspace_context.log_tag,
        )
        return get_metrics_text()


class MySQLSchemaProvider(SchemaProvider):
    """Fetch schema metadata directly from MySQL information_schema.

    This provider is used by the iterative metric loop to get fresh
    schema information without relying on Cube API.
    """

    workspace_id: str
    workspace_context: WorkspaceContext
    database_config: DatabaseConfig
    fallback_provider: SchemaProvider
    _schema_cache: dict[str, list[dict[str, object]]] | None

    def __init__(
        self,
        database_config: DatabaseConfig | None = None,
        workspace_id: str | None = None,
        fallback_provider: SchemaProvider | None = None,
    ) -> None:
        self.workspace_id = workspace_id or "default"
        self.workspace_context = get_workspace_context(self.workspace_id)
        self.database_config = database_config or config.database
        self.fallback_provider = fallback_provider or StaticSchemaProvider(workspace_id=self.workspace_id)
        self._schema_cache = None

    @override
    def get_schema_text(self) -> str:
        """Return MySQL schema context for planner prompts."""
        try:
            schema = self._fetch_mysql_schema()
            return self._format_schema_text(schema)
        except Exception as exc:
            logger.warning(
                "MySQL schema fetch failed for workspace=%s tag=%s (%s), falling back to static",
                self.workspace_context.workspace_id,
                self.workspace_context.log_tag,
                exc,
            )
            return self.fallback_provider.get_schema_text()

    @override
    def get_metrics_text(self) -> str:
        """Return metrics context - delegates to fallback for now."""
        return self.fallback_provider.get_metrics_text()

    def _fetch_mysql_schema(self) -> dict[str, list[dict[str, object]]]:
        """Fetch table and column metadata from MySQL information_schema."""
        if self._schema_cache is not None:
            return self._schema_cache
        if mysql_connector is None:
            raise RuntimeError("mysql.connector is not installed")

        conn = mysql_connector.connect(
            host=self.database_config.host,
            port=self.database_config.port,
            user=self.database_config.user,
            password=self.database_config.password,
            database="information_schema",
            charset=self.database_config.charset,
        )

        cursor = conn.cursor(dictionary=True)
        try:
            query = """
                SELECT
                    t.TABLE_NAME,
                    t.TABLE_COMMENT,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.COLUMN_COMMENT,
                    c.IS_NULLABLE,
                    c.COLUMN_KEY
                FROM information_schema.TABLES t
                JOIN information_schema.COLUMNS c ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
                    AND t.TABLE_NAME = c.TABLE_NAME
                WHERE t.TABLE_SCHEMA = %s
                ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION
            """

            _ = cursor.execute(query, (self.database_config.database,))
            rows = cursor.fetchall()

            schema: dict[str, list[dict[str, object]]] = {}
            for row in rows:
                row_dict = cast(dict[str, object], row)
                table_name = str(row_dict["TABLE_NAME"])
                if table_name not in schema:
                    schema[table_name] = []

                schema[table_name].append(
                    {
                        "column_name": str(row_dict["COLUMN_NAME"]),
                        "data_type": str(row_dict["DATA_TYPE"]),
                        "column_comment": str(row_dict["COLUMN_COMMENT"] or ""),
                        "is_nullable": str(row_dict["IS_NULLABLE"]),
                        "column_key": str(row_dict["COLUMN_KEY"]),
                    }
                )

            self._schema_cache = schema
            return schema
        finally:
            _ = cursor.close()
            _ = conn.close()

    def _format_schema_text(self, schema: dict[str, list[dict[str, object]]]) -> str:
        """Format schema dictionary into planner-friendly text."""
        lines: list[str] = []

        for table_name, columns in sorted(schema.items()):
            lines.append(f"Table: {table_name}")

            for col in columns:
                col_name = str(col["column_name"])
                data_type = str(col["data_type"])
                comment = str(col.get("column_comment", ""))
                is_key = str(col.get("column_key", ""))

                parts = [f"  Column: {col_name} ({data_type})"]
                if is_key == "PRI":
                    parts.append(" PRIMARY KEY")
                if comment:
                    parts.append(f" - {comment}")

                lines.append("".join(parts))

            lines.append("")

        return "\n".join(lines)


def get_schema_provider(workspace_id: str | None = None) -> SchemaProvider:
    """Return the default schema provider for the current runtime."""
    _ = config
    return StaticSchemaProvider(workspace_id=workspace_id)
