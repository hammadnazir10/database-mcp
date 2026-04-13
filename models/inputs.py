import re
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ResponseFormat(str, Enum):
    MARKDOWN = "markdown"
    JSON = "json"


class ListSchemasInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


class ListTablesInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    schema_name: Optional[str] = Field(
        default=None,
        description="PostgreSQL schema to list tables from (defaults to PG_SCHEMA env var)",
        max_length=128,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


class DescribeTableInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table_name: str = Field(
        ...,
        description="Name of the table to describe (e.g. 'users', 'orders')",
        min_length=1,
        max_length=128,
    )
    schema_name: Optional[str] = Field(
        default=None,
        description="Schema name (defaults to PG_SCHEMA)",
        max_length=128,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError(
                "Table name must start with a letter or underscore and "
                "contain only letters, numbers, and underscores."
            )
        return v


class QueryInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    sql: str = Field(
        ...,
        description=(
            "SQL SELECT query to execute. Only read-only queries allowed. "
            "Examples: 'SELECT * FROM users WHERE age > 25'"
        ),
        min_length=1,
        max_length=5000,
    )
    limit: Optional[int] = Field(
        default=50,
        description="Maximum rows to return (1-100). Defaults to 50.",
        ge=1,
        le=100,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )


class ExecuteInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    sql: str = Field(
        ...,
        description=(
            "SQL statement to execute (INSERT, UPDATE, or DELETE). "
            "Examples: \"INSERT INTO users (name, email) VALUES ('Ali', 'ali@test.com')\""
        ),
        min_length=1,
        max_length=5000,
    )


class SearchInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    search_term: str = Field(
        ...,
        description="Text to search for across all text columns in the specified table",
        min_length=1,
        max_length=200,
    )
    table_name: str = Field(
        ...,
        description="Table to search in (e.g. 'users', 'products')",
        min_length=1,
        max_length=128,
    )
    schema_name: Optional[str] = Field(
        default=None,
        description="Schema name (defaults to PG_SCHEMA)",
        max_length=128,
    )
    limit: Optional[int] = Field(
        default=20,
        description="Maximum rows to return (1-100)",
        ge=1,
        le=100,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Invalid table name format.")
        return v


class TableStatsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table_name: str = Field(
        ...,
        description="Table to get statistics for",
        min_length=1,
        max_length=128,
    )
    schema_name: Optional[str] = Field(
        default=None,
        description="Schema name (defaults to PG_SCHEMA)",
        max_length=128,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="Output format: 'markdown' or 'json'",
    )

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Invalid table name format.")
        return v
