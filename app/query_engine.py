"""LLM-driven NL → SQL pipeline using LangChain."""

from __future__ import annotations

import logging
import os , re
from typing import Any, Iterable, Optional,Tuple
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from langchain_experimental.sql import SQLDatabaseChain
from langchain_community.utilities.sql_database import SQLDatabase
from langchain.prompts import PromptTemplate
from app.model import QueryResponse

logger = logging.getLogger(__name__)
load_dotenv()

DB_URI = "sqlite:///data/company.db"
PROMPT_PATH = "app/prompts/sql_prompt2.txt"


def _load_prompt() -> PromptTemplate:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return PromptTemplate(
            input_variables=["input", "table_info", "dialect", "top_k"],
            template=f.read(),
        )


def _make_llm():
    # Initialize the LLM (OpenAI model)
    return AzureChatOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_API_VERSION"),
        azure_deployment=os.getenv("AZURE_DEPLOYMENT_MODEL"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        temperature=0
    )

def _clean_sql(text: str) -> str:
    s = text.strip()

    if "UNSUPPORTED" in s.upper():
        return "UNSUPPORTED"

    s = re.sub(r"^```[a-zA-Z0-9]*\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    s = re.sub(r"^\s*sql\s*\n", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*SQL\s*:\s*", "", s, flags=re.IGNORECASE)

    parts = [p.strip() for p in s.split(";")]
    first = next((p for p in parts if p), "")
    return first + ";" if first else ""


def _enforce_select_only(sql: str) -> str:
    """
    Block non-SELECT statements for safety.
    """
    if not re.match(r"^\s*select\b", sql, flags=re.IGNORECASE):
        raise ValueError("Only SELECT statements are allowed.")
    return sql


def _apply_limit(sql: str, limit: Optional[int]) -> str:
    """
    Apply a LIMIT if the SQL doesn't already include one and a limit is given.
    Works for SQLite.
    """
    if not limit:
        return sql

    # crude check for 'limit' presence; avoids double-appending
    if re.search(r"\blimit\s+\d+\b", sql, flags=re.IGNORECASE):
        return sql

    # insert LIMIT before the trailing semicolon if present
    sql_no_sc = sql.rstrip().rstrip(";")
    return f"{sql_no_sc} LIMIT {int(limit)};"


def _format_rows_for_answer(rows: Iterable[tuple]) -> str:
    """
    Provide a succinct natural-language summary alongside the table data.
    """
    rows = list(rows)
    if not rows:
        return "No rows matched your query."

    if len(rows) == 1 and len(rows[0]) == 1:
        return f"The result is {rows[0][0]}."

    preview = min(len(rows), 5)
    return f"Found {len(rows)} row(s). Showing top {preview}."


DB = SQLDatabase.from_uri(DB_URI)
LLM = _make_llm()
PROMPT = _load_prompt()

DB_CHAIN = SQLDatabaseChain.from_llm(
    llm=LLM,
    db=DB,
    prompt=PROMPT,
    return_intermediate_steps=True,
    verbose=True,
)


def _generate_sql(question: str, top_k: int = 5) -> str:
    """
    Ask the LLM for a SELECT query against our schema.
    Prompt strongly to include useful columns (period label, item/line_item, value).
    """
    schema = DB.get_table_info()
    dialect = DB.dialect

    prompt = PROMPT.format(
        input=question,
        table_info=schema,
        dialect=dialect,
        top_k=str(top_k),
    )
    raw = LLM.predict(prompt)
    sql = _clean_sql(raw)
    return _enforce_select_only(sql)


def _execute_sql(sql: str) -> Tuple[list[str], list[list[Any]]]:
    """
    Execute SQL and return (columns, rows).
    """
    if not sql:
        raise ValueError("Generated SQL was empty after sanitization.")

    engine = DB._engine  # SQLAlchemy engine
    with engine.connect() as conn:
        result = conn.exec_driver_sql(sql)
        if not result.returns_rows:
            return [], []
        columns = list(result.keys())
        rows = [list(r) for r in result.fetchall()]
        return columns, rows

def _summarize_table(question: str, columns: list[str], rows: list[list[Any]]) -> str:
    """
    Use LLM to convert a table + question into a conversational summary.
    """
    if not columns or not rows:
        return "There was no data to summarize."

    # Flatten table into Markdown
    header = " | ".join(columns)
    sep = " | ".join(["---"] * len(columns))
    body = "\n".join(" | ".join(map(str, row)) for row in rows[:30])  # cap for brevity

    table_md = f"{header}\n{sep}\n{body}"
    prompt = (
        f"The user asked: '{question}'\n\n"
        f"The following table was returned:\n\n{table_md}\n\n"
        "Write a short, conversational summary of this data. Highlight interesting values, trends, or anomalies."
    )

    try:
        response = LLM.predict(prompt)
        return response.strip()
    except Exception as exc:
        logger.warning("Summarization failed: %s", str(exc))
        return "Here is the data as requested."
    
def process_question(
    question: str, return_table: bool = True, limit: Optional[int] = None
) -> QueryResponse:
    try:
        sql = _generate_sql(question)
        if sql.strip().upper() == "UNSUPPORTED":
            return QueryResponse(
                sql=None,
                answer="Sorry, I cannot answer that question based on the available data.",
                error=None,
                columns=[],
                rows=[],
            )

        sql = _apply_limit(sql, limit)
        columns, rows = _execute_sql(sql)

        answer = _summarize_table(question, columns, rows) if return_table else _format_rows_for_answer(rows)

        return QueryResponse(
            sql=sql,
            answer=answer,
            columns=columns if return_table else None,
            rows=rows if return_table else None,
        )

    except Exception as exc:
        logger.exception("Error while processing: %s", question)
        return QueryResponse(
            sql=None,
            answer="An error occurred while processing your question.",
            error=str(exc),
        )