from __future__ import annotations

from typing import Any, List, Optional

from pydantic import BaseModel


class QueryRequest(BaseModel):
    question: str
    return_table: bool = True


class QueryResponse(BaseModel):
    sql: Optional[str] = None
    answer: str
    error: Optional[str] = None

    # Tabular result (optional)
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
