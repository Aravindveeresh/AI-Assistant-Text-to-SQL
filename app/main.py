"""FastAPI app for Text-to-SQL."""

from __future__ import annotations

import logging

from fastapi import FastAPI
from typing import Any, Dict, List
from app.model import QueryRequest, QueryResponse
from app.query_engine import process_question
import uvicorn

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Text-to-SQL Assistant (Azure OpenAI)",
    description="Ask business questions. Get SQL + answers + tables.",
    version="1.0.0",
)


@app.post("/ask", response_model=QueryResponse)
async def ask_question(req: QueryRequest) -> QueryResponse:
    return process_question(req.question, return_table=req.return_table)

@app.post("/ask/records")
async def ask_question_records(req: QueryRequest) -> Dict[str, Any]:
    resp = process_question(req.question, return_table=True)
    records: List[Dict[str, Any]] = []
    if resp.columns and resp.rows:
        cols = resp.columns
        for r in resp.rows:
            records.append({cols[i]: r[i] for i in range(len(cols))})
    return {
        "sql": resp.sql,
        "answer": resp.answer,
        "error": resp.error,
        "count": len(records),
        "records": records,
    }


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)

