"""Knowledge search preview endpoint logic."""
from __future__ import annotations
import json, re, sqlite3
from typing import Any
from knowledge.embeddings import cosine_similarity, deserialize_embedding

def knowledge_search_preview(conn: sqlite3.Connection, query: str, *, limit: int=5, embedder: Any=None) -> dict[str,Any]:
    q=(query or "").strip()
    if not q: raise ValueError("query must not be empty")
    if limit <= 0: raise ValueError("limit must be positive")
    rows=_load_knowledge_rows(conn)
    query_embedding=_query_embedding(embedder,q)
    results=[]
    for row in rows:
        metadata=_metadata(row.get("metadata"))
        haystack=" ".join(_clean(v) for v in (row.get("title"),row.get("source_id"),row.get("content"),row.get("insight"),metadata.get("title"),metadata.get("excerpt")) if _clean(v))
        score=None
        if query_embedding is not None and row.get("embedding"):
            try: score=cosine_similarity(query_embedding,deserialize_embedding(row["embedding"]))
            except Exception: score=None
        text_score=_text_score(q,haystack)
        rank_score=score if score is not None else text_score
        if rank_score <= 0 and text_score <= 0: continue
        results.append({"title":_title(row,metadata),"canonical_url":_canonical_url(row,metadata),"source_type":_clean(row.get("source_type")) or "unknown","recency_timestamp":_recency(row),"similarity_score":score,"excerpt":_excerpt(q,row,metadata),"_rank_score":rank_score})
    results.sort(key=lambda r:(r["_rank_score"],r.get("recency_timestamp") or ""),reverse=True)
    for result in results: result.pop("_rank_score",None)
    return {"query":q,"results":results[:limit]}

def knowledge_search_preview_endpoint(conn: sqlite3.Connection, query: str, *, limit: int=5, embedder: Any=None) -> dict[str,Any]:
    return knowledge_search_preview(conn,query,limit=limit,embedder=embedder)

def build_knowledge_search_preview(conn: sqlite3.Connection, query: str, *, limit: int=5, embedder: Any=None) -> dict[str,Any]:
    return knowledge_search_preview(conn,query,limit=limit,embedder=embedder)

def _load_knowledge_rows(conn):
    try:
        columns={r[1] for r in conn.execute("PRAGMA table_info(knowledge)").fetchall()}
    except sqlite3.Error:
        return []
    if not columns: return []
    wanted=("id","source_type","source_id","source_url","canonical_url","url","title","content","insight","embedding","published_at","ingested_at","created_at","updated_at","metadata","approved")
    select=[c for c in wanted if c in columns]
    if not select: return []
    where=" WHERE approved = 1" if "approved" in columns else ""
    conn.row_factory=sqlite3.Row
    return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM knowledge{where}").fetchall()]

def _query_embedding(embedder,query):
    if embedder is None or not hasattr(embedder,"embed"): return None
    try: return embedder.embed(query)
    except Exception: return None

def _text_score(query,text):
    terms=[t for t in re.findall(r"[a-z0-9]+",query.lower()) if t]
    words=re.findall(r"[a-z0-9]+",text.lower())
    if not terms or not words: return 0.0
    word_set=set(words)
    hits=sum(1 for t in terms if t in word_set)
    phrase=1 if query.lower() in text.lower() else 0
    return (hits/len(terms))+(0.25*phrase)

def _title(row,metadata):
    link=metadata.get("link_metadata") if isinstance(metadata.get("link_metadata"),dict) else {}
    return _clean(row.get("title") or metadata.get("title") or link.get("title") or row.get("source_id")) or "Untitled source"

def _canonical_url(row,metadata):
    link=metadata.get("link_metadata") if isinstance(metadata.get("link_metadata"),dict) else {}
    return _clean(row.get("canonical_url") or row.get("source_url") or row.get("url") or metadata.get("canonical_url") or link.get("canonical_url"))

def _recency(row):
    return _clean(row.get("published_at") or row.get("ingested_at") or row.get("updated_at") or row.get("created_at")) or None

def _excerpt(query,row,metadata):
    source=_clean(metadata.get("excerpt") or row.get("insight") or row.get("content"))
    if not source: return ""
    normalized=re.sub(r"\s+"," ",source).strip()
    terms=re.findall(r"[a-z0-9]+",query.lower())
    lower=normalized.lower()
    hit=min([lower.find(t) for t in terms if lower.find(t)>=0] or [0])
    start=max(hit-70,0); end=min(start+220,len(normalized))
    return normalized[start:end].strip()

def _metadata(value):
    if isinstance(value,dict): return value
    if not value: return {}
    try:
        parsed=json.loads(value)
    except (TypeError,json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed,dict) else {}

def _clean(value):
    return str(value or "").strip()
