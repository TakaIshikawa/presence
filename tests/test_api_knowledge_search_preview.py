from __future__ import annotations
import json, sqlite3
import pytest
from api.knowledge_search_preview import knowledge_search_preview
from knowledge.embeddings import serialize_embedding

class Embedder:
    def __init__(self, vector=None, error=False):
        self.vector=vector or [1.0,0.0]; self.error=error
    def embed(self, _text):
        if self.error: raise RuntimeError("missing provider")
        return self.vector

def _conn(with_embeddings=True):
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row
    c.execute("""CREATE TABLE knowledge (id INTEGER PRIMARY KEY, source_type TEXT, source_id TEXT, source_url TEXT, title TEXT, content TEXT, insight TEXT, embedding BLOB, approved INTEGER, published_at TEXT, metadata TEXT)""")
    c.execute("INSERT INTO knowledge (source_type,source_id,source_url,title,content,insight,embedding,approved,published_at,metadata) VALUES (?,?,?,?,?,?,?,?,?,?)",("curated_article","a","https://example.com/a","Vector Operations","Embeddings improve retrieval precision.","Semantic ranking notes.",serialize_embedding([1.0,0.0]) if with_embeddings else None,1,"2026-05-01T00:00:00+00:00",json.dumps({"link_metadata":{"canonical_url":"https://canonical.example/a","title":"Canonical Vector"}})))
    c.execute("INSERT INTO knowledge (source_type,source_id,source_url,title,content,insight,embedding,approved,published_at,metadata) VALUES (?,?,?,?,?,?,?,?,?,?)",("blog","b","https://example.com/b","Queue Fallback","Simple text matching keeps preview useful without embeddings.","Fallback notes.",serialize_embedding([0.0,1.0]) if with_embeddings else None,1,"2026-05-02T00:00:00+00:00","{}"))
    c.commit(); return c

def test_preview_returns_semantic_score_shaped_rows():
    payload=knowledge_search_preview(_conn(), "retrieval", embedder=Embedder([1.0,0.0]))
    assert payload["results"][0]["title"]=="Vector Operations"
    assert payload["results"][0]["canonical_url"]=="https://example.com/a"
    assert payload["results"][0]["source_type"]=="curated_article"
    assert payload["results"][0]["recency_timestamp"]=="2026-05-01T00:00:00+00:00"
    assert payload["results"][0]["similarity_score"]==pytest.approx(1.0)
    assert "Semantic ranking" in payload["results"][0]["excerpt"]

def test_preview_falls_back_to_text_when_embeddings_missing_or_provider_fails():
    missing=knowledge_search_preview(_conn(with_embeddings=False), "text matching", embedder=Embedder(error=True))
    assert missing["results"][0]["title"]=="Queue Fallback"
    assert missing["results"][0]["similarity_score"] is None

def test_empty_query_validation_and_no_results():
    with pytest.raises(ValueError): knowledge_search_preview(_conn(), " ")
    assert knowledge_search_preview(_conn(), "unmatched phrase", embedder=None)["results"]==[]
