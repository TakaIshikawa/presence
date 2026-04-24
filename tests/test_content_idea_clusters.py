"""Tests for content idea clustering reports."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from content_idea_clusters import format_json_report, format_text_report
from evaluation.content_idea_clusters import cluster_content_ideas, clusters_to_dicts


def test_clusters_open_ideas_by_topic_and_lexical_similarity(db):
    first = db.add_content_idea(
        "Turn flaky validation failures into a lesson about test feedback loops",
        topic="testing",
        priority="high",
        source="manual",
    )
    second = db.add_content_idea(
        "Explain how validation feedback loops changed the implementation",
        topic="Testing",
        priority="normal",
        source="scratchpad",
    )
    third = db.add_content_idea(
        "Capture publish queue retry failure diagnostics for operations review",
        topic="operations",
        priority="low",
        source="manual",
    )
    fourth = db.add_content_idea(
        "Retry failure diagnostics from the publish queue as an incident story",
        topic=None,
        priority="normal",
        source="manual",
    )

    clusters = cluster_content_ideas(db.get_content_ideas(status="open"))
    payload = clusters_to_dicts(clusters)

    assert payload == [
        {
            "label": "operations",
            "idea_ids": [third, fourth],
            "representative_note": (
                "Retry failure diagnostics from the publish queue as an incident story"
            ),
            "shared_terms": ["diagnostics", "failure", "publish", "queue", "retry"],
            "sources": ["manual"],
            "priority_mix": {"normal": 1, "low": 1},
        },
        {
            "label": "testing",
            "idea_ids": [first, second],
            "representative_note": (
                "Turn flaky validation failures into a lesson about test feedback loops"
            ),
            "shared_terms": ["feedback", "loops", "testing", "validation"],
            "sources": ["manual", "scratchpad"],
            "priority_mix": {"high": 1, "normal": 1},
        },
    ]


def test_min_cluster_size_hides_singletons(db):
    db.add_content_idea("One standalone content seed", topic="solo")
    first = db.add_content_idea("Agent evaluation loop notes", topic="evaluation")
    second = db.add_content_idea("Evaluation loop follow up", topic="evaluation")

    clusters = cluster_content_ideas(
        db.get_content_ideas(status="open"),
        min_cluster_size=2,
    )

    assert [cluster.idea_ids for cluster in clusters] == [[first, second]]


def test_json_output_is_deterministic(db):
    first = db.add_content_idea(
        "Source metadata release note",
        source="github",
        source_metadata={"source": "github_release_seed", "release_id": 42},
    )
    second = db.add_content_idea(
        "Different wording for the same release",
        source="github",
        source_metadata={"release_id": 42, "source": "github_release_seed"},
    )

    ideas = list(reversed(db.get_content_ideas(status="open")))
    first_output = format_json_report(cluster_content_ideas(ideas))
    second_output = format_json_report(cluster_content_ideas(list(reversed(ideas))))

    assert first_output == second_output
    payload = json.loads(first_output)
    assert payload["clusters"][0]["idea_ids"] == [first, second]
    assert payload["clusters"][0]["label"] == "github"


def test_text_output_includes_representative_note_and_ids(db):
    first = db.add_content_idea(
        "Compare hand rolled heuristics with embedding backed clustering",
        topic="clustering",
    )
    second = db.add_content_idea(
        "Embedding backed clustering should still have deterministic heuristics",
        topic="clustering",
    )

    text = format_text_report(cluster_content_ideas(db.get_content_ideas(status="open")))

    assert f"Idea IDs: {first}, {second}" in text
    assert "Representative note: Compare hand rolled heuristics" in text
    assert "Shared terms:" in text


def test_optional_embeddings_can_link_sparse_ideas(db):
    first = {"id": 1, "note": "Alpha beta", "topic": None, "priority": "normal"}
    second = {"id": 2, "note": "Gamma delta", "topic": None, "priority": "normal"}

    clusters = cluster_content_ideas(
        [first, second],
        min_cluster_size=2,
        embeddings={1: [1.0, 0.0], 2: [0.98, 0.02]},
    )

    assert len(clusters) == 1
    assert clusters[0].idea_ids == [1, 2]


def test_min_cluster_size_validation():
    with pytest.raises(ValueError, match="min_cluster_size"):
        cluster_content_ideas([], min_cluster_size=0)
