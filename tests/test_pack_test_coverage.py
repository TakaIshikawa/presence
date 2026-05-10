"""Tests for pack test coverage and assertion quality analyzer."""

import pytest

from synthesis.pack_test_coverage import analyze_pack_test_coverage


# --- Helpers ---


def _make_record(test_content: str, source_file_count: int = 1) -> dict:
    return {
        "session_id": "s1",
        "test_files": [{"path": "tests/test_example.py", "content": test_content}],
        "source_file_count": source_file_count,
    }


# --- Input validation ---


def test_none_input_returns_empty_result():
    result = analyze_pack_test_coverage(None)

    assert result["total_sessions"] == 0
    assert result["test_density"] == 0.0
    assert result["assertion_quality"] == 0.0
    assert result["test_isolation"] == 1.0


def test_empty_list_returns_empty_result():
    result = analyze_pack_test_coverage([])

    assert result["total_sessions"] == 0
    assert result["total_test_functions"] == 0


def test_non_list_input_raises():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_test_coverage({"test_files": []})


def test_non_mapping_records_are_skipped():
    result = analyze_pack_test_coverage(["not_a_dict", 42, None])

    assert result["total_sessions"] == 0


# --- Test function counting ---


def test_counts_test_functions():
    content = """\
def test_alpha():
    assert True

def test_beta():
    assert True

def helper_not_a_test():
    pass
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["total_test_functions"] == 2


def test_counts_zero_when_no_test_functions():
    content = """\
def helper():
    pass

def another_helper():
    pass
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["total_test_functions"] == 0


def test_counts_indented_test_functions_in_classes():
    content = """\
class TestSuite:
    def test_one(self):
        assert True

    def test_two(self):
        assert True
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["total_test_functions"] == 2


# --- Assertion counting ---


def test_assertions_per_test_calculated():
    content = """\
def test_alpha():
    assert 1 == 1
    assert 2 == 2
    assert 3 == 3

def test_beta():
    assert True
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    # alpha: 3, beta: 1 -> mean = 2.0
    assert result["assertions_per_test"] == 2.0


def test_assertions_per_test_zero_when_no_tests():
    content = "# no tests here\n"
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["assertions_per_test"] == 0.0


def test_pytest_raises_counts_as_assertion():
    content = """\
def test_raises_error():
    with pytest.raises(ValueError):
        raise ValueError("oops")
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["assertions_per_test"] == 1.0
    assert result["total_pytest_raises"] == 1


def test_mixed_assertions_and_raises():
    content = """\
def test_mixed():
    assert something == True
    with pytest.raises(KeyError):
        do_thing()
    assert other == False
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    # 2 asserts + 1 raises = 3
    assert result["assertions_per_test"] == 3.0


# --- Test density score ---


def test_density_score_full_with_five_tests_per_source():
    content = """\
def test_a():
    assert True
def test_b():
    assert True
def test_c():
    assert True
def test_d():
    assert True
def test_e():
    assert True
"""
    result = analyze_pack_test_coverage([_make_record(content, source_file_count=1)])

    assert result["test_density"] == 1.0


def test_density_score_partial():
    content = """\
def test_a():
    assert True
def test_b():
    assert True
"""
    result = analyze_pack_test_coverage([_make_record(content, source_file_count=1)])

    # 2/5 = 0.4
    assert result["test_density"] == 0.4


def test_density_score_capped_at_one():
    content = "\n".join(
        f"def test_{i}():\n    assert True\n" for i in range(10)
    )
    result = analyze_pack_test_coverage([_make_record(content, source_file_count=1)])

    assert result["test_density"] == 1.0


def test_density_zero_no_source_files_no_tests():
    result = analyze_pack_test_coverage([{
        "session_id": "s1",
        "test_files": [],
        "source_file_count": 0,
    }])

    assert result["test_density"] == 0.0


# --- Assertion quality score ---


def test_assertion_quality_full_with_three_or_more():
    content = """\
def test_full():
    assert a == 1
    assert b == 2
    assert c == 3
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["assertion_quality"] == 1.0


def test_assertion_quality_partial():
    content = """\
def test_one_assert():
    assert True
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    # 1/3 = 0.333
    assert result["assertion_quality"] == pytest.approx(0.333, abs=0.01)


def test_assertion_quality_zero_for_no_assertions():
    content = """\
def test_no_assertions():
    pass
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["assertion_quality"] == 0.0


# --- Edge case coverage ---


def test_edge_case_detected_by_name_keywords():
    content = """\
def test_empty_input():
    assert True

def test_none_value():
    assert True

def test_invalid_data():
    assert True

def test_normal_case():
    assert True
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["edge_case_tests_count"] == 3


def test_edge_case_keywords_comprehensive():
    keywords_tests = [
        "test_zero_length",
        "test_negative_value",
        "test_boundary_check",
        "test_error_handling",
        "test_missing_field",
        "test_raises_exception",
        "test_fail_on_bad_input",
        "test_overflow_protection",
        "test_limit_exceeded",
        "test_max_value",
        "test_min_value",
        "test_default_behavior",
        "test_no_items",
        "test_without_config",
        "test_malformed_input",
    ]
    content = "\n".join(
        f"def {name}():\n    assert True\n" for name in keywords_tests
    )
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["edge_case_tests_count"] == len(keywords_tests)


def test_edge_case_score_full_at_thirty_percent():
    # 3 edge cases out of 10 tests = 30% -> score 1.0
    edge_tests = [f"def test_empty_{i}():\n    assert True\n" for i in range(3)]
    normal_tests = [f"def test_normal_{i}():\n    assert True\n" for i in range(7)]
    content = "\n".join(edge_tests + normal_tests)

    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["edge_case_coverage"] == 1.0


def test_edge_case_score_partial():
    # 1 edge case out of 10 tests = 10% -> 10/30 = 0.333
    edge_tests = ["def test_empty():\n    assert True\n"]
    normal_tests = [f"def test_normal_{i}():\n    assert True\n" for i in range(9)]
    content = "\n".join(edge_tests + normal_tests)

    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["edge_case_coverage"] == pytest.approx(0.333, abs=0.01)


def test_edge_case_score_zero_no_tests():
    content = "# empty\n"
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["edge_case_coverage"] == 0.0


# --- Fixture usage ---


def test_fixture_count():
    content = """\
import pytest

@pytest.fixture
def sample_data():
    return [1, 2, 3]

@pytest.fixture
def mock_client():
    return MockClient()

def test_uses_fixture(sample_data):
    assert len(sample_data) == 3
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["fixture_usage_count"] == 2


def test_no_fixtures():
    content = """\
def test_standalone():
    assert True
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["fixture_usage_count"] == 0


# --- Test isolation ---


def test_isolation_violations_global_statement():
    content = """\
counter = 0

def test_uses_global():
    global counter
    counter += 1
    assert counter == 1
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["test_isolation_violations"] >= 1
    assert result["test_isolation"] < 1.0


def test_isolation_violations_order_dependency():
    content = """\
import pytest

@pytest.mark.order(1)
def test_first():
    assert True

@pytest.mark.order(2)
def test_second():
    assert True
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["test_isolation_violations"] >= 1
    assert result["test_isolation"] < 1.0


def test_isolation_perfect_no_violations():
    content = """\
def test_independent_a():
    data = [1, 2, 3]
    assert len(data) == 3

def test_independent_b():
    data = {"key": "value"}
    assert data["key"] == "value"
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["test_isolation_violations"] == 0
    assert result["test_isolation"] == 1.0


def test_isolation_score_floors_at_zero():
    # Many violations relative to test count
    content = """\
def test_one():
    global a
    global b
    global c
    assert True
"""
    result = analyze_pack_test_coverage([_make_record(content)])

    assert result["test_isolation"] >= 0.0


# --- Multi-session aggregation ---


def test_multi_session_aggregates():
    records = [
        {
            "session_id": "s1",
            "test_files": [
                {"path": "tests/test_a.py", "content": "def test_a():\n    assert True\n"}
            ],
            "source_file_count": 1,
        },
        {
            "session_id": "s2",
            "test_files": [
                {"path": "tests/test_b.py", "content": "def test_b():\n    assert True\n    assert True\n"}
            ],
            "source_file_count": 1,
        },
    ]
    result = analyze_pack_test_coverage(records)

    assert result["total_sessions"] == 2
    assert result["total_test_functions"] == 2
    assert result["total_source_files"] == 2
    assert result["tests_per_source_file"] == 1.0


def test_multi_test_files_per_session():
    records = [
        {
            "session_id": "s1",
            "test_files": [
                {"path": "tests/test_a.py", "content": "def test_a():\n    assert True\n"},
                {"path": "tests/test_b.py", "content": "def test_b():\n    assert True\n"},
            ],
            "source_file_count": 2,
        },
    ]
    result = analyze_pack_test_coverage(records)

    assert result["total_test_functions"] == 2


# --- Edge cases ---


def test_empty_test_file_content():
    result = analyze_pack_test_coverage([_make_record("")])

    assert result["total_test_functions"] == 0
    assert result["total_assertions"] == 0


def test_test_files_not_a_list():
    records = [{"session_id": "s1", "test_files": "not_a_list", "source_file_count": 1}]
    result = analyze_pack_test_coverage(records)

    assert result["total_sessions"] == 1
    assert result["total_test_functions"] == 0


def test_test_file_entry_not_mapping():
    records = [{"session_id": "s1", "test_files": ["not_a_dict"], "source_file_count": 1}]
    result = analyze_pack_test_coverage(records)

    assert result["total_test_functions"] == 0


def test_test_file_content_not_string():
    records = [
        {
            "session_id": "s1",
            "test_files": [{"path": "tests/test_x.py", "content": 123}],
            "source_file_count": 1,
        }
    ]
    result = analyze_pack_test_coverage(records)

    assert result["total_test_functions"] == 0


def test_source_file_count_none_treated_as_zero():
    records = [
        {
            "session_id": "s1",
            "test_files": [{"path": "tests/test_x.py", "content": "def test_x():\n    assert True\n"}],
            "source_file_count": None,
        }
    ]
    result = analyze_pack_test_coverage(records)

    assert result["total_source_files"] == 0
    assert result["tests_per_source_file"] == 0.0


def test_boolean_source_count_treated_as_zero():
    records = [
        {
            "session_id": "s1",
            "test_files": [],
            "source_file_count": True,
        }
    ]
    result = analyze_pack_test_coverage(records)

    assert result["total_source_files"] == 0


# --- Score boundaries ---


def test_all_scores_between_zero_and_one():
    content = """\
def test_empty_input():
    assert True

def test_normal():
    assert True
    assert True
"""
    result = analyze_pack_test_coverage([_make_record(content, source_file_count=3)])

    for key in ("test_density", "assertion_quality", "edge_case_coverage", "test_isolation"):
        assert 0.0 <= result[key] <= 1.0, f"{key} out of range: {result[key]}"


def test_result_keys_present():
    result = analyze_pack_test_coverage([])

    expected_keys = {
        "total_sessions", "total_test_functions", "total_source_files",
        "tests_per_source_file", "assertions_per_test", "total_assertions",
        "total_pytest_raises", "edge_case_tests_count", "fixture_usage_count",
        "test_isolation_violations", "test_density", "assertion_quality",
        "edge_case_coverage", "test_isolation",
    }
    assert set(result.keys()) == expected_keys
