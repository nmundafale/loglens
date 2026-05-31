from loglens.processors.apache_spark.utils import chunk_log_by_lines

def test_chunk_log_by_lines():
    """Test standard chunking logic."""
    text = "line1\nline2\nline3\n"
    chunks = chunk_log_by_lines(text, target_chunk_chars=12)
    assert chunks == ["line1\nline2\n", "line3\n"]
