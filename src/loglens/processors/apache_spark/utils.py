from loglens.processors.apache_spark.config import APP_CONFIG
from loglens.core.engine import AIEngine

# Initialize the core engine once
engine = AIEngine(APP_CONFIG)

def chunk_log_by_lines(log_text: str, target_chunk_chars: int) -> list:
    """Safely chunks logs without cutting lines in half."""
    lines = log_text.splitlines(keepends=True)
    chunks, current_chunk = [], ""
    
    for line in lines:
        if len(current_chunk) + len(line) > target_chunk_chars and current_chunk:
            chunks.append(current_chunk)
            current_chunk = line
        else:
            current_chunk += line
            
    if current_chunk: 
        chunks.append(current_chunk)
    return chunks
