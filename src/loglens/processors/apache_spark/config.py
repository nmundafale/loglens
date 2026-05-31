from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
from typing import Any

class Settings(BaseSettings):
    """Application settings, loaded from environment variables."""
    gemini_api_key: str | None = Field(default=None, alias="GEMINI_API_KEY")
    ollama_host: str = Field(default="http://localhost:11434", alias="OLLAMA_HOST")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    github_app_id: str | None = Field(default=None, alias="GITHUB_APP_ID")
    github_app_private_key: str | None = Field(default=None, alias="GITHUB_APP_PRIVATE_KEY")
    github_app_private_key_path: str | None = Field(default=None, alias="GITHUB_APP_PRIVATE_KEY_PATH")
    github_installation_id: str | None = Field(default=None, alias="GITHUB_INSTALLATION_ID")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()

# Load environment variables
load_dotenv()

# Centralized Application Configuration
APP_CONFIG: dict[str, Any] = {
    # System Prompts
    "prompts": {
        "diagnosis_map": "Extract a strict JSON array of Spark errors, warnings, and JVM metrics. No filler text.",
        "diagnosis_reduce": "You are a Senior Data Engineer. Analyze this distilled Spark telemetry. Identify the root cause. Return a strictly valid JSON object with EXACTLY two keys: 'explanation' (Markdown explanation of the root cause) and 'fixed_code' (The complete, raw, compilable fixed Python source code). Provide the exact code or config to fix the issue based on the user's intent: {intent}",
        "utilization_map": "Extract a strict JSON array containing all executor memory allocations, task runtimes, partition counts, hardware capacities, and data skew indicators. Ignore normal errors. No filler text.",
        "utilization_reduce": "You are a Senior Data Engineering Architect. Analyze this distilled Spark cluster utilization telemetry. Identify if the cluster is under-utilized, over-provisioned, or if there is data skew slowing down tasks. Provide exact Spark configuration changes to optimize resource usage based on the user's intent: {intent}"
    },
    
    # Text Processing Limits
    "processing": {
        "target_chunk_chars": 5000, # Approx 3,500 tokens
    },
    
    # Task Routing
    "loglens_tasks": {
        "map_phase": 1,    # Route extraction to the local/cheap tier
        "reduce_phase": 1  # Using Ollama for testing
    },
    
    # Provider Tiers (LiteLLM Format)
    "ai_tiers": {
        1: {
            "model": "ollama/qwen3:8b", # Uses local ollama natively via litellm
            "max_concurrent": 1,
            "temperature": 0.1
        },
        10: {
            "model": "gemini/gemini-2.5-flash",
            "max_concurrent": 10,
            "temperature": 0.4
        }
    }
}