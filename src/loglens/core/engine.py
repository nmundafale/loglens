import litellm
import logging

logger = logging.getLogger(__name__)

class AIEngine:
    """The central router that directs requests to the correct model using LiteLLM."""
    def __init__(self, config_dict: dict):
        self.config = config_dict

    async def call_model(self, tier: int, system_prompt: str, message: str) -> str:
        """Calls the configured AI tier using LiteLLM."""
        tiers_config = self.config.get("ai_tiers", {})
        if tier not in tiers_config:
            raise ValueError(f"Tier {tier} not found in configuration.")

        tier_info = tiers_config[tier]
        model = tier_info["model"]
        temperature = tier_info.get("temperature", 0.0)

        # Standard LiteLLM/OpenAI messages format
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message}
        ]

        logger.info(f"Routing request to LiteLLM Model: {model}")
        
        # Call Universal LiteLLM Adapter
        response = await litellm.acompletion(
            model=model,
            messages=messages,
            temperature=temperature
        )

        content = response.choices[0].message.content
        return content if content is not None else ""