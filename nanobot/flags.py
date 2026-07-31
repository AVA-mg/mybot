"""
Feature Flags for Nanobot Scalability

This module provides runtime feature flags that enable/disable scalability features
without requiring code redeployment. All flags default to False for safe rollouts.

Usage:
    from nanobot.flags import is_feature_enabled, FEATURE_FLAGS
    
    if is_feature_enabled("async_tools"):
        # Use async tool execution
        pass
    
    # Or check directly
    if FEATURE_FLAGS.get("parallel_tools"):
        # Enable parallel tool calls
        pass

To override flags at runtime, set environment variables:
    export NANOBOT_FLAG_ASYNC_TOOLS=true
    export NANOBOT_FLAG_PARALLEL_TOOLS=true
"""

import os
from typing import Any


# Master feature flag configuration
# All flags default to False for safe progressive rollouts
FEATURE_FLAGS: dict[str, bool] = {
    # Phase 1: Async Core + Tool Execution
    "async_tools": False,           # Enable async tool execution
    "parallel_tools": False,        # Enable parallel tool call execution
    "redis_sessions": False,        # Use Redis for session storage
    "tool_deduplication": False,    # Prevent duplicate tool calls in same turn
    "tool_timeout": False,          # Enforce timeouts on tool execution
    "streaming_progress": False,    # Stream tool execution progress to users
    
    # Phase 2: Distributed Systems
    "distributed_locks": False,     # Use Redis distributed locks
    "gateway_worker_split": False,  # Separate gateway and worker processes
    "rate_limiting": False,         # Enable rate limiting per user/session
    "circuit_breaker_tools": False, # Circuit breaker for failing tools
    
    # Phase 3: Provider Optimization
    "provider_pool": False,         # Connection pooling for LLM providers
    "background_dream": False,      # Background processing for dream/long tasks
    "llm_batching": False,          # Batch multiple LLM requests
    "prompt_caching": False,        # Cache system prompts and templates
    
    # Phase 4: Resilience & Monitoring
    "circuit_breakers": False,      # Global circuit breakers
    "health_checks": False,         # Enhanced health check endpoints
    "metrics_export": False,        # Export metrics to monitoring systems
    "structured_logging": False,    # JSON-formatted structured logging
    
    # Legacy/Compatibility flags
    "enable_asyncio_event_loop": False,  # Force asyncio event loop optimization
}


def is_feature_enabled(feature_name: str) -> bool:
    """
    Check if a feature flag is enabled.
    
    Priority order:
    1. Environment variable (NANOBOT_FLAG_<FEATURE_NAME>=true/false)
    2. Default value in FEATURE_FLAGS dict
    3. Returns False if feature doesn't exist
    
    Args:
        feature_name: Name of the feature to check
        
    Returns:
        bool: True if feature is enabled, False otherwise
    """
    # Normalize feature name
    normalized_name = feature_name.lower().replace("-", "_")
    
    # Check environment variable override
    env_var_name = f"NANOBOT_FLAG_{normalized_name.upper()}"
    env_value = os.getenv(env_var_name)
    
    if env_value is not None:
        return env_value.lower() in ("true", "1", "yes", "on")
    
    # Fall back to default value
    return FEATURE_FLAGS.get(normalized_name, False)


def get_feature_flag(feature_name: str, default: bool = False) -> bool:
    """
    Get feature flag with custom default.
    
    Args:
        feature_name: Name of the feature
        default: Default value if feature doesn't exist
        
    Returns:
        bool: Feature state
    """
    normalized_name = feature_name.lower().replace("-", "_")
    env_var_name = f"NANOBOT_FLAG_{normalized_name.upper()}"
    env_value = os.getenv(env_var_name)
    
    if env_value is not None:
        return env_value.lower() in ("true", "1", "yes", "on")
    
    return FEATURE_FLAGS.get(normalized_name, default)


def set_feature_flag(feature_name: str, enabled: bool) -> None:
    """
    Dynamically set a feature flag at runtime.
    
    Note: This only affects the current process. For persistent changes,
    use environment variables or update FEATURE_FLAGS dict.
    
    Args:
        feature_name: Name of the feature
        enabled: Whether to enable or disable the feature
    """
    normalized_name = feature_name.lower().replace("-", "_")
    FEATURE_FLAGS[normalized_name] = enabled


def get_all_flags() -> dict[str, bool]:
    """
    Get all feature flags with their current state.
    
    Returns:
        dict[str, bool]: All feature flags and their states
    """
    # Merge defaults with environment overrides
    result = FEATURE_FLAGS.copy()
    
    for feature_name in FEATURE_FLAGS.keys():
        env_var_name = f"NANOBOT_FLAG_{feature_name.upper()}"
        env_value = os.getenv(env_var_name)
        if env_value is not None:
            result[feature_name] = env_value.lower() in ("true", "1", "yes", "on")
    
    return result


def list_enabled_features() -> list[str]:
    """
    List all currently enabled features.
    
    Returns:
        list[str]: Names of enabled features
    """
    return [name for name, enabled in get_all_flags().items() if enabled]


def validate_feature_flags() -> list[str]:
    """
    Validate feature flag configuration and return warnings.
    
    Returns:
        list[str]: List of warning messages for invalid configurations
    """
    warnings = []
    
    # Check for dependency conflicts
    if FEATURE_FLAGS.get("parallel_tools") and not FEATURE_FLAGS.get("async_tools"):
        warnings.append(
            "parallel_tools enabled without async_tools - may cause blocking behavior"
        )
    
    if FEATURE_FLAGS.get("redis_sessions") and not FEATURE_FLAGS.get("distributed_locks"):
        warnings.append(
            "redis_sessions enabled without distributed_locks - consider enabling for consistency"
        )
    
    if FEATURE_FLAGS.get("circuit_breakers") and not FEATURE_FLAGS.get("metrics_export"):
        warnings.append(
            "circuit_breakers enabled without metrics_export - monitoring recommended"
        )
    
    return warnings
