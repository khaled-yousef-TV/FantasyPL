"""
FANTASYPL Multi-Agent System - Configuration Settings
Centralized configuration management for the FPL system
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration for FANTASYPL
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'fantasypl_data'),  # Updated for FANTASYPL
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD'),
    'port': int(os.getenv('DB_PORT', 5432))
}

# Redis configuration (optional caching)
REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'localhost'),
    'port': int(os.getenv('REDIS_PORT', 6379)),
    'db': int(os.getenv('REDIS_DB', 0)),
    'decode_responses': True
}

# FPL API settings
FPL_CONFIG = {
    'base_url': os.getenv('FPL_BASE_URL', 'https://fantasy.premierleague.com/api'),
    'rate_limit_seconds': float(os.getenv('RATE_LIMIT_SECONDS', 1.0)),
    'timeout_seconds': int(os.getenv('API_TIMEOUT', 30))
}

# Application settings
CURRENT_SEASON = os.getenv('CURRENT_SEASON', '2024-25')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'data/logs/fantasypl_agent.log')

# Historical seasons to analyze
HISTORICAL_SEASONS = [
    '2022-23',
    '2023-24', 
    '2024-25'
]

# Player position mapping
POSITIONS = {
    1: 'GK',   # Goalkeeper
    2: 'DEF',  # Defender  
    3: 'MID',  # Midfielder
    4: 'FWD'   # Forward
}

# Team strength categories (for analysis)
BIG_SIX_TEAMS = [1, 2, 3, 4, 5, 6]  # Arsenal, Chelsea, Liverpool, Man City, Man United, Tottenham

# Analysis settings
ANALYSIS_CONFIG = {
    'min_games_for_analysis': 3,
    'form_games_lookback': 5,
    'consistency_min_games': 5,
    'differential_max_ownership': 10.0,
    'captain_min_ownership': 5.0,
    'value_min_points': 20
}

# File paths (relative to project root)
PATHS = {
    'exports': 'data/exports/',
    'logs': 'data/logs/',
    'cache': 'data/cache/'
}

# Validation function
def validate_config():
    """Validate that all required configuration is present"""
    required_vars = ['DB_PASSWORD']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    return True

# Auto-validate when imported
if __name__ != "__main__":
    try:
        validate_config()
    except ValueError as e:
        print(f"⚠️  Configuration Warning: {e}")
        print("Please check your .env file")