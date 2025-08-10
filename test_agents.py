# Test script: test_agents.py
import sys
sys.path.append('src')

from agents.data_buff import EnhancedDataBuffAgent
from agents.fixture_agent import FixtureAgent
from agents.news_agent import NewsAgent
from config.settings import DATABASE_CONFIG, REDIS_CONFIG

print("Testing Data Buff Agent...")
data_agent = EnhancedDataBuffAgent(DATABASE_CONFIG, REDIS_CONFIG)
data_agent.initialize()
print("✅ Data Buff Agent OK")

print("\nTesting Fixture Agent...")
fixture_agent = FixtureAgent(DATABASE_CONFIG, REDIS_CONFIG)
fixture_agent.initialize()
print("✅ Fixture Agent OK")

print("\nTesting News Agent...")
news_agent = NewsAgent(DATABASE_CONFIG, REDIS_CONFIG)
news_agent.initialize()
print("✅ News Agent OK")

print("\n✅ All agents initialized successfully!")