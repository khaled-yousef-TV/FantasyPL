#!/usr/bin/env python3
"""
Simple test file for FANTASYPL Fixture Agent
Place this in your root directory and run: python test_fixture.py
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_fixture_agent():
    """Test the fixture agent functionality"""
    print("🧪 Testing FANTASYPL Fixture Agent...")
    
    try:
        # Import after setting path
        from agents.fixture_agent import FixtureAgent
        from config.settings import DATABASE_CONFIG
        
        print("✅ Imports successful!")
        
        # Test initialization
        agent = FixtureAgent(DATABASE_CONFIG, {})
        agent.initialize()
        print("✅ Fixture Agent initialization successful!")
        
        # Test fixture fetching
        print("📡 Fetching fixture data...")
        agent.fetch_and_store_fixtures()
        print("✅ Fixture data fetching successful!")
        
        # Test analysis
        print("🔍 Analyzing upcoming fixtures...")
        analyses = agent.analyze_upcoming_fixtures(4)
        print(f"✅ Fixture analysis successful! Analyzed {len(analyses)} fixtures")
        
        # Test fixture runs
        print("📊 Analyzing fixture runs...")
        runs = agent.analyze_fixture_runs(4)
        print(f"✅ Fixture runs analysis successful! Analyzed {len(runs)} teams")
        
        # Test best teams
        print("🎯 Finding best fixture teams...")
        best_teams = agent.get_best_fixture_teams(4)
        print(f"✅ Best fixture teams analysis successful! Found {len(best_teams)} recommendations")
        
        # Show sample results
        if best_teams:
            print(f"\n🏆 Top 3 Teams with Best Fixtures:")
            for i, team in enumerate(best_teams[:3], 1):
                print(f"{i}. {team['team_name']} - Score: {team['fixture_score']:.1f}/100")
                print(f"   {team['fixture_count']} fixtures, avg difficulty: {team['average_difficulty']}")
        
        # Test export
        print("\n💾 Testing export...")
        export_file = agent.export_fixture_analysis_to_json()
        if export_file:
            print(f"✅ Export successful: {export_file}")
        
        print(f"\n🎉 All Fixture Agent tests passed!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you've created src/agents/fixture_agent.py with the fixture agent code")
        return False
    except Exception as e:
        print(f"❌ Fixture Agent test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_fixture_agent()