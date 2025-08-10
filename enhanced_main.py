#!/usr/bin/env python3
"""
FANTASYPL - Fixture Agent Integration with Data Buff Agent
Enhanced main execution with fixture analysis
"""

import sys
import os
import schedule
import time
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import agents and configuration
from agents.data_buff import EnhancedDataBuffAgent
from agents.fixture_agent import FixtureAgent, get_team_fixture_favorability
from config.settings import DATABASE_CONFIG, REDIS_CONFIG

def enhanced_main():
    """Enhanced main execution with both Data Buff and Fixture Agents"""
    print("🏆 FANTASYPL Multi-Agent System Starting...")
    print("🤖 Data Buff Agent + Fixture Agent")
    print("=" * 60)
    
    try:
        # Initialize both agents
        print("🔧 Initializing agents...")
        data_agent = EnhancedDataBuffAgent(DATABASE_CONFIG, REDIS_CONFIG)
        fixture_agent = FixtureAgent(DATABASE_CONFIG, REDIS_CONFIG)
        
        data_agent.initialize()
        fixture_agent.initialize()
        
        print("✅ Both agents initialized successfully!")
        print("📊 Data Buff Agent: Player analysis & recommendations")
        print("📅 Fixture Agent: Advanced fixture analysis")
        
        # Schedule daily updates for both agents
        schedule.every().day.at("02:00").do(data_agent.enhanced_daily_update)
        schedule.every().day.at("02:30").do(fixture_agent.daily_update)
        print("⏰ Daily updates scheduled: Data at 2:00 AM, Fixtures at 2:30 AM")
        
        # Run initial updates
        print("\n🔄 Running initial system updates...")
        print("⚠️  This may take 10-15 minutes for first run...")
        
        # 1. Data Agent Update
        print("\n📊 Data Buff Agent - Updating player data...")
        data_agent.enhanced_daily_update()
        print("✅ Player data update completed!")
        
        # 2. Fixture Agent Update
        print("\n📅 Fixture Agent - Analyzing fixtures...")
        fixture_agent.daily_update()
        print("✅ Fixture analysis completed!")
        
        # Generate enhanced recommendations
        print("\n🎯 Generating Enhanced Recommendations...")
        generate_enhanced_recommendations(data_agent, fixture_agent)
        
        # Export combined analysis
        print("\n💾 Exporting combined analysis...")
        data_agent.export_recommendations_to_json("data/exports/player_recommendations.json")
        fixture_agent.export_fixture_analysis_to_json("data/exports/fixture_analysis.json")
        
        print("✅ All exports completed!")
        
        print(f"\n🔄 FANTASYPL Multi-Agent System is now running...")
        print("📅 Automatic daily updates scheduled")
        print("📊 Check data/exports/ for latest analysis")
        print("📝 Check data/logs/ for system logs")
        print("\n⌨️  Press Ctrl+C to stop the system")
        print("=" * 60)
        
        # Keep running and check for scheduled tasks
        while True:
            schedule.run_pending()
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\n👋 FANTASYPL Multi-Agent System stopped by user")
        print("Thank you for using the system!")
    except Exception as e:
        print(f"❌ Error in FANTASYPL system: {e}")
        print("Check the logs in data/logs/ for more details")
        raise

def generate_enhanced_recommendations(data_agent, fixture_agent):
    """Generate enhanced recommendations combining both agents"""
    
    try:
        print("🔍 Analyzing player recommendations with fixture data...")
        
        # Get player recommendations from Data Buff Agent
        player_recs = data_agent.generate_player_recommendations(gameweeks_ahead=5)
        
        # Get fixture analysis for teams
        best_fixture_teams = fixture_agent.get_best_fixture_teams(gameweeks_ahead=4)
        transfer_timing = fixture_agent.get_transfer_timing_recommendations()
        
        # Enhanced analysis
        print(f"\n📈 Enhanced Player Analysis:")
        print("-" * 40)
        
        if player_recs:
            # Group recommendations by position
            positions = {}
            for rec in player_recs[:20]:  # Top 20
                pos = rec.position
                if pos not in positions:
                    positions[pos] = []
                
                # Get fixture data for player's team
                fixture_data = get_team_fixture_favorability(
                    fixture_agent, 
                    get_team_id_by_name(data_agent, rec.team)
                )
                
                enhanced_rec = {
                    'name': rec.name,
                    'team': rec.team,
                    'price': rec.price,
                    'predicted_points': rec.predicted_points,
                    'confidence': rec.confidence_score,
                    'fixture_favorability': fixture_data['favorability_score'],
                    'fixture_difficulty': fixture_data['average_difficulty'],
                    'combined_score': calculate_combined_score(rec, fixture_data)
                }
                
                positions[pos].append(enhanced_rec)
            
            # Display top recommendations by position
            for pos in ['GK', 'DEF', 'MID', 'FWD']:
                if pos in positions:
                    print(f"\n🎯 Top {pos} Recommendations:")
                    # Sort by combined score
                    positions[pos].sort(key=lambda x: x['combined_score'], reverse=True)
                    
                    for i, rec in enumerate(positions[pos][:3], 1):
                        print(f"{i}. {rec['name']} ({rec['team']}) - £{rec['price']}m")
                        print(f"   📊 Expected: {rec['predicted_points']} pts | Confidence: {rec['confidence']}%")
                        print(f"   📅 Fixtures: {rec['fixture_favorability']:.1f}/100 | Difficulty: {rec['fixture_difficulty']}")
                        print(f"   🔥 Combined Score: {rec['combined_score']:.1f}")
                        print()
        
        # Fixture-based recommendations
        print(f"📅 Best Fixture Teams (Next 4 Gameweeks):")
        print("-" * 45)
        for i, team in enumerate(best_fixture_teams[:5], 1):
            print(f"{i}. {team['team_name']}")
            print(f"   📊 {team['fixture_count']} fixtures | Avg Difficulty: {team['average_difficulty']}")
            print(f"   🏠 {team['home_fixtures']} home | 🎯 {team['easy_fixtures']} easy")
            print(f"   📈 Fixture Score: {team['fixture_score']:.1f}/100")
            print(f"   💡 {team['recommendation']}")
            print()
        
        # Transfer timing recommendations
        print(f"⏰ Transfer Timing Recommendations:")
        print("-" * 35)
        for i, rec in enumerate(transfer_timing[:5], 1):
            print(f"{i}. {rec['team_name']} - Transfer before GW{rec['recommended_transfer_gameweek']}")
            print(f"   📊 {rec['easy_fixtures']} easy fixtures | Avg: {rec['average_difficulty']}")
            print(f"   💡 {rec['reasoning']}")
            print()
        
        # Captain recommendations with fixtures
        print(f"⭐ Enhanced Captain Recommendations:")
        print("-" * 38)
        captains = data_agent.analyze_captain_options()
        
        for i, cap in enumerate(captains[:3], 1):
            team_id = get_team_id_by_name(data_agent, cap['team'])
            fixture_data = get_team_fixture_favorability(fixture_agent, team_id, 1)
            
            print(f"{i}. {cap['name']} ({cap['team']})")
            print(f"   🏆 Captain Score: {cap['captain_score']}")
            print(f"   📈 Expected Points: {cap['expected_points']}")
            print(f"   📅 Next Fixture: {fixture_data['average_difficulty']}/5 difficulty")
            print(f"   🛡️  Safety: {cap['safety_level']}")
            print()
        
    except Exception as e:
        print(f"⚠️  Error generating enhanced recommendations: {e}")
        print("Individual agent recommendations still available in exports.")

def calculate_combined_score(player_rec, fixture_data):
    """Calculate combined score from player and fixture data"""
    
    # Weight: 60% player data, 40% fixture data
    player_score = (player_rec.predicted_points * 10) + (player_rec.confidence_score * 0.5)
    fixture_score = fixture_data['favorability_score']
    
    combined = (player_score * 0.6) + (fixture_score * 0.4)
    
    return round(combined, 1)

def get_team_id_by_name(data_agent, team_name):
    """Get team ID from team name"""
    query = "SELECT id FROM teams WHERE name = %s"
    result = data_agent.db.execute_query(query, (team_name,))
    return result[0]['id'] if result else 1

def test_integration():
    """Test the integrated system"""
    print("🧪 Testing FANTASYPL Multi-Agent Integration...")
    
    try:
        # Test both agents
        data_agent = EnhancedDataBuffAgent(DATABASE_CONFIG, {})
        fixture_agent = FixtureAgent(DATABASE_CONFIG, {})
        
        data_agent.initialize()
        fixture_agent.initialize()
        
        print("✅ Both agents initialized successfully!")
        
        # Test integration function
        fixture_data = get_team_fixture_favorability(fixture_agent, 1, 3)
        print(f"✅ Fixture favorability test successful!")
        print(f"   Team 1 fixture score: {fixture_data['favorability_score']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_integration()
    else:
        enhanced_main()