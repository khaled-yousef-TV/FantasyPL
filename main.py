#!/usr/bin/env python3
"""
FANTASYPL Multi-Agent System - Main Execution File
FPL Data Buff Agent with Enhanced Analytics
"""

import sys
import os
import schedule
import time
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import our agent and configuration
from agents.data_buff import EnhancedDataBuffAgent
from config.settings import DATABASE_CONFIG, REDIS_CONFIG

def main():
    """Main execution function for FANTASYPL system"""
    print("🏆 FANTASYPL Data Buff Agent Starting...")
    print("=" * 50)
    
    try:
        # Initialize agent
        print("🔧 Initializing Enhanced Data Buff Agent...")
        agent = EnhancedDataBuffAgent(DATABASE_CONFIG, REDIS_CONFIG)
        agent.initialize()
        
        print("✅ Agent initialized successfully!")
        print("📊 Database tables created/verified")
        
        # Schedule daily updates at 2 AM
        schedule.every().day.at("02:00").do(agent.enhanced_daily_update)
        print("⏰ Daily updates scheduled for 2:00 AM")
        
        # Check if we should run initial update
        print("\n🔄 Running initial data update...")
        print("⚠️  This may take 5-10 minutes for first run...")
        
        # Run initial update (this will populate the database)
        agent.enhanced_daily_update()
        
        print("✅ Initial data update completed!")
        
        # Generate sample recommendations
        print("\n📊 Generating sample recommendations...")
        
        try:
            # Get top midfielder recommendations under £10m
            mid_recommendations = agent.generate_player_recommendations(
                position="MID", 
                max_price=10.0,
                gameweeks_ahead=5
            )
            
            if mid_recommendations:
                print(f"\n🎯 Top 5 Midfielder Recommendations (under £10m):")
                print("-" * 60)
                for i, rec in enumerate(mid_recommendations[:5], 1):
                    print(f"{i}. {rec.name} ({rec.team}) - £{rec.price}m")
                    print(f"   📈 Expected Points: {rec.predicted_points}")
                    print(f"   🎲 Confidence: {rec.confidence_score}% | Risk: {rec.risk_level}")
                    print(f"   💰 Value Rating: {rec.value_rating}")
                    print()
            else:
                print("⚠️  No midfielder recommendations available yet. Database may still be populating.")
            
            # Get differential picks
            print("🎯 Top 3 Differential Picks (<5% ownership):")
            print("-" * 45)
            differentials = agent.get_differential_picks(max_ownership=5.0)
            if differentials:
                for i, diff in enumerate(differentials[:3], 1):
                    ownership = diff.key_stats['ownership']
                    print(f"{i}. {diff.name} ({diff.team}) - {ownership}% owned")
                    print(f"   💰 Price: £{diff.price}m | 📈 Expected: {diff.predicted_points} pts")
                    print()
            else:
                print("⚠️  No differential picks available yet.")
            
            # Captain analysis
            print("⭐ Top 3 Captain Options:")
            print("-" * 30)
            captains = agent.analyze_captain_options()
            if captains:
                for i, cap in enumerate(captains[:3], 1):
                    print(f"{i}. {cap['name']} ({cap['team']})")
                    print(f"   🏆 Captain Score: {cap['captain_score']}")
                    print(f"   📈 Expected Points: {cap['expected_points']}")
                    print(f"   🛡️  Safety Level: {cap['safety_level']}")
                    print()
            else:
                print("⚠️  No captain options available yet.")
                
        except Exception as e:
            print(f"⚠️  Error generating recommendations: {e}")
            print("This is normal on first run - database is still being populated.")
        
        # Export recommendations to JSON
        print("💾 Exporting recommendations to JSON...")
        export_result = agent.export_recommendations_to_json("data/exports/latest_recommendations.json")
        if export_result:
            print("✅ Recommendations exported to data/exports/latest_recommendations.json")
        else:
            print("⚠️  Export completed with basic structure (check logs for details)")
        
        print(f"\n🔄 FANTASYPL Agent is now running...")
        print("📅 Daily updates scheduled for 2:00 AM")
        print("📊 Check data/exports/ for JSON exports")
        print("📝 Check data/logs/ for system logs")
        print("\n⌨️  Press Ctrl+C to stop the agent")
        print("=" * 50)
        
        # Keep running and check for scheduled tasks
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("\n👋 FANTASYPL Agent stopped by user")
        print("Thank you for using the system!")
    except Exception as e:
        print(f"❌ Error in FANTASYPL system: {e}")
        print("Check the logs in data/logs/ for more details")
        raise

if __name__ == "__main__":
    main()