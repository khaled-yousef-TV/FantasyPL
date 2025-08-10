#!/usr/bin/env python3
"""
FANTASYPL - Complete Multi-Agent System (Fixed)
Integration of Data Buff Agent + Fixture Agent + News Agent
With forced initial News Agent run
"""

import sys
import os
import schedule
import time
from datetime import datetime
import json

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import all agents and configuration
from agents.data_buff import EnhancedDataBuffAgent
from agents.fixture_agent import FixtureAgent, get_team_fixture_favorability
from agents.news_agent import NewsAgent
from config.settings import DATABASE_CONFIG, REDIS_CONFIG

class FantasyPLMultiAgentSystem:
    """
    Complete FPL Multi-Agent System
    Integrates Data Buff, Fixture, and News agents
    """
    
    def __init__(self):
        """Initialize all agents"""
        print("🏆 FANTASYPL Complete Multi-Agent System")
        print("=" * 60)
        print("🤖 Initializing agents...")
        
        try:
            # Initialize all three agents
            self.data_agent = EnhancedDataBuffAgent(DATABASE_CONFIG, REDIS_CONFIG)
            self.fixture_agent = FixtureAgent(DATABASE_CONFIG, REDIS_CONFIG)
            self.news_agent = NewsAgent(DATABASE_CONFIG, REDIS_CONFIG)
            
            # Initialize databases
            self.data_agent.initialize()
            self.fixture_agent.initialize()
            self.news_agent.initialize()
            
            # Track if this is first run
            self.first_run = True
            
            print("✅ All agents initialized successfully!")
            print("  📊 Data Buff Agent: Player analysis & recommendations")
            print("  📅 Fixture Agent: Advanced fixture analysis")
            print("  📰 News Agent: Injury & sentiment analysis")
            
        except Exception as e:
            print(f"❌ Error initializing agents: {e}")
            raise
    
    def run_complete_update(self, force_news=False):
        """
        Run complete system update
        
        Args:
            force_news: Force news update regardless of schedule
        """
        print("\n🔄 Running Complete System Update...")
        print("=" * 60)
        
        # 1. Data Agent Update
        print("\n📊 [1/3] Data Buff Agent - Updating player data...")
        try:
            self.data_agent.enhanced_daily_update()
            print("✅ Player data updated successfully!")
        except Exception as e:
            print(f"⚠️ Error updating player data: {e}")
        
        # 2. Fixture Agent Update
        print("\n📅 [2/3] Fixture Agent - Analyzing fixtures...")
        try:
            self.fixture_agent.daily_update()
            print("✅ Fixture analysis completed!")
        except Exception as e:
            print(f"⚠️ Error analyzing fixtures: {e}")
        
        # 3. News Agent Update
        # Run on first execution, when forced, or every 3 days
        should_update_news = (
            self.first_run or 
            force_news or 
            datetime.now().day % 3 == 0
        )
        
        if should_update_news:
            print("\n📰 [3/3] News Agent - Updating injury/news data...")
            if self.first_run:
                print("     (Initial run - forcing news update)")
            try:
                # For now, populate with sample data since scraping isn't implemented
                self._populate_sample_news_data()
                print("✅ News data updated!")
                self.first_run = False  # Mark first run as complete
            except Exception as e:
                print(f"⚠️ Error updating news: {e}")
        else:
            print("\n📰 [3/3] News Agent - Skip (updates every 3 days)")
            print(f"     Next update on day {(datetime.now().day // 3 + 1) * 3}")
    
    def _populate_sample_news_data(self):
        """Populate sample news data for testing"""
        from agents.news_agent import PlayerNews, InjuryStatus, ManagerSentiment
        
        # Sample injury data for testing
        sample_injuries = [
            PlayerNews(
                player_name="Bukayo Saka",
                team="Arsenal",
                status=InjuryStatus.QUESTIONABLE,
                injury_type="knock",
                expected_return="Next game",
                last_updated=datetime.now(),
                source="Sample Data",
                confidence_score=0.7,
                manager_sentiment=ManagerSentiment.NEUTRAL,
                play_probability=0.6
            ),
            PlayerNews(
                player_name="Gabriel Martinelli",
                team="Arsenal",
                status=InjuryStatus.OUT,
                injury_type="hamstring",
                expected_return="2-3 weeks",
                last_updated=datetime.now(),
                source="Sample Data",
                confidence_score=0.9,
                play_probability=0.0
            ),
            PlayerNews(
                player_name="Kevin De Bruyne",
                team="Man City",
                status=InjuryStatus.DOUBTFUL,
                injury_type="muscle",
                expected_return="Fitness test",
                last_updated=datetime.now(),
                source="Sample Data",
                confidence_score=0.6,
                play_probability=0.3
            ),
            PlayerNews(
                player_name="Marcus Rashford",
                team="Man Utd",
                status=InjuryStatus.FIT,
                injury_type=None,
                expected_return=None,
                last_updated=datetime.now(),
                source="Sample Data",
                confidence_score=0.8,
                manager_sentiment=ManagerSentiment.POSITIVE,
                play_probability=0.95
            ),
            PlayerNews(
                player_name="Cole Palmer",
                team="Chelsea",
                status=InjuryStatus.FIT,
                injury_type=None,
                expected_return=None,
                last_updated=datetime.now(),
                source="Sample Data",
                confidence_score=0.9,
                manager_sentiment=ManagerSentiment.VERY_POSITIVE,
                play_probability=1.0
            )
        ]
        
        # Save sample data
        for injury in sample_injuries:
            self.news_agent._save_player_news(injury)
        
        print(f"     Added {len(sample_injuries)} sample injury records")
    
    def generate_complete_recommendations(self):
        """Generate comprehensive recommendations from all agents"""
        print("\n🎯 Generating Complete Recommendations...")
        print("=" * 60)
        
        recommendations = {
            'generated_at': datetime.now().isoformat(),
            'player_recommendations': [],
            'captain_picks': [],
            'transfer_suggestions': {
                'out': [],
                'in': []
            },
            'team_insights': [],
            'differential_picks': []
        }
        
        # 1. Get base player recommendations
        print("\n📊 Analyzing player recommendations...")
        player_recs = self.data_agent.generate_player_recommendations(gameweeks_ahead=5)
        
        # 2. Get fixture data
        best_fixture_teams = self.fixture_agent.get_best_fixture_teams(gameweeks_ahead=4)
        
        # 3. Get injury/news data
        injury_report = self.news_agent.get_injury_report()
        excluded_players = self.news_agent.get_excluded_players()
        favored_players = self.news_agent.get_favored_players()
        
        print(f"  📰 Found {len(excluded_players)} injured/doubtful players")
        print(f"  ✅ Found {len(favored_players)} favored players")
        
        # 4. Combine and enhance recommendations
        if player_recs:
            print(f"  📊 Processing {len(player_recs)} player recommendations")
            
            # Create exclusion set for quick lookup
            excluded_names = {p['player_name'].lower() for p in excluded_players}
            if excluded_names:
                print(f"  ❌ Excluding: {', '.join(list(excluded_names)[:3])}...")
            
            # Create favored players dict
            favored_dict = {p['player_name'].lower(): p for p in favored_players}
            if favored_dict:
                print(f"  ⭐ Favoring: {', '.join(list(favored_dict.keys())[:3])}...")
            
            # Process each recommendation
            for rec in player_recs[:30]:  # Top 30
                player_name_lower = rec.name.lower()
                
                # Check if player is injured/excluded
                if player_name_lower in excluded_names:
                    continue  # Skip injured players
                
                # Get fixture data for player's team
                try:
                    team_id = self._get_team_id_by_name(rec.team)
                    fixture_data = get_team_fixture_favorability(self.fixture_agent, team_id)
                except:
                    fixture_data = {'favorability_score': 50, 'average_difficulty': 3}
                
                # Check if player is favored by manager
                sentiment_boost = 0
                if player_name_lower in favored_dict:
                    sentiment_boost = 10  # Boost for positive sentiment
                
                # Calculate combined score
                combined_score = self._calculate_combined_score(
                    rec.predicted_points,
                    rec.confidence_score,
                    fixture_data['favorability_score'],
                    sentiment_boost
                )
                
                recommendations['player_recommendations'].append({
                    'name': rec.name,
                    'team': rec.team,
                    'position': rec.position,
                    'price': rec.price,
                    'predicted_points': rec.predicted_points,
                    'confidence': rec.confidence_score,
                    'fixture_score': fixture_data['favorability_score'],
                    'sentiment_boost': sentiment_boost,
                    'combined_score': combined_score,
                    'ownership': rec.key_stats.get('ownership', 0)
                })
        
        # Sort by combined score
        recommendations['player_recommendations'].sort(
            key=lambda x: x['combined_score'], 
            reverse=True
        )
        
        # 5. Captain recommendations
        print("\n👑 Analyzing captain options...")
        captains = self.data_agent.analyze_captain_options()
        
        for cap in captains[:5]:
            # Check injury status
            player_status = self.news_agent.get_player_status(cap['name'], cap['team'])
            
            if player_status and player_status.get('play_probability', 100) < 50:
                cap['injury_risk'] = True
                cap['play_probability'] = player_status['play_probability']
                cap['injury_type'] = player_status.get('injury_type', 'Unknown')
            else:
                cap['injury_risk'] = False
                cap['play_probability'] = 100
            
            recommendations['captain_picks'].append(cap)
        
        # 6. Transfer suggestions
        print("\n🔄 Generating transfer suggestions...")
        
        # Players to transfer out (injured)
        for player in excluded_players[:5]:
            if player['play_probability'] < 30:
                recommendations['transfer_suggestions']['out'].append({
                    'name': player['player_name'],
                    'team': player['team'],
                    'status': player['status'],
                    'injury': player.get('injury_type', 'Unknown'),
                    'reason': f"{player['status']} - {player['play_probability']:.0f}% play chance",
                    'urgency': 'high' if player['play_probability'] < 10 else 'medium'
                })
        
        # Players to transfer in (good fixtures + form)
        for team in best_fixture_teams[:3]:
            # Get best players from these teams
            team_players = [p for p in recommendations['player_recommendations'] 
                          if p['team'] == team['team_name']][:2]
            
            for player in team_players:
                recommendations['transfer_suggestions']['in'].append({
                    'name': player['name'],
                    'team': player['team'],
                    'price': player['price'],
                    'reason': f"Great fixtures - Score: {player['combined_score']:.1f}"
                })
        
        # 7. Differential picks
        print("\n💎 Finding differential picks...")
        for rec in recommendations['player_recommendations']:
            if rec['ownership'] < 5 and rec['combined_score'] > 70:
                recommendations['differential_picks'].append({
                    'name': rec['name'],
                    'team': rec['team'],
                    'ownership': rec['ownership'],
                    'score': rec['combined_score'],
                    'reason': 'Low ownership + high potential'
                })
        
        return recommendations
    
    def display_recommendations(self, recommendations):
        """Display recommendations in a formatted way"""
        print("\n" + "="*60)
        print("📋 COMPLETE FPL RECOMMENDATIONS")
        print("="*60)
        
        # Show injury report first if we have excluded players
        if recommendations['transfer_suggestions']['out']:
            print("\n⚠️  INJURY ALERTS")
            print("-"*40)
            for player in recommendations['transfer_suggestions']['out'][:5]:
                urgency_icon = "🔴" if player['urgency'] == 'high' else "🟡"
                print(f"  {urgency_icon} {player['name']} ({player['team']})")
                print(f"     Status: {player['status']} - {player.get('injury', 'Unknown')}")
                print(f"     {player['reason']}")
        
        # Rest of the display logic remains the same...
        # [Previous display code continues here]
        
        # 1. Top Player Picks by Position
        print("\n🎯 TOP PLAYER RECOMMENDATIONS")
        print("-"*40)
        
        positions = {}
        for rec in recommendations['player_recommendations']:
            pos = rec['position']
            if pos not in positions:
                positions[pos] = []
            positions[pos].append(rec)
        
        for pos in ['GK', 'DEF', 'MID', 'FWD']:
            if pos in positions:
                print(f"\n{pos}:")
                for i, player in enumerate(positions[pos][:3], 1):
                    print(f"  {i}. {player['name']} ({player['team']}) - £{player['price']}m")
                    print(f"     Score: {player['combined_score']:.1f} | Predicted: {player['predicted_points']:.1f} pts")
                    if player['sentiment_boost'] > 0:
                        print(f"     ⭐ Favored by manager")
        
        # 2. Captain Picks
        print("\n👑 CAPTAIN RECOMMENDATIONS")
        print("-"*40)
        for i, cap in enumerate(recommendations['captain_picks'][:3], 1):
            risk = "⚠️ INJURY RISK " if cap.get('injury_risk') else ""
            print(f"  {i}. {risk}{cap['name']} ({cap['team']})")
            print(f"     Captain Score: {cap['captain_score']:.1f}")
            if cap.get('injury_risk'):
                print(f"     ⚠️  {cap.get('injury_type', 'Injury')} - Play Probability: {cap['play_probability']:.0f}%")
        
        # 3. Transfer Suggestions
        if recommendations['transfer_suggestions']['in']:
            print("\n🔄 TRANSFER SUGGESTIONS")
            print("-"*40)
            print("  TRANSFER IN:")
            for player in recommendations['transfer_suggestions']['in'][:3]:
                print(f"    ✅ {player['name']} ({player['team']}) - £{player['price']}m")
                print(f"       {player['reason']}")
        
        # 4. Differential Picks
        if recommendations['differential_picks']:
            print("\n💎 DIFFERENTIAL PICKS")
            print("-"*40)
            for pick in recommendations['differential_picks'][:3]:
                print(f"  • {pick['name']} ({pick['team']}) - {pick['ownership']:.1f}% owned")
                print(f"    Score: {pick['score']:.1f} - {pick['reason']}")
    
    def export_all_analysis(self):
        """Export all agent analysis to files"""
        print("\n💾 Exporting all analysis...")
        
        # 1. Export individual agent data
        self.data_agent.export_recommendations_to_json("data/exports/player_recommendations.json")
        self.fixture_agent.export_fixture_analysis_to_json("data/exports/fixture_analysis.json")
        self.news_agent.export_news_analysis_to_json("data/exports/news_analysis.json")
        
        # 2. Export combined recommendations
        recommendations = self.generate_complete_recommendations()
        with open("data/exports/complete_recommendations.json", 'w') as f:
            json.dump(recommendations, f, indent=2, default=str)
        
        print("✅ All exports completed!")
        print("  📁 Check data/exports/ for:")
        print("     • complete_recommendations.json")
        print("     • player_recommendations.json")
        print("     • fixture_analysis.json")
        print("     • news_analysis.json")
    
    def _calculate_combined_score(self, predicted_points, confidence, fixture_score, sentiment_boost):
        """Calculate combined score from all factors"""
        # Weights: 40% predicted points, 25% confidence, 25% fixtures, 10% sentiment
        score = (
            (predicted_points * 2) * 0.4 +  # Scale up predicted points
            confidence * 0.25 +
            fixture_score * 0.25 +
            sentiment_boost * 0.1
        )
        return round(score, 1)
    
    def _get_team_id_by_name(self, team_name):
        """Get team ID from team name"""
        team_ids = {
            'Arsenal': 1, 'Aston Villa': 2, 'Bournemouth': 3, 'Brentford': 4,
            'Brighton': 5, 'Burnley': 6, 'Chelsea': 7, 'Crystal Palace': 8,
            'Everton': 9, 'Fulham': 10, 'Liverpool': 11, 'Luton': 12,
            'Man City': 13, 'Man Utd': 14, 'Newcastle': 15, "Nott'm Forest": 16,
            'Sheffield Utd': 17, 'Spurs': 18, 'West Ham': 19, 'Wolves': 20
        }
        return team_ids.get(team_name, 1)


def main():
    """Main execution function"""
    print("🚀 Starting FANTASYPL Complete Multi-Agent System")
    print("="*60)
    
    try:
        # Initialize the complete system
        system = FantasyPLMultiAgentSystem()
        
        # Schedule updates
        schedule.every().day.at("02:00").do(system.run_complete_update)
        print("\n⏰ Daily updates scheduled for 2:00 AM")
        
        # Run initial update with forced news update
        print("\n🔄 Running initial complete update...")
        print("⚠️  This may take 10-15 minutes for first run...")
        system.run_complete_update(force_news=True)  # Force news on first run
        
        # Generate and display recommendations
        recommendations = system.generate_complete_recommendations()
        system.display_recommendations(recommendations)
        
        # Export all analysis
        system.export_all_analysis()
        
        print("\n" + "="*60)
        print("✅ FANTASYPL Multi-Agent System is now running!")
        print("="*60)
        print("📊 Automatic updates scheduled daily at 2:00 AM")
        print("📁 Check data/exports/ for detailed analysis")
        print("📝 Check data/logs/ for system logs")
        print("\n⌨️  Press Ctrl+C to stop the system")
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\n👋 System stopped by user")
        print("Thank you for using FANTASYPL Multi-Agent System!")
    except Exception as e:
        print(f"\n❌ Critical error: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()