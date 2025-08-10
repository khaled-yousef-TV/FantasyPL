# FANTASYPL Fixture Agent - Advanced Fixture Analysis
# Analyzes fixture difficulty, congestion, and optimal timing

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
import numpy as np
from dataclasses import dataclass
import os
import sys

# Add config to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/fantasypl_fixture_agent.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class FixtureAnalysis:
    """Data class for fixture analysis results"""
    team_id: int
    team_name: str
    fixture_id: int
    opponent_id: int
    opponent_name: str
    gameweek: int
    is_home: bool
    kickoff_time: datetime
    fpl_difficulty: int
    advanced_difficulty: float
    form_adjusted_difficulty: float
    congestion_impact: float
    favorability_score: float
    confidence: int
    analysis_factors: Dict

@dataclass
class FixtureRun:
    """Data class for analyzing runs of fixtures"""
    team_id: int
    team_name: str
    start_gameweek: int
    end_gameweek: int
    fixture_count: int
    average_difficulty: float
    easy_fixtures: int
    hard_fixtures: int
    home_fixtures: int
    away_fixtures: int
    congestion_level: str
    recommendation: str


class FixtureAPIWrapper:
    """Extended API wrapper for fixture-specific data"""
    
    def __init__(self):
        self.base_url = "https://fantasy.premierleague.com/api"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.last_request_time = 0
        self.min_request_interval = 1.0
    
    def _rate_limit(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
    
    def get_fixtures(self) -> List[Dict]:
        """Get all fixtures with enhanced data"""
        self._rate_limit()
        try:
            response = self.session.get(f"{self.base_url}/fixtures/")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching fixtures: {e}")
            raise
    
    def get_team_results(self, team_id: int, last_n_games: int = 6) -> List[Dict]:
        """Get recent results for form analysis"""
        # This would typically come from a football data API
        # For now, we'll simulate with FPL data
        try:
            fixtures = self.get_fixtures()
            team_fixtures = []
            
            for fixture in fixtures:
                if (fixture['team_h'] == team_id or fixture['team_a'] == team_id) and fixture['finished']:
                    team_fixtures.append(fixture)
            
            # Sort by kickoff time and get last n games
            team_fixtures.sort(key=lambda x: x['kickoff_time'], reverse=True)
            return team_fixtures[:last_n_games]
            
        except Exception as e:
            logger.error(f"Error fetching team results for {team_id}: {e}")
            return []


class FormAnalyzer:
    """Analyzes team form to adjust fixture difficulty"""
    
    def __init__(self, db_manager):
        self.db = db_manager
    
    def calculate_team_form(self, team_id: int, games_back: int = 6) -> Dict:
        """Calculate team's recent form"""
        # Ensure team_id is integer
        team_id = int(team_id)
        games_back = int(games_back)
        
        query = """
            SELECT 
                f.team_h, f.team_a, f.team_h_score, f.team_a_score,
                f.kickoff_time, f.finished
            FROM fixtures f
            WHERE (f.team_h = %s OR f.team_a = %s) 
            AND f.finished = TRUE
            AND f.team_h_score IS NOT NULL
            AND f.team_a_score IS NOT NULL
            ORDER BY f.kickoff_time DESC
            LIMIT %s
        """
        
        results = self.db.execute_query(query, (team_id, team_id, games_back))
        
        if not results:
            return {
                'games_played': 0,
                'wins': 0,
                'draws': 0,
                'losses': 0,
                'goals_for': 0,
                'goals_against': 0,
                'form_score': 50.0,  # Neutral
                'attack_strength': 1.0,
                'defense_strength': 1.0
            }
        
        wins = draws = losses = 0
        goals_for = goals_against = 0
        
        for result in results:
            try:
                # Ensure all values are integers
                team_h = int(result['team_h'])
                team_a = int(result['team_a'])
                team_h_score = int(result['team_h_score']) if result['team_h_score'] is not None else 0
                team_a_score = int(result['team_a_score']) if result['team_a_score'] is not None else 0
                
                is_home = team_h == team_id
                team_score = team_h_score if is_home else team_a_score
                opp_score = team_a_score if is_home else team_h_score
                
                goals_for += team_score
                goals_against += opp_score
                
                if team_score > opp_score:
                    wins += 1
                elif team_score == opp_score:
                    draws += 1
                else:
                    losses += 1
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping invalid result data for team {team_id}: {e}")
                continue
        
        games_played = len(results)
        points = (wins * 3) + draws
        form_score = (points / (games_played * 3)) * 100 if games_played > 0 else 50.0
        
        # Calculate attack and defense strength relative to league average
        avg_goals_per_game = 1.3  # Premier League average
        attack_strength = (goals_for / games_played) / avg_goals_per_game if games_played > 0 else 1.0
        defense_strength = avg_goals_per_game / (goals_against / games_played) if goals_against > 0 and games_played > 0 else 1.0
        
        return {
            'games_played': games_played,
            'wins': wins,
            'draws': draws,
            'losses': losses,
            'goals_for': goals_for,
            'goals_against': goals_against,
            'form_score': round(form_score, 1),
            'attack_strength': round(attack_strength, 2),
            'defense_strength': round(defense_strength, 2)
        }
    
    def get_head_to_head_record(self, team1_id: int, team2_id: int, seasons_back: int = 3) -> Dict:
        """Get historical head-to-head record"""
        # Ensure integer types
        team1_id = int(team1_id)
        team2_id = int(team2_id)
        seasons_back = int(seasons_back)
        
        query = """
            SELECT 
                f.team_h, f.team_a, f.team_h_score, f.team_a_score,
                f.kickoff_time
            FROM fixtures f
            WHERE ((f.team_h = %s AND f.team_a = %s) OR 
                   (f.team_h = %s AND f.team_a = %s))
            AND f.finished = TRUE
            AND f.team_h_score IS NOT NULL
            AND f.team_a_score IS NOT NULL
            ORDER BY f.kickoff_time DESC
            LIMIT %s
        """
        
        results = self.db.execute_query(query, (team1_id, team2_id, team2_id, team1_id, seasons_back * 2))
        
        team1_wins = team1_draws = team1_losses = 0
        total_games = 0
        
        for result in results:
            try:
                # Ensure integer conversion
                team_h = int(result['team_h'])
                team_a = int(result['team_a'])
                team_h_score = int(result['team_h_score']) if result['team_h_score'] is not None else 0
                team_a_score = int(result['team_a_score']) if result['team_a_score'] is not None else 0
                
                team1_is_home = team_h == team1_id
                team1_score = team_h_score if team1_is_home else team_a_score
                team2_score = team_a_score if team1_is_home else team_h_score
                
                if team1_score > team2_score:
                    team1_wins += 1
                elif team1_score == team2_score:
                    team1_draws += 1
                else:
                    team1_losses += 1
                
                total_games += 1
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping invalid H2H data for teams {team1_id} vs {team2_id}: {e}")
                continue
        
        return {
            'total_games': total_games,
            'team1_wins': team1_wins,
            'team1_draws': team1_draws,
            'team1_losses': team1_losses,
            'team1_win_rate': team1_wins / total_games if total_games > 0 else 0.5
        }


class CongestionAnalyzer:
    """Analyzes fixture congestion and its impact"""
    
    def __init__(self, api_wrapper, db_manager):
        self.api = api_wrapper
        self.db = db_manager  # Add database manager
        # Teams in European competitions (would be updated dynamically)
        self.european_teams = {
            1: "Arsenal", 2: "Aston Villa", 3: "Chelsea", 4: "Liverpool", 
            5: "Man City", 6: "Man Utd", 7: "Newcastle", 8: "Tottenham"
        }
    
    def calculate_fixture_congestion(self, team_id: int, gameweek: int, window_days: int = 14) -> Dict:
        """Calculate fixture congestion around a specific gameweek"""
        
        # Get fixtures in the analysis window
        query = """
            SELECT f.*, 
                   CASE WHEN f.team_h = %s THEN 'H' ELSE 'A' END as venue
            FROM fixtures f
            WHERE (f.team_h = %s OR f.team_a = %s)
            AND f.gameweek BETWEEN %s AND %s
            ORDER BY f.kickoff_time
        """
        
        start_gw = max(1, gameweek - 2)
        end_gw = min(38, gameweek + 2)
        
        # Convert to int to ensure type safety
        team_id = int(team_id)
        start_gw = int(start_gw)
        end_gw = int(end_gw)
        
        fixtures = self.db.execute_query(query, (team_id, team_id, team_id, start_gw, end_gw))
        
        if not fixtures:
            return {
                'fixture_count': 0,
                'congestion_level': 'none',
                'congestion_score': 0,
                'days_between_fixtures': [],
                'has_european_football': team_id in self.european_teams
            }
        
        # Calculate days between fixtures
        fixture_dates = []
        for fixture in fixtures:
            if fixture['kickoff_time']:
                try:
                    kickoff_str = str(fixture['kickoff_time'])
                    if kickoff_str and kickoff_str != 'None':
                        if isinstance(fixture['kickoff_time'], str):
                            if 'T' in kickoff_str:
                                # ISO format
                                fixture_dates.append(datetime.fromisoformat(kickoff_str.replace('Z', '+00:00')))
                            else:
                                # Skip invalid format
                                continue
                        else:
                            # Already a datetime object
                            fixture_dates.append(fixture['kickoff_time'])
                except Exception as e:
                    logger.warning(f"Skipping invalid kickoff time: {fixture['kickoff_time']}")
                    continue
        
        fixture_dates.sort()
        days_between = []
        
        for i in range(1, len(fixture_dates)):
            days_diff = (fixture_dates[i] - fixture_dates[i-1]).days
            days_between.append(days_diff)
        
        # Determine congestion level
        fixture_count = len(fixtures)
        avg_days_between = np.mean(days_between) if days_between else 7
        
        if fixture_count >= 4 and avg_days_between < 4:
            congestion_level = 'high'
            congestion_score = 0.8
        elif fixture_count >= 3 and avg_days_between < 5:
            congestion_level = 'medium'
            congestion_score = 0.6
        elif fixture_count >= 2 and avg_days_between < 3:
            congestion_level = 'medium'
            congestion_score = 0.5
        else:
            congestion_level = 'low'
            congestion_score = 0.2
        
        # Extra penalty for European teams
        if team_id in self.european_teams:
            congestion_score += 0.2
        
        return {
            'fixture_count': fixture_count,
            'congestion_level': congestion_level,
            'congestion_score': min(congestion_score, 1.0),
            'days_between_fixtures': days_between,
            'avg_days_between': round(avg_days_between, 1),
            'has_european_football': team_id in self.european_teams
        }


class AdvancedDifficultyCalculator:
    """Calculates advanced difficulty scores beyond basic FPL ratings"""
    
    def __init__(self, form_analyzer, congestion_analyzer):
        self.form_analyzer = form_analyzer
        self.congestion_analyzer = congestion_analyzer
    
    def calculate_advanced_difficulty(self, fixture: Dict, team_id: int) -> Dict:
        """Calculate comprehensive difficulty analysis"""
        
        try:
            # Ensure integers
            team_id = int(team_id)
            is_home = int(fixture['team_h']) == team_id
            opponent_id = int(fixture['team_a']) if is_home else int(fixture['team_h'])
            gameweek = int(fixture.get('gameweek') or fixture.get('event', 1))
            
            # Base FPL difficulty
            base_difficulty = int(fixture['team_h_difficulty']) if is_home else int(fixture['team_a_difficulty'])
            
            logger.debug(f"Starting advanced difficulty calculation for team {team_id} vs {opponent_id}")
            
            # Get team and opponent form
            logger.debug("Calculating team form...")
            team_form = self.form_analyzer.calculate_team_form(team_id)
            logger.debug("Calculating opponent form...")
            opponent_form = self.form_analyzer.calculate_team_form(opponent_id)
            
            # Get head-to-head record
            logger.debug("Getting head-to-head record...")
            h2h = self.form_analyzer.get_head_to_head_record(team_id, opponent_id)
            
            # Get congestion analysis
            logger.debug("Calculating fixture congestion...")
            congestion = self.congestion_analyzer.calculate_fixture_congestion(team_id, gameweek)
            
            # Calculate form-adjusted difficulty
            logger.debug("Calculating form multiplier...")
            form_multiplier = self._calculate_form_multiplier(team_form, opponent_form, is_home)
            form_adjusted_difficulty = base_difficulty * form_multiplier
            
            # Calculate favorability score (0-100, higher is better for the team)
            logger.debug("Calculating favorability score...")
            favorability_score = self._calculate_favorability_score(
                base_difficulty, team_form, opponent_form, h2h, congestion, is_home
            )
            
            # Advanced difficulty (1-10 scale, lower is easier)
            logger.debug("Calculating final difficulty...")
            advanced_difficulty = self._calculate_final_difficulty(
                base_difficulty, form_multiplier, congestion['congestion_score'], h2h['team1_win_rate']
            )
            
            # Confidence in the analysis
            logger.debug("Calculating confidence...")
            confidence = self._calculate_confidence(team_form, opponent_form, h2h, congestion)
            
            return {
                'base_difficulty': base_difficulty,
                'advanced_difficulty': round(advanced_difficulty, 2),
                'form_adjusted_difficulty': round(form_adjusted_difficulty, 2),
                'favorability_score': round(favorability_score, 1),
                'confidence': confidence,
                'team_form': team_form,
                'opponent_form': opponent_form,
                'head_to_head': h2h,
                'congestion': congestion,
                'form_multiplier': round(form_multiplier, 2)
            }
            
        except Exception as e:
            logger.error(f"Error in calculate_advanced_difficulty for team {team_id}: {e}")
            # Return simplified analysis if advanced fails
            base_difficulty = int(fixture['team_h_difficulty']) if int(fixture['team_h']) == int(team_id) else int(fixture['team_a_difficulty'])
            return {
                'base_difficulty': base_difficulty,
                'advanced_difficulty': float(base_difficulty),
                'form_adjusted_difficulty': float(base_difficulty),
                'favorability_score': (6 - base_difficulty) * 20,
                'confidence': 50,
                'team_form': {'form_score': 50.0},
                'opponent_form': {'form_score': 50.0},
                'head_to_head': {'team1_win_rate': 0.5},
                'congestion': {'congestion_score': 0.2},
                'form_multiplier': 1.0
            }
    
    def _calculate_form_multiplier(self, team_form: Dict, opponent_form: Dict, is_home: bool) -> float:
        """Calculate how current form affects difficulty"""
        
        # Team form impact (better form = easier for opponents)
        team_form_score = team_form['form_score']
        opponent_form_score = opponent_form['form_score']
        
        # Relative form difference
        form_difference = opponent_form_score - team_form_score
        
        # Convert to multiplier (range 0.7 - 1.3)
        base_multiplier = 1.0 + (form_difference / 200)  # /200 to scale
        
        # Home advantage
        if is_home:
            base_multiplier *= 0.9  # Slightly easier at home
        else:
            base_multiplier *= 1.1  # Slightly harder away
        
        # Bound the multiplier
        return max(0.5, min(1.5, base_multiplier))
    
    def _calculate_favorability_score(self, base_difficulty: int, team_form: Dict, 
                                    opponent_form: Dict, h2h: Dict, congestion: Dict, 
                                    is_home: bool) -> float:
        """Calculate overall favorability (0-100, higher is better)"""
        
        # Start with inverse of difficulty (5 = easiest, 1 = hardest)
        base_score = (6 - base_difficulty) * 15  # Scale 15-75
        
        # Form adjustments
        form_bonus = (team_form['form_score'] - opponent_form['form_score']) / 4
        base_score += form_bonus
        
        # Home advantage
        if is_home:
            base_score += 10
        
        # Head-to-head bonus
        h2h_bonus = (h2h['team1_win_rate'] - 0.5) * 20  # -10 to +10
        base_score += h2h_bonus
        
        # Congestion penalty
        congestion_penalty = congestion['congestion_score'] * 15
        base_score -= congestion_penalty
        
        # Bound to 0-100
        return max(0, min(100, base_score))
    
    def _calculate_final_difficulty(self, base_difficulty: int, form_multiplier: float, 
                                  congestion_score: float, h2h_win_rate: float) -> float:
        """Calculate final advanced difficulty score"""
        
        # Start with form-adjusted difficulty
        difficulty = base_difficulty * form_multiplier
        
        # Add congestion impact
        difficulty += congestion_score * 2  # Up to +2 for high congestion
        
        # Adjust for historical performance
        h2h_adjustment = (0.5 - h2h_win_rate) * 2  # -1 to +1
        difficulty += h2h_adjustment
        
        # Bound to reasonable range
        return max(1.0, min(10.0, difficulty))
    
    def _calculate_confidence(self, team_form: Dict, opponent_form: Dict, 
                            h2h: Dict, congestion: Dict) -> int:
        """Calculate confidence in the analysis (0-100)"""
        
        confidence = 50  # Base confidence
        
        # More games = higher confidence
        if team_form['games_played'] >= 5:
            confidence += 20
        elif team_form['games_played'] >= 3:
            confidence += 10
        
        if opponent_form['games_played'] >= 5:
            confidence += 15
        elif opponent_form['games_played'] >= 3:
            confidence += 8
        
        # Head-to-head history
        if h2h['total_games'] >= 6:
            confidence += 10
        elif h2h['total_games'] >= 3:
            confidence += 5
        
        # Less confidence if high congestion (unpredictable)
        if congestion['congestion_level'] == 'high':
            confidence -= 15
        elif congestion['congestion_level'] == 'medium':
            confidence -= 8
        
        return max(0, min(100, confidence))


class FixtureAgent:
    """Main Fixture Agent for advanced fixture analysis"""
    
    def __init__(self, db_config: Dict[str, str], redis_config: Dict[str, str] = None):
        from agents.data_buff import DatabaseManager, CacheManager
        
        self.api = FixtureAPIWrapper()
        self.db = DatabaseManager(db_config)
        self.cache = CacheManager(redis_config or {})
        
        self.form_analyzer = FormAnalyzer(self.db)
        self.congestion_analyzer = CongestionAnalyzer(self.api, self.db)  # Pass db_manager
        self.difficulty_calculator = AdvancedDifficultyCalculator(
            self.form_analyzer, self.congestion_analyzer
        )
        
        self.current_season = "2024-25"
    
    def initialize(self):
        """Initialize the fixture agent"""
        self._create_fixture_tables()
        logger.info("Fixture Agent initialized successfully")
    
    def _create_fixture_tables(self):
        """Create additional tables for fixture analysis"""
        if not self.db.connection:
            self.db.connect()
        
        cursor = self.db.connection.cursor()
        
        # Fixture analysis table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fixture_analysis (
                id SERIAL PRIMARY KEY,
                fixture_id INTEGER,
                team_id INTEGER,
                opponent_id INTEGER,
                gameweek INTEGER,
                is_home BOOLEAN,
                base_difficulty INTEGER,
                advanced_difficulty DECIMAL(4,2),
                form_adjusted_difficulty DECIMAL(4,2),
                favorability_score DECIMAL(4,1),
                confidence INTEGER,
                congestion_level VARCHAR(10),
                congestion_score DECIMAL(3,2),
                analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                season VARCHAR(7)
            )
        """)
        
        # Fixture runs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fixture_runs (
                id SERIAL PRIMARY KEY,
                team_id INTEGER,
                start_gameweek INTEGER,
                end_gameweek INTEGER,
                fixture_count INTEGER,
                average_difficulty DECIMAL(4,2),
                easy_fixtures INTEGER,
                hard_fixtures INTEGER,
                congestion_level VARCHAR(10),
                recommendation TEXT,
                analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                season VARCHAR(7)
            )
        """)
        
        self.db.connection.commit()
        cursor.close()
        logger.info("Fixture analysis tables created successfully")
    
    def analyze_upcoming_fixtures(self, gameweeks_ahead: int = 6) -> List[FixtureAnalysis]:
        """Analyze upcoming fixtures for all teams"""
        
        current_gameweek = self._get_current_gameweek()
        end_gameweek = min(38, current_gameweek + gameweeks_ahead)
        
        # Get upcoming fixtures
        query = """
            SELECT * FROM fixtures 
            WHERE gameweek BETWEEN %s AND %s 
            AND finished = FALSE
            ORDER BY gameweek, kickoff_time
        """
        
        fixtures = self.db.execute_query(query, (current_gameweek, end_gameweek))
        analyses = []
        
        logger.info(f"Analyzing {len(fixtures)} upcoming fixtures...")
        
        for fixture in fixtures:
            # Analyze for both teams
            for team_id in [fixture['team_h'], fixture['team_a']]:
                try:
                    # Advanced analysis with detailed error tracking
                    logger.debug(f"Starting analysis for fixture {fixture['id']}, team {team_id}")
                    
                    # Ensure all fixture data is properly typed
                    typed_fixture = {
                        'id': int(fixture['id']),
                        'team_h': int(fixture['team_h']),
                        'team_a': int(fixture['team_a']),
                        'team_h_difficulty': int(fixture['team_h_difficulty']),
                        'team_a_difficulty': int(fixture['team_a_difficulty']),
                        'gameweek': int(fixture.get('gameweek') or fixture.get('event', 1)),
                        'kickoff_time': fixture['kickoff_time'],
                        'finished': fixture['finished']
                    }
                    
                    analysis_data = self.difficulty_calculator.calculate_advanced_difficulty(
                        typed_fixture, int(team_id)
                    )
                    
                    is_home = int(fixture['team_h']) == int(team_id)
                    opponent_id = int(fixture['team_a']) if is_home else int(fixture['team_h'])
                    
                    # Get team names
                    team_name = self._get_team_name(team_id)
                    opponent_name = self._get_team_name(opponent_id)
                    
                    # Safe datetime parsing
                    def parse_kickoff_time(kickoff_str):
                        """Safely parse kickoff time string"""
                        if not kickoff_str or kickoff_str == 'None':
                            return datetime.now()
                        
                        try:
                            # Handle different possible formats
                            if 'T' in str(kickoff_str):
                                # ISO format: 2024-08-17T14:00:00Z
                                return datetime.fromisoformat(str(kickoff_str).replace('Z', '+00:00'))
                            else:
                                # Try parsing as timestamp or other format
                                return datetime.now()  # Fallback to now
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Could not parse kickoff time '{kickoff_str}': {e}")
                            return datetime.now()
                    
                    analysis = FixtureAnalysis(
                        team_id=int(team_id),
                        team_name=team_name,
                        fixture_id=int(fixture['id']),
                        opponent_id=opponent_id,
                        opponent_name=opponent_name,
                        gameweek=int(fixture.get('gameweek') or fixture.get('event', 1)),
                        is_home=is_home,
                        kickoff_time=parse_kickoff_time(fixture.get('kickoff_time')),
                        fpl_difficulty=int(fixture['team_h_difficulty']) if is_home else int(fixture['team_a_difficulty']),
                        advanced_difficulty=analysis_data['advanced_difficulty'],
                        form_adjusted_difficulty=analysis_data['form_adjusted_difficulty'],
                        congestion_impact=analysis_data['congestion']['congestion_score'],
                        favorability_score=analysis_data['favorability_score'],
                        confidence=analysis_data['confidence'],
                        analysis_factors=analysis_data
                    )
                    
                    analyses.append(analysis)
                    
                except Exception as e:
                    logger.error(f"Error analyzing fixture {fixture['id']} for team {team_id}: {e}")
                    # Add more detailed debug info
                    logger.error(f"Fixture data: team_h={fixture.get('team_h')}, team_a={fixture.get('team_a')}, "
                               f"difficulty_h={fixture.get('team_h_difficulty')}, difficulty_a={fixture.get('team_a_difficulty')}")
                    
                    # Add stack trace for debugging
                    import traceback
                    logger.error(f"Stack trace: {traceback.format_exc()}")
                    continue
        
        logger.info(f"Successfully analyzed {len(analyses)} fixture analyses")
        return analyses
    
    def analyze_fixture_runs(self, gameweeks_ahead: int = 6) -> List[FixtureRun]:
        """Analyze runs of fixtures for each team"""
        
        current_gameweek = self._get_current_gameweek()
        end_gameweek = min(38, current_gameweek + gameweeks_ahead)
        
        # Get all teams
        teams = self.db.execute_query("SELECT id, name FROM teams")
        fixture_runs = []
        
        for team in teams:
            team_id = team['id']
            team_name = team['name']
            
            # Get team's fixtures
            query = """
                SELECT f.*, 
                       CASE WHEN f.team_h = %s THEN f.team_h_difficulty 
                            ELSE f.team_a_difficulty END as difficulty
                FROM fixtures f
                WHERE (f.team_h = %s OR f.team_a = %s)
                AND f.gameweek BETWEEN %s AND %s
                AND f.finished = FALSE
                ORDER BY f.gameweek
            """
            
            fixtures = self.db.execute_query(query, (team_id, team_id, team_id, current_gameweek, end_gameweek))
            
            if not fixtures:
                continue
            
            # Calculate run statistics
            difficulties = [f['difficulty'] for f in fixtures]
            avg_difficulty = np.mean(difficulties)
            
            easy_fixtures = sum(1 for d in difficulties if d <= 2)
            hard_fixtures = sum(1 for d in difficulties if d >= 4)
            home_fixtures = sum(1 for f in fixtures if f['team_h'] == team_id)
            away_fixtures = len(fixtures) - home_fixtures
            
            # Determine congestion level
            congestion = self.congestion_analyzer.calculate_fixture_congestion(
                team_id, current_gameweek + 3, 14
            )
            
            # Generate recommendation
            recommendation = self._generate_fixture_recommendation(
                avg_difficulty, easy_fixtures, hard_fixtures, congestion['congestion_level']
            )
            
            fixture_run = FixtureRun(
                team_id=team_id,
                team_name=team_name,
                start_gameweek=current_gameweek,
                end_gameweek=end_gameweek,
                fixture_count=len(fixtures),
                average_difficulty=round(avg_difficulty, 2),
                easy_fixtures=easy_fixtures,
                hard_fixtures=hard_fixtures,
                home_fixtures=home_fixtures,
                away_fixtures=away_fixtures,
                congestion_level=congestion['congestion_level'],
                recommendation=recommendation
            )
            
            fixture_runs.append(fixture_run)
        
        # Sort by most favorable runs first
        fixture_runs.sort(key=lambda x: x.average_difficulty)
        
        return fixture_runs
    
    def get_best_fixture_teams(self, gameweeks_ahead: int = 4, min_fixtures: int = 2) -> List[Dict]:
        """Get teams with the best upcoming fixture runs"""
        
        fixture_runs = self.analyze_fixture_runs(gameweeks_ahead)
        
        best_teams = []
        for run in fixture_runs:
            if run.fixture_count >= min_fixtures:
                score = self._calculate_fixture_score(run)
                
                best_teams.append({
                    'team_id': run.team_id,
                    'team_name': run.team_name,
                    'fixture_count': run.fixture_count,
                    'average_difficulty': run.average_difficulty,
                    'easy_fixtures': run.easy_fixtures,
                    'hard_fixtures': run.hard_fixtures,
                    'home_fixtures': run.home_fixtures,
                    'congestion_level': run.congestion_level,
                    'fixture_score': score,
                    'recommendation': run.recommendation
                })
        
        # Sort by fixture score (higher is better)
        best_teams.sort(key=lambda x: x['fixture_score'], reverse=True)
        
        return best_teams[:10]  # Top 10 teams
    
    def get_transfer_timing_recommendations(self) -> List[Dict]:
        """Get recommendations for optimal transfer timing"""
        
        current_gameweek = self._get_current_gameweek()
        recommendations = []
        
        # Look ahead for teams with improving fixtures
        for gw_offset in range(1, 4):  # Next 3 gameweeks
            target_gameweek = current_gameweek + gw_offset
            
            # Get teams with good upcoming runs starting from target gameweek
            upcoming_runs = self.analyze_fixture_runs(6)  # 6 gameweeks ahead
            
            for run in upcoming_runs:
                if run.average_difficulty <= 2.5 and run.easy_fixtures >= 2:
                    recommendations.append({
                        'team_name': run.team_name,
                        'recommended_transfer_gameweek': target_gameweek,
                        'fixture_run_start': run.start_gameweek,
                        'average_difficulty': run.average_difficulty,
                        'easy_fixtures': run.easy_fixtures,
                        'reasoning': f"Easy run of {run.easy_fixtures} fixtures starting GW{run.start_gameweek}"
                    })
        
        # Remove duplicates and sort by transfer gameweek
        seen_teams = set()
        unique_recommendations = []
        
        for rec in recommendations:
            if rec['team_name'] not in seen_teams:
                unique_recommendations.append(rec)
                seen_teams.add(rec['team_name'])
        
        unique_recommendations.sort(key=lambda x: x['recommended_transfer_gameweek'])
        
        return unique_recommendations[:15]  # Top 15 recommendations
    
    def _get_current_gameweek(self) -> int:
        """Get current gameweek from fixtures"""
        query = """
            SELECT MIN(gameweek) as current_gw 
            FROM fixtures 
            WHERE finished = FALSE
        """
        result = self.db.execute_query(query)
        return result[0]['current_gw'] if result and result[0]['current_gw'] else 1
    
    def _get_team_name(self, team_id: int) -> str:
        """Get team name from ID"""
        query = "SELECT name FROM teams WHERE id = %s"
        result = self.db.execute_query(query, (team_id,))
        return result[0]['name'] if result else f"Team {team_id}"
    
    def _generate_fixture_recommendation(self, avg_difficulty: float, easy_fixtures: int, 
                                       hard_fixtures: int, congestion_level: str) -> str:
        """Generate fixture run recommendation"""
        
        if avg_difficulty <= 2.0 and easy_fixtures >= 3:
            return "EXCELLENT - Strong target for transfers"
        elif avg_difficulty <= 2.5 and easy_fixtures >= 2:
            return "GOOD - Consider for transfers"
        elif avg_difficulty >= 4.0 or hard_fixtures >= 3:
            return "AVOID - Difficult fixture run"
        elif congestion_level == 'high':
            return "CAUTION - High fixture congestion"
        else:
            return "NEUTRAL - Average fixture difficulty"
    
    def _calculate_fixture_score(self, run: FixtureRun) -> float:
        """Calculate fixture score (higher is better)"""
        
        # Base score from difficulty (invert so lower difficulty = higher score)
        base_score = (6 - run.average_difficulty) * 20  # 20-100 range
        
        # Bonus for easy fixtures
        easy_bonus = run.easy_fixtures * 10
        
        # Penalty for hard fixtures
        hard_penalty = run.hard_fixtures * 15
        
        # Home advantage bonus
        home_bonus = run.home_fixtures * 5
        
        # Congestion penalty
        congestion_penalty = 0
        if run.congestion_level == 'high':
            congestion_penalty = 25
        elif run.congestion_level == 'medium':
            congestion_penalty = 15
        
        final_score = base_score + easy_bonus - hard_penalty + home_bonus - congestion_penalty
        
        return max(0, min(100, final_score))
    
    def fetch_and_store_fixtures(self):
        """Fetch and store fixture data"""
        logger.info("Fetching fixture data...")
        fixtures = self.api.get_fixtures()
        
        logger.info(f"Retrieved {len(fixtures)} fixtures from API")
        
        # Debug: Show first fixture structure
        if fixtures:
            logger.info(f"Sample fixture keys: {list(fixtures[0].keys())}")
        
        for fixture in fixtures:
            try:
                query = """
                    INSERT INTO fixtures (id, gameweek, team_h, team_a, team_h_difficulty,
                                        team_a_difficulty, kickoff_time, finished,
                                        team_h_score, team_a_score, season)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        gameweek = EXCLUDED.gameweek,
                        team_h_score = EXCLUDED.team_h_score,
                        team_a_score = EXCLUDED.team_a_score,
                        finished = EXCLUDED.finished
                """
                params = (
                    fixture['id'], 
                    fixture.get('event') or fixture.get('gameweek', 1),  # Handle both field names
                    fixture['team_h'], fixture['team_a'],
                    fixture['team_h_difficulty'], fixture['team_a_difficulty'],
                    fixture['kickoff_time'], fixture['finished'],
                    fixture['team_h_score'], fixture['team_a_score'],
                    self.current_season
                )
                self.db.execute_insert(query, params)
                
            except Exception as e:
                logger.error(f"Error storing fixture {fixture.get('id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Stored {len(fixtures)} fixtures")
    
    def store_fixture_analysis(self, analyses: List[FixtureAnalysis]):
        """Store fixture analysis results"""
        
        # Clear old analysis for current season
        delete_query = "DELETE FROM fixture_analysis WHERE season = %s"
        self.db.execute_insert(delete_query, (self.current_season,))
        
        for analysis in analyses:
            try:
                query = """
                    INSERT INTO fixture_analysis (
                        fixture_id, team_id, opponent_id, gameweek, is_home,
                        base_difficulty, advanced_difficulty, form_adjusted_difficulty,
                        favorability_score, confidence, congestion_level,
                        congestion_score, season
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                params = (
                    analysis.fixture_id, analysis.team_id, analysis.opponent_id,
                    analysis.gameweek, analysis.is_home, analysis.fpl_difficulty,
                    analysis.advanced_difficulty, analysis.form_adjusted_difficulty,
                    analysis.favorability_score, analysis.confidence,
                    analysis.analysis_factors['congestion']['congestion_level'],
                    analysis.congestion_impact, self.current_season
                )
                self.db.execute_insert(query, params)
                
            except Exception as e:
                logger.error(f"Error storing analysis for fixture {analysis.fixture_id}: {e}")
                continue
    
    def daily_update(self):
        """Perform daily fixture analysis update"""
        logger.info("Starting fixture agent daily update...")
        
        try:
            # 1. Fetch latest fixture data
            self.fetch_and_store_fixtures()
            
            # 2. Analyze upcoming fixtures
            analyses = self.analyze_upcoming_fixtures(8)
            self.store_fixture_analysis(analyses)
            
            # 3. Generate fixture run analysis
            fixture_runs = self.analyze_fixture_runs(6)
            
            logger.info(f"Analyzed {len(analyses)} fixtures and {len(fixture_runs)} fixture runs")
            logger.info("Fixture agent daily update completed successfully")
            
        except Exception as e:
            logger.error(f"Error during fixture agent daily update: {e}")
            raise
    
    def export_fixture_analysis_to_json(self, filename: str = None):
        """Export fixture analysis to JSON"""
        if not filename:
            filename = f"data/exports/fixture_analysis_{datetime.now().strftime('%Y%m%d')}.json"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        try:
            # Get fixture analyses
            analyses = self.analyze_upcoming_fixtures(6)
            fixture_runs = self.analyze_fixture_runs(6)
            best_teams = self.get_best_fixture_teams(4)
            transfer_timing = self.get_transfer_timing_recommendations()
            
            # Convert to JSON-serializable format
            json_data = {
                'generated_at': datetime.now().isoformat(),
                'season': self.current_season,
                'current_gameweek': self._get_current_gameweek(),
                'summary': {
                    'total_analyses': len(analyses),
                    'teams_analyzed': len(fixture_runs),
                    'best_fixture_teams': len(best_teams),
                    'transfer_recommendations': len(transfer_timing)
                },
                'best_fixture_teams': best_teams,
                'transfer_timing_recommendations': transfer_timing,
                'fixture_runs': [],
                'upcoming_fixtures': []
            }
            
            # Add fixture runs
            for run in fixture_runs[:20]:  # Top 20
                json_data['fixture_runs'].append({
                    'team_name': run.team_name,
                    'gameweeks': f"GW{run.start_gameweek}-{run.end_gameweek}",
                    'fixture_count': run.fixture_count,
                    'average_difficulty': run.average_difficulty,
                    'easy_fixtures': run.easy_fixtures,
                    'hard_fixtures': run.hard_fixtures,
                    'home_fixtures': run.home_fixtures,
                    'away_fixtures': run.away_fixtures,
                    'congestion_level': run.congestion_level,
                    'recommendation': run.recommendation
                })
            
            # Add fixture analyses
            for analysis in analyses[:50]:  # Top 50
                json_data['upcoming_fixtures'].append({
                    'team_name': analysis.team_name,
                    'opponent_name': analysis.opponent_name,
                    'gameweek': analysis.gameweek,
                    'venue': 'Home' if analysis.is_home else 'Away',
                    'fpl_difficulty': analysis.fpl_difficulty,
                    'advanced_difficulty': analysis.advanced_difficulty,
                    'favorability_score': analysis.favorability_score,
                    'confidence': analysis.confidence,
                    'congestion_impact': analysis.congestion_impact
                })
            
            with open(filename, 'w') as f:
                json.dump(json_data, f, indent=2)
            
            logger.info(f"Fixture analysis exported to {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error exporting fixture analysis: {e}")
            return None


# Integration helper functions
def get_team_fixture_favorability(fixture_agent: FixtureAgent, team_id: int, 
                                gameweeks_ahead: int = 5) -> Dict:
    """Get fixture favorability for a specific team (for Decision Maker)"""
    
    current_gameweek = fixture_agent._get_current_gameweek()
    end_gameweek = min(38, current_gameweek + gameweeks_ahead)
    
    # Get team's upcoming fixtures
    query = """
        SELECT f.*,
               CASE WHEN f.team_h = %s THEN f.team_h_difficulty 
                    ELSE f.team_a_difficulty END as difficulty
        FROM fixtures f
        WHERE (f.team_h = %s OR f.team_a = %s)
        AND f.gameweek BETWEEN %s AND %s
        AND f.finished = FALSE
        ORDER BY f.gameweek
    """
    
    fixtures = fixture_agent.db.execute_query(
        query, (team_id, team_id, team_id, current_gameweek, end_gameweek)
    )
    
    if not fixtures:
        return {
            'team_id': team_id,
            'fixture_count': 0,
            'average_difficulty': 3.0,
            'favorability_score': 50.0,
            'recommendation': 'neutral'
        }
    
    # Calculate averages
    difficulties = [f['difficulty'] for f in fixtures]
    avg_difficulty = np.mean(difficulties)
    
    # Simple favorability score
    favorability_score = (6 - avg_difficulty) * 20  # 20-100 scale
    
    # Recommendation
    if avg_difficulty <= 2.0:
        recommendation = 'excellent'
    elif avg_difficulty <= 2.5:
        recommendation = 'good'
    elif avg_difficulty >= 4.0:
        recommendation = 'avoid'
    else:
        recommendation = 'neutral'
    
    return {
        'team_id': team_id,
        'fixture_count': len(fixtures),
        'average_difficulty': round(avg_difficulty, 2),
        'favorability_score': round(favorability_score, 1),
        'easy_fixtures': sum(1 for d in difficulties if d <= 2),
        'hard_fixtures': sum(1 for d in difficulties if d >= 4),
        'recommendation': recommendation
    }


# Test function
def test_fixture_agent():
    """Test the fixture agent functionality"""
    print("🧪 Testing FANTASYPL Fixture Agent...")
    
    # Add src to path for imports
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    
    # Import after adding path
    from agents.data_buff import DatabaseManager, CacheManager
    from config.settings import DATABASE_CONFIG
    
    try:
        # Use config from settings (which reads from .env)
        agent = FixtureAgent(DATABASE_CONFIG, {})
        agent.initialize()
        print("✅ Fixture Agent initialization successful!")
        
        # Test fixture fetching
        agent.fetch_and_store_fixtures()
        print("✅ Fixture data fetching successful!")
        
        # Test analysis
        analyses = agent.analyze_upcoming_fixtures(4)
        print(f"✅ Fixture analysis successful! Analyzed {len(analyses)} fixtures")
        
        # Test fixture runs
        runs = agent.analyze_fixture_runs(4)
        print(f"✅ Fixture runs analysis successful! Analyzed {len(runs)} teams")
        
        # Test best teams
        best_teams = agent.get_best_fixture_teams(4)
        print(f"✅ Best fixture teams analysis successful! Found {len(best_teams)} recommendations")
        
        return True
        
    except Exception as e:
        print(f"❌ Fixture Agent test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_fixture_agent()