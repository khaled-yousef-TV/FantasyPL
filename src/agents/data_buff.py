# FANTASYPL Enhanced Data Buff Agent - Complete Clean Version
# Multi-agent FPL system with historical data, advanced metrics, and caching

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
    print("⚠️  Redis not available. Caching disabled.")

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("⚠️  BeautifulSoup not available. Web scraping limited.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/fantasypl_agent.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class PlayerRecommendation:
    """Data class for player recommendations"""
    player_id: int
    name: str
    position: str
    team: str
    price: float
    predicted_points: float
    confidence_score: int  # 0-100
    risk_level: str  # low/medium/high
    value_rating: float
    form_indicator: str
    key_stats: Dict


class FPLAPIWrapper:
    """Wrapper for FPL API with rate limiting and error handling"""
    
    def __init__(self):
        self.base_url = "https://fantasy.premierleague.com/api"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.last_request_time = 0
        self.min_request_interval = 1.0  # 1 second between requests
    
    def _rate_limit(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
    
    def get_bootstrap_data(self) -> Dict:
        """Get all static FPL data (players, teams, gameweeks)"""
        self._rate_limit()
        try:
            response = self.session.get(f"{self.base_url}/bootstrap-static/")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching bootstrap data: {e}")
            raise
    
    def get_player_details(self, player_id: int) -> Dict:
        """Get detailed player data including gameweek history"""
        self._rate_limit()
        try:
            response = self.session.get(f"{self.base_url}/element-summary/{player_id}/")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching player {player_id} details: {e}")
            raise
    
    def get_fixtures(self) -> List[Dict]:
        """Get all fixtures with difficulty ratings"""
        self._rate_limit()
        try:
            response = self.session.get(f"{self.base_url}/fixtures/")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching fixtures: {e}")
            raise


class CacheManager:
    """Redis cache manager for frequently accessed data"""
    
    def __init__(self, redis_config: Dict):
        self.redis_client = None
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.Redis(**redis_config)
                self.redis_client.ping()
                logger.info("Redis cache connected successfully")
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}. Proceeding without cache.")
                self.redis_client = None
        else:
            logger.info("Redis not available. Proceeding without cache.")
    
    def get(self, key: str) -> Optional[str]:
        """Get cached data"""
        if not self.redis_client:
            return None
        try:
            return self.redis_client.get(key)
        except:
            return None
    
    def set(self, key: str, value: str, expire: int = 3600):
        """Set cached data with expiration"""
        if not self.redis_client:
            return
        try:
            self.redis_client.setex(key, expire, value)
        except:
            pass


class DatabaseManager:
    """Handle PostgreSQL database operations"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.connection = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def create_tables(self):
        """Create database schema for FPL data"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        # Teams table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                short_name VARCHAR(3) NOT NULL,
                strength_overall_home INTEGER,
                strength_overall_away INTEGER,
                strength_attack_home INTEGER,
                strength_attack_away INTEGER,
                strength_defence_home INTEGER,
                strength_defence_away INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Players table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY,
                web_name VARCHAR(50) NOT NULL,
                first_name VARCHAR(50),
                second_name VARCHAR(50),
                team_id INTEGER REFERENCES teams(id),
                element_type INTEGER,  -- 1=GK, 2=DEF, 3=MID, 4=FWD
                now_cost INTEGER,      -- Price in 0.1m units
                total_points INTEGER,
                form DECIMAL(6,1),     -- Increased precision
                selected_by_percent DECIMAL(6,1),  -- Increased precision
                transfers_in INTEGER,
                transfers_out INTEGER,
                goals_scored INTEGER,
                assists INTEGER,
                clean_sheets INTEGER,
                goals_conceded INTEGER,
                saves INTEGER,
                penalties_saved INTEGER,
                penalties_missed INTEGER,
                yellow_cards INTEGER,
                red_cards INTEGER,
                bonus INTEGER,
                influence DECIMAL(8,1),   -- Increased precision
                creativity DECIMAL(8,1),  -- Increased precision
                threat DECIMAL(8,1),      -- Increased precision
                ict_index DECIMAL(8,1),   -- Increased precision
                season VARCHAR(7),     -- e.g., "2024-25"
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(id, season)
            )
        """)
        
        # Gameweek performance table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gameweek_performance (
                id SERIAL PRIMARY KEY,
                player_id INTEGER,
                gameweek INTEGER,
                opponent_team INTEGER,
                was_home BOOLEAN,
                total_points INTEGER,
                minutes INTEGER,
                goals_scored INTEGER,
                assists INTEGER,
                clean_sheets INTEGER,
                goals_conceded INTEGER,
                saves INTEGER,
                penalties_saved INTEGER,
                penalties_missed INTEGER,
                yellow_cards INTEGER,
                red_cards INTEGER,
                bonus INTEGER,
                influence DECIMAL(8,1),   -- Increased precision
                creativity DECIMAL(8,1),  -- Increased precision
                threat DECIMAL(8,1),      -- Increased precision
                ict_index DECIMAL(8,1),   -- Increased precision
                expected_goals DECIMAL(6,2),     -- Increased precision
                expected_assists DECIMAL(6,2),   -- Increased precision
                expected_goal_involvements DECIMAL(6,2),  -- Increased precision
                expected_goals_conceded DECIMAL(6,2),     -- Increased precision
                season VARCHAR(7),
                kickoff_time TIMESTAMP,
                fixture_difficulty INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(player_id, gameweek, season)
            )
        """)
        
        # Fixtures table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fixtures (
                id INTEGER PRIMARY KEY,
                gameweek INTEGER,
                team_h INTEGER,
                team_a INTEGER,
                team_h_difficulty INTEGER,
                team_a_difficulty INTEGER,
                kickoff_time TIMESTAMP,
                finished BOOLEAN DEFAULT FALSE,
                team_h_score INTEGER,
                team_a_score INTEGER,
                season VARCHAR(7),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Player recommendations table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_recommendations (
                id SERIAL PRIMARY KEY,
                player_id INTEGER,
                gameweek INTEGER,
                predicted_points DECIMAL(4,2),
                confidence_score INTEGER,
                risk_level VARCHAR(10),
                value_rating DECIMAL(5,2),
                form_indicator VARCHAR(20),
                fixture_favorability DECIMAL(3,2),
                consistency_score DECIMAL(4,1),
                recommendation_strength INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                season VARCHAR(7)
            )
        """)
        
        self.connection.commit()
        cursor.close()
        logger.info("Database tables created successfully")
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a query and return results"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        return [dict(row) for row in results]
    
    def execute_insert(self, query: str, params: tuple = None):
        """Execute an insert/update query"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        self.connection.commit()
        cursor.close()


class AdvancedAnalytics:
    """Advanced analytics and machine learning predictions"""
    
    def __init__(self, db_manager):
        self.db = db_manager
    
    def calculate_expected_points(self, player_id: int, gameweeks_ahead: int = 5) -> float:
        """Calculate expected points for upcoming gameweeks"""
        
        # Get player's recent performance
        query = """
            SELECT gp.*, 3 as fixture_difficulty
            FROM gameweek_performance gp
            WHERE gp.player_id = %s 
            AND gp.season = '2024-25'
            ORDER BY gp.gameweek DESC
            LIMIT 10
        """
        
        recent_data = self.db.execute_query(query, (player_id,))
        
        if not recent_data:
            return 0.0
        
        df = pd.DataFrame(recent_data)
        
        # Weight recent games more heavily
        weights = np.exp(np.linspace(-1, 0, len(df)))
        weights = weights / weights.sum()
        
        # Calculate weighted averages
        weighted_points = (df['total_points'] * weights).sum()
        
        # Adjust for fixture difficulty (simplified)
        avg_difficulty = df['fixture_difficulty'].mean()
        difficulty_adjustment = 1.0 - (avg_difficulty - 3) * 0.1
        
        # Calculate expected points
        expected_points = weighted_points * difficulty_adjustment
        
        return round(expected_points, 2)
    
    def calculate_value_score(self, player_id: int) -> float:
        """Calculate value score (points per million)"""
        query = """
            SELECT p.now_cost, COALESCE(SUM(gp.total_points), 0) as total_points
            FROM players p
            LEFT JOIN gameweek_performance gp ON p.id = gp.player_id AND gp.season = p.season
            WHERE p.id = %s AND p.season = '2024-25'
            GROUP BY p.id, p.now_cost
        """
        
        result = self.db.execute_query(query, (player_id,))
        if not result:
            return 0.0
        
        price_millions = result[0]['now_cost'] / 10.0
        total_points = result[0]['total_points'] or 0
        
        return round(total_points / price_millions, 2) if price_millions > 0 else 0.0
    
    def calculate_consistency_score(self, player_id: int) -> float:
        """Calculate consistency score (inverse of coefficient of variation)"""
        query = """
            SELECT total_points FROM gameweek_performance 
            WHERE player_id = %s AND season = '2024-25' AND minutes > 0
            ORDER BY gameweek
        """
        
        points_data = self.db.execute_query(query, (player_id,))
        if len(points_data) < 3:
            return 50.0  # Default neutral score
        
        points = [row['total_points'] for row in points_data]
        mean_points = np.mean(points)
        std_points = np.std(points)
        
        if mean_points == 0:
            return 0.0
        
        # Coefficient of variation (lower is more consistent)
        cv = std_points / mean_points
        
        # Convert to consistency score (0-100, higher is more consistent)
        consistency_score = max(0, 100 - (cv * 50))
        
        return round(consistency_score, 1)
    
    def calculate_form_trend(self, player_id: int) -> Tuple[str, float]:
        """Calculate form trend (improving/declining/stable)"""
        query = """
            SELECT total_points, gameweek FROM gameweek_performance 
            WHERE player_id = %s AND season = '2024-25' AND minutes > 0
            ORDER BY gameweek DESC
            LIMIT 8
        """
        
        form_data = self.db.execute_query(query, (player_id,))
        if len(form_data) < 4:
            return "insufficient_data", 0.0
        
        points = [row['total_points'] for row in reversed(form_data)]
        
        # Calculate trend using linear regression
        x = np.arange(len(points))
        slope = np.polyfit(x, points, 1)[0]
        
        # Determine trend
        if slope > 0.5:
            trend = "improving"
        elif slope < -0.5:
            trend = "declining"
        else:
            trend = "stable"
        
        # Calculate form score (recent 4 games vs previous 4)
        if len(points) >= 8:
            recent_avg = np.mean(points[-4:])
            previous_avg = np.mean(points[:4])
            form_score = recent_avg - previous_avg
        else:
            form_score = np.mean(points[-4:])
        
        return trend, round(form_score, 2)


class EnhancedDataBuffAgent:
    """Enhanced Data Buff agent with all advanced features"""
    
    def __init__(self, db_config: Dict[str, str], redis_config: Dict[str, str] = None):
        self.api = FPLAPIWrapper()
        self.db = DatabaseManager(db_config)
        self.cache = CacheManager(redis_config or {})
        self.analytics = AdvancedAnalytics(self.db)
        
        self.current_season = "2024-25"
        self.positions = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
    
    def initialize(self):
        """Initialize the enhanced agent"""
        self.db.create_tables()
        logger.info("Enhanced Data Buff Agent initialized successfully")
    
    def fetch_and_store_bootstrap_data(self):
        """Fetch and store all static FPL data"""
        logger.info("Fetching bootstrap data...")
        data = self.api.get_bootstrap_data()
        
        # Store teams
        teams = data['teams']
        for team in teams:
            query = """
                INSERT INTO teams (id, name, short_name, strength_overall_home, 
                                 strength_overall_away, strength_attack_home, 
                                 strength_attack_away, strength_defence_home, 
                                 strength_defence_away)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    strength_overall_home = EXCLUDED.strength_overall_home,
                    strength_overall_away = EXCLUDED.strength_overall_away,
                    strength_attack_home = EXCLUDED.strength_attack_home,
                    strength_attack_away = EXCLUDED.strength_attack_away,
                    strength_defence_home = EXCLUDED.strength_defence_home,
                    strength_defence_away = EXCLUDED.strength_defence_away
            """
            params = (
                team['id'], team['name'], team['short_name'],
                team['strength_overall_home'], team['strength_overall_away'],
                team['strength_attack_home'], team['strength_attack_away'],
                team['strength_defence_home'], team['strength_defence_away']
            )
            self.db.execute_insert(query, params)
        
        # Store players with better error handling
        players = data['elements']
        for player in players:
            try:
                query = """
                    INSERT INTO players (id, web_name, first_name, second_name, team_id,
                                       element_type, now_cost, total_points, form,
                                       selected_by_percent, transfers_in, transfers_out,
                                       goals_scored, assists, clean_sheets, goals_conceded,
                                       saves, penalties_saved, penalties_missed,
                                       yellow_cards, red_cards, bonus, influence,
                                       creativity, threat, ict_index, season, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (id, season) DO UPDATE SET
                        now_cost = EXCLUDED.now_cost,
                        total_points = EXCLUDED.total_points,
                        form = EXCLUDED.form,
                        selected_by_percent = EXCLUDED.selected_by_percent,
                        transfers_in = EXCLUDED.transfers_in,
                        transfers_out = EXCLUDED.transfers_out,
                        goals_scored = EXCLUDED.goals_scored,
                        assists = EXCLUDED.assists,
                        clean_sheets = EXCLUDED.clean_sheets,
                        goals_conceded = EXCLUDED.goals_conceded,
                        saves = EXCLUDED.saves,
                        penalties_saved = EXCLUDED.penalties_saved,
                        penalties_missed = EXCLUDED.penalties_missed,
                        yellow_cards = EXCLUDED.yellow_cards,
                        red_cards = EXCLUDED.red_cards,
                        bonus = EXCLUDED.bonus,
                        influence = EXCLUDED.influence,
                        creativity = EXCLUDED.creativity,
                        threat = EXCLUDED.threat,
                        ict_index = EXCLUDED.ict_index,
                        updated_at = CURRENT_TIMESTAMP
                """
                
                # Safely convert values with bounds checking
                def safe_decimal(value, max_val=999999.9):
                    try:
                        val = float(value) if value else 0.0
                        return min(val, max_val)  # Cap at maximum
                    except (ValueError, TypeError):
                        return 0.0
                
                params = (
                    player['id'], player['web_name'], player['first_name'],
                    player['second_name'], player['team'], player['element_type'],
                    player['now_cost'], player['total_points'], 
                    safe_decimal(player['form']),
                    safe_decimal(player['selected_by_percent']),
                    player['transfers_in'], player['transfers_out'], 
                    player['goals_scored'], player['assists'],
                    player['clean_sheets'], player['goals_conceded'], player['saves'],
                    player['penalties_saved'], player['penalties_missed'],
                    player['yellow_cards'], player['red_cards'], player['bonus'],
                    safe_decimal(player['influence']),
                    safe_decimal(player['creativity']),
                    safe_decimal(player['threat']),
                    safe_decimal(player['ict_index']),
                    self.current_season
                )
                self.db.execute_insert(query, params)
                
            except Exception as e:
                logger.warning(f"Error storing player {player.get('web_name', 'unknown')}: {e}")
                continue
        
        logger.info(f"Stored {len(teams)} teams and {len(players)} players")
    
    def fetch_player_gameweek_data(self, player_id: int):
        """Fetch and store detailed gameweek data for a specific player"""
        try:
            data = self.api.get_player_details(player_id)
            history = data['history']
            
            for gw in history:
                query = """
                    INSERT INTO gameweek_performance (
                        player_id, gameweek, opponent_team, was_home, total_points,
                        minutes, goals_scored, assists, clean_sheets, goals_conceded,
                        saves, penalties_saved, penalties_missed, yellow_cards,
                        red_cards, bonus, influence, creativity, threat, ict_index,
                        expected_goals, expected_assists, expected_goal_involvements,
                        expected_goals_conceded, season, kickoff_time, fixture_difficulty
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (player_id, gameweek, season) DO UPDATE SET
                        total_points = EXCLUDED.total_points,
                        minutes = EXCLUDED.minutes,
                        goals_scored = EXCLUDED.goals_scored,
                        assists = EXCLUDED.assists,
                        clean_sheets = EXCLUDED.clean_sheets,
                        goals_conceded = EXCLUDED.goals_conceded,
                        saves = EXCLUDED.saves,
                        penalties_saved = EXCLUDED.penalties_saved,
                        penalties_missed = EXCLUDED.penalties_missed,
                        yellow_cards = EXCLUDED.yellow_cards,
                        red_cards = EXCLUDED.red_cards,
                        bonus = EXCLUDED.bonus,
                        influence = EXCLUDED.influence,
                        creativity = EXCLUDED.creativity,
                        threat = EXCLUDED.threat,
                        ict_index = EXCLUDED.ict_index,
                        expected_goals = EXCLUDED.expected_goals,
                        expected_assists = EXCLUDED.expected_assists,
                        expected_goal_involvements = EXCLUDED.expected_goal_involvements,
                        expected_goals_conceded = EXCLUDED.expected_goals_conceded
                """
                params = (
                    player_id, gw['round'], gw['opponent_team'], gw['was_home'],
                    gw['total_points'], gw['minutes'], gw['goals_scored'],
                    gw['assists'], gw['clean_sheets'], gw['goals_conceded'],
                    gw['saves'], gw['penalties_saved'], gw['penalties_missed'],
                    gw['yellow_cards'], gw['red_cards'], gw['bonus'],
                    float(gw['influence']), float(gw['creativity']),
                    float(gw['threat']), float(gw['ict_index']),
                    float(gw['expected_goals']), float(gw['expected_assists']),
                    float(gw['expected_goal_involvements']),
                    float(gw['expected_goals_conceded']), self.current_season,
                    gw['kickoff_time'], gw['difficulty']
                )
                self.db.execute_insert(query, params)
        
        except Exception as e:
            logger.error(f"Error fetching data for player {player_id}: {e}")
    
    def generate_player_recommendations(self, position: str = None, 
                                      max_price: float = None,
                                      gameweeks_ahead: int = 5) -> List[PlayerRecommendation]:
        """Generate comprehensive player recommendations"""
        
        # Build query with filters
        where_conditions = ["p.season = %s"]
        params = [self.current_season]
        
        if position:
            position_id = {v: k for k, v in self.positions.items()}.get(position.upper())
            if position_id:
                where_conditions.append("p.element_type = %s")
                params.append(position_id)
        
        if max_price:
            where_conditions.append("p.now_cost <= %s")
            params.append(int(max_price * 10))  # Convert to API format
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
            SELECT p.*, t.name as team_name
            FROM players p
            JOIN teams t ON p.team_id = t.id
            WHERE {where_clause}
            AND p.total_points > 10  -- Filter out non-playing players
            ORDER BY p.total_points DESC
            LIMIT 100
        """
        
        players = self.db.execute_query(query, params)
        
        recommendations = []
        
        for player in players:
            player_id = player['id']
            
            # Calculate all metrics
            expected_points = self.analytics.calculate_expected_points(player_id, gameweeks_ahead)
            value_score = self.analytics.calculate_value_score(player_id)
            consistency = self.analytics.calculate_consistency_score(player_id)
            form_trend, form_score = self.analytics.calculate_form_trend(player_id)
            
            # Calculate confidence score
            confidence_factors = [
                min(consistency * 0.3, 30),  # Consistency (0-30)
                min(abs(form_score) * 10, 25),  # Form strength (0-25)
                min(expected_points * 5, 25),  # Expected performance (0-25)
                min(player['total_points'] / 5, 20)  # Season performance (0-20)
            ]
            confidence_score = int(sum(confidence_factors))
            
            # Determine risk level
            if consistency > 70 and form_trend != "declining":
                risk_level = "low"
            elif consistency > 40 and expected_points > 3:
                risk_level = "medium"
            else:
                risk_level = "high"
            
            # Create recommendation
            recommendation = PlayerRecommendation(
                player_id=player_id,
                name=player['web_name'],
                position=self.positions[player['element_type']],
                team=player['team_name'],
                price=player['now_cost'] / 10.0,
                predicted_points=expected_points,
                confidence_score=confidence_score,
                risk_level=risk_level,
                value_rating=value_score,
                form_indicator=form_trend,
                key_stats={
                    'total_points': player['total_points'],
                    'form': float(player['form']) if player['form'] else 0,
                    'consistency': consistency,
                    'form_score': form_score,
                    'ownership': float(player['selected_by_percent'])
                }
            )
            
            recommendations.append(recommendation)
        
        # Sort by predicted points and confidence
        recommendations.sort(
            key=lambda x: (x.predicted_points * (x.confidence_score / 100)), 
            reverse=True
        )
        
        return recommendations[:50]  # Return top 50 recommendations
    
    def get_differential_picks(self, max_ownership: float = 10.0) -> List[PlayerRecommendation]:
        """Get low-owned differential picks"""
        query = """
            SELECT p.*, t.name as team_name
            FROM players p
            JOIN teams t ON p.team_id = t.id
            WHERE p.season = %s
            AND p.selected_by_percent < %s
            AND p.total_points > 20
            AND p.now_cost >= 45  -- Minimum price to filter out bench players
            ORDER BY p.total_points DESC
            LIMIT 20
        """
        
        players = self.db.execute_query(query, (self.current_season, max_ownership))
        recommendations = []
        
        for player in players:
            player_id = player['id']
            expected_points = self.analytics.calculate_expected_points(player_id)
            value_score = self.analytics.calculate_value_score(player_id)
            
            recommendation = PlayerRecommendation(
                player_id=player_id,
                name=player['web_name'],
                position=self.positions[player['element_type']],
                team=player['team_name'],
                price=player['now_cost'] / 10.0,
                predicted_points=expected_points,
                confidence_score=75,  # Medium confidence for differentials
                risk_level="medium",
                value_rating=value_score,
                form_indicator="differential",
                key_stats={
                    'total_points': player['total_points'],
                    'ownership': float(player['selected_by_percent']),
                    'differential_score': player['total_points'] / max(float(player['selected_by_percent']), 0.1)
                }
            )
            recommendations.append(recommendation)
        
        return recommendations
    
    def analyze_captain_options(self) -> List[Dict]:
        """Analyze best captain options for upcoming gameweek"""
        query = """
            SELECT p.*, t.name as team_name
            FROM players p
            JOIN teams t ON p.team_id = t.id
            WHERE p.season = %s
            AND p.total_points > 50
            AND p.selected_by_percent > 5  -- Reasonable ownership for captains
            ORDER BY p.total_points DESC
            LIMIT 20
        """
        
        players = self.db.execute_query(query, (self.current_season,))
        captain_options = []
        
        for player in players:
            player_id = player['id']
            expected_points = self.analytics.calculate_expected_points(player_id, 1)  # Next gameweek only
            form_trend, form_score = self.analytics.calculate_form_trend(player_id)
            
            # Captain score calculation
            captain_score = (
                expected_points * 2 +  # Base expected points
                3.0 * 0.5 +  # Fixture difficulty (simplified)
                form_score * 0.3 +  # Recent form
                (float(player['selected_by_percent']) / 100) * 0.2  # Safety (ownership)
            )
            
            captain_option = {
                'player_id': player_id,
                'name': player['web_name'],
                'team': player['team_name'],
                'position': self.positions[player['element_type']],
                'expected_points': expected_points,
                'captain_score': round(captain_score, 2),
                'fixture_favorability': 3.0,  # Simplified
                'form_trend': form_trend,
                'ownership': float(player['selected_by_percent']),
                'safety_level': 'safe' if float(player['selected_by_percent']) > 20 else 'risky'
            }
            
            captain_options.append(captain_option)
        
        # Sort by captain score
        captain_options.sort(key=lambda x: x['captain_score'], reverse=True)
        
        return captain_options[:10]
    
    def enhanced_daily_update(self):
        """Enhanced daily update with all features"""
        logger.info("Starting enhanced daily update...")
        
        try:
            # 1. Update core FPL data
            self.fetch_and_store_bootstrap_data()
            
            # 2. Update gameweek data for sample of players (to avoid long delays)
            self.update_sample_player_data()
            
            logger.info("Enhanced daily update completed successfully")
            
        except Exception as e:
            logger.error(f"Error during daily update: {e}")
            raise
    
    def update_sample_player_data(self):
        """Update gameweek data for a sample of players to avoid timeouts"""
        query = """
            SELECT id, total_points FROM players
            WHERE season = %s 
            AND total_points > 30  -- Focus on active players
            ORDER BY total_points DESC
            LIMIT 50  -- Sample size for testing
        """
        players = self.db.execute_query(query, (self.current_season,))
        
        total_players = len(players)
        logger.info(f"Updating data for {total_players} top players")
        
        for i, player in enumerate(players):
            try:
                if i % 10 == 0:  # Log progress every 10 players
                    logger.info(f"Processing player {i+1}/{total_players}")
                
                self.fetch_player_gameweek_data(player['id'])
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logger.error(f"Error updating player {player['id']}: {e}")
                continue
    
    def export_recommendations_to_json(self, filename: str = None):
        """Export recommendations to JSON file"""
        if not filename:
            filename = f"data/exports/fantasypl_recommendations_{datetime.now().strftime('%Y%m%d')}.json"
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        try:
            logger.info("Generating recommendations for export...")
            recommendations = self.generate_player_recommendations()
            
            if not recommendations:
                logger.warning("No recommendations generated - database might be empty")
                return None
            
            # Convert to JSON-serializable format
            json_data = {
                'generated_at': datetime.now().isoformat(),
                'season': self.current_season,
                'total_recommendations': len(recommendations),
                'recommendations': []
            }
            
            for rec in recommendations:
                json_data['recommendations'].append({
                    'player_id': rec.player_id,
                    'name': rec.name,
                    'position': rec.position,
                    'team': rec.team,
                    'price': rec.price,
                    'predicted_points': rec.predicted_points,
                    'confidence_score': rec.confidence_score,
                    'risk_level': rec.risk_level,
                    'value_rating': rec.value_rating,
                    'form_indicator': rec.form_indicator,
                    'key_stats': rec.key_stats
                })
            
            with open(filename, 'w') as f:
                json.dump(json_data, f, indent=2)
            
            logger.info(f"Recommendations exported to {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error exporting recommendations: {e}")
            logger.error(f"Error details: {str(e)}")
            # Return a basic structure even if recommendations fail
            basic_data = {
                'generated_at': datetime.now().isoformat(),
                'season': self.current_season,
                'error': str(e),
                'recommendations': []
            }
            
            try:
                with open(filename, 'w') as f:
                    json.dump(basic_data, f, indent=2)
                logger.info(f"Basic export file created at {filename}")
            except Exception as file_error:
                logger.error(f"Could not create export file: {file_error}")
            
            return None


# Test function to verify everything works
def test_agent():
    """Test the agent functionality"""
    print("🧪 Testing FANTASYPL Data Buff Agent...")
    
    # Simple database config for testing
    test_db_config = {
        'host': 'localhost',
        'database': 'fantasypl_data',
        'user': 'postgres',
        'password': 'your_password',  # You'll need to update this
        'port': 5432
    }
    
    try:
        # Test initialization
        agent = EnhancedDataBuffAgent(test_db_config, {})
        agent.initialize()
        print("✅ Agent initialization successful!")
        
        # Test API connection
        bootstrap_data = agent.api.get_bootstrap_data()
        print(f"✅ FPL API connection successful! Found {len(bootstrap_data['elements'])} players")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


# Only run test if this file is executed directly
if __name__ == "__main__":
    test_agent()