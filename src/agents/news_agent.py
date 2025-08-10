#!/usr/bin/env python3
"""
Modified News & Sentiment Agent for FANTASYPL Integration
Simplified and optimized for integration with existing agents
"""

import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import re
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import time
from dataclasses import dataclass
from enum import Enum
import logging
from fuzzywuzzy import fuzz
import hashlib

# Sentiment analysis imports (optional)
try:
    from textblob import TextBlob
    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False
    print("⚠️ TextBlob not available, using basic sentiment analysis")

# ============= Data Classes =============

class InjuryStatus(Enum):
    """Injury severity levels"""
    OUT = "out"
    DOUBTFUL = "doubtful"
    QUESTIONABLE = "questionable"
    PROBABLE = "probable"
    FIT = "fit"
    SUSPENDED = "suspended"

class ManagerSentiment(Enum):
    """Manager sentiment towards player"""
    VERY_POSITIVE = "very_positive"
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"
    VERY_NEGATIVE = "very_negative"

@dataclass
class PlayerNews:
    """Player news/injury data"""
    player_name: str
    team: str
    status: InjuryStatus
    injury_type: Optional[str]
    expected_return: Optional[str]
    last_updated: datetime
    source: str
    confidence_score: float
    manager_sentiment: Optional[ManagerSentiment] = None
    play_probability: float = 50.0

# ============= Simplified News Agent =============

class NewsAgent:
    """
    Simplified News Agent for FANTASYPL system integration
    """
    
    def __init__(self, db_config: dict, redis_config: dict = None):
        """Initialize the News Agent"""
        self.db_config = db_config
        self.redis_config = redis_config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data/logs/news_agent.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('NewsAgent')
        
        # Team name mappings
        self.team_mappings = {
            'Manchester United': 'Man Utd',
            'Manchester City': 'Man City',
            'Tottenham Hotspur': 'Spurs',
            'Tottenham': 'Spurs',
            'Newcastle United': 'Newcastle',
            'Nottingham Forest': "Nott'm Forest",
            'Wolverhampton Wanderers': 'Wolves',
            'Brighton & Hove Albion': 'Brighton',
            'Leicester City': 'Leicester',
            'West Ham United': 'West Ham'
        }
        
        # Injury keywords
        self.injury_keywords = {
            InjuryStatus.OUT: ['out', 'ruled out', 'sidelined', 'unavailable'],
            InjuryStatus.DOUBTFUL: ['doubt', 'doubtful', 'major doubt', 'unlikely'],
            InjuryStatus.QUESTIONABLE: ['questionable', 'uncertain', '50-50'],
            InjuryStatus.PROBABLE: ['probable', 'likely', 'expected to play'],
            InjuryStatus.FIT: ['fit', 'available', 'returned to training'],
            InjuryStatus.SUSPENDED: ['suspended', 'ban', 'banned', 'red card']
        }

    def initialize(self):
        """Initialize database tables"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Simplified player news table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS player_news (
                    id SERIAL PRIMARY KEY,
                    player_name VARCHAR(100),
                    team VARCHAR(50),
                    status VARCHAR(30),
                    injury_type VARCHAR(100),
                    expected_return VARCHAR(100),
                    play_probability FLOAT,
                    manager_sentiment VARCHAR(30),
                    source VARCHAR(100),
                    confidence_score FLOAT,
                    last_updated TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    news_hash VARCHAR(64) UNIQUE
                )
            """)
            
            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_player_news_name ON player_news(player_name)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_player_news_status ON player_news(status)")
            
            conn.commit()
            self.logger.info("News Agent database tables initialized")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
        finally:
            cur.close()
            conn.close()

    def daily_update(self):
        """Daily update - simplified for integration"""
        self.logger.info("News Agent - Starting daily update...")
        
        try:
            # Scrape injury news (simplified)
            injuries = self._scrape_basic_injuries()
            
            # Save to database
            for injury in injuries:
                self._save_player_news(injury)
            
            # Clean old data
            self._cleanup_old_data()
            
            self.logger.info(f"News Agent - Update complete. Processed {len(injuries)} items")
            
        except Exception as e:
            self.logger.error(f"Error in daily update: {e}")

    def _scrape_basic_injuries(self) -> List[PlayerNews]:
        """Simplified injury scraping"""
        injuries = []
        
        # This is a simplified version - in production, you'd scrape actual sites
        # For now, return sample data or implement basic scraping
        
        try:
            # Example: Scrape a simple injury list page
            url = "https://www.premierinjuries.com/injury-table.php"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                # Parse injuries (simplified)
                # This would need actual parsing logic based on the site structure
                pass
            
        except Exception as e:
            self.logger.error(f"Error scraping injuries: {e}")
        
        return injuries

    def get_excluded_players(self, min_probability: float = 0.3) -> List[Dict]:
        """Get players to exclude from selection"""
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cur.execute("""
                SELECT DISTINCT ON (player_name, team)
                    player_name,
                    team,
                    status,
                    injury_type,
                    expected_return,
                    play_probability,
                    manager_sentiment
                FROM player_news
                WHERE play_probability < %s
                    AND last_updated > NOW() - INTERVAL '7 days'
                ORDER BY player_name, team, last_updated DESC
            """, (min_probability,))
            
            return cur.fetchall()
            
        except Exception as e:
            self.logger.error(f"Error getting excluded players: {e}")
            return []
        finally:
            cur.close()
            conn.close()

    def get_favored_players(self, min_sentiment: float = 0.7) -> List[Dict]:
        """Get players with positive manager sentiment"""
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cur.execute("""
                SELECT DISTINCT ON (player_name, team)
                    player_name,
                    team,
                    status,
                    play_probability,
                    manager_sentiment,
                    confidence_score
                FROM player_news
                WHERE play_probability >= %s
                    AND last_updated > NOW() - INTERVAL '7 days'
                    AND (manager_sentiment = 'positive' OR manager_sentiment = 'very_positive')
                ORDER BY player_name, team, last_updated DESC
            """, (min_sentiment,))
            
            return cur.fetchall()
            
        except Exception as e:
            self.logger.error(f"Error getting favored players: {e}")
            return []
        finally:
            cur.close()
            conn.close()

    def get_injury_report(self) -> Dict:
        """Get comprehensive injury report for integration"""
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Get all recent injury news
            cur.execute("""
                SELECT DISTINCT ON (player_name, team)
                    player_name,
                    team,
                    status,
                    injury_type,
                    expected_return,
                    play_probability,
                    last_updated
                FROM player_news
                WHERE last_updated > NOW() - INTERVAL '7 days'
                ORDER BY player_name, team, last_updated DESC
            """)
            
            all_news = cur.fetchall()
            
            # Categorize by status
            report = {
                'out': [],
                'doubtful': [],
                'questionable': [],
                'probable': [],
                'suspended': [],
                'last_updated': datetime.now().isoformat()
            }
            
            for news in all_news:
                status = news['status']
                if status in report:
                    report[status].append({
                        'name': news['player_name'],
                        'team': news['team'],
                        'injury': news['injury_type'],
                        'return': news['expected_return'],
                        'probability': news['play_probability']
                    })
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error getting injury report: {e}")
            return {}
        finally:
            cur.close()
            conn.close()

    def _save_player_news(self, news: PlayerNews):
        """Save player news to database"""
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        
        try:
            # Create unique hash
            news_text = f"{news.player_name}{news.team}{news.status}{news.injury_type}"
            news_hash = hashlib.sha256(news_text.encode()).hexdigest()
            
            cur.execute("""
                INSERT INTO player_news 
                (player_name, team, status, injury_type, expected_return,
                 play_probability, manager_sentiment, source, confidence_score, 
                 last_updated, news_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (news_hash) DO UPDATE
                SET status = EXCLUDED.status,
                    play_probability = EXCLUDED.play_probability,
                    last_updated = EXCLUDED.last_updated
            """, (
                news.player_name, news.team, news.status.value,
                news.injury_type, news.expected_return, news.play_probability,
                news.manager_sentiment.value if news.manager_sentiment else None,
                news.source, news.confidence_score, news.last_updated, news_hash
            ))
            
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error saving player news: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def _cleanup_old_data(self, days: int = 30):
        """Remove old news data"""
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        
        try:
            cur.execute("""
                DELETE FROM player_news 
                WHERE last_updated < NOW() - INTERVAL '%s days'
            """, (days,))
            
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def export_news_analysis_to_json(self, filepath: str = "data/exports/news_analysis.json"):
        """Export news analysis to JSON for integration"""
        analysis = {
            'generated_at': datetime.now().isoformat(),
            'injury_report': self.get_injury_report(),
            'excluded_players': self.get_excluded_players(),
            'favored_players': self.get_favored_players()
        }
        
        with open(filepath, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        
        self.logger.info(f"News analysis exported to {filepath}")
        return analysis

    def get_player_status(self, player_name: str, team: str = None) -> Dict:
        """Get specific player's injury/news status"""
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            query = """
                SELECT * FROM player_news
                WHERE LOWER(player_name) = LOWER(%s)
            """
            params = [player_name]
            
            if team:
                query += " AND LOWER(team) = LOWER(%s)"
                params.append(team)
            
            query += " ORDER BY last_updated DESC LIMIT 1"
            
            cur.execute(query, params)
            return cur.fetchone() or {}
            
        except Exception as e:
            self.logger.error(f"Error getting player status: {e}")
            return {}
        finally:
            cur.close()
            conn.close()