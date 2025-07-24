#!/usr/bin/env python3
"""
SQL Anywhere to Web API Sync Tool
Connects to SQL Anywhere database via ODBC and syncs data to web API
"""

import json
import logging
import os
import sys
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional

import pyodbc
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class DatabaseConfig:
    """Configuration manager for database and API settings"""
    
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ Configuration file '{self.config_file}' not found!")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in configuration file: {e}")
            sys.exit(1)
    
    @property
    def dsn(self) -> str:
        return self.config["database"]["dsn"]
    
    @property
    def username(self) -> str:
        return self.config["database"]["username"]
    
    @property
    def password(self) -> str:
        return self.config["database"]["password"]
    
    @property
    def api_base_url(self) -> str:
        return self.config["api"]["base_url"]
    
    @property
    def upload_endpoint(self) -> str:
        return self.config["api"]["upload_endpoint"]
    
    @property
    def api_timeout(self) -> int:
        return self.config["api"].get("timeout", 30)
    
    @property
    def table_name(self) -> str:
        return self.config["settings"].get("table_name", "acc_users")
    
    @property
    def batch_size(self) -> int:
        return self.config["settings"].get("batch_size", 1000)


class DatabaseConnector:
    """Handles SQL Anywhere database connections via ODBC"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        
    def connect(self) -> bool:
        """Establish connection to SQL Anywhere database"""
        try:
            # Build connection string
            connection_string = (
                f"DSN={self.config.dsn};"
                f"UID={self.config.username};"
                f"PWD={self.config.password};"
            )
            
            logging.info(f"Connecting to database DSN: {self.config.dsn}")
            self.connection = pyodbc.connect(connection_string, timeout=10)
            logging.info("✅ Successfully connected to database")
            return True
            
        except pyodbc.Error as e:
            logging.error(f"❌ Database connection failed: {e}")
            print(f"❌ Failed to connect to database: {e}")
            return False
    
    def fetch_users(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch user data from acc_users table"""
        if not self.connection:
            logging.error("No database connection available")
            return None
            
        try:
            cursor = self.connection.cursor()
            
            # Execute query to fetch user data
            query = f"SELECT id, pass, role, accountcode FROM {self.config.table_name}"
            logging.info(f"Executing query: {query}")
            
            cursor.execute(query)
            
            # Fetch all rows and convert to list of dictionaries
            columns = [column[0] for column in cursor.description]
            users = []
            
            for row in cursor.fetchall():
                user_dict = {}
                for i, value in enumerate(row):
                    # Handle None values and strip whitespace from strings
                    if value is None:
                        user_dict[columns[i]] = None
                    elif isinstance(value, str):
                        user_dict[columns[i]] = value.strip()
                    else:
                        user_dict[columns[i]] = value
                users.append(user_dict)
            
            logging.info(f"✅ Successfully fetched {len(users)} user records")
            return users
            
        except pyodbc.Error as e:
            logging.error(f"❌ Error fetching users: {e}")
            print(f"❌ Error fetching data from database: {e}")
            return None
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed")


class APIClient:
    """Handles communication with the web API"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
            backoff_factor=1
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'SQL-Anywhere-Sync-Tool/1.0'
        })
        
        return session
    
    def upload_users(self, users: List[Dict[str, Any]]) -> bool:
        """Upload users to web API"""
        try:
            url = self.config.api_base_url + self.config.upload_endpoint
            
            logging.info(f"Uploading {len(users)} users to API: {url}")
            
            # Send POST request with user data
            response = self.session.post(
                url,
                json=users,
                timeout=self.config.api_timeout
            )
            
            if response.status_code in [200, 201]:
                logging.info("✅ Successfully uploaded users to API")
                response_data = response.json()
                print(f"✅ {response_data.get('message', 'Data uploaded successfully')}")
                return True
            else:
                logging.error(f"❌ API upload failed with status {response.status_code}: {response.text}")
                print(f"❌ API upload failed with status {response.status_code}")
                try:
                    error_data = response.json()
                    print(f"Error details: {error_data.get('error', 'Unknown error')}")
                except:
                    print(f"Error response: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            logging.error("❌ API request timed out")
            print("❌ API request timed out. Please check your network connection.")
            return False
        except requests.exceptions.ConnectionError:
            logging.error("❌ Failed to connect to API")
            print("❌ Failed to connect to API. Please check the API URL and your internet connection.")
            return False
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ API request failed: {e}")
            print(f"❌ API request failed: {e}")
            return False


class SyncTool:
    """Main synchronization tool"""
    
    def __init__(self):
        self.config = None
        self.db_connector = None
        self.api_client = None
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_filename = f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        logging.info("=== SQL Anywhere Sync Tool Started ===")
    
    def initialize(self) -> bool:
        """Initialize configuration and connections"""
        try:
            # Load configuration
            self.config = DatabaseConfig()
            
            # Initialize database connector
            self.db_connector = DatabaseConnector(self.config)
            
            # Initialize API client
            self.api_client = APIClient(self.config)
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Initialization failed: {e}")
            print(f"❌ Initialization failed: {e}")
            return False
    
    def validate_user_data(self, users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate and clean user data"""
        valid_users = []
        
        for i, user in enumerate(users):
            # Check required fields
            if not user.get('id'):
                logging.warning(f"Skipping user at index {i}: missing 'id' field")
                continue
                
            if not user.get('pass'):
                logging.warning(f"Skipping user at index {i}: missing 'pass' field")
                continue
            
            # Ensure all required fields are present
            cleaned_user = {
                'id': str(user['id']).strip(),
                'pass': str(user['pass']).strip(),
                'role': user.get('role', '').strip() if user.get('role') else None,
                'accountcode': user.get('accountcode', '').strip() if user.get('accountcode') else None
            }
            
            valid_users.append(cleaned_user)
        
        logging.info(f"Validated {len(valid_users)} out of {len(users)} users")
        return valid_users
    
    def run(self) -> bool:
        """Execute the sync process"""
        try:
            print("🔄 Starting SQL Anywhere to Web API sync...")
            
            # Initialize
            if not self.initialize():
                return False
            
            # Connect to database
            if not self.db_connector.connect():
                return False
            
            # Fetch users from database
            print("📊 Fetching user data from database...")
            users = self.db_connector.fetch_users()
            
            if users is None:
                return False
            
            if len(users) == 0:
                print("ℹ️  No users found in database")
                return True
            
            print(f"📊 Found {len(users)} users in database")
            
            # Validate user data
            valid_users = self.validate_user_data(users)
            
            if len(valid_users) == 0:
                print("❌ No valid users to sync")
                return False
            
            if len(valid_users) != len(users):
                print(f"⚠️  {len(users) - len(valid_users)} users were skipped due to validation errors")
            
            # Upload to API
            print("🚀 Uploading data to web API...")
            success = self.api_client.upload_users(valid_users)
            
            return success
            
        except KeyboardInterrupt:
            print("\n❌ Sync cancelled by user")
            logging.info("Sync cancelled by user")
            return False
        except Exception as e:
            logging.error(f"❌ Unexpected error: {e}")
            logging.error(traceback.format_exc())
            print(f"❌ An unexpected error occurred: {e}")
            return False
        finally:
            # Cleanup
            if self.db_connector:
                self.db_connector.close()
    
    def run_interactive(self):
        """Run with user interaction"""
        print("=" * 60)
        print("    SQL Anywhere to Web API Sync Tool")
        print("=" * 60)
        print()
        
        try:
            success = self.run()
            
            if success:
                print("\n✅ Sync completed successfully!")
                logging.info("=== Sync completed successfully ===")
            else:
                print("\n❌ Sync failed!")
                logging.info("=== Sync failed ===")
            
        except Exception as e:
            print(f"\n❌ Critical error: {e}")
            logging.error(f"Critical error: {e}")
        
        # Wait for user input before closing (useful for double-click execution)
        print("\nPress Enter to exit...")
        input()


def main():
    """Main entry point"""
    sync_tool = SyncTool()
    sync_tool.run_interactive()


if __name__ == "__main__":
    main()




#sajith