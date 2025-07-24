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
    def upload_endpoints(self) -> Dict[str, str]:
        return self.config["api"]["upload_endpoints"]
    
    @property
    def api_timeout(self) -> int:
        return self.config["api"].get("timeout", 30)
    
    @property
    def table_name_users(self) -> str:
        return self.config["settings"].get("table_name_users", "acc_users")
    
    @property
    def table_name_misel(self) -> str:
        return self.config["settings"].get("table_name_misel", "misel")
    
    @property
    def batch_size(self) -> int:
        return self.config["settings"].get("batch_size", 1000)
    
    @property
    def log_level(self) -> str:
        return self.config["settings"].get("log_level", "INFO")


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
            query = f"SELECT id, pass, role, accountcode FROM {self.config.table_name_users}"
            logging.info(f"Executing query: {query}")
            
            cursor.execute(query)
            
            # Fetch all rows and convert to list of dictionaries
            columns = [column[0] for column in cursor.description]
            users = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            logging.info(f"✅ Successfully fetched {len(users)} user records")
            return users
            
        except pyodbc.Error as e:
            logging.error(f"❌ Error fetching users: {e}")
            print(f"❌ Error fetching data from database: {e}")
            return None
    
    def fetch_misel(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch misel data from misel table"""
        if not self.connection:
            logging.error("No database connection available")
            return None
            
        try:
            cursor = self.connection.cursor()
            
            # Execute query to fetch misel data
            query = f"SELECT firm_name, address, phones, mobile, address1, address2, address3, pagers, tinno FROM {self.config.table_name_misel}"
            logging.info(f"Executing query: {query}")
            
            cursor.execute(query)
            
            # Fetch all rows and convert to list of dictionaries
            columns = [column[0] for column in cursor.description]
            misel = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            logging.info(f"✅ Successfully fetched {len(misel)} misel records")
            return misel
            
        except pyodbc.Error as e:
            logging.error(f"❌ Error fetching misel: {e}")
            print(f"❌ Error fetching misel data from database: {e}")
            return None
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed")


class WebAPIClient:
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
            url = self.config.api_base_url + self.config.upload_endpoints["users"]
            
            logging.info(f"Uploading {len(users)} users to API: {url}")
            
            # Send POST request with user data
            response = self.session.post(
                url,
                json=users,
                timeout=self.config.api_timeout
            )
            
            if response.status_code in [200, 201]:
                logging.info("✅ Successfully uploaded users to API")
                try:
                    response_data = response.json()
                    print(f"✅ Users: {response_data.get('message', 'Data uploaded successfully')}")
                except:
                    print("✅ Users data uploaded successfully")
                return True
            else:
                logging.error(f"❌ Users API upload failed with status {response.status_code}: {response.text}")
                print(f"❌ Users API upload failed with status {response.status_code}")
                try:
                    error_data = response.json()
                    print(f"Error details: {error_data.get('error', 'Unknown error')}")
                except:
                    print(f"Error response: {response.text}")
                return False
        except requests.exceptions.Timeout:
            logging.error("❌ Users API request timed out")
            print("❌ Users API request timed out. Please check your network connection.")
            return False
        except requests.exceptions.ConnectionError:
            logging.error("❌ Failed to connect to Users API")
            print("❌ Failed to connect to Users API. Please check the API URL and your internet connection.")
            return False
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Users API request failed: {e}")
            print(f"❌ Users API request failed: {e}")
            return False
    
    def upload_misel(self, misel: List[Dict[str, Any]]) -> bool:
        """Upload misel to web API"""
        try:
            url = self.config.api_base_url + self.config.upload_endpoints["misel"]
            
            logging.info(f"Uploading {len(misel)} misel records to API: {url}")
            
            # Send POST request with misel data
            response = self.session.post(
                url,
                json=misel,
                timeout=self.config.api_timeout
            )
            
            if response.status_code in [200, 201]:
                logging.info("✅ Successfully uploaded misel to API")
                try:
                    response_data = response.json()
                    print(f"✅ Misel: {response_data.get('message', 'Data uploaded successfully')}")
                except:
                    print("✅ Misel data uploaded successfully")
                return True
            else:
                logging.error(f"❌ Misel API upload failed with status {response.status_code}: {response.text}")
                print(f"❌ Misel API upload failed with status {response.status_code}")
                try:
                    error_data = response.json()
                    print(f"Error details: {error_data.get('error', 'Unknown error')}")
                except:
                    print(f"Error response: {response.text}")
                return False
        except requests.exceptions.Timeout:
            logging.error("❌ Misel API request timed out")
            print("❌ Misel API request timed out. Please check your network connection.")
            return False
        except requests.exceptions.ConnectionError:
            logging.error("❌ Failed to connect to Misel API")
            print("❌ Failed to connect to Misel API. Please check the API URL and your internet connection.")
            return False
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Misel API request failed: {e}")
            print(f"❌ Misel API request failed: {e}")
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

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)  # Only output to console
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
            self.api_client = WebAPIClient(self.config)
            
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
        
    def validate_misel_data(self, misel: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate and clean misel data"""
        valid_misel = []
        
        for i, entry in enumerate(misel):
            # Check required fields
            if not entry.get('firm_name'):
                logging.warning(f"Skipping misel entry at index {i}: missing 'firm_name' field")
                continue
            
            # Ensure all required fields are present
            cleaned_entry = {
                'firm_name': str(entry['firm_name']).strip(),
                'address': str(entry.get('address', '') or '').strip(),
                'phones': str(entry.get('phones', '') or '').strip(),
                'mobile': str(entry.get('mobile', '') or '').strip(),
                'address1': str(entry.get('address1', '') or '').strip(),
                'address2': str(entry.get('address2', '') or '').strip(),
                'address3': str(entry.get('address3', '') or '').strip(),
                'pagers': str(entry.get('pagers', '') or '').strip(),
                'tinno': str(entry.get('tinno', '') or '').strip(),
            }
            
            valid_misel.append(cleaned_entry)
        
        logging.info(f"Validated {len(valid_misel)} out of {len(misel)} misel entries")
        return valid_misel
    
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
            
            # Track success status for both syncs
            users_success = False
            misel_success = False
            
            # Fetch and sync users from database
            print("📊 Fetching user data from database...")
            users = self.db_connector.fetch_users()
            
            if users is not None:
                if len(users) == 0:
                    print("ℹ️  No users found in database")
                    users_success = True
                else:
                    print(f"📊 Found {len(users)} users in database")
                    
                    # Validate user data
                    valid_users = self.validate_user_data(users)
                    
                    if len(valid_users) == 0:
                        print("❌ No valid users to sync")
                    else:
                        if len(valid_users) != len(users):
                            print(f"⚠️  {len(users) - len(valid_users)} users were skipped due to validation errors")
                        
                        # Upload users to API
                        print("🚀 Uploading user data to web API...")
                        users_success = self.api_client.upload_users(valid_users)
            
            # Fetch and sync misel from database
            print("📊 Fetching misel data from database...")
            misel = self.db_connector.fetch_misel()
            
            if misel is not None:
                if len(misel) == 0:
                    print("ℹ️  No misel entries found in database")
                    misel_success = True
                else:
                    print(f"📊 Found {len(misel)} misel entries in database")
                    
                    # Validate misel data
                    valid_misel = self.validate_misel_data(misel)
                    
                    if len(valid_misel) == 0:
                        print("❌ No valid misel entries to sync")
                    else:
                        if len(valid_misel) != len(misel):
                            print(f"⚠️  {len(misel) - len(valid_misel)} misel entries were skipped due to validation errors")
                        
                        # Upload misel to API
                        print("🚀 Uploading misel data to web API...")
                        misel_success = self.api_client.upload_misel(valid_misel)
            
            # Return success only if both syncs succeeded (or had no data to sync)
            return users_success and misel_success
            
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