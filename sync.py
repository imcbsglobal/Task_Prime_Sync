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
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
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
    def dsn(self): return self.config["database"]["dsn"]
    @property
    def username(self): return self.config["database"]["username"]
    @property
    def password(self): return self.config["database"]["password"]
    @property
    def api_base_url(self): return self.config["api"]["base_url"]
    @property
    def upload_endpoints(self): return self.config["api"]["upload_endpoints"]
    @property
    def api_timeout(self): return self.config["api"].get("timeout", 30)
    @property
    def client_id(self): return self.config["settings"]["client_id"]
    @property
    def table_name_users(self): return self.config["settings"].get("table_name_users", "acc_users")
    @property
    def table_name_misel(self): return self.config["settings"].get("table_name_misel", "misel")
    @property
    def batch_size(self): return self.config["settings"].get("batch_size", 1000)
    @property
    def large_table_batch_size(self): return self.config["settings"].get("large_table_batch_size", 500)
    @property
    def log_level(self): return self.config["settings"].get("log_level", "INFO")


class DatabaseConnector:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None

    def connect(self) -> bool:
        try:
            conn_str = f"DSN={self.config.dsn};UID={self.config.username};PWD={self.config.password};"
            logging.info(f"Connecting to database DSN: {self.config.dsn}")
            self.connection = pyodbc.connect(conn_str, timeout=10)
            logging.info("✅ Successfully connected to database")
            return True
        except pyodbc.Error as e:
            logging.error(f"❌ Database connection failed: {e}")
            print(f"❌ Failed to connect to database: {e}")
            return False

    # ------------------------------------------------------------------
    # NEW FETCHER (acc_tt_servicemaster)  -- 3 fields only
    # ------------------------------------------------------------------
    def fetch_accttservicemaster(self) -> Optional[List[Dict[str, Any]]]:
        try:
            cursor = self.connection.cursor()
            query = """
                SELECT slno, type, code, name
                FROM dba.acc_tt_servicemaster
                WHERE UPPER(TRIM(type)) = 'AREA'
            """
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"❌ Failed fetching acc_tt_servicemaster: {e}")
            return None

    # ------------------------------------------------------------------
    # EXISTING FETCHERS (unchanged)
    # ------------------------------------------------------------------
    def fetch_users(self) -> Optional[List[Dict[str, Any]]]:
        try:
            cursor = self.connection.cursor()
            query = f"SELECT id, pass, role, accountcode FROM {self.config.table_name_users}"
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"❌ Failed fetching users: {e}")
            return None

    def fetch_misel(self) -> Optional[List[Dict[str, Any]]]:
        try:
            cursor = self.connection.cursor()
            query = f"SELECT firm_name, address, phones, mobile, address1, address2, address3, pagers, tinno FROM {self.config.table_name_misel}"
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"❌ Failed fetching misel: {e}")
            return None

    def fetch_acc_master(self) -> Optional[List[Dict[str, Any]]]:
        try:
            cursor = self.connection.cursor()
            query = """
                SELECT 
                    acc_master.code,
                    acc_master.name,
                    acc_master.opening_balance,
                    acc_master.debit,
                    acc_master.credit,
                    acc_master.place,
                    acc_master.phone2,
                    acc_departments.department AS openingdepartment,
                    COALESCE(acc_tt_servicemaster.name, 'No Area') AS area
                FROM acc_master
                LEFT JOIN acc_departments 
                    ON acc_master.openingdepartment = acc_departments.department_id
                LEFT JOIN acc_tt_servicemaster
                    ON acc_master.area = acc_tt_servicemaster.code
                WHERE acc_master.super_code = 'DEBTO';
            """
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Debug logging for area field
            logging.info(f"📊 Fetched {len(results)} acc_master records")
            area_count = sum(1 for r in results if r.get('area') and str(r['area']).strip())
            logging.info(f"🔍 Records with non-empty area field: {area_count}")
            
            # Log first few area values for debugging
            for i, record in enumerate(results[:5]):
                area_value = record.get('area')
                logging.info(f"🔎 Record {i+1} - Code: {record.get('code')}, Area: '{area_value}' (type: {type(area_value)})")
            
            return results
        except Exception as e:
            logging.error(f"❌ Failed fetching acc_master: {e}")
            logging.error(f"{traceback.format_exc()}")
            return None

    def fetch_acc_ledgers(self) -> Optional[List[Dict[str, Any]]]:
        try:
            cursor = self.connection.cursor()

            logging.info("Checking acc_ledgers table structure...")
            cursor.execute("SELECT TOP 1 * FROM acc_ledgers")
            logging.info(f"acc_ledgers columns: {[col[0] for col in cursor.description]}")
            cursor.fetchall()

            # Debug: log a few code samples
            logging.info("🧪 Debug: Sampling acc_master codes with DEBTO...")
            cursor.execute("SELECT TOP 5 code FROM acc_master WHERE super_code = 'DEBTO'")
            for row in cursor.fetchall():
                logging.info(f"🔎 acc_master code: [{row[0]}]")

            logging.info("🧪 Debug: Sampling acc_ledgers codes...")
            cursor.execute("SELECT TOP 5 code FROM acc_ledgers")
            for row in cursor.fetchall():
                logging.info(f"🔎 acc_ledgers code: [{row[0]}]")

            # Use IN instead of JOIN
            query = """
                SELECT
                    code,
                    particulars,
                    debit,
                    credit,
                    entry_mode,
                    "date" AS entry_date,
                    voucher_no,
                    narration
                FROM acc_ledgers
                WHERE TRIM(code) IN (
                    SELECT TRIM(code) FROM acc_master WHERE TRIM(super_code) = 'DEBTO'
                )
            """

            logging.info("Trying simplified IN query...")
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logging.info(f"✅ IN-query succeeded! Returned {len(result)} records")
            return result

        except Exception as e:
            logging.error(f"❌ Critical error in fetch_acc_ledgers: {e}")
            logging.error(f"{traceback.format_exc()}")
            return None

    def fetch_acc_invmast(self) -> Optional[List[Dict[str, Any]]]:
        try:
            cursor = self.connection.cursor()
            
            # Try different query variations for acc_invmast
            queries_to_try = [
                # Option 1: With DBA schema prefix
                """
                SELECT
                    inv.modeofpayment,
                    inv.customerid,
                    inv.invdate,
                    inv.nettotal,
                    inv.paid,
                    inv.type || '-' || inv.billno AS bill_ref
                FROM DBA.acc_invmast AS inv
                INNER JOIN DBA.acc_master AS cust
                    ON inv.customerid = cust.code
                WHERE cust.super_code = 'DEBTO'
                AND inv.paid < inv.nettotal
                AND inv.modeofpayment = 'C'
                """,
                
                # Option 2: Without DBA schema prefix
                """
                SELECT
                    inv.modeofpayment,
                    inv.customerid,
                    inv.invdate,
                    inv.nettotal,
                    inv.paid,
                    CONCAT(inv.type, '-', inv.billno) AS bill_ref
                FROM acc_invmast AS inv
                INNER JOIN acc_master AS cust
                    ON inv.customerid = cust.code
                WHERE cust.super_code = 'DEBTO'
                AND inv.paid < inv.nettotal
                AND inv.modeofpayment = 'C'
                """,
            ]
            
            for i, query in enumerate(queries_to_try, 1):
                try:
                    logging.info(f"Trying acc_invmast query variation {i}...")
                    cursor.execute(query)
                    columns = [column[0] for column in cursor.description]
                    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    logging.info(f"✅ acc_invmast query variation {i} succeeded! Returned {len(result)} records")
                    return result
                except Exception as query_e:
                    logging.error(f"❌ acc_invmast query variation {i} failed: {query_e}")
                    continue
            
            logging.error("❌ All acc_invmast query variations failed. Returning empty list.")
            return []
            
        except Exception as e:
            logging.error(f"❌ Failed fetching acc_invmast: {e}")
            return None

    def fetch_cashandbankaccmaster(self) -> Optional[List[Dict[str, Any]]]:
        try:
            cursor = self.connection.cursor()
            query = """
                SELECT code, name, super_code, opening_balance, opening_date, debit, credit
                FROM acc_master
                WHERE super_code IN ('CASH', 'BANK')
            """
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"❌ Failed fetching cashandbankaccmaster: {e}")
            return None

    def close(self):
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed")


class WebAPIClient:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({'Content-Type': 'application/json'})
        return session

    # ------------------------------------------------------------------
    # NEW UPLOADER (acc_tt_servicemaster)
    # ------------------------------------------------------------------
    def upload_accttservicemaster(self, rows: List[Dict[str, Any]]) -> bool:
        url = f"{self.config.api_base_url}/upload-accttservicemaster/?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=rows, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ acc_tt_servicemaster uploaded successfully")
                return True
            else:
                logging.error(f"❌ acc_tt_servicemaster upload failed: {res.status_code} – {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_accttservicemaster: {e}")
            return False

    # ------------------------------------------------------------------
    # EXISTING UPLOADERS (unchanged)
    # ------------------------------------------------------------------
    def upload_users(self, users: List[Dict[str, Any]]) -> bool:
        url = f"{self.config.api_base_url}{self.config.upload_endpoints['users']}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=users, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ Users uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_users: {e}")
            return False

    def upload_misel(self, misel: List[Dict[str, Any]]) -> bool:
        url = f"{self.config.api_base_url}{self.config.upload_endpoints['misel']}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=misel, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ Misel uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_misel: {e}")
            return False

    def upload_acc_master(self, acc_master: List[Dict[str, Any]]) -> bool:
        url = f"{self.config.api_base_url}{self.config.upload_endpoints['acc_master']}?client_id={self.config.client_id}&force_clear=true"
        try:
            # For acc_master, always clear existing data first by sending empty array
            logging.info("🧹 Clearing existing acc_master data...")
            clear_res = self.session.post(url, json=[], timeout=60)
            
            if clear_res.status_code not in [200, 201]:
                logging.error(f"❌ Failed to clear existing acc_master data: {clear_res.status_code} - {clear_res.text}")
                return False
            
            # Now upload the new data
            logging.info(f"📤 Uploading {len(acc_master)} acc_master records...")
            res = self.session.post(url, json=acc_master, timeout=self.config.api_timeout)
            
            if res.status_code in [200, 201]:
                logging.info("✅ Acc_Master uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
                
        except Exception as e:
            logging.error(f"❌ Exception in upload_acc_master: {e}")
            return False

    def _upload_in_batches(self, endpoint_key: str, data: List[Dict[str, Any]], batch_size: int = None) -> bool:
        """Upload large datasets in batches to avoid timeouts"""
        if not data:
            return True
        
        if batch_size is None:
            batch_size = self.config.batch_size
            
        total_records = len(data)
        url = f"{self.config.api_base_url}{self.config.upload_endpoints[endpoint_key]}?client_id={self.config.client_id}"
        
        # For large datasets, clear existing data first with empty batch
        if total_records > batch_size:
            try:
                logging.info(f"🧹 Clearing existing {endpoint_key} data...")
                res = self.session.post(url, json=[], timeout=60)  # Extended timeout for clearing
                if res.status_code not in [200, 201]:
                    logging.error(f"❌ Failed to clear existing data: {res.status_code} - {res.text}")
            except Exception as e:
                logging.error(f"❌ Exception clearing data: {e}")
        
        # Process in batches
        success_count = 0
        for i in range(0, total_records, batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_records + batch_size - 1) // batch_size
            
            try:
                logging.info(f"📤 Uploading {endpoint_key} batch {batch_num}/{total_batches} ({len(batch)} records)")
                
                # Use longer timeout for large batches
                timeout = min(120, max(60, len(batch) // 10))
                
                # For first batch of large dataset, it will replace. For subsequent batches, we append
                batch_url = url
                if total_records > batch_size and i > 0:
                    # Add append parameter for subsequent batches
                    batch_url = f"{url}&append=true"
                
                res = self.session.post(batch_url, json=batch, timeout=timeout)
                
                if res.status_code in [200, 201]:
                    success_count += len(batch)
                    logging.info(f"✅ Batch {batch_num}/{total_batches} uploaded successfully")
                else:
                    logging.error(f"❌ Batch {batch_num} failed: {res.status_code} - {res.text}")
                    return False
                    
            except Exception as e:
                logging.error(f"❌ Exception in batch {batch_num}: {e}")
                return False
        
        logging.info(f"✅ {endpoint_key.title()} uploaded successfully ({success_count}/{total_records} records)")
        return True

    def upload_acc_ledgers(self, acc_ledgers: List[Dict[str, Any]]) -> bool:
        return self._upload_in_batches('acc_ledgers', acc_ledgers, self.config.large_table_batch_size)

    def upload_acc_invmast(self, acc_invmast: List[Dict[str, Any]]) -> bool:
        # Small dataset, use direct upload
        url = f"{self.config.api_base_url}{self.config.upload_endpoints['acc_invmast']}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=acc_invmast, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ AccInvmast uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_acc_invmast: {e}")
            return False

    def upload_cashandbankaccmaster(self, cashandbankaccmaster: List[Dict[str, Any]]) -> bool:
        url = f"{self.config.api_base_url}{self.config.upload_endpoints['cashandbankaccmaster']}?client_id={self.config.client_id}"
        try:
            res = self.session.post(url, json=cashandbankaccmaster, timeout=self.config.api_timeout)
            if res.status_code in [200, 201]:
                logging.info("✅ CashAndBankAccMaster uploaded successfully")
                return True
            else:
                logging.error(f"❌ Upload failed: {res.status_code} - {res.text}")
                return False
        except Exception as e:
            logging.error(f"❌ Exception in upload_cashandbankaccmaster: {e}")
            return False


class SyncTool:
    def __init__(self):
        self.config = None
        self.db_connector = None
        self.api_client = None
        self._setup_logging()

    def _setup_logging(self):
        level = logging.INFO
        if self.config and self.config.log_level:
            level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
        logging.info("=== SQL Anywhere Sync Tool Started ===")

    def initialize(self) -> bool:
        try:
            self.config = DatabaseConfig()
            self.db_connector = DatabaseConnector(self.config)
            self.api_client = WebAPIClient(self.config)
            return True
        except Exception as e:
            logging.error(f"Initialization failed: {e}")
            return False

    # ------------------------------------------------------------------
    # NEW VALIDATOR (acc_tt_servicemaster)
    # ------------------------------------------------------------------
    def validate_accttservicemaster_data(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for r in rows:
            try:
                valid.append({
                    'slno': int(r['slno']),
                    'type': str(r['type']) if r.get('type') else None,
                    'code': str(r['code']) if r.get('code') else None,
                    'name': str(r['name']) if r.get('name') else None
                })
            except (ValueError, TypeError):
                continue
        return valid

    # ------------------------------------------------------------------
    # EXISTING VALIDATORS (unchanged)
    # ------------------------------------------------------------------
    def validate_user_data(self, users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid_users = []
        for i, user in enumerate(users):
            if not user.get('id') or not user.get('pass'):
                continue
            valid_users.append({
                'id': str(user['id']).strip(),
                'pass': str(user['pass']).strip(),
                'role': user.get('role', '').strip() if user.get('role') else None,
                'accountcode': user.get('accountcode', '').strip() if user.get('accountcode') else None
            })
        return valid_users

    def validate_misel_data(self, misel: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for i, m in enumerate(misel):
            if not m.get('firm_name'):
                continue
            valid.append({
                'firm_name': m['firm_name'],
                'address': m.get('address', ''),
                'phones': m.get('phones', ''),
                'mobile': m.get('mobile', ''),
                'address1': m.get('address1', ''),
                'address2': m.get('address2', ''),
                'address3': m.get('address3', ''),
                'pagers': m.get('pagers', ''),
                'tinno': m.get('tinno', ''),
            })
        return valid

    def validate_acc_master_data(self, acc_master: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for i, m in enumerate(acc_master):
            if not m.get('code'):
                continue
            
            # Handle area field properly - don't skip 'No Area' values
            area_value = m.get('area', '')
            if area_value and area_value != 'No Area':
                area_clean = str(area_value).strip()
            else:
                area_clean = None  # Set to None instead of empty string for 'No Area'
            
            validated_record = {
                'code': str(m['code']).strip(),
                'name': str(m.get('name', '')).strip() if m.get('name') else '',
                'opening_balance': float(m['opening_balance']) if m.get('opening_balance') is not None else None,
                'debit': float(m['debit']) if m.get('debit') is not None else None,
                'credit': float(m['credit']) if m.get('credit') is not None else None,
                'place': str(m.get('place', '')).strip() if m.get('place') else '',
                'phone2': str(m.get('phone2', '')).strip() if m.get('phone2') else '',
                'openingdepartment': str(m.get('openingdepartment', '')).strip() if m.get('openingdepartment') else '',
                'area': area_clean  # Fixed: properly handle area field
            }
            
            valid.append(validated_record)
            
        # Debug logging
        area_count = sum(1 for r in valid if r.get('area'))
        logging.info(f"📊 After validation - Records with area data: {area_count}/{len(valid)}")
        
        return valid

    def validate_acc_ledgers_data(self, acc_ledgers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for i, l in enumerate(acc_ledgers):
            if not l.get('code'):
                continue
            
            entry_date = None
            if l.get('entry_date'):
                try:
                    if hasattr(l['entry_date'], 'strftime'):
                        entry_date = l['entry_date'].strftime('%Y-%m-%d')
                    elif isinstance(l['entry_date'], str):
                        # Try to parse string date
                        from datetime import datetime
                        try:
                            parsed_date = datetime.strptime(l['entry_date'], '%Y-%m-%d')
                            entry_date = parsed_date.strftime('%Y-%m-%d')
                        except ValueError:
                            # Try other common formats
                            for fmt in ['%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                                try:
                                    parsed_date = datetime.strptime(l['entry_date'], fmt)
                                    entry_date = parsed_date.strftime('%Y-%m-%d')
                                    break
                                except ValueError:
                                    continue
                    else:
                        entry_date = str(l['entry_date'])
                except Exception as date_e:
                    logging.warning(f"Could not parse date {l['entry_date']}: {date_e}")
                    entry_date = None
            
            voucher_no = None
            if l.get('voucher_no') is not None:
                try:
                    if isinstance(l['voucher_no'], (int, float)):
                        voucher_no = int(l['voucher_no'])
                    elif isinstance(l['voucher_no'], str) and l['voucher_no'].strip():
                        voucher_no = int(float(l['voucher_no'].strip()))
                except (ValueError, TypeError) as voucher_e:
                    logging.warning(f"Could not parse voucher_no {l['voucher_no']}: {voucher_e}")
                    voucher_no = None
            
            debit = None
            credit = None
            try:
                if l.get('debit') is not None:
                    debit = float(l['debit'])
            except (ValueError, TypeError):
                debit = None
                
            try:
                if l.get('credit') is not None:
                    credit = float(l['credit'])
            except (ValueError, TypeError):
                credit = None
            
            valid.append({
                'code': str(l['code']).strip(),
                'particulars': l.get('particulars', ''),
                'debit': debit,
                'credit': credit,
                'entry_mode': l.get('entry_mode', ''),
                'entry_date': entry_date,
                'voucher_no': voucher_no,
                'narration': l.get('narration', '')
            })
        return valid

    def validate_acc_invmast_data(self, acc_invmast: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for i, inv in enumerate(acc_invmast):
            # Handle date conversion
            invdate = None
            if inv.get('invdate'):
                try:
                    if hasattr(inv['invdate'], 'strftime'):
                        invdate = inv['invdate'].strftime('%Y-%m-%d')
                    else:
                        invdate = str(inv['invdate'])
                except Exception:
                    invdate = None
            
            # Handle numeric fields safely
            nettotal = None
            paid = None
            try:
                if inv.get('nettotal') is not None:
                    nettotal = float(inv['nettotal'])
            except (ValueError, TypeError):
                nettotal = None
                
            try:
                if inv.get('paid') is not None:
                    paid = float(inv['paid'])
            except (ValueError, TypeError):
                paid = None
            
            valid.append({
                'modeofpayment': inv.get('modeofpayment', ''),
                'customerid': inv.get('customerid', ''),
                'invdate': invdate,
                'nettotal': nettotal,
                'paid': paid,
                'bill_ref': inv.get('bill_ref', '')
            })
        return valid

    def validate_cashandbankaccmaster_data(self, cashandbankaccmaster: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid = []
        for i, m in enumerate(cashandbankaccmaster):
            if not m.get('code'):
                continue
            valid.append({
                'code': str(m['code']).strip(),
                'name': m.get('name', ''),
                'super_code': m.get('super_code', ''),
                'opening_balance': float(m['opening_balance']) if m.get('opening_balance') else None,
                'opening_date': m['opening_date'].strftime('%Y-%m-%d') if m.get('opening_date') else None,
                'debit': float(m['debit']) if m.get('debit') else None,
                'credit': float(m['credit']) if m.get('credit') else None,
                'client_id': self.config.client_id
            })
        return valid

    # ------------------------------------------------------------------
    # RUN METHOD  (only the new block added before close())
    # ------------------------------------------------------------------
    def run(self) -> bool:
        print("🔄 Starting SQL Anywhere to Web API sync...")
        if not self.initialize():
            return False
        if not self.db_connector.connect():
            return False

        # Sync Users
        users = self.db_connector.fetch_users()
        if users:
            print(f"📊 Found {len(users)} users")
            valid_users = self.validate_user_data(users)
            if valid_users:
                self.api_client.upload_users(valid_users)
            else:
                print("❌ No valid user data")

        # Sync Misel
        misel = self.db_connector.fetch_misel()
        if misel:
            print(f"📊 Found {len(misel)} misel entries")
            valid_misel = self.validate_misel_data(misel)
            if valid_misel:
                self.api_client.upload_misel(valid_misel)
            else:
                print("❌ No valid misel data")

        # Sync AccMaster (with enhanced area field processing)
        acc_master = self.db_connector.fetch_acc_master()
        if acc_master:
            print(f"📊 Found {len(acc_master)} acc_master entries")
            valid_acc_master = self.validate_acc_master_data(acc_master)
            if valid_acc_master:
                # Log area field statistics before upload
                area_records = [r for r in valid_acc_master if r.get('area')]
                print(f"📊 Records with area data: {len(area_records)}/{len(valid_acc_master)}")
                
                # Show sample area values
                if area_records:
                    sample_areas = [r['area'] for r in area_records[:5]]
                    print(f"🔍 Sample area values: {sample_areas}")
                
                self.api_client.upload_acc_master(valid_acc_master)
            else:
                print("❌ No valid acc_master data")

        # Sync AccLedgers
        acc_ledgers = self.db_connector.fetch_acc_ledgers()
        if acc_ledgers is not None:
            if acc_ledgers:
                print(f"📊 Found {len(acc_ledgers)} acc_ledgers entries")
                valid_acc_ledgers = self.validate_acc_ledgers_data(acc_ledgers)
                if valid_acc_ledgers:
                    self.api_client.upload_acc_ledgers(valid_acc_ledgers)
                else:
                    print("❌ No valid acc_ledgers data")
            else:
                print("📊 Found 0 acc_ledgers entries")
        else:
            print("❌ Failed to fetch acc_ledgers data")

        # Sync AccInvmast
        acc_invmast = self.db_connector.fetch_acc_invmast()
        if acc_invmast is not None:
            if acc_invmast:
                print(f"📊 Found {len(acc_invmast)} acc_invmast entries")
                valid_acc_invmast = self.validate_acc_invmast_data(acc_invmast)
                if valid_acc_invmast:
                    self.api_client.upload_acc_invmast(valid_acc_invmast)
                else:
                    print("❌ No valid acc_invmast data")
            else:
                print("📊 Found 0 acc_invmast entries")
        else:
            print("❌ Failed to fetch acc_invmast data")

        # Sync CashAndBankAccMaster
        cashandbankaccmaster = self.db_connector.fetch_cashandbankaccmaster()
        if cashandbankaccmaster:
            print(f"📊 Found {len(cashandbankaccmaster)} cashandbankaccmaster entries")
            valid_cashandbankaccmaster = self.validate_cashandbankaccmaster_data(cashandbankaccmaster)
            if valid_cashandbankaccmaster:
                self.api_client.upload_cashandbankaccmaster(valid_cashandbankaccmaster)
            else:
                print("❌ No valid cashandbankaccmaster data")

        # ------------------------------------------------------------------
        # NEW SYNC BLOCK (acc_tt_servicemaster)
        # ------------------------------------------------------------------
        acctt = self.db_connector.fetch_accttservicemaster()
        if acctt:
            print(f"📊 Found {len(acctt)} acc_tt_servicemaster rows")
            valid = self.validate_accttservicemaster_data(acctt)
            if valid:
                self.api_client.upload_accttservicemaster(valid)
            else:
                print("❌ No valid acc_tt_servicemaster data")

        self.db_connector.close()
        return True

    def run_interactive(self):
        print("=" * 60)
        print("    SQL Anywhere to Web API Sync Tool")
        print("=" * 60)
        print()
        try:
            if self.run():
                print("\n✅ Sync completed successfully!")
            else:
                print("\n❌ Sync failed!")
        except Exception as e:
            print(f"❌ Critical error: {e}")
        print("\nPress Enter to exit...")
        input()


def main():
    sync_tool = SyncTool()
    sync_tool.run_interactive()
    

if __name__ == "__main__":
    main()