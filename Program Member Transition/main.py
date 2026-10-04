#!/usr/bin/env python3
"""
Export Static List Counts for Migration Analysis

This script exports counts of all migration analysis static lists
created in source programs. It reads from data_final.json and 
produces a summary report with member counts and direct URLs for each list
in both JSON and CSV formats.

Author: Migration Analysis Tool
Version: 1.3 - Fixed member count retrieval
"""

import os
import sys
import json
import csv
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple, Optional

# ============================================
# CONFIGURATION
# ============================================
# Load credentials from environment
A_MUNCHKIN = os.environ.get('MKTO_A_MUNCHKIN', "256-EYY-782")
A_CLIENT_ID = os.environ.get('MKTO_A_CLIENT_ID', "13d8c425-e81a-49fa-90cd-38820cb1cb14")
A_CLIENT_SECRET = os.environ.get('MKTO_A_CLIENT_SECRET', "HrDZDk8xekGFMASfhpj0uyq4tDBijyou")

# URL configuration for Instance A
# URL structure: BASE_URL + LIST_ID + SUFFIX
# Example: https://experience.adobe.com/#/@contentsquaresas/so:256-EYY-782/marketo-engage/classic/ST16845A1LA1
INSTANCE_A_BASE_URL = "https://experience.adobe.com/#/@contentsquaresas/so:256-EYY-782/marketo-engage/classic/ST"
LIST_URL_SUFFIX = "A1LA1"

# API limits
API_TIMEOUT = 30
RATE_LIMIT_PAUSE = 0.5
MAX_BATCH_SIZE = 300  # Marketo's max batch size for leads endpoint

# Static list names to look for (exact matches)
TARGET_LIST_NAMES = [
    "All Program Members",
    "All Program Members in OneMAP",
    "All Program Members in OneMAP AND In OneMap Program",
    "All Program Members missing from OneMAP",
    "All Program Members in OneMAP but NOT in Destination Program"
]

# ============================================
# LOGGING SETUP
# ============================================
os.makedirs('logs', exist_ok=True)
log_filename = f'logs/export_lists_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# OAUTH TOKEN MANAGER (from main script)
# ============================================
class OAuthToken:
    """Manages authentication tokens for Marketo API access"""

    def __init__(self, munchkin: str, client_id: str, client_secret: str):
        self.identity_url = f"https://{munchkin}.mktorest.com/identity"
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.expiry = datetime.utcnow()

    def get(self) -> str:
        """Get a valid token, refreshing if needed"""
        if not self.token or datetime.utcnow() >= self.expiry:
            self._refresh_token()
        return self.token

    def _refresh_token(self):
        """Get a new authentication token from Marketo"""
        url = f"{self.identity_url}/oauth/token"
        params = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }

        logger.info(f"Getting new authentication token...")

        try:
            resp = requests.get(url, params=params, timeout=API_TIMEOUT)
            resp.raise_for_status()

            data = resp.json()
            self.token = data['access_token']
            expires_in = data.get('expires_in', 3600) - 600  # 10 minute buffer
            self.expiry = datetime.utcnow() + timedelta(seconds=expires_in)

            logger.info("[SUCCESS] Successfully authenticated with Marketo")

        except requests.exceptions.RequestException as e:
            logger.error(f"[ERROR] Failed to authenticate with Marketo: {str(e)}")
            raise

# ============================================
# MARKETO CLIENT (from main script)
# ============================================
class MarketoClient:
    """Base class for interacting with Marketo's API"""

    def __init__(self, munchkin: str, client_id: str, client_secret: str, instance_name: str = ""):
        self.instance_name = instance_name
        self.token_manager = OAuthToken(munchkin, client_id, client_secret)
        self.rest_base = f"https://{munchkin}.mktorest.com/rest/v1"
        self.asset_base = f"https://{munchkin}.mktorest.com/rest/asset/v1"

    def _headers(self) -> Dict[str, str]:
        """Get headers with current authentication token"""
        return {
            'Authorization': f"Bearer {self.token_manager.get()}",
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def _handle_response(self, response: requests.Response, operation: str) -> Dict:
        """Check if Marketo API call was successful and return the data"""
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"[ERROR] HTTP error during {operation}: {str(e)}")
            raise

        try:
            data = response.json()
        except json.JSONDecodeError as e:
            logger.error(f"[ERROR] Invalid JSON response during {operation}")
            raise

        if not data.get('success', True):
            errors = data.get('errors', [])
            if errors:
                error_msg = errors[0].get('message', 'Unknown error')
                error_code = errors[0].get('code', 'Unknown')
                logger.error(f"[ERROR] Marketo API error during {operation}: [{error_code}] {error_msg}")
                raise RuntimeError(f"{operation} failed: {error_msg}")

        return data

# ============================================
# STATIC LIST EXPORT CLASS
# ============================================
class StaticListExporter(MarketoClient):
    """Exports static list information from programs"""

    def get_program_static_lists(self, program_id: int) -> List[Dict]:
        """Get all static lists in a program"""
        url = f"{self.asset_base}/staticLists.json"
        params = {
            'folder': json.dumps({"id": program_id, "type": "Program"}),
            'maxReturn': 200
        }

        try:
            resp = requests.get(url, headers=self._headers(), params=params, timeout=API_TIMEOUT)
            data = self._handle_response(resp, f"Get static lists in program {program_id}")

            lists = []
            if 'result' in data:
                for item in data['result']:
                    lists.append({
                        'id': item.get('id'),
                        'name': item.get('name'),
                        'createdAt': item.get('createdAt'),
                        'updatedAt': item.get('updatedAt')
                    })

            return lists

        except Exception as e:
            logger.error(f"Failed to get static lists for program {program_id}: {str(e)}")
            return []

    def get_list_member_count(self, list_id: int) -> int:
        """Get the actual count of members in a static list by paginating through all members"""

        # First, try to get basic info about the list
        url = f"{self.rest_base}/lists/{list_id}/leads.json"
        params = {
            'fields': 'id',  # Only get IDs to minimize data transfer
            'batchSize': MAX_BATCH_SIZE
        }

        total_count = 0
        next_page_token = None
        page = 0

        try:
            while True:
                page += 1

                # Add nextPageToken if we have one
                if next_page_token:
                    params['nextPageToken'] = next_page_token

                resp = requests.get(url, headers=self._headers(), params=params, timeout=API_TIMEOUT)
                data = self._handle_response(resp, f"Get members for list {list_id} (page {page})")

                # Count the results in this batch
                if 'result' in data:
                    batch_count = len(data['result'])
                    total_count += batch_count
                    logger.debug(f"  List {list_id} - Page {page}: {batch_count} members")

                # Check if there are more results
                if data.get('moreResult', False) and data.get('nextPageToken'):
                    next_page_token = data['nextPageToken']
                    # Small delay to avoid rate limiting
                    time.sleep(0.1)
                else:
                    # No more results
                    break

            logger.info(f"  Total members in list {list_id}: {total_count}")
            return total_count

        except Exception as e:
            logger.error(f"Failed to get member count for list {list_id}: {str(e)}")
            return 0

    def get_list_info(self, list_id: int) -> Dict:
        """Get detailed information about a static list including member count"""
        url = f"{self.asset_base}/staticList/{list_id}.json"

        try:
            resp = requests.get(url, headers=self._headers(), timeout=API_TIMEOUT)
            data = self._handle_response(resp, f"Get info for list {list_id}")

            if 'result' in data and len(data['result']) > 0:
                list_info = data['result'][0]

                # Debug: Log what fields are available in the response
                logger.debug(f"Static list {list_id} metadata fields: {list(list_info.keys())}")

                return {
                    'id': list_info.get('id'),
                    'name': list_info.get('name'),
                    'createdAt': list_info.get('createdAt'),
                    'updatedAt': list_info.get('updatedAt')
                }

            return {}

        except Exception as e:
            logger.error(f"Failed to get info for list {list_id}: {str(e)}")
            return {}

    def export_program_lists(self, program_id: int, program_name: str) -> Dict:
        """Export all migration analysis lists from a program"""
        logger.info(f"\n[EXPORT] Processing program: {program_name} (ID: {program_id})")

        # Get all static lists in the program
        all_lists = self.get_program_static_lists(program_id)
        logger.info(f"  Found {len(all_lists)} total static lists in program")

        # Filter for our target lists
        migration_lists = {}
        found_lists = []

        for list_item in all_lists:
            list_name = list_item.get('name', '')
            if list_name in TARGET_LIST_NAMES:
                found_lists.append(list_name)

                # Get basic info first
                list_info = self.get_list_info(list_item['id'])

                if list_info:
                    # Now get the actual member count using the leads endpoint
                    member_count = self.get_list_member_count(list_item['id'])

                    # Create a key based on the list purpose
                    if list_name == "All Program Members":
                        key = "all_members"
                    elif list_name == "All Program Members in OneMAP":
                        key = "in_onemap"
                    elif list_name == "All Program Members in OneMAP AND In OneMap Program":
                        key = "in_both"
                    elif list_name == "All Program Members missing from OneMAP":
                        key = "missing_from_onemap"
                    elif list_name == "All Program Members in OneMAP but NOT in Destination Program":
                        key = "in_onemap_not_in_dest"

                    # Generate the list URL
                    list_url = f"{INSTANCE_A_BASE_URL}{list_info['id']}{LIST_URL_SUFFIX}"

                    migration_lists[key] = {
                        'id': list_info['id'],
                        'name': list_info['name'],
                        'memberCount': member_count,  # Use the actual count we retrieved
                        'url': list_url,
                        'createdAt': list_info.get('createdAt'),
                        'updatedAt': list_info.get('updatedAt')
                    }

                    logger.info(f"    ✓ {list_name}: {member_count} members")
                    logger.debug(f"      URL: {list_url}")

                # Rate limiting
                time.sleep(RATE_LIMIT_PAUSE)

        # Log any missing lists
        missing_lists = set(TARGET_LIST_NAMES) - set(found_lists)
        if missing_lists:
            logger.warning(f"  Missing lists: {', '.join(missing_lists)}")

        return {
            'program_id': program_id,
            'program_name': program_name,
            'lists_found': len(migration_lists),
            'lists': migration_lists,
            'missing_lists': list(missing_lists)
        }

# ============================================
# CSV EXPORT FUNCTION
# ============================================
def create_csv_from_results(summary: Dict, csv_filename: str):
    """Create a CSV file from the export results"""
    logger.info(f"\n[CSV] Creating CSV export: {csv_filename}")

    # Define CSV headers
    headers = [
        'Program Name',
        'Old Program ID',
        'New Program ID',
        'List Type',
        'List Name',
        'List ID',
        'Member Count',
        'List URL',
        'Created At',
        'Updated At'
    ]

    rows = []

    # Process each program's results
    for result in summary.get('results', []):
        program_name = result.get('program_name', '')
        old_program_id = result.get('program_id', '')
        new_program_id = result.get('new_program_id', '')

        # If there's an error, add a row indicating the error
        if 'error' in result:
            rows.append({
                'Program Name': program_name,
                'Old Program ID': old_program_id,
                'New Program ID': new_program_id,
                'List Type': 'ERROR',
                'List Name': f"Error: {result['error']}",
                'List ID': '',
                'Member Count': 0,
                'List URL': '',
                'Created At': '',
                'Updated At': ''
            })
            continue

        # Process each list in the program
        lists = result.get('lists', {})

        # Define the order and display names for list types
        list_type_mapping = {
            'all_members': 'All Members',
            'in_onemap': 'In OneMAP',
            'in_both': 'In Both Programs',
            'missing_from_onemap': 'Missing from OneMAP',
            'in_onemap_not_in_dest': 'In OneMAP but Not in Dest'
        }

        # If no lists found, add a row indicating this
        if not lists:
            rows.append({
                'Program Name': program_name,
                'Old Program ID': old_program_id,
                'New Program ID': new_program_id,
                'List Type': 'NONE',
                'List Name': 'No migration lists found',
                'List ID': '',
                'Member Count': 0,
                'List URL': '',
                'Created At': '',
                'Updated At': ''
            })
            continue

        # Add a row for each list
        for list_key, display_name in list_type_mapping.items():
            if list_key in lists:
                list_info = lists[list_key]
                rows.append({
                    'Program Name': program_name,
                    'Old Program ID': old_program_id,
                    'New Program ID': new_program_id,
                    'List Type': display_name,
                    'List Name': list_info.get('name', ''),
                    'List ID': list_info.get('id', ''),
                    'Member Count': list_info.get('memberCount', 0),
                    'List URL': list_info.get('url', ''),
                    'Created At': list_info.get('createdAt', ''),
                    'Updated At': list_info.get('updatedAt', '')
                })

    # Write CSV file
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    # Add summary rows at the end
    with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([])  # Empty row
        writer.writerow(['SUMMARY'])
        writer.writerow(['Export Date', summary.get('export_date', '')])
        writer.writerow(['Total Programs', summary.get('total_programs', 0)])
        writer.writerow(['Programs Processed', summary.get('programs_processed', 0)])
        writer.writerow(['Programs with Lists', summary.get('programs_with_lists', 0)])
        writer.writerow(['Total Lists Found', summary.get('total_lists_found', 0)])
        writer.writerow([])
        writer.writerow(['AGGREGATE STATISTICS'])

        aggregate_stats = summary.get('aggregate_statistics', {})
        writer.writerow(['All Members Total', aggregate_stats.get('all_members_total', 0)])
        writer.writerow(['In OneMAP Total', aggregate_stats.get('in_onemap_total', 0)])
        writer.writerow(['In Both Programs Total', aggregate_stats.get('in_both_total', 0)])
        writer.writerow(['Missing from OneMAP Total', aggregate_stats.get('missing_from_onemap_total', 0)])
        writer.writerow(['In OneMAP but Not in Dest Total', aggregate_stats.get('in_onemap_not_in_dest_total', 0)])

    logger.info(f"[CSV] Created CSV with {len(rows)} data rows")
    return len(rows)

# ============================================
# MAIN EXPORT FUNCTION
# ============================================
def export_all_program_lists():
    """Export static list counts for all programs in data_final.json"""
    start_time = datetime.now()

    print("\n[START] Exporting Static List Counts")
    print("=" * 60)

    # Load program mappings
    data_file = 'data_final.json'
    if not os.path.exists(data_file):
        # Try test file as fallback
        data_file = 'data_test.json'
        if not os.path.exists(data_file):
            print(f"[ERROR] No data file found (looked for data_final.json and data_test.json)")
            return

    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            mappings = json.load(f)
        print(f"[SUCCESS] Loaded {len(mappings)} programs from {data_file}")
    except Exception as e:
        print(f"[ERROR] Failed to load {data_file}: {str(e)}")
        return

    # Initialize exporter
    exporter = StaticListExporter(A_MUNCHKIN, A_CLIENT_ID, A_CLIENT_SECRET, "Source")

    # Process each program
    all_results = []
    programs_with_lists = 0
    total_lists_found = 0

    for i, mapping in enumerate(mappings, 1):
        program_name = mapping.get('Program Name', 'Unknown')
        old_program_id = mapping.get('Old Program ID')

        if not old_program_id:
            logger.warning(f"Skipping program '{program_name}' - no Old Program ID")
            continue

        print(f"\n[{i}/{len(mappings)}] Processing: {program_name}")

        try:
            # Export lists for this program
            result = exporter.export_program_lists(old_program_id, program_name)

            # Add destination program ID to results
            result['new_program_id'] = mapping.get('New Program ID')

            all_results.append(result)

            if result['lists_found'] > 0:
                programs_with_lists += 1
                total_lists_found += result['lists_found']

        except Exception as e:
            logger.error(f"Failed to export lists for program '{program_name}': {str(e)}")
            all_results.append({
                'program_id': old_program_id,
                'program_name': program_name,
                'error': str(e),
                'lists_found': 0
            })

        # Rate limiting between programs
        if i < len(mappings):
            time.sleep(RATE_LIMIT_PAUSE * 2)

    # Generate summary statistics
    elapsed_time = datetime.now() - start_time

    summary = {
        'export_date': datetime.now().isoformat(),
        'data_source': data_file,
        'total_programs': len(mappings),
        'programs_processed': len(all_results),
        'programs_with_lists': programs_with_lists,
        'total_lists_found': total_lists_found,
        'processing_time_seconds': elapsed_time.total_seconds(),
        'results': all_results
    }

    # Calculate aggregate statistics
    aggregate_stats = {
        'all_members_total': 0,
        'in_onemap_total': 0,
        'in_both_total': 0,
        'missing_from_onemap_total': 0,
        'in_onemap_not_in_dest_total': 0
    }

    for result in all_results:
        if 'lists' in result:
            for list_type, list_info in result['lists'].items():
                aggregate_stats[f'{list_type}_total'] += list_info['memberCount']

    summary['aggregate_statistics'] = aggregate_stats

    # Save results as JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_output_file = f'static_list_export_{timestamp}.json'
    with open(json_output_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)

    # Save results as CSV
    csv_output_file = f'static_list_export_{timestamp}.csv'
    csv_rows = create_csv_from_results(summary, csv_output_file)

    # Print summary
    print("\n" + "=" * 60)
    print("[COMPLETE] Export Summary")
    print("=" * 60)
    print(f"Programs processed: {len(all_results)}")
    print(f"Programs with migration lists: {programs_with_lists}")
    print(f"Total lists found: {total_lists_found}")
    print(f"\nAggregate Member Counts:")
    print(f"  All Members: {aggregate_stats['all_members_total']:,}")
    print(f"  In OneMAP: {aggregate_stats['in_onemap_total']:,}")
    print(f"  In Both Programs: {aggregate_stats['in_both_total']:,}")
    print(f"  In OneMAP but Not in Dest: {aggregate_stats['in_onemap_not_in_dest_total']:,}")
    print(f"  Missing from OneMAP: {aggregate_stats['missing_from_onemap_total']:,}")
    print(f"\nResults saved to:")
    print(f"  JSON: {json_output_file}")
    print(f"  CSV:  {csv_output_file} ({csv_rows} rows)")
    print(f"  Both files include member counts and direct URLs for each list")
    print(f"\nLog file: {log_filename}")
    print(f"Time taken: {elapsed_time.seconds // 60} minutes {elapsed_time.seconds % 60} seconds")

# ============================================
# ENTRY POINT
# ============================================
if __name__ == '__main__':
    try:
        export_all_program_lists()
    except KeyboardInterrupt:
        print("\n\n[WARNING] Export interrupted by user")
    except Exception as e:
        print(f"\n\n[ERROR] Unexpected error: {str(e)}")
        logger.exception("Fatal error in export execution")
