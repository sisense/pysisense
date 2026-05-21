# Sisense JAQL API Checker (High-Performance, API-Only)
#
# This script uses a direct, asynchronous API-only approach to test widget
# health by replicating the exact JAQL queries Sisense's frontend uses.
# This method is significantly faster and more resource-efficient than
# browser-based automation.
#
# --- SETUP ---
# 1. Place 'SisenseRESTAPIClientClass.py' in the same directory as this script.
#
# 2. Install necessary Python libraries:
#    pip install requests urllib3 httpx
#
# 3. No browser or WebDriver is needed for this version.

import time
import os
import getpass
import asyncio
from urllib.parse import urlparse, quote

import httpx

# Import the custom API client
try:
    from SisenseRESTAPIClientClass import SisenseRestApiClient, SisenseRestAPIError
except ImportError:
    print("ERROR: Could not find 'SisenseRESTAPIClientClass.py'.")
    print("Please make sure the API client file is in the same directory as this script.")
    exit()

# --- CONFIGURATION ---
SISENSE_BASE_URL = "https://your-sisense-instance.example.com"
MAX_CONCURRENT_REQUESTS = 100  # Number of parallel API requests.


def generate_jaql_payloads(client):
    """
    Fetches all dashboards, then for each dashboard, uses the dedicated
    /widgets endpoint to get full widget definitions and generate JAQL payloads.
    """
    print("--- Using API to build a list of all JAQL query payloads ---")
    payloads_to_check = []

    try:
        # Step 1: Get a list of all dashboard IDs first.
        print("Fetching list of all dashboard IDs...")
        dashboards_summary = client.get_dashboards(fields=["oid", "title"])
        if not dashboards_summary:
            print("No dashboards found.")
            return []

        print(f"Found {len(dashboards_summary)} dashboards. Now fetching widgets for each...")

        # Step 2: Iterate through each dashboard and get its full widget definitions.
        for i, summary in enumerate(dashboards_summary):
            dash_id = summary.get('oid')
            dash_title = summary.get('title', 'Untitled')
            if not dash_id:
                continue

            print(f"  ({i + 1}/{len(dashboards_summary)}) Processing: {dash_title}", end="")

            try:
                # REVISED: Use the dedicated get_widgets endpoint for reliability.
                widgets = client.get_widgets(dashboard_id=dash_id)

                if not widgets:
                    print(" -> No widgets found.")
                    continue

                print(f" -> Found {len(widgets)} widgets.")

                # Step 3: Iterate through every widget returned.
                for widget in widgets:
                    # Step 4: Iterate through every panel in the widget's metadata.
                    for panel in widget.get('metadata', {}).get('panels', []):
                        # Step 5: Iterate through every item in the panel to find JAQL queries.
                        for item in panel.get('items', []):
                            if 'jaql' in item:
                                # Found a JAQL query. Construct the payload.
                                payload = {
                                    "datasource": widget.get('datasource'),
                                    "metadata": [item],
                                    "widgetType": widget.get('type'),
                                    "dashboard": dash_id,
                                    "widget": str(widget.get('oid')),
                                    "format": "json"
                                }
                                payloads_to_check.append(payload)

            except Exception as e:
                error_message = str(e)
                if hasattr(e, 'result') and hasattr(e.result, 'text'):
                    error_message = e.result.text
                print(f"\n    - ERROR processing widgets for dashboard ID {dash_id}. Details: {error_message}")

        print(f"\nSuccessfully generated {len(payloads_to_check)} unique JAQL payloads to test.")
        return payloads_to_check

    except (SisenseRestAPIError, Exception) as e:
        error_text = getattr(e, 'result', {}).get('text', str(e))
        print(f"FATAL: Could not retrieve dashboards list from Sisense API. Details: {error_text}")
        return None


async def check_widget_api(client: httpx.AsyncClient, payload: dict, api_token: str):
    """
    Checks a single widget's health by executing its JAQL query directly via the API.
    """
    widget_id = payload["widget"]
    datasource = payload.get("datasource")

    if not datasource or not isinstance(datasource, dict) or not datasource.get('title'):
        return {"widget_id": widget_id, "status": "Invalid Datasource", "http_code": None}

    # URL-encode the datasource title for the endpoint URL
    datasource_title_encoded = quote(datasource['title'])
    jaql_url = f"{SISENSE_BASE_URL}/api/datasources/{datasource_title_encoded}/jaql"

    headers = {
        'Content-Type': "application/json",
        'Accept': "application/json",
        'Authorization': f"Bearer {api_token}"
    }

    try:
        response = await client.post(jaql_url, headers=headers, json=payload, timeout=60.0)

        return {
            "widget_id": widget_id,
            "status": "OK" if response.is_success else "API Error",
            "http_code": response.status_code
        }
    except httpx.RequestError as e:
        print(f"ERROR checking widget {widget_id}: {e.__class__.__name__}")
        return {"widget_id": widget_id, "status": "Request Failed", "http_code": None}


async def main():
    """Main function to orchestrate the API calls and checks."""

    print("--- Please provide your Sisense credentials ---")
    sisense_username = input("Enter your Sisense username: ")
    sisense_password = getpass.getpass("Enter your Sisense password: ")

    print("\n--- Initializing Sisense API Client ---")
    parsed_url = urlparse(SISENSE_BASE_URL)
    try:
        sync_api_client = SisenseRestApiClient(server_domain=parsed_url.netloc, protocol=parsed_url.scheme, port=443,
                                               api_token="dummy")
        print("Authenticating with API...")
        api_token = sync_api_client._SisenseRestApiClient__create_token(sisense_username, sisense_password)
        if not api_token: raise Exception("Failed to acquire API token.")
        print("API Client authenticated successfully.")
    except Exception as e:
        print(f"FATAL: Could not initialize or authenticate API client: {e}")
        return

    payloads = generate_jaql_payloads(sync_api_client)
    if not payloads:
        print("Could not generate any payloads to test. Exiting.")
        return

    print(f"\n--- Starting high-performance check of {len(payloads)} JAQL queries ---")
    all_results = []
    completed_count = 0
    total_payloads = len(payloads)

    limits = httpx.Limits(max_connections=MAX_CONCURRENT_REQUESTS, max_keepalive_connections=20)
    async with httpx.AsyncClient(limits=limits, verify=False) as client:
        tasks = [check_widget_api(client, p, api_token) for p in payloads]
        # Use asyncio.as_completed to process results as they finish.
        for future in asyncio.as_completed(tasks):
            completed_count += 1
            try:
                res = await future
                all_results.append(res)  # Save for final report

                # Print real-time progress for this individual result
                code = res.get('http_code')
                status = res.get('status')
                status_color = "\033[92m" if code and 200 <= code < 300 else "\033[91m"
                end_color = "\033[0m"
                progress_prefix = f"[{completed_count}/{total_payloads}]"
                print(
                    f"{progress_prefix:<12} Widget: {res.get('widget_id', 'Unknown'):<30} -> Status: {status_color}{status:<20}{end_color} | HTTP Code: {code if code else 'N/A'}")

            except Exception as e:
                print(f"[{completed_count}/{total_payloads}] An unexpected error occurred during a check: {e}")

    print("\n\n--- FINAL SUMMARY REPORT ---")
    print("-" * 80)
    all_results.sort(key=lambda x: (x.get('widget_id', '')))

    for res in all_results:
        if isinstance(res, Exception):
            # This case should be handled in the loop above, but as a fallback.
            print(f"An unexpected error was recorded for a task: {res}")
            continue

        code = res.get('http_code')
        status = res.get('status')
        status_color = "\033[92m" if code and 200 <= code < 300 else "\033[91m"
        end_color = "\033[0m"

        print(
            f"Widget: {res.get('widget_id', 'Unknown'):<30} -> Status: {status_color}{status:<20}{end_color} | HTTP Code: {code if code else 'N/A'}")

    print("-" * 80)


if __name__ == "__main__":
    asyncio.run(main())
