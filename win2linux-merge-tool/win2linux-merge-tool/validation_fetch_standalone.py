#!/usr/bin/env python3
"""
Standalone script to fetch dashboards. Called as subprocess by server.
Reads server config from JSON file, writes result to another JSON file.
Usage: python validation_fetch_standalone.py <config_json_path> <result_json_path>
"""
import sys
import json
import os
import warnings

# Suppress pkg_resources deprecation warning (e.g. when run under PyInstaller bundle)
warnings.filterwarnings("ignore", message=".*pkg_resources is deprecated.*", category=UserWarning)

def main():
    if len(sys.argv) != 3:
        sys.exit(2)
    config_path = sys.argv[1]
    result_path = sys.argv[2]
    with open(config_path, "r") as f:
        server_config = json.load(f)
    try:
        from SisenseRESTAPIClientClass import SisenseRestApiClient
        from utils.utils import fetch_all_dashboards
        import logging
        logger = logging.getLogger(__name__)
        verify_ssl = server_config.get("verify", True)
        api_client = SisenseRestApiClient(
            server_domain=server_config["host"],
            port=server_config["port"],
            protocol=server_config["protocol"],
            operating_system=server_config["os"],
            api_token=server_config["api_token"],
            verify=verify_ssl,
            timeout=60,
            logger=logger,
        )
        dashboards_list = fetch_all_dashboards(api_client, logger, chunk_size=100)
        with open(result_path, "w") as f:
            json.dump({"ok": True, "data": dashboards_list}, f)
    except Exception as e:
        with open(result_path, "w") as f:
            json.dump({"ok": False, "error": str(e)}, f)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
