# utils/api_clients.py

from SisenseRESTAPIClientClass import SisenseRestApiClient
import config_loader  # Import the central config loader
from . import logger_setup  # Use relative import for logger_setup

# --- Global API client instances ---
src_api = None
target_api = None

def init_api_clients():
    """
    Initializes the global src_api and target_api clients using settings
    from the centrally loaded config_loader.
    """
    global src_api, target_api

    # Ensure settings are loaded before trying to use them
    if not config_loader.settings:
        logger_setup.logger.critical("Configuration not loaded. Cannot initialize API clients.")
        # Depending on desired behavior, you might raise an exception or call sys.exit
        # For now, let's print and return, letting the caller handle the failure.
        return

    logger = logger_setup.logger  # Use the initialized logger

    try:
        logger.info("Initializing Source API client...")
        src_api = SisenseRestApiClient(
            api_token=config_loader.settings.get('src_api_token'),
            operating_system=config_loader.settings.get('src_os'),
            protocol=config_loader.settings.get('src_protocol'),
            server_domain=config_loader.settings.get('src_host'),
            port=config_loader.settings.get('src_port'),
            verify=config_loader.settings.get('verify', True),  # Default to True if not specified
            logger=logger
        )
        logger.info("Source API client initialized.")

        logger.info("Initializing Target API client...")
        target_api = SisenseRestApiClient(
            api_token=config_loader.settings.get('target_api_token'),
            operating_system=config_loader.settings.get('target_os'),
            protocol=config_loader.settings.get('target_protocol'),
            server_domain=config_loader.settings.get('target_host'),
            port=config_loader.settings.get('target_port'),
            verify=config_loader.settings.get('verify', True),  # Default to True if not specified
            logger=logger
        )
        logger.info("Target API client initialized.")

    except KeyError as e:
        logger.critical(f"Missing a required setting for API client initialization: {e}")
        # Depending on desired behavior, you might want to exit here
        # import sys
        # sys.exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during API client initialization: {e}", exc_info=True)
        # import sys
        # sys.exit(1)