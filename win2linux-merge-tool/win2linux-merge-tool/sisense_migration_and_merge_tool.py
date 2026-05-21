import asyncio
import importlib
import sys
import traceback
from datetime import datetime
import connection_update_functions
import urllib3
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure

import json
from alive_progress import alive_it, alive_bar, config_handler

from SisenseRESTAPIClientClass import (
    SisenseRestApiClient,
    InputError,
    SisenseRestAPIError,
)

from MigrationReportClass import (MigrationReport)
from migration.dashboards_migration import save_dashboard_oid_map, get_target_dashboards_owned_by_migration_user, \
    load_dashboard_oid_map
from migration.folders_migration import save_folders_map, load_folders_map

# import codecs
import os.path
import time

from colorama import init  # Just because it's fun!

from migration.preflight import share_source_dashboards_with_migration_user_async
from utils import logger_setup, api_clients
from utils.utils import load_yaml_config, get_user_config_path, fetch_all_dashboards

import config_loader

from utils.progress_reporter import ProgressReporter

import logging

import shutil

# Init colorama
init()
urllib3.disable_warnings()

# if getattr(sys, 'frozen', False):
#     # --- Case 1: Application is frozen ---
#     application_path = os.path.dirname(sys.executable)
# else:
#     # --- Case 2: Application is a standard script ---
#     try:
#
#         application_path = os.path.dirname(os.path.abspath(__file__))
#     except NameError:
#
#         application_path = os.getcwd() # Use current working directory as fallback
#

import os
import yaml

# Add argument parsing for settings file
import argparse

from config_loader import (SETTINGS_FILE, SERVERS_FILE, APP_NAME)
from utils.utils import load_yaml_config, get_user_config_path

# Create an argument parser
# parser = argparse.ArgumentParser(description="Migration script")

# Add argument for custom settings file location
# parser.add_argument(
#     "-settings",
#     "--settings-file",
#     help="Specify the location of the settings file",
#     default=get_user_config_path(SETTINGS_FILE, APP_NAME),
# )

# SERVERS_FILE_NAME = "servers.yaml"
# Parse arguments and load the settings file
# args = parser.parse_args()

# settings = load_yaml_config(args.settings_file, APP_NAME)
# settings = yaml.load(
#     open(args.settings_file, "r"), Loader=yaml.FullLoader
# )

# servers = yaml.load(
#     open(os.path.join(application_path, SERVERS_FILE_NAME), "r"), Loader=yaml.FullLoader
# )

# servers = load_yaml_config(SERVERS_FILE, APP_NAME)

# config_loader.load_config()

# src_server = servers[config_loader.settings["selected_source_server"]]
# config_loader.settings["src_host"] = src_server["host"]
# config_loader.settings["src_port"] = src_server["port"]
# config_loader.settings["src_protocol"] = src_server["protocol"]
# config_loader.settings["api_clients.src_api_token"] = src_server["api_token"]
# config_loader.settings["src_os"] = src_server["os"]
#
# target_server = servers[config_loader.settings["selected_target_server"]]
# config_loader.settings["target_host"] = target_server["host"]
# config_loader.settings["target_port"] = target_server["port"]
# config_loader.settings["target_protocol"] = target_server["protocol"]
# config_loader.settings["api_clients.target_api_token"] = target_server["api_token"]
# config_loader.settings["target_os"] = target_server["os"]

# Global Variables

# logger = logging.getLogger(__name__)

roles_map = []
groups_map = []
users_map = []
folders_map = []
dashboard_oid_map = {}
# api_clients.src_api = None
# api_clients.target_api = None

#  Reports
report_dashboards_missing_owner = MigrationReport("Dashboards missing owner")
report_move_dashboard_to_folder_error = MigrationReport("Moved dashboard to folder error")
report_users_migration_errors = MigrationReport("Users Migration Errors")
report_dashboards_skipped_already_exists = MigrationReport("Dashboards skipped already exists",
                                                           "This is a list of dashboards that where skipped since they "
                                                           "already exist in the target server and the dashboard migration mode is "
                                                           "set to 'skip'")
report_folder_owner_update_errors = MigrationReport("Folder owner update errors",
                                                    "The owner for the folders in this list could not be updated")
# report_share_src_dashboards_with_migration_user = MigrationReport("Share src dashboards with migration user")
report_add_folder_errors = MigrationReport("Add folder errors")
report_dashboard_migration_errors = MigrationReport("Dashboard migration errors")

blox_action_map = []
START_TIME = None


def settings_validation(settings):
    if settings.get("api_clients.src_api_token", '') == '':
        return False

    return True


def main():
    """
    Main function to orchestrate the migration process.
    """
    # --- Disable alive-progress if running as a non-interactive worker ---
    # The server launches this script as a subprocess. In that context,
    # the animated progress bar causes encoding errors on Windows and is not visible anyway.
    # We can detect this by checking if stdout is a TTY (terminal).
    if not sys.stdout.isatty():
        print("INFO: Non-interactive mode detected (stdout is not a TTY). Disabling alive-progress.")
        config_handler.set_global(disable=True)
    # --------------------------------------------------------------------

    # global api_clients.src_api, api_clients.target_api, START_TIME  # Declare globals that main will initialize/modify
    global START_TIME  # Declare globals that main will initialize/modify

    # Load configuration using the config_loader
    try:
        servers = load_yaml_config(SERVERS_FILE, APP_NAME)
        config_loader.load_config()

        src_server = servers[config_loader.settings["selected_source_server"]]
        config_loader.settings["src_host"] = src_server["host"]
        config_loader.settings["src_port"] = src_server["port"]
        config_loader.settings["src_protocol"] = src_server["protocol"]
        config_loader.settings["src_api_token"] = src_server["api_token"]
        config_loader.settings["src_os"] = src_server["os"]

        target_server = servers[config_loader.settings["selected_target_server"]]
        config_loader.settings["target_host"] = target_server["host"]
        config_loader.settings["target_port"] = target_server["port"]
        config_loader.settings["target_protocol"] = target_server["protocol"]
        config_loader.settings["target_api_token"] = target_server["api_token"]
        config_loader.settings["target_os"] = target_server["os"]

    except SystemExit:
        print("CRITICAL: Configuration loading failed. Exiting.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"CRITICAL: An unexpected error occurred during configuration loading: {e}", file=sys.stderr)
        sys.exit(1)

    # Initialize logging as early as possible, using loaded settings
    # Note: init_logging itself now uses config_loader.settings
    # logger = init_logging()
    #
    # logger_setup.logger.debug("application path: " + config_loader.application_path)
    local_logger_instance = logger_setup.init_logging()

    if not logger_setup.logger or not logger_setup.logger.hasHandlers():
        print("CRITICAL: Logger initialization failed. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Validate settings using the function from config_loader
    try:
        logger_setup.logger.info("Validating settings...")
        config_loader.settings_validation(config_loader.settings)
        logger_setup.logger.info("Settings validated successfully.")
    except SystemExit:
        logger_setup.logger.critical("Settings validation failed. Exiting.")
        sys.exit(1)
    except Exception as e:
        logger_setup.logger.critical(f"An unexpected error occurred during settings validation: {e}")
        sys.exit(1)

    START_TIME = datetime.now()
    logger_setup.logger.info(f"Win2Linux Merge Tool started at {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}")
    logger_setup.logger.info(
        f"Source OS: {config_loader.settings.get('src_os')}, Target OS: {config_loader.settings.get('target_os')}")

    # Initialize API Clients (Example - replace with your actual class and init)
    try:
        logger_setup.logger.info("Initializing Sisense API clients...")
        api_clients.init_api_clients()
        if not api_clients.src_api or not api_clients.target_api:
            logger_setup.logger.critical("API clients were not successfully initialized by api_clients.py. Exiting.")
            sys.exit(1)

        logger_setup.logger.info("API clients are ready.")
    except SystemExit:  # If init_api_clients calls sys.exit
        logger_setup.logger.critical("API client initialization failed by SystemExit. Exiting.")
        raise  # Re-raise

    except Exception as e:  # Catch any other unexpected error from init_api_clients
        logger_setup.logger.critical(f"An unexpected error occurred during API client initialization: {e}",
                                     exc_info=True)
        sys.exit(1)

    try:

        logger_setup.logger.info("--- Testing Server Connections ---")

        # Test Source Server
        logger_setup.logger.info(f"Testing source server: '{config_loader.settings['selected_source_server']}'")
        source_ok = api_clients.src_api.test_http_connection()

        # Test Target Server
        logger_setup.logger.info(f"Testing target server: '{config_loader.settings['selected_target_server']}'")
        target_ok = api_clients.target_api.test_http_connection()

        if not source_ok or not target_ok:
            logger_setup.logger.error("One or more server connection tests failed. Please check the logs above and verify your server settings.")
            sys.exit(1)

        logger_setup.logger.info("--- Server Connections OK ---")

        try:
            gen_roles_map()
            logger_setup.logger.debug("roles_map = {}".format(roles_map))
        except ValueError as e:
            logger_setup.logger.error(e)
            sys.exit(0)

        if config_loader.settings["migrate_groups"]:
            migrate_groups(api_clients.src_api, api_clients.target_api, logger_setup.get_logger_for_migration('groups'))

        gen_groups_map()
        logger_setup.logger.debug("groups_map = {}".format(groups_map))

        if config_loader.settings["migrate_users"]:
            migrate_users(api_clients.src_api, api_clients.target_api, logger_setup.get_logger_for_migration('users'))

        gen_users_map()

        # reset_folders_map(settings['src_host'], settings=settings, logger=logger)

        if config_loader.settings.get("update_users_password", False):
            asyncio.run(update_users_password(logger_setup.logger))

        if config_loader.settings["dashboard_share_with_migration_user"]:
            asyncio.run(share_source_dashboards_with_migration_user_async(src_api=api_clients.src_api,
                                                                          logger=logger_setup.get_logger_for_migration('preflight'),
                                                                          concurrency_limit=5))

        if config_loader.settings["migrate_folders"]:
            migrate_folders(api_clients.src_api, api_clients.target_api, logger_setup.get_logger_for_migration('folders'))

        # Run datamodel migration logic if either datamodels, their formulas, or saved filters are being migrated
        if (config_loader.settings.get("migrate_datamodels") or config_loader.settings.get("migrate_saved_formulas")
                or config_loader.settings.get("migrate_saved_filters")):
            migrate_datamodels(logger_setup.get_logger_for_migration('datamodels'))

        if config_loader.settings["migrate_datasecurity"]:
            migrate_datasecurity(logger_setup.get_logger_for_migration('datasecurity'))

        # if config_loader.settings["migrate_datamodels_init"]:
        #     migrate_datamodels_init()

        if config_loader.settings["migrate_dashboards"]:
            dash_logger = logger_setup.get_logger_for_migration('dashboards')
            migrate_dashboards(api_clients.src_api, api_clients.target_api, dash_logger)
            dsh_oid_map = load_dashboard_oid_map(config_loader.settings, dash_logger)
            run_second_scan(dsh_oid_map, config_loader.settings, api_clients.target_api, api_clients.src_api,
                            dash_logger)

        if config_loader.settings["migrate_blox_actions"]:
            migrate_blox_actions(logger_setup.get_logger_for_migration('blox'))

        if config_loader.settings.get("migrate_custom_code", False):
            migrate_custom_code(logger_setup.get_logger_for_migration('custom_code'))

        logger_setup.logger.info("* Migration  completed *")

    except IOError as e:
        traceback.print_exc(file=sys.stdout)
        logger_setup.logger.critical(e)
        sys.exit(0)

    except InputError as e:
        traceback.print_exc(file=sys.stdout)
        logger_setup.logger.critical(e.message)
        sys.exit(0)

    except SisenseRestAPIError as e:
        logger_setup.logger.error(
            f"SisenseRestAPIError - Request url: {e.result.url} Status code: {e.result.status_code} - {e.result.text}"
        )

    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        logger_setup.logger.critical(e)

    # Calculate and log elapsed time
    elapsed_time = datetime.now() - START_TIME
    days, remainder = divmod(elapsed_time.total_seconds(), 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = elapsed_time.microseconds // 1000

    output_string_parts = []
    if days > 0:
        output_string_parts.append(f"{int(days)}d")
    if hours > 0:
        output_string_parts.append(f"{int(hours)}h")
    if minutes > 0:
        output_string_parts.append(f"{int(minutes)}m")
    if seconds > 0 or not output_string_parts:  # show seconds if it's the only unit or non-zero
        output_string_parts.append(f"{int(seconds)}s")
    output_string_parts.append(f"{int(milliseconds)}ms")

    output_string = " ".join(output_string_parts)

    logger_setup.logger.info(f"Win2Linux Merge Tool finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger_setup.logger.info(f"Total execution time: {output_string}")


def get_users_to_migrate(src_api: SisenseRestApiClient, target_api: SisenseRestApiClient, logger=logger_setup.logger):
    logger_setup.logger.info(f"\tCollecting users to migrate.")
    target_users_email_list = [x["email"].lower() for x in api_clients.target_api.get_users() if
                               isinstance(x, dict) and "email" in x]
    logger_setup.logger.debug(f"target_users_email_list={target_users_email_list}")

    src_users_list = [u for u in api_clients.src_api.get_users() if isinstance(u, dict)]

    # The logic to handle super users is now performed directly in gen_roles_map
    # by mapping the source 'super' role to the target 'admin' role when the target is multi-tenant.
    # No user object manipulation is needed here.

    result = filter(
        lambda x: isinstance(x, dict) and x.get("email", "").lower() not in target_users_email_list, src_users_list
    )
    return list(result)


def get_target_role_id(src_role_id):
    # Attempt to find the matching role in roles_map
    res = next((role for role in roles_map if role["src_id"] == src_role_id), None)
    
    if res is not None and res.get("name") == "super":
        admin_res = next((role for role in roles_map if role.get("name") == "admin"), None)
        if admin_res:
            return admin_res["target_id"]

    if res is None:
        logger_setup.logger.error(f"No match found in roles_map for src_role_id: {src_role_id}")
        return None

    logger_setup.logger.debug("res = {}".format(res))

    return res["target_id"]


def get_target_group_id(src_group_id):
    res = next((group for group in groups_map if group["src_id"] == src_group_id), None)

    logger_setup.logger.debug("res = {}".format(res))

    if res:
        logger_setup.logger.debug(f"found target id {res['target_id']}")
        return res["target_id"]
    else:
        return None


def update_user_role_id(user):
    target_role_id = get_target_role_id(user["roleId"])
    logger_setup.logger.debug(
        "src role id = {0}, target role id = {1}".format(user["roleId"], target_role_id)
    )
    user["roleId"] = target_role_id
    logger_setup.logger.debug(f"user = {user}")
    return user


def update_user_group_ids(user):
    if "groups" in user:
        target_group_ids = list(map(get_target_group_id, user["groups"]))
        logger_setup.logger.debug(
            "src group ids: {0}, target group ids: {1}".format(
                user["groups"], target_group_ids
            )
        )
        user["groups"] = list(filter(None, target_group_ids))

    return user


def prep_user_migration_payload(users_to_migrate):
    users_to_migrate_payload = []
    for x in users_to_migrate:
        user_dict = {
            "email": x["email"],
            "userName": x["userName"],
            "firstName": x["firstName"],
        }

        if "lastName" in x:
            user_dict["lastName"] = x["lastName"]
        else:
            user_dict["lastName"] = " "
        user_dict["roleId"] = x["roleId"]

        if "groups" in x:
            user_dict["groups"] = x["groups"]
        else:
            user_dict["groups"] = []

        user_dict["preferences"] = {"localeId": "en-US"}
        user_dict["uiSettings"] = {}

        users_to_migrate_payload.append(user_dict)

    return users_to_migrate_payload


def migrate_users(src_api: SisenseRestApiClient, target_api: SisenseRestApiClient, logger=None):
    if logger is None:
        logger = logger_setup.get_logger_for_migration('users')
    logger.info(f"Migrating users...")

    chunk_error = False  # Flag to indicate if an error occurred while migrating a chunk of users

    global report_users_migration_errors  # Global variable to store user migration errors

    grand_total = 0  # Total number of users migrated across all chunks and retries

    initial_number_of_users = 0  # Initial number of users fetched for migration in the first iteration
    # Variable to keep track of successfully migrated users in the current iteration attempt (before retries with smaller chunk sizes)
    total = 0

    # Loop to fetch and migrate users until no more users are found or an unrecoverable error occurs
    while True:
        # Get the list of users to migrate from the source to the target system
        users_to_migrate = get_users_to_migrate(api_clients.src_api, api_clients.target_api, logger)

        # Check if there are any users to migrate
        if users_to_migrate:
            logger.info(f"\tFound {len(users_to_migrate)} users to migrate.")
            # Update user role IDs for the target system
            users_to_migrate = list(map(update_user_role_id, users_to_migrate))
            # Update user group IDs for the target system

            users_to_migrate = list(map(update_user_group_ids, users_to_migrate))
            # Prepare the payload for user migration

            users_to_migrate_payload = prep_user_migration_payload(users_to_migrate)

            # Initialize step size for chunking users, only if no chunk error occurred previously
            if not chunk_error:
                # Use configured chunk size if it's valid and there are enough users
                if config_loader.settings["migrate_users_chunk_size"] != -1 and len(users_to_migrate) >= \
                        config_loader.settings["migrate_users_chunk_size"]:
                    step = config_loader.settings["migrate_users_chunk_size"]
                else:
                    # Otherwise, use the total number of users as the step size (i.e., migrate all in one chunk)
                    step = len(users_to_migrate)

            total = 0  # Reset total for the current batch of users_to_migrate

            # Set the initial number of users if it hasn't been set yet
            if initial_number_of_users == 0:
                initial_number_of_users = len(users_to_migrate)

            # Iterate over users in chunks
            for i in range(0, len(users_to_migrate), step):
                x = i
                # Get the current chunk of users
                chunk = users_to_migrate_payload[x: x + step]

                try:
                    # Log the migration of the current chunk

                    logger.info(
                        f"\tMigrating next chunk (chunk size = {len(chunk)}.)"
                    )
                    # Add users in bulk to the target system

                    api_clients.target_api.add_users_bulk(json.dumps(chunk))
                    # Increment the total number of migrated users in this attempt

                    total += len(chunk)
                    # Increment the grand total of migrated users
                    grand_total += len(chunk)  # accumulate grand_total here after successful migration

                    # Log successful chunk migration
                    logger.info(
                        f"\t>> Chunk migrated successfully. {total} users of {len(users_to_migrate)} migrated."
                    )

                    # If configured, wait between chunks

                    if len(chunk) >= config_loader.settings["wait_chunk_size_threshold"]:
                        logger.info(
                            f"Waiting {config_loader.settings['wait_between_chunks']} sec between chunks..."
                        )
                        time.sleep(config_loader.settings["wait_between_chunks"])
                    # Log the emails of migrated users for debugging

                    logger.debug(f"\tMigrated users {[u['email'] for u in chunk]}")

                except SisenseRestAPIError as e:
                    # Set chunk_error flag to true if an API error occurs

                    chunk_error = True
                    # Log the error

                    logger.error(
                        f"Encountered an error migrating a user in this batch. Will retry at the end")
                    # If step size is 1 (or less), it means individual user migration failed

                    if step <= 1:
                        logger.error(
                            f"\tError migrating users {e.result.text}, {e.result.status_code}, {e.result.reason}"
                        )
                        # Log the specific users that were not migrated

                        logger.debug(
                            f"\tThe following user(s) were not migrated: {[u['email'] for u in chunk]}"
                        )
                        # Add the error to the report

                        report_users_migration_errors.add_report_entry({
                            "users": f"\tThe following user(s) were not migrated: {[u['email'] for u in chunk]}",
                            "error": e.result.text
                        })



                except Exception as e:
                    # Log any other exceptions during migration
                    logger.error(f"\tError migrating users.")
                    # Commented out retry counter: retries_s += 1

            # If a chunk error occurred, adjust step size and retry
            if chunk_error:

                # If step size is already 1, break the loop as further reduction is not possible
                if step == 1:
                    logger.info(
                        f"\tReached minimum chunk size (1) and still encountering errors. Aborting retries for this batch.")
                    break
                else:
                    # Reduce step size for retry

                    if step <= 50:  # Below 50, step should be 1
                        step = 1
                    else:
                        step = int(step / 2)
                    # Ensure step size is at least 1

                    if step == 0:
                        step = 1
                    logger.info(f"\tRetrying with smaller chunk size: {step}...")
            else:  # No chunk error in this iteration for users_to_migrate
                if total == len(users_to_migrate):  # All users in the current batch migrated successfully
                    logger.info(
                        f"\tAll {len(users_to_migrate)} users in this batch migrated successfully.")
                # Reset chunk_error for the next potential batch from get_users_to_migrate
                chunk_error = False
                # Successfully migrated all users in the current `users_to_migrate` list, continue to fetch next batch
        else:
            # No more users to migrate, break the loop
            break

    # Check if some users from the *last fetched batch* were not migrated (and there were users in that batch)

    if report_users_migration_errors.get_errors_count() > 0:  # A more reliable check for errors
        report_users_migration_errors.save_report_to_excel('reports/users_migration_report.xlsx')
        logger.info(f"\t==> Migration complete with some errors. See report file for more info.")
        # Log the final migration status

        logger.info(f"\tTotal {grand_total} users of {initial_number_of_users} migrated")

    elif 0 < initial_number_of_users == grand_total:  # All fetched users migrated
        logger.info(f"\tAll {initial_number_of_users} users migrated successfully.")
    elif initial_number_of_users == 0 and grand_total == 0:  # No users were found to migrate initially
        logger.info("\tNo new users to migrate.")


from typing import Optional


async def update_users_password(logger) -> Optional[bool]:
    """
    Updates passwords for users with a specific role asynchronously.
    """
    contributor_role_id = next(
        (i['target_id'] for i in roles_map if isinstance(i, dict) and i.get('name') == 'contributor'), None)
    if contributor_role_id is None:
        logger_setup.logger.error("Contributor role not found.")
        return False

    users_ids_to_update = [u['_id'] for u in api_clients.target_api.get_users() if
                           isinstance(u, dict) and u.get('roleId') == contributor_role_id and '_id' in u]
    default_password = config_loader.settings.get("new_password_for_migrated_users")
    if not default_password:
        default_password = "Change_Me_On_First_Login!"

    async def _async_change_user_password(user_id: str, new_password: str) -> None:
        await asyncio.to_thread(api_clients.target_api.change_user_password, user_id, new_password)
        logger_setup.logger.info(f"Password changed for user: {user_id}")

    tasks = [
        _async_change_user_password(user_id, default_password)
        for user_id in users_ids_to_update
    ]
    await asyncio.gather(*tasks)


def get_groups_to_migrate(src_api: SisenseRestApiClient, target_api: SisenseRestApiClient, logger=logger_setup.logger):
    logger_setup.logger.info(f"\tCollecting groups to migrate.")
    target_groups_names_list = [x["name"] for x in api_clients.target_api.get_groups()]
    logger_setup.logger.debug(f"target_groups_names_list={target_groups_names_list}")

    src_groups_list = api_clients.src_api.get_groups()
    result = filter(
        lambda x: x["name"] not in target_groups_names_list, src_groups_list
    )
    logger_setup.logger.debug(f"new groups to migrate: {result}")

    return list(result)


def migrate_groups(src_api: SisenseRestApiClient, target_api: SisenseRestApiClient, logger=None):
    if logger is None:
        logger = logger_setup.get_logger_for_migration('groups')
    logger.info(f"Migrating groups...")
    groups_to_migrate = get_groups_to_migrate(api_clients.src_api, api_clients.target_api, logger)

    if groups_to_migrate:
        logger.info(f"\tFound {len(groups_to_migrate)} groups to migrate.")
        res = [{"name": x["name"]} for x in groups_to_migrate]
        api_clients.target_api.add_groups_bulk(json.dumps(res))

        logger.info("{} groups migrated.".format(len(res)))
        logger.debug(
            "\tMigrated groups: {}".format([x["name"] for x in groups_to_migrate])
        )

    else:
        logger.info("\tNo new groups to migrate.")


def gen_roles_map():
    src_roles = [role for role in api_clients.src_api.get_roles() if isinstance(role, dict)]
    logger_setup.logger.debug(
        f"Roles names in the source system: {[role['name'] for role in src_roles if 'name' in role]}")

    target_roles = [role for role in api_clients.target_api.get_roles() if isinstance(role, dict)]
    logger_setup.logger.debug(
        f"Roles names in the target system: {[role['name'] for role in target_roles if 'name' in role]}")

    is_target_multitenant = bool(api_clients.target_api.tenant_path)

    ignore_custom_roles = config_loader.settings.get("ignore_custom_roles", False)

    for role in src_roles:
        role_name_to_match = role.get("name")

        # If ignore_custom_roles is enabled, strip "custom_" prefix from role name
        if ignore_custom_roles and role_name_to_match and role_name_to_match.startswith("custom_"):
            role_name_to_match = role_name_to_match[7:]  # Strip "custom_" (7 characters)
            logger_setup.logger.debug(f"Ignoring custom_ prefix: '{role.get('name')}' -> '{role_name_to_match}'")

        # If migrating to a multi-tenant system, map 'super' and 'admin' roles to the tenant 'tenantAdmin' role.
        if is_target_multitenant and role_name_to_match in ["super", "admin"]:
            logger_setup.logger.info(f"Target is multi-tenant: Mapping source '{role_name_to_match}' role to target 'tenantAdmin' role.")
            role_name_to_match = "tenantAdmin"

        # When target server is Windows, map source 'tenantAdmin' role to target 'admin' role.
        if config_loader.settings.get("target_os") == "Windows" and role_name_to_match == "tenantAdmin":
            logger_setup.logger.info("Target is Windows: Mapping source 'tenantAdmin' role to target 'admin' role.")
            role_name_to_match = "admin"

        res = next(
            (
                target_role
                for target_role in target_roles
                if isinstance(target_role, dict) and (
                    target_role.get("name") == role_name_to_match or
                    (ignore_custom_roles and target_role.get("name") and 
                     target_role.get("name").startswith("custom_") and 
                     target_role.get("name")[7:] == role_name_to_match)
                )
            ),
            None,
        )

        if res:
            roles_map.append(
                {
                    "name": role.get("name"),  # Keep original name for reference
                    "src_id": role.get("_id"),
                    "target_id": str(res.get("_id")),
                }
            )
        else:
            # If we were looking for 'tenantAdmin' and didn't find it, the error is critical.
            if role_name_to_match == "tenantAdmin":
                raise ValueError(
                    f"No matching 'tenantAdmin' role found in the target tenant for source role '{role.get('name')}'. "
                    f"Please ensure the target tenant has a 'tenantAdmin' role.")
            else:
                # For other roles, the original error message is fine.
                raise ValueError(
                    f"No matching target role found for source role '{role.get('name')}'. Please create the missing role "
                    f"before continuing.")


def gen_groups_map():
    logger_setup.logger.debug("Generating groups map...")
    src_groups = [group for group in api_clients.src_api.get_groups() if isinstance(group, dict)]
    logger_setup.logger.debug("src_groups = " + str(src_groups))
    target_groups = [group for group in api_clients.target_api.get_groups() if isinstance(group, dict)]
    for group in src_groups:
        res = next(
            (
                target_group
                for target_group in target_groups
                if isinstance(target_group, dict) and target_group.get("name") == group.get("name")
            ),
            None,
        )

        if res:
            groups_map.append(
                {
                    "name": group.get("name"),
                    "src_id": group.get("_id"),
                    "target_id": str(res.get("_id")),
                }
            )

    logger_setup.logger.debug("Group Map Done")


def gen_users_map():
    logger_setup.logger.info("Generating users map...")

    logger_setup.logger.info("Fetching source users...")
    src_users = [user for user in api_clients.src_api.get_users() if isinstance(user, dict)]

    logger_setup.logger.debug(f"src_users = {src_users}")

    logger_setup.logger.info("Fetching target users...")
    target_users = [user for user in api_clients.target_api.get_users() if isinstance(user, dict)]

    logger_setup.logger.debug(f"target_users = {target_users}")

    bar = alive_it(src_users)
    for user in bar:
        res = next(
            (
                t_user
                for t_user in target_users
                if
            isinstance(t_user, dict) and 'email' in t_user and 'email' in user and t_user['email'].lower() == user[
                'email'].lower()
            ),
            None,
        )

        if res:
            users_map.append(
                {
                    "email": user.get("email", "").lower(),
                    "src_id": user.get("_id"),
                    "target_id": str(res.get("_id")),
                    "src_role_id": user.get("roleId")
                }
            )

            bar.text(f'ok: {user.get("email", "").lower()}')

    logger_setup.logger.debug("users_map = {}".format(users_map))


def collect_dashboards_to_migrate():
    dash_list = {"count": 0, "items": []}
    chunk_size = config_loader.settings.get('dashboard_fetch_chunk_size', 100)
    if config_loader.settings['dashboard_migration_mode'] == 'ALL':
        logger_setup.logger.info(f"Collecting dashboards to migrate.")
        dash_list['items'] = fetch_all_dashboards(api_clients.src_api, logger_setup.logger, chunk_size)
        dash_list['count'] = len(dash_list['items'])
    elif config_loader.settings['dashboard_migration_mode'] in ('By Name', 'By OID'):
        logger_setup.logger.info(
            f"Collecting dashboards to migrate. Only the following dashboard(s) will be migrated: '{config_loader.settings['dashboard_include_list']}'")

        if config_loader.settings['dashboard_migration_mode'] == 'By Name':
            # Collect dashboards by title
            for title in config_loader.settings['dashboard_include_list'].split(',') if isinstance(
                    config_loader.settings['dashboard_include_list'], str) else \
                    config_loader.settings['dashboard_include_list']:

                title = title.strip()

                dash_by_title_list = api_clients.src_api.search_get_dashboards_by_title(title)

                if config_loader.settings.get("exact_match_in_dashboard_search", False):
                    dash = [d for d in dash_by_title_list['items'] if d['title'] == title]

                    dash_by_title_list['items'] = dash
                    dash_by_title_list['count'] = len(dash)

                if dash_by_title_list:
                    dash_list['items'] = dash_list['items'] + dash_by_title_list['items']
                    dash_list['count'] = dash_list['count'] + dash_by_title_list['count']

        elif config_loader.settings['dashboard_migration_mode'] == 'By OID':
            # Collect dashboards by oid
            for oid in config_loader.settings['dashboard_include_list'].split(',') if isinstance(
                    config_loader.settings['dashboard_include_list'], str) else \
                    config_loader.settings['dashboard_include_list']:

                oid = oid.strip()
                
                # Skip empty OIDs
                if not oid:
                    continue

                res = api_clients.src_api.get_dashboards_admin({
                    "id": oid,
                    "ownershipType": "allRoot",
                    # "asObject": False,
                })

                if len(res) > 0:
                    dash_list['items'].append(res[0])
                    dash_list['count'] += 1
                else:
                    logger_setup.logger.warning(f"Dashboard with OID '{oid}' not found. Skipping.")
    else:
        logger_setup.logger.warning(f"Unknown dashboard_migration_mode: '{config_loader.settings['dashboard_migration_mode']}'. No dashboards will be collected.")

    logger_setup.logger.debug("dash_list= {}".format(dash_list))
    return dash_list


import re


def remove_parentheses_content(input_string):
    """
    Removes the last content in parentheses containing only numbers
    (including the parentheses) and trims trailing spaces.
    Example: "david_test (2)" -> "david_test"
             "example (1) (2)" -> "example (1)"
             "example (a)" -> "example (a)"
    """
    return re.sub(r'\s*\(\d+\)\s*$', '', input_string).strip()


def change_folders_owner_to_migration_user(folders_map, src_api: SisenseRestApiClient, target_api: SisenseRestApiClient,
                                           logger=logger_setup.logger):
    logger_setup.logger.info(f"Temporarily changing folders owner to migration user.")
    global report_dashboard_migration_errors
    my_user = api_clients.target_api.get_my_user()

    fm = [f for f in folders_map if f["name"] != "rootFolder" and f['src_host'] == config_loader.settings['src_host']]

    if not fm:
        # Check if the user has opted-in to run this automatically
        if config_loader.settings.get('folders', {}).get("run_folder_migration_if_missing", False):
            logger.info(f"No folders found from '{config_loader.settings['src_host']}'. "
                        f"Automatically running folder migration as configured.")

            # Kick off folders migration
            config_loader.settings['folders']["share_source_dashboards_with_migration_user"] = True
            config_loader.settings['folders']["update_target_folders_owner"] = True
            migrate_folders(api_clients.src_api, api_clients.target_api, logger=logger)

            # Reload the folders map to continue
            fm = load_folders_map(settings=config_loader.settings, logger=logger)
            if not fm:
                logger.error("Folder migration ran but the folders map is still empty. Halting dashboard migration.")
                return  # Stop if folders are still missing after the run
        else:
            # If the setting is false, just warn the user and continue.
            # The dashboard migration might still succeed if only root-level dashboards are targeted.
            logger.warning(f"No folders found from '{config_loader.settings['src_host']}'.")
            logger.warning("Dashboard migration will proceed, but may fail for dashboards in sub-folders.")
            logger.warning(
                "To fix, run a folder migration first or enable 'run_folder_migration_if_missing' in the settings.")
            # We don't exit here, allowing the process to continue.

    bar = alive_it(fm)  # wrap in progress bar
    for f in bar:

        if f['name'] != "rootFolder":
            try:

                payload = {
                    "owner": my_user["_id"],
                }
                res = api_clients.target_api.update_folder(f["target_oid"], json.dumps(payload))

                bar.text(f'ok: {f["name"]}')
                logger_setup.logger.debug(f"\t\t{res}")

            except SisenseRestAPIError as e:
                logger_setup.logger.error(
                    f"Could not update folder owner {f['name']} to {f.get('target_owner_id', None)}: {e.result.text}. Skipping...")
                bar.text(f'error: {f["name"]}')
                report_dashboard_migration_errors.add_report_entry({
                    "name": f['name'],
                    "error": f"Could not update folder owner {f['name']} to {f.get('target_owner_id', None)}: {e.result.text}",
                })
                continue


def restore_folders_owners(folders_map):
    # Restore the folders owners
    logger_setup.logger.info(f'Restoring folders owners.')

    fm = [f for f in folders_map if f["name"] != "rootFolder" and f['src_host'] == config_loader.settings['src_host']]
    bar = alive_it(fm)
    for f in bar:

        if "target_owner_id" in f:
            try:
                payload = {
                    "owner": f["target_owner_id"],
                }
                res = api_clients.target_api.update_folder(f["target_oid"], json.dumps(payload))

                bar.text(f'ok: {f["name"]}')
                logger_setup.logger.debug(f"\t\t{res}")

            except SisenseRestAPIError as e:
                logger_setup.logger.error(
                    f"Could not update folder owner {f['name']} to {f['target_owner_id']}: {e.result.text}. Skipping...")
                bar.text(f'error: {f["name"]}')
                report_dashboard_migration_errors.add_report_entry({
                    "name": f['name'],
                    "error": f"Could not update folder owner {f['name']} to {f['target_owner_id']}: {e.result.text}",
                })
                continue


def fetch_all_dashboards_from_mongo(connection_string: str, logger) -> list | None:
    """
    Fetches all dashboard OIDs and owner IDs directly from the Sisense MongoDB
    for significantly improved performance over the API.

    Args:
        connection_string: The MongoDB connection string.
        logger: The logger instance for logging messages.

    Returns:
        A list of dictionaries, where each dictionary contains the 'oid' and 'owner'
        of a dashboard, or None if an error occurs.
    """
    logger.info("Fetching all target dashboards directly from MongoDB...")

    # Standard Sisense collection and DB names
    DB_NAME = "prismWebDB"
    DASHBOARD_COLLECTION = "dashboards"

    client = None
    try:
        # Check for required dnspython dependency if using srv URI
        if "mongodb+srv" in connection_string:
            try:
                import dns.resolver
            except ImportError:
                logger.critical("The 'dnspython' library is required for 'mongodb+srv' connection strings.")
                logger.critical("Please install it using: pip install pymongo[srv]")
                return None

        # Establish connection to MongoDB
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)  # Add timeout

        # Verify connection by sending a ping
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB server.")

        # Explicitly select the database
        db = client[DB_NAME]
        logger.info(f"Selected database: '{DB_NAME}'.")

        if DASHBOARD_COLLECTION not in db.list_collection_names():
            logger.error(f"Collection '{DASHBOARD_COLLECTION}' not found in the database '{DB_NAME}'.")
            logger.error("Please verify the database name and that it is the correct Sisense application database.")
            if client:
                client.close()
            return None  # Return None to signal failure

        dashboards_collection = db[DASHBOARD_COLLECTION]

        # Define the query to filter for dashboards with instanceType "owner"
        query = {"instanceType": "owner"}

        # Define the projection to only retrieve the fields we absolutely need
        projection = {'oid': 1, 'owner': 1, '_id': 0}

        logger.info(f"Querying collection '{DASHBOARD_COLLECTION}' with filter: {query}")

        # Execute the query and convert the cursor to a list
        # Using a list comprehension for efficiency
        dashboards_list = [
            {
                'oid': str(doc.get('oid')) if doc.get('oid') is not None else None,
                'owner': str(doc.get('owner')) if doc.get('owner') is not None else None
            }
            for doc in dashboards_collection.find(query, projection)
        ]

        logger.info(f"Successfully fetched {len(dashboards_list)} dashboards from MongoDB with instanceType 'owner'.")
        return dashboards_list

    except ConnectionFailure as e:
        logger.critical(f"MongoDB connection failed: {e}")
        logger.critical(
            "This may be due to a DNS issue (if connecting to a Kubernetes service from outside the cluster), "
            "incorrect credentials, or a network firewall.")
        logger.critical(
            "Please check your 'target_mongo_connection_string' in the settings and network access to the DB.")
        return None  # Return None to signal failure
    except OperationFailure as e:
        logger.critical(f"MongoDB operation failed (likely authentication or permission error): {e}")
        logger.critical("Please check the username/password and authSource in your connection string.")
        return None  # Return None to signal failure
    except Exception as e:
        logger.critical(f"An unexpected error occurred while fetching dashboards from MongoDB: {e}")
        return None  # Return None to signal failure
    finally:
        # Ensure the connection is closed
        if client:
            client.close()
            logger.info("MongoDB connection closed.")


def migrate_dashboards(src_api: SisenseRestApiClient, target_api: SisenseRestApiClient, logger=None):
    """
    Migrates dashboards based on provided settings. This function performs the
    migration of dashboards from one environment to another, based on specified
    import modes and filters. It ensures that dashboards match the correct ownership
    criteria and are properly filtered before migration. The function also logs any
    excluded dashboards and provides debug information for each dashboard selected.

    :global: folders_map - A global mapping of folder IDs required for migration.

    :raises SystemExit: Exits the process if the user denies overwriting existing
        dashboards when the import mode is set to overwrite.

    :return: None
    """
    if logger is None:
        logger = logger_setup.get_logger_for_migration('dashboards')
    global folders_map
    enable_analytics = config_loader.settings.get('enable_runtime_analytics', False)
    if enable_analytics:
        analytics = {
            'collect_dashboards_to_migrate': {},
            'fetch_target_dashboards': {},
            'filter_dashboards': {},
            'per_dashboard': [],
            'total': {},
        }
        total_start = time.perf_counter()
    # --- END RUNTIME ANALYTICS ---

    if not folders_map:
        folders_map = load_folders_map(settings=config_loader.settings, logger=logger)

    if config_loader.settings.get('use_custom_dashboard_oid', None):
        # Validate there a code defined.
        try:
            config_loader.settings['oid_host_mapping'][config_loader.settings['src_host']]
        except KeyError as e:
            logger.error(
                f'Could not fine the code for {config_loader.settings["src_host"]} in oid_host_mapping in the settings.yaml.'
                f'\nPlease update the settings.yaml and try again.')
            sys.exit(1)

    # --- TIMING: Collect dashboards to migrate ---
    t0 = time.perf_counter() if enable_analytics else None
    dash_list = collect_dashboards_to_migrate()
    t1 = time.perf_counter() if enable_analytics else None
    if enable_analytics:
        analytics['collect_dashboards_to_migrate']['seconds'] = t1 - t0
        logger.info(f"[Analytics] Collected dashboards to migrate in {t1 - t0:.3f} seconds.")

    logger.info(f"\t\tExcluded dashboards by name: {config_loader.settings['exclude_dashboards_by_name']}")
    logger.info(f"\t\tExcluded dashboards by oid: {config_loader.settings['exclude_dashboards_by_oid']}")

    # --- TIMING: Fetch target dashboards ---
    t2 = time.perf_counter() if enable_analytics else None
    target_search_result = []
    # Conditionally fetch from Mongo or API based on settings
    if config_loader.settings.get('use_mongo_for_target_dashboards', False):
        connection_string = config_loader.settings.get('target_mongo_connection_string')
        if not connection_string:
            logger.error("'use_mongo_for_target_dashboards' is true but 'target_mongo_connection_string' is missing.")
            logger.info("Falling back to API fetch.")
            target_search_result = fetch_all_dashboards(api_clients.target_api, logger=logger,
                                                        chunk_size=config_loader.settings.get(
                                                            'dashboard_fetch_chunk_size', 100))
        else:
            # The new function returns data, or None on failure.
            target_search_result = fetch_all_dashboards_from_mongo(connection_string, logger)

            # If the fetch from MongoDB fails, halt the dashboard migration process.
            if target_search_result is None:
                logger.critical("Failed to fetch dashboards from MongoDB. Halting dashboard migration.")
                return  # Stop the function execution here

    else:
        # Get current dashboard oids in target via the original, slower API method
        logger.info("Fetching all target dashboards via API...")
        chunk_size = config_loader.settings.get('dashboard_fetch_chunk_size', 100)
        target_search_result = fetch_all_dashboards(api_clients.target_api, logger=logger,
                                                    chunk_size=chunk_size)
    t3 = time.perf_counter() if enable_analytics else None
    if enable_analytics:
        analytics['fetch_target_dashboards']['seconds'] = t3 - t2
        logger.info(f"[Analytics] Fetched target dashboards in {t3 - t2:.3f} seconds.")

    # This part of the code now works seamlessly with the result from either function
    current_target_dashboard_oids_owners = [{'oid': i['oid'],
                                             'owner': i['owner'],
                                             }
                                            for i in target_search_result]

    # --- TIMING: Filter dashboards ---
    t4 = time.perf_counter() if enable_analytics else None
    dash_list_filtered = [
        dashboard for dashboard in dash_list.get("items", [])
        if should_include_dashboard(dashboard, current_target_dashboard_oids_owners)
    ]
    t5 = time.perf_counter() if enable_analytics else None
    if enable_analytics:
        analytics['filter_dashboards']['seconds'] = t5 - t4
        analytics['filter_dashboards']['count'] = len(dash_list_filtered)
        logger.info(
            f"[Analytics] Filtered dashboards in {t5 - t4:.3f} seconds. {len(dash_list_filtered)} dashboards to migrate.")

    for i in dash_list_filtered:
        title = i.get('title') or i.get('name') or 'N/A'
        dash_type = i.get('type', 'N/A')
        owner = i.get('owner', 'N/A')
        ownership_type = i.get('ownershipType', i.get('instanceType', 'N/A'))
        logger.debug(f"{title}, {dash_type}, {owner}, {ownership_type}")

    logger.info(f"Found {len(dash_list_filtered)} dashboard/s to migrate")

    # Get list of target dashboard oids
    target_dash_oids = []
    global dashboard_oid_map

    if len(dash_list_filtered) > 0:
        change_folders_owner_to_migration_user(folders_map, src_api=api_clients.src_api,
                                               target_api=api_clients.target_api, logger=logger)

        # --- TIMING: Per-dashboard migration ---
        concurrency = config_loader.settings.get('dashboard_migration_concurrency', 5)
        if enable_analytics:
            logger.info(f"[Analytics] Running dashboard migration with concurrency={concurrency}")
        per_dashboard_times = []
        # Use the parallel migration function
        migration_results = asyncio.run(
            run_parallel_migrations(dash_list_filtered, folders_map, dashboard_oid_map, concurrency_limit=concurrency)
        )
        # Collect timing info if available from results (if you want to extend run_parallel_migrations to return timing)
        # For now, just record the count
        if enable_analytics:
            analytics['per_dashboard'] = [
                {'oid': dash.get('oid'), 'title': dash.get('title', dash.get('oid', 'unknown'))}
                for dash in dash_list_filtered
            ]

        restore_folders_owners(folders_map)

        if config_loader.settings.get("validate_dashboards_migration", True):
            if not asyncio.run(validate_dashboards_migration()):
                logger.info(
                    f"Some discrepancies found between source and target dashboards. Please check migration report.")

    save_dashboard_oid_map(dashboard_oid_map, settings=config_loader.settings, logger=logger)

    if report_dashboard_migration_errors.content:
        report_dashboard_migration_errors.save_report_to_excel("reports/dashboards_migration_report.xlsx")
    # if report_dashboards_missing_owner.content:
    #      report_dashboards_missing_owner.save_report_to_excel("reports/dashboards_migration_report.xlsx")
    # if report_dashboards_skipped_already_exists.content:
    #     report_dashboards_skipped_already_exists.save_report_to_excel("reports/dashboards_migration_report.xlsx")
    # if report_move_dashboard_to_folder_error.content:
    #     report_move_dashboard_to_folder_error.save_report_to_excel("reports/move_dashboard_to_folder_error_report.xlsx")

    # --- TIMING: Total ---
    if enable_analytics:
        total_end = time.perf_counter()
        analytics['total']['seconds'] = total_end - total_start
        logger.info(f"[Analytics] Total dashboard migration time: {total_end - total_start:.3f} seconds.")
        try:
            with open("dashboard_migration_timing.json", "w") as f:
                json.dump(analytics, f, indent=2)
            logger.info(f"[Analytics] Timing analytics written to dashboard_migration_timing.json")
        except Exception as e:
            logger.error(f"[Analytics] Failed to write timing analytics: {e}")


# Run second scan to check for any missed dashboards
def run_second_scan(dashboard_oid_map, settings, target_api: SisenseRestApiClient, src_api: SisenseRestApiClient,
                    logger=None):
    if logger is None:
        logger = logger_setup.get_logger_for_migration('dashboards')
    logger.info("Running second scan to check for any missed dashboards that are still owned by the migration user.")

    # Get the migration user's ID to filter dashboards
    my_user = api_clients.target_api.get_my_user()
    my_user_id = my_user.get('_id')
    if not my_user_id:
        logger.error("Could not retrieve migration user ID from target. Skipping second scan.")
        return

    all_target_dashboards = []
    # Conditionally fetch from Mongo or API based on settings
    if config_loader.settings.get('use_mongo_for_target_dashboards', False):
        connection_string = config_loader.settings.get('target_mongo_connection_string')
        if not connection_string:
            logger.error("'use_mongo_for_target_dashboards' is true but 'target_mongo_connection_string' is missing.")
            logger.info("Falling back to API fetch for second scan.")
            all_target_dashboards = fetch_all_dashboards(api_clients.target_api, logger=logger)
        else:
            all_target_dashboards = fetch_all_dashboards_from_mongo(connection_string, logger)
            if all_target_dashboards is None:
                logger.critical("Failed to fetch dashboards from MongoDB. Halting second scan.")
                return
    else:
        logger.info("Fetching all target dashboards via API for second scan...")
        all_target_dashboards = fetch_all_dashboards(api_clients.target_api, logger=logger)

    # Filter the fetched list to find dashboards owned by the migration user
    migration_user_dashboards = [d for d in all_target_dashboards if d.get('owner') == my_user_id]
    logger.info(f"Found {len(migration_user_dashboards)} dashboards owned by the migration user on the target.")

    if not migration_user_dashboards:
        logger.info("No dashboards requiring ownership change found. Second scan complete.")
        return

    oids_to_migrate = []
    for i in migration_user_dashboards:
        if i.get('oid') in dashboard_oid_map:
            # Map the target OID back to the original source OID for re-migration
            oids_to_migrate.append(dashboard_oid_map[i['oid']])

    if not oids_to_migrate:
        logger.info("No re-migration candidates found from dashboards owned by migration user. Second scan complete.")
        return

    logger.info(f"Found {len(oids_to_migrate)} dashboards to re-migrate in second scan.")

    # Temporarily override settings for the second-pass migration
    original_dashboard_migration_mode = settings.get('dashboard_migration_mode')
    original_dashboard_include_list = settings.get('dashboard_include_list')
    original_dashboard_import_mode = settings.get('dashboard_import_mode')

    try:
        settings['dashboard_migration_mode'] = "By OID"
        settings['dashboard_include_list'] = ",".join(oids_to_migrate)
        settings['dashboard_import_mode'] = "overwrite"

        # Re-run the migration with the new, specific dashboard list
        migrate_dashboards(src_api=api_clients.src_api, target_api=api_clients.target_api, logger=logger)
    finally:
        # Restore original settings to avoid side effects
        logger.info("Restoring original dashboard migration settings after second scan.")
        settings['dashboard_migration_mode'] = original_dashboard_migration_mode
        settings['dashboard_include_list'] = original_dashboard_include_list
        settings['dashboard_import_mode'] = original_dashboard_import_mode

async def run_parallel_migrations(dash_list, folders_map, dashboard_oid_map, concurrency_limit=5, job_id=None):
    """
    Runs dashboard migrations in parallel, reporting progress for both terminal and web UI.
    """
    semaphore = asyncio.Semaphore(concurrency_limit)
    tasks = []
    results = []
    total_tasks = len(dash_list)

    if total_tasks == 0:
        logger_setup.logger.info("No dashboards to migrate in this batch.")
        return []

    logger_setup.logger.info(f"Creating migration tasks for {total_tasks} dashboards...")
    for dash in dash_list:
        task = asyncio.create_task(worker(semaphore, dash, folders_map, dashboard_oid_map))
        tasks.append(task)

    logger_setup.logger.info(f"Running migrations with concurrency limit: {concurrency_limit}")

    # alive_bar is disabled globally when not in a TTY, so this check is for printing progress
    is_interactive = sys.stdout.isatty()

    if not is_interactive:
        # For non-interactive mode, print initial progress state for the web UI
        print(f"MIGRATION_PROGRESS::{json.dumps({'current': 0, 'total': total_tasks})}", flush=True)

    completed_count = 0
    # alive_bar will show in the terminal if interactive, and do nothing if not.
    with alive_bar(total_tasks, title="Migrating Dashboards") as bar:
        for future in asyncio.as_completed(tasks):
            try:
                result = await future
                results.append(result)
            except Exception as e:
                logger_setup.logger.exception(f"Unexpected error processing a migration future:")
                results.append({"status": "future_error", "message": str(e)})

            completed_count += 1
            bar()  # This progresses the bar in interactive mode.

            if not is_interactive:
                # Print structured progress for the server to parse
                print(f"MIGRATION_PROGRESS::{json.dumps({'current': completed_count, 'total': total_tasks})}", flush=True)

    return results


async def get_src_dashboards_async():
    logger_setup.logger.debug("Wrapping synchronous source dashboard call...")
    return await asyncio.to_thread(api_clients.src_api.search_get_all_dashboards)


async def get_target_dashboards_async():
    logger_setup.logger.debug("Wrapping synchronous target dashboard call...")
    return await asyncio.to_thread(api_clients.target_api.search_get_all_dashboards)


async def validate_dashboards_migration():
    logger_setup.logger.info(f"Validating dashboards migration...")
    # Run both wrapped synchronous API calls concurrently
    src_task = asyncio.create_task(get_src_dashboards_async())
    target_task = asyncio.create_task(get_target_dashboards_async())

    # Wait for both tasks to complete
    src_search_result, target_search_result = await asyncio.gather(src_task, target_task)

    src_dashboards_oids = [i['oid'] for i in src_search_result["items"]]
    target_dashboards_oids = [i['oid'] for i in target_search_result["items"]]
    not_migrated_dashboards_oids = [i['oid'] for i in report_dashboards_missing_owner.content]

    only_in_src = [i for i in src_dashboards_oids if i not in target_dashboards_oids]
    only_in_src = [i for i in only_in_src if i not in not_migrated_dashboards_oids]

    return True if not only_in_src else False


def should_include_dashboard(dashboard, target_dashboard_oids_owners):
    """Determine if a dashboard should be included in the filtered list, with detailed logging for exclusions."""
    dash_title = dashboard.get('title', 'Untitled')
    dash_oid = dashboard.get('oid', 'N/A')

    # Check 1: Ownership type must be 'owner' or 'root'
    logger_setup.logger.debug(f'{dashboard.get("ownershipType")} {dashboard.get("instanceType")}')
    if dashboard.get("ownershipType") not in ["owner", "root"] and dashboard.get("instanceType") not in ["owner"]:
        logger_setup.logger.info(
            f"Excluding dashboard '{dash_title}' (OID: {dash_oid}) - Invalid ownershipType: '{dashboard.get('ownershipType')}'.")
        return False

    # Check 2: Exclude dashboards by name
    exclude_by_name_str = config_loader.settings.get("exclude_dashboards_by_name", "")
    if exclude_by_name_str and dash_title in [name.strip() for name in exclude_by_name_str.split(',')]:
        logger_setup.logger.info(
            f"Excluding dashboard '{dash_title}' (OID: {dash_oid}) - Found in 'exclude_dashboards_by_name' list.")
        return False

    # Check 3: Exclude dashboards by OID
    exclude_by_oid_str = config_loader.settings.get("exclude_dashboards_by_oid", "")
    if exclude_by_oid_str and dash_oid in [oid.strip() for oid in exclude_by_oid_str.split(',')]:
        logger_setup.logger.info(
            f"Excluding dashboard '{dash_title}' (OID: {dash_oid}) - Found in 'exclude_dashboards_by_oid' list.")
        return False

    # Check 4: Skip dashboards with missing owners if setting is enabled
    if config_loader.settings.get("skip_dashboards_with_missing_owner", False):
        # Handle both data structures: search_get_all_dashboards (with ownerInfo) and get_dashboards_admin (without ownerInfo)
        owner_email = dashboard.get("ownerInfo", {}).get("email")
        owner_id = dashboard.get("owner")
        
        # If no owner email from ownerInfo, but we have an owner ID, that's still valid
        # (This handles the fallback method case where ownerInfo might not be present)
        if not owner_email and not owner_id:
            logger_setup.logger.info(
                f"Excluding dashboard '{dash_title}' (OID: {dash_oid}) - Missing owner information.")
            global report_dashboards_missing_owner
            report_dashboards_missing_owner.add_report_entry({
                "title": dash_title,
                "owner": dashboard.get("owner", None),
                "oid": dash_oid,
            })
            return False

    # Check 5: Skip dashboards with missing ancestor folder if setting is enabled
    if config_loader.settings.get("skip_dashboards_with_missing_ancestor_folder", False):
        parent_folder_oid = dashboard.get('parentFolder')
        if parent_folder_oid and not is_folder_in_map(parent_folder_oid):
            logger_setup.logger.info(
                f"Excluding dashboard '{dash_title}' (OID: {dash_oid}) - Parent folder (OID: {parent_folder_oid}) not found in target map.")
            return False

    # Check 6: Skip dashboards that already exist on the target if import mode is 'skip'
    if config_loader.settings.get('dashboard_import_mode', "skip") == "skip":
        target_oid_to_check = None
        if config_loader.settings.get('use_custom_dashboard_oid', False):
            try:
                target_oid_to_check = encode_hex_oid_with_hostcode(dash_oid)
            except (ValueError, KeyError, LookupError) as e:
                logger_setup.logger.warning(
                    f"Could not generate custom OID for dashboard '{dash_title}' (OID: {dash_oid}) during check. Error: {e}. Assuming it doesn't exist.")
                target_oid_to_check = None  # Cannot check, so assume it doesn't exist
        else:
            target_oid_to_check = dash_oid

        if target_oid_to_check and target_oid_to_check in [i['oid'] for i in target_dashboard_oids_owners]:
            logger_setup.logger.info(
                f"Excluding dashboard '{dash_title}' (OID: {dash_oid}) - Already exists on target (as OID {target_oid_to_check}) and import mode is 'skip'.")
            global report_dashboards_skipped_already_exists
            report_dashboards_skipped_already_exists.add_report_entry({
                "title": dash_title,
                "oid": dash_oid,
            })
            return False

    # If all checks pass, include the dashboard
    logger_setup.logger.debug(f"Including dashboard '{dash_title}' (OID: {dash_oid}) for migration.")
    return True


def encode_hex_oid_with_hostcode(original_hex_oid: str) -> str | None:
    """
    Encodes server info into a 24-digit hex OID by replacing the first
    4 hex digits with a mapped 4-digit hex code.

    Args:
        original_hex_oid: The base OID string (e.g., "1a2b3c4d5e6f708090a0b0c0").
                          Must be exactly 24 hexadecimal digits.


    Returns:
        The modified 24-digit hex OID string with the first 4 digits replaced,
        or None if inputs are invalid, the server name is not in the map,
        or an error occurs.

    Raises:
        ValueError: If original_hex_oid format is invalid (not 24 hex digits),
                    server_name is empty, host_to_code_map is empty,
                    or the mapped code is not a valid 4-digit hex string.
        KeyError: If server_name is not found in host_to_code_map.
        Exception: For other potential errors during processing.
    """
    # --- Input Validation ---
    if not original_hex_oid or not re.fullmatch(r'[0-9a-fA-F]{24}', original_hex_oid):
        raise ValueError("Invalid original_hex_oid format. Expected exactly 24 hexadecimal digits.")

    try:
        # --- Encoding Process ---
        # 1. Look up the 4-digit hex code for the server name
        #    Raises KeyError if server_name is not found.

        # --- Get the host map ---

        # Use .get() to avoid errors if the key is missing
        host_map_from_yaml = config_loader.settings.get('oid_host_mapping', {})
        if not isinstance(host_map_from_yaml, dict):
            print("Warning: 'oid_host_mapping' in settings.yaml is not a valid map/dictionary. Using empty map.")
            host_map_from_yaml = {}
        hex_code = host_map_from_yaml[config_loader.settings['src_host']]

        if not hex_code:
            raise LookupError(
                f'Could not fine the code for {config_loader.settings["src_host"]} in oid_host_mapping in the settings.yaml.'
                f'\nPlease update the settings.yaml and try again. ')

        # 2. Validate the retrieved hex code format (must be exactly 4 hex digits)
        if not re.fullmatch(r'[0-9a-fA-F]{4}', hex_code):
            raise ValueError(
                f"Invalid hex code '{hex_code}' for server '{config_loader.settings['src_host']}'. Must be 4 hex digits.")

        # 3. Get the remaining part (last 20 digits) of the original OID
        oid_tail = original_hex_oid[4:]  # Get characters from index 4 to the end

        # 4. Construct the new OID by concatenating the code and the tail
        #    Ensure the code is lowercase to match potential map key format during decode.
        modified_hex_oid = hex_code.lower() + oid_tail

        # 5. Final check (should always be 24 digits if inputs were valid)
        if len(modified_hex_oid) != 24:
            # This should ideally not happen if logic is correct
            raise RuntimeError(f"Internal error: Generated OID length is not 24. OID: {modified_hex_oid}")

        return modified_hex_oid

    # Allow KeyError to propagate naturally if server_name not found
    # Allow ValueError to propagate naturally on invalid format/code
    except Exception as e:
        print(f"Error during encoding: {e}")
        # raise
        return None


def update_nested_drill_target_oids(data_structure):
    """
    Recursively scans a dictionary or list to find 'drillTarget' objects
    and updates their 'oid' field using encode_hex_oid_with_hostcode.

    Args:
        data_structure (dict or list): The JSON data to scan.
    """
    # If the structure is a dictionary, check for drillTarget and recurse
    if isinstance(data_structure, dict):
        # The 'options' key is a common place for drillTarget in widgets
        options = data_structure.get('options', {})
        if isinstance(options, dict) and 'drillTarget' in options:
            drill_target = options['drillTarget']
            if isinstance(drill_target, dict) and 'oid' in drill_target:
                original_oid = drill_target['oid']
                if original_oid:
                    # Update the oid using the provided encoding function
                    drill_target['oid'] = encode_hex_oid_with_hostcode(original_oid)
                    logger_setup.logger.info(
                        f"Found and updated nested drillTarget oid: {original_oid} -> {drill_target['oid']}"
                    )

        # Continue scanning through all values in the dictionary
        for value in data_structure.values():
            update_nested_drill_target_oids(value)

    # If the structure is a list, iterate over its elements and recurse
    elif isinstance(data_structure, list):
        for item in data_structure:
            update_nested_drill_target_oids(item)


def update_datasource_references(obj, dashboard_title, path=""):
    """
    Recursively scan and update all datasource references in a dashboard JSON object.
    
    Args:
        obj: The JSON object to scan (can be dict, list, or other)
        dashboard_title (str): Name of the dashboard for logging
        path (str): Current path in the JSON structure for logging
        
    Returns:
        int: Number of datasource updates made
    """
    updates_made = 0
    
    if isinstance(obj, dict):
        # Check if this dict has datasource-like properties
        if 'address' in obj and ('title' in obj or 'fullname' in obj):
            # This looks like a datasource object
            if obj.get('address') != "LocalHost":
                old_address = obj.get('address', 'Unknown')
                obj['address'] = "LocalHost"

                # Update fullname if it exists
                if 'title' in obj:
                    obj['fullname'] = f"LocalHost/{obj['title']}"
                    logger_setup.logger.info(
                        f"Updated datasource at {path}: address '{old_address}' -> 'LocalHost', fullname -> 'LocalHost/{obj['title']}'"
                    )
                elif 'fullname' in obj:
                    # Extract title from existing fullname if possible
                    fullname_parts = obj['fullname'].split('/', 1)
                    if len(fullname_parts) > 1:
                        title = fullname_parts[1]
                        obj['fullname'] = f"LocalHost/{title}"
                        logger_setup.logger.info(
                            f"Updated datasource at {path}: address '{old_address}' -> 'LocalHost', fullname -> 'LocalHost/{title}'"
                        )
                    else:
                        logger_setup.logger.info(
                            f"Updated datasource at {path}: address '{old_address}' -> 'LocalHost' (fullname unchanged)"
                        )
                else:
                    logger_setup.logger.info(
                        f"Updated datasource at {path}: address '{old_address}' -> 'LocalHost'"
                    )
                updates_made += 1
        
        # Recursively process all values in the dictionary
        for key, value in obj.items():
            current_path = f"{path}.{key}" if path else key
            updates_made += update_datasource_references(value, dashboard_title, current_path)
    
    elif isinstance(obj, list):
        # Recursively process all items in the list
        for i, item in enumerate(obj):
            current_path = f"{path}[{i}]" if path else f"[{i}]"
            updates_made += update_datasource_references(item, dashboard_title, current_path)
    
    return updates_made


def scan_and_fix_jaql(obj: dict | list, root_obj=None) -> bool:
    """
    Recursively scans a dictionary or list to perform data integrity fixes on JAQL objects.

    Args:
        obj: The dictionary or list to scan.
        root_obj: The root object to scan for global replacements (used internally).

    Returns:
        True if any data was modified, otherwise False.
    """
    changes_made = False

    if root_obj is None:
        root_obj = obj

    if not isinstance(obj, (dict, list)):
        return False

    if isinstance(obj, list):
        for item in obj:
            if scan_and_fix_jaql(item, root_obj):
                changes_made = True
        return changes_made

    # --- PERFORM CHECKS AND MODIFICATIONS ON THE CURRENT OBJECT ---

    # Check 1: Enforce that 'dim' is exactly '[table.column]'
    if all(k in obj for k in ['dim', 'table', 'column']) and isinstance(obj.get('dim'), str):
        expected_dim = f"[{obj['table']}.{obj['column']}]"
        if obj['dim'] != expected_dim:
            logger_setup.logger.info(f"  - (Format Check) 'dim' value is incorrect.")
            logger_setup.logger.info(f"    - Updating 'dim' from \"{obj['dim']}\" to \"{expected_dim}\"")
            wrong_dim = obj['dim']

            # Scan the entire root_obj for all occurrences of wrong_dim and replace with expected_dim
            def replace_dim_in_obj(o):
                modified = False
                if isinstance(o, dict):
                    for k, v in o.items():
                        # Replace in dict value
                        if isinstance(v, str) and v == wrong_dim:
                            o[k] = expected_dim
                            modified = True
                        elif isinstance(v, (dict, list)):
                            if replace_dim_in_obj(v):
                                modified = True
                    # Also check if the dict itself has a 'dim' key (legacy logic)
                    if o.get('dim') == wrong_dim:
                        o['dim'] = expected_dim
                        modified = True
                elif isinstance(o, list):
                    for idx, item in enumerate(o):
                        if isinstance(item, str) and item == wrong_dim:
                            o[idx] = expected_dim
                            modified = True
                        elif isinstance(item, (dict, list)):
                            if replace_dim_in_obj(item):
                                modified = True
                return modified

            if replace_dim_in_obj(root_obj):
                changes_made = True

    # Check 2: Remove double brackets at the end of 'dim' (cleans up old errors)
    if 'dim' in obj and isinstance(obj.get('dim'), str) and obj['dim'].endswith(']]'):
        old_dim = obj['dim']
        obj['dim'] = obj['dim'][:-1]  # Remove the last character
        logger_setup.logger.info(f"  - (Double Bracket Check) Found and removed extra ']' at the end of 'dim'.")
        logger_setup.logger.info(f"    - Updating 'dim' from \"{old_dim}\" to \"{obj['dim']}\"")
        changes_made = True

    # --- RECURSE INTO CHILD PROPERTIES ---
    for key in obj:
        if scan_and_fix_jaql(obj[key], root_obj):
            changes_made = True

    return changes_made


def migrate_single_dashboard(dash, folders_map, dashboard_oid_map):
    try:
        dash_json = api_clients.src_api.export_dash_by_id(dash["oid"])
    except SisenseRestAPIError as e:
        logger_setup.logger.error(f"Error exporting dashboard {dash['title']}. Reason: {e.result.reason}. Skipping.")
        report_dashboard_migration_errors.add_report_entry({
            "src_oid": dash["oid"],
            "title": dash["title"],
            "src_host": config_loader.settings["src_host"],
            "error": e.result.reason,
        })
        return
    except Exception as e:
        logger_setup.logger.error(f"Error exporting dashboard {dash['title']}. Reason: {e}. Skipping.")
        report_dashboard_migration_errors.add_report_entry({
            "src_oid": dash["oid"],
            "title": dash["title"],
            "src_host": config_loader.settings["src_host"],
            "error": e,
        })
        return

    res = None
    retries_s = 0
    retries_e = 1
    retry = True

    scan_and_fix_jaql(dash_json)

    # Update all datasource references in the dashboard JSON
    total_updates = update_datasource_references(dash_json, dash['title'])
    if total_updates > 0:
        logger_setup.logger.info(f"Dashboard '{dash['title']}': Updated {total_updates} datasource reference(s)")
    else:
        logger_setup.logger.debug(f"Dashboard '{dash['title']}': No datasource updates needed")

    while retry:
        try:
            # Import is hard coded to overwrite since the skip logic is handled by the script.
            # If we get here, the dashboard should be migrated.
            if config_loader.settings.get('use_custom_dashboard_oid', False):
                orig_oid = dash_json['oid']

                # set the new custom oid
                dash_json['oid'] = encode_hex_oid_with_hostcode(orig_oid)

                # save the original oid and the new custom oid in the mad
                dashboard_oid_map[dash_json['oid']] = orig_oid

            # Scan the entire dashboard JSON for nested 'drillTarget' objects and update their 'oid' fields.
            update_nested_drill_target_oids(dash_json)

            res = api_clients.target_api.import_dashboards_bulk(
                json.dumps([dash_json]), "overwrite"

            )
            retry = False
        except SisenseRestAPIError as e:
            logger_setup.logger.error(
                f"\tError migrating dashboard {e.result.text}, {e.result.status_code}, {e.result.reason}"
            )
            report_dashboard_migration_errors.add_report_entry({
                "src_oid": dash["oid"],
                "title": dash["title"],
                "src_host": config_loader.settings["src_host"],
                "error": e.result.reason,
            })
            retries_s += 1
            if retries_s <= retries_e:
                logger_setup.logger.info(f"\tRetrying ({retries_s}/{retries_e})...")
            else:
                retry = False
                res = None
                logger_setup.logger.info("Skipping.....")

            logger_setup.logger.info(f"Waiting {config_loader.settings['wait_between_retries']} sec...")
            time.sleep(config_loader.settings["wait_between_retries"])

    if res and len(res["succeded"]) > 0:
        new_dash = res["succeded"][0]
        update_dashboard_title(new_dash, dash)
        move_and_share_dashboard(new_dash, dash, dash_json, folders_map)


def update_dashboard_title(new_dash, dash):
    new_title = remove_parentheses_content(dash['title'])
    if new_title != new_dash['title']:
        logger_setup.logger.info(f"Renaming dashboard {new_dash['title']} to {new_title}")
        # Rename dashboard
        try:
            api_clients.target_api.rename_dashboard(new_dash["oid"], new_title)
        except SisenseRestAPIError as e:
            logger_setup.logger.error(
                f"Error renaming dashboard {new_dash['title']}. Reason: {e.result.reason}. Skipping.")
            report_dashboard_migration_errors.add_report_entry({
                "src_oid": dash["oid"],
                "title": dash["title"],
                "src_host": config_loader.settings["src_host"],
                "error": e.result.reason,
            })
            return


def is_folder_in_map(src_folder_oid):
    f = next(
        (
            folder
            for folder in folders_map
            if folder["src_oid"] == src_folder_oid and
               folder['src_host'] == config_loader.settings['src_host']
        ),
        None,
    )
    return f


def move_and_share_dashboard(new_dash, dash, dash_json, folders_map):
    # Move dashboard to the correct folder
    if "parentFolder" in dash_json:

        f = is_folder_in_map(dash_json["parentFolder"])

        if f:
            try:
                api_clients.target_api.move_dashboard_to_folder(
                    new_dash["oid"], f["target_oid"]
                )
            except SisenseRestAPIError as e:
                logger_setup.logger.error(
                    f"Request url: {e.result.url} Status code: {e.result.status_code} - {e.result.text}")
                report_dashboard_migration_errors.add_report_entry({
                    "title": new_dash["title"],
                    "oid": new_dash["oid"],
                    "error": e.result.text,
                })

    logger_setup.logger.info("Sharing dashboard")

    updated_shares = []
    for s in dash["shares"]:
        try:
            if s["type"] == "user":
                u = next(
                    (
                        user
                        for user in users_map
                        if user["src_id"] == s["shareId"]
                    ),
                    None,
                )

                if s["shareId"] == dash["owner"]:
                    s["rule"] = "edit"

                s["shareId"] = u["target_id"]

            elif s["type"] == "group":
                g = next(
                    (
                        group
                        for group in groups_map
                        if group["src_id"] == s["shareId"]
                    ),
                    None,
                )
                s["shareId"] = g["target_id"]

            updated_shares.append({
                "shareId": s["shareId"],
                "rule": s["rule"],
                "type": s["type"],
                "subscribe": s.get("subscribe", False),
            })

        except Exception as e:
            logger_setup.logger.warning(f"Review shares for dashboard {dash['title']}")

    # Check that the migration user in the target is included in the shares otherwise add it
    my_user_id = api_clients.target_api.get_my_user()
    if my_user_id.get('_id', None) not in [s['shareId'] for s in updated_shares]:
        updated_shares.append({
            "shareId": my_user_id['_id'],
            "rule": "edit",
            "type": "user",
            "subscribe": False
        })

    logger_setup.logger.debug(f"updated_shares = {updated_shares}")

    updated_subscription = dash_json.get('subscription', {})
    if updated_subscription:

        if updated_subscription.get('context', None):
            updated_subscription['context']['dashboardid'] = new_dash["oid"]

    shares_payload = {
        "sharesTo": updated_shares,
        "sharesToNew": [],
        "subscription": updated_subscription,
    }

    # --- ANALYTICS: Log payload size and timing for share_dashboard ---
    import time
    payload_json = json.dumps(shares_payload)
    logger_setup.logger.info(
        f"[Analytics] Sharing dashboard OID={new_dash['oid']} Title='{new_dash.get('title', '')}' with {len(updated_shares)} users/groups. Payload size: {len(payload_json)} bytes.")
    t_share_start = time.perf_counter()
    api_clients.target_api.share_dashboard(new_dash["oid"], payload_json)
    t_share_end = time.perf_counter()
    logger_setup.logger.info(
        f"[Analytics] share_dashboard call for OID={new_dash['oid']} took {t_share_end - t_share_start:.3f} seconds.")

    # todo: add exception handling
    api_clients.target_api.share_dashboard(new_dash["oid"], json.dumps(shares_payload))

    # Check if the owner needs to be updated
    u = next(
        (user for user in users_map if user["src_id"] == dash["owner"]),
        None,
    )

    # Ensure `u` is not None before proceeding
    if u:
        if new_dash["owner"] != u["target_id"]:
            logger_setup.logger.info(f"Updating owner id")
            change_dash_owner(
                new_dash["oid"], new_dash["owner"], u["target_id"]
            )

            new_dash["owner"] = u["target_id"]
    else:
        logger_setup.logger.warning(
            f"Owner for dashboard '{dash['title']}' not found in users_map. Skipping owner update.")


def change_dash_owner(dash_oid, curr_owner_id, target_owner_id):
    user = api_clients.target_api.get_user_by_id(target_owner_id)

    try:
        can_be_owned = api_clients.target_api.can_be_owned(dash_oid, user["email"])
        logger_setup.logger.debug(f"can_be_owned = {can_be_owned}")
        if can_be_owned["canBeOwned"] is True:
            try:
                api_clients.target_api.change_dashboard_owner(dash_oid, target_owner_id)

            except SisenseRestAPIError as e:
                logger_setup.logger.warning(
                    f"Dashboard owner cannot be changed. Reason: {e.result.text}"
                )

        else:
            logger_setup.logger.warning(
                f'Dashboard owner cannot be changed. Reason: {can_be_owned["reasonMessage"]}'
            )

    except SisenseRestAPIError as e:
        if e.result.status_code == 404:
            logger_setup.logger.warning(
                f"Dashboard owner cannot be changed. Reason: {e.result.text}"
            )


# --- Concurrency Management for Dashboard Migration---

async def worker(semaphore, dash, folders_map, dashboard_oid_map):
    """
    A worker task that acquires the semaphore before running the migration.
    Ensures that only a limited number of migrations run concurrently.
    """
    title = dash.get('title', 'Unknown Dashboard')
    async with semaphore:
        # Semaphore acquired, proceed with the migration
        logger_setup.logger.info(f"Semaphore acquired, starting migration process for: {title}")
        result = await migrate_single_dashboard_async(dash, folders_map, dashboard_oid_map)
        # Semaphore automatically released upon exiting the 'async with' block
        logger_setup.logger.info(f"Finished migration process for: {title} (Semaphore released)")
        return result


async def migrate_single_dashboard_async(dash, folders_map, dashboard_oid_map):
    """
    Your original async wrapper using asyncio.to_thread.
    This correctly runs the blocking function in a separate thread
    without blocking the asyncio event loop.
    """
    title = dash.get('title', 'Unknown Dashboard')
    logger_setup.logger.debug(f"Scheduling migration for: {title} onto thread pool")
    try:
        # CORRECTED: Pass the function and arguments separately to to_thread
        result = await asyncio.to_thread(migrate_single_dashboard, dash, folders_map, dashboard_oid_map)
        return result
    except Exception as e:
        # Use logger.exception() here as well for scheduling/awaiting errors
        logger_setup.logger.exception(f"Error scheduling/awaiting migration for {title}:")
        return {"title": title, "status": "scheduling_error", "message": str(e)}


def migrate_datamodels_init():
    # assign directory
    directory = config_loader.settings["init_datamodel_directory"]

    # iterate over files in
    # that directory
    for filename in os.scandir(directory):
        if filename.is_file():
            fn = filename.path.strip()
            logger_setup.logger.info(f"\tImporting datamodel {fn}...")
            with open(fn) as f:
                data = json.load(f)

            new_dm = api_clients.target_api.import_datamodel(json.dumps(data))

            logger_setup.logger.info(f"\tDatamodel imported successfully.")

            logger_setup.logger.debug(f"new_dm={new_dm}")


def get_filtered_datamodels(all_datamodels, log_filter_info=True):
    """
    Filter datamodels based on include_datamodels and exclude_datamodels settings.
    Returns a list of filtered datamodel dictionaries.
    
    Args:
        all_datamodels: List of all datamodel dictionaries to filter
        log_filter_info: If True, log the filter configuration (default: True)
    """
    # Process include_datamodels - handle both string and list types
    include_datamodels_raw = config_loader.settings.get("include_datamodels", "")
    if isinstance(include_datamodels_raw, list):
        include_datamodels_str = ", ".join(str(x) for x in include_datamodels_raw if x)
    else:
        include_datamodels_str = str(include_datamodels_raw).strip() if include_datamodels_raw else ""
    
    # Process exclude_datamodels - handle both string and list types
    exclude_datamodels_raw = config_loader.settings.get("exclude_datamodels", "")
    if isinstance(exclude_datamodels_raw, list):
        exclude_list = [str(x).strip() for x in exclude_datamodels_raw if x]
    else:
        exclude_datamodels_str = str(exclude_datamodels_raw).strip() if exclude_datamodels_raw else ""
        exclude_list = [title.strip() for title in exclude_datamodels_str.split(",") if title.strip()] if exclude_datamodels_str else []
    
    # Determine include list
    include_all = False
    include_list = []
    if not include_datamodels_str or include_datamodels_str == "ALL":
        include_all = True
        if log_filter_info:
            logger_setup.logger.info("include_datamodels is empty or 'ALL' - will migrate all datamodels (except excluded ones).")
    else:
        include_list = [title.strip() for title in include_datamodels_str.split(",") if title.strip()]
        if log_filter_info:
            logger_setup.logger.info(f"include_datamodels filter active - will only migrate: {include_list}")
    
    if exclude_list and log_filter_info:
        logger_setup.logger.info(f"exclude_datamodels filter active - will exclude: {exclude_list}")

    # Filter datamodels
    filtered_datamodels = []
    for dm in all_datamodels:
        # Determine if this datamodel should be included
        should_include = False
        if include_all:
            should_include = True
        else:
            should_include = dm['title'] in include_list
        
        # Check exclusion list
        if should_include and dm['title'] not in exclude_list:
            filtered_datamodels.append(dm)
    
    return filtered_datamodels


# Helper function to find matching notebook using prefix matching
# Due to system limitations, we match if BOTH conditions are true:
# 1. notebook_id matches the beginning of the target notebook's id field
# 2. notebook_id matches the prefix of the ipynb file in the codePath field
def update_existing_notebook(api_clients, notebook_id, notebook_payload, target_notebook, logger_prefix=""):
    """
    Update an existing notebook using PATCH, then update ID and codePath if needed.
    
    Args:
        api_clients: API client instance
        notebook_id: Source notebook ID (desired ID)
        notebook_payload: Notebook payload from source to update with
        target_notebook: Existing notebook from target server
        logger_prefix: Prefix for log messages (default: "")
    
    Returns:
        tuple: (success: bool, updated_notebook_id: str or None, updated_notebook_uuid: str or None)
    """
    if not target_notebook:
        return False, None, None
    
    target_notebook_id = target_notebook.get('id')
    target_notebook_uuid = target_notebook.get('uuid') or target_notebook.get('oid')
    update_identifier = target_notebook_uuid if target_notebook_uuid else target_notebook_id
    original_target_code_path = ''  # Will be set when we fetch the notebook
    
    logger_setup.logger.info(f"{logger_prefix}Overwrite mode: Updating existing notebook '{target_notebook.get('displayName', target_notebook_id)}' using PATCH...")
    
    # Fetch the full notebook from target server to get all server-generated fields
    # Always use get_notebooks() because export_notebook() may return incomplete data (missing shares, permissions, notebookType)
    target_notebook_full = None
    try:
        target_notebooks_response = api_clients.target_api.get_notebooks()
        target_notebooks = target_notebooks_response if isinstance(target_notebooks_response, list) else target_notebooks_response.get('data', []) if isinstance(target_notebooks_response, dict) else []
        # Find the notebook by UUID
        for nb in target_notebooks:
            nb_uuid = nb.get('uuid') or nb.get('oid')
            if nb_uuid == update_identifier:
                target_notebook_full = nb
                break
        if target_notebook_full:
            # Use the UUID from the fetched notebook for the update (must match URL path)
            if isinstance(target_notebook_full, dict) and target_notebook_full.get('uuid'):
                update_identifier = target_notebook_full['uuid']
            # Start with the full notebook from target, then update with source changes
            update_payload = target_notebook_full.copy() if isinstance(target_notebook_full, dict) else {}
            # Save the original target codePath BEFORE merging (server may ignore codePath in PATCH)
            original_target_code_path = target_notebook_full.get('codePath', '') if isinstance(target_notebook_full, dict) else ''
        else:
            raise Exception(f"Could not find notebook with UUID {update_identifier} in get_notebooks() response")
    except Exception as get_error:
        # If get_notebooks fails, fall back to using source payload
        logger_setup.logger.debug(f"{logger_prefix}Could not fetch full notebook from target, using source payload: {str(get_error)}")
        update_payload = notebook_payload.copy() if isinstance(notebook_payload, dict) else {}
        original_target_code_path = ''  # Unknown, will need to fetch after PATCH
    
    # Update the payload with source notebook data (preserving server-generated fields)
    # Copy over fields from source notebook_payload, but keep server-generated fields from target
    for key, value in notebook_payload.items():
        # Skip fields that shouldn't be updated or are server-generated
        if key not in ['uuid', 'oid', '_id', '_rev', 'createdAt', 'updatedAt', 'createdBy', 'updatedBy', 'shares', 'permissions']:
            update_payload[key] = value
    
    # Ensure UUID in payload matches the UUID in the URL path
    if update_identifier and not update_identifier.startswith('/'):
        update_payload['uuid'] = update_identifier
    elif isinstance(target_notebook_full, dict) and target_notebook_full.get('uuid'):
        update_payload['uuid'] = target_notebook_full['uuid']
    
    # Remove notebookCode from PATCH payload - it causes 401 errors when updating existing notebooks
    if 'notebookCode' in update_payload:
        del update_payload['notebookCode']
    
    # Update the notebook using PATCH
    try:
        update_response = api_clients.target_api.update_notebook(update_identifier, update_payload)
        logger_setup.logger.info(f"{logger_prefix}✓ Successfully updated notebook using PATCH - Response: {update_response}")
        
        # Extract the updated notebook ID and UUID from response
        updated_notebook_id = target_notebook_id  # Default to existing ID
        updated_notebook_uuid = target_notebook_uuid
        
        if isinstance(update_response, dict):
            updated_data = update_response.get('data', update_response)
            updated_notebook_id = updated_data.get('id', target_notebook_id)
            updated_notebook_uuid = updated_data.get('uuid') or updated_data.get('oid') or target_notebook_uuid
        elif isinstance(update_response, list) and len(update_response) > 0:
            updated_data = update_response[0] if isinstance(update_response[0], dict) else None
            if updated_data:
                updated_notebook_id = updated_data.get('id', target_notebook_id)
                updated_notebook_uuid = updated_data.get('uuid') or updated_data.get('oid') or target_notebook_uuid
        
        # Now update ID and codePath if needed
        if updated_notebook_id and updated_notebook_id != notebook_id:
            update_notebook_id_and_path(
                api_clients, notebook_id, updated_notebook_id,
                updated_notebook_uuid, notebook_payload, logger_prefix=logger_prefix
            )
            return True, notebook_id, updated_notebook_uuid
        else:
            # ID already matches, but we need to restore the target's original codePath
            # The PATCH may have changed the codePath, so we restore it to what it was on the target
            if original_target_code_path:
                # Always restore the target's original codePath after PATCH update
                # (PATCH may have changed it, so we restore it to preserve target structure)
                # Fetch the full notebook again to get the latest state after PATCH
                try:
                    target_notebooks_response = api_clients.target_api.get_notebooks()
                    target_notebooks = target_notebooks_response if isinstance(target_notebooks_response, list) else target_notebooks_response.get('data', []) if isinstance(target_notebooks_response, dict) else []
                    latest_notebook = None
                    for nb in target_notebooks:
                        nb_uuid = nb.get('uuid') or nb.get('oid')
                        if nb_uuid == update_identifier:
                            latest_notebook = nb
                            break
                    
                    if latest_notebook:
                        update_code_path_payload = latest_notebook.copy()
                        update_code_path_payload['codePath'] = original_target_code_path
                        # Ensure UUID is in the payload
                        if update_identifier and not update_identifier.startswith('/'):
                            update_code_path_payload['uuid'] = update_identifier
                        # Remove notebookCode from codePath update - it causes 500 errors
                        if 'notebookCode' in update_code_path_payload:
                            del update_code_path_payload['notebookCode']
                        api_clients.target_api.update_notebook(update_identifier, update_code_path_payload)
                        logger_setup.logger.info(f"{logger_prefix}✓ Restored notebook 'codePath' to '{original_target_code_path}'")
                    else:
                        logger_setup.logger.warning(f"{logger_prefix}Could not find notebook after PATCH to restore codePath")
                except Exception as code_path_error:
                    logger_setup.logger.warning(f"{logger_prefix}Failed to restore notebook codePath: {str(code_path_error)}")
            
            return True, updated_notebook_id, updated_notebook_uuid
    except SisenseRestAPIError as update_error:
        status_code = update_error.result.status_code if hasattr(update_error, 'result') and hasattr(update_error.result, 'status_code') else None
        if status_code == 401:
            logger_setup.logger.warning(f"{logger_prefix}Got 401 error updating notebook (notebook may have been updated)")
        else:
            logger_setup.logger.warning(f"{logger_prefix}Failed to update notebook: {str(update_error)}")
        return False, target_notebook_id, target_notebook_uuid
    except Exception as update_error:
        logger_setup.logger.warning(f"{logger_prefix}Failed to update notebook: {str(update_error)}")
        return False, target_notebook_id, target_notebook_uuid


def update_notebook_id_and_path(api_clients, notebook_id, created_notebook_id, created_notebook_uuid, notebook_payload, logger_prefix="\t\t"):
    """
    Update a notebook's ID and codePath after creation when the created ID differs from the source ID.
    
    Args:
        api_clients: API client instance
        notebook_id: Source notebook ID (desired ID)
        created_notebook_id: ID that was actually created on target
        created_notebook_uuid: UUID of the created notebook
        notebook_payload: Original notebook payload from source
        logger_prefix: Prefix for log messages (default: "\t\t")
    
    Returns:
        tuple: (success: bool, update_identifier: str or None)
    """
    if not created_notebook_id or created_notebook_id == notebook_id:
        return True, None
    
    update_identifier = created_notebook_uuid if created_notebook_uuid else created_notebook_id
    logger_setup.logger.info(f"{logger_prefix}Created notebook ID ({created_notebook_id}) differs from source ID ({notebook_id}). Updating target notebook ID...")
    
    # Fetch the full notebook from target server to get all server-generated fields (shares, permissions, etc.)
    target_notebook_full = None
    try:
        target_notebook_full = api_clients.target_api.export_notebook(update_identifier)
        if isinstance(target_notebook_full, dict) and 'data' in target_notebook_full:
            target_notebook_full = target_notebook_full['data']
        # Use the UUID from the fetched notebook for the update (must match URL path)
        if isinstance(target_notebook_full, dict) and target_notebook_full.get('uuid'):
            update_identifier = target_notebook_full['uuid']
        # Start with the full notebook from target, then update with our changes
        update_payload = target_notebook_full.copy() if isinstance(target_notebook_full, dict) else {}
    except Exception as export_error:
        # If export fails, try get_notebooks() to find the notebook by UUID
        try:
            target_notebooks_response = api_clients.target_api.get_notebooks()
            target_notebooks = target_notebooks_response if isinstance(target_notebooks_response, list) else target_notebooks_response.get('data', []) if isinstance(target_notebooks_response, dict) else []
            # Find the notebook by UUID
            for nb in target_notebooks:
                nb_uuid = nb.get('uuid') or nb.get('oid')
                if nb_uuid == update_identifier:
                    target_notebook_full = nb
                    break
            if target_notebook_full:
                # Use the UUID from the fetched notebook for the update (must match URL path)
                if isinstance(target_notebook_full, dict) and target_notebook_full.get('uuid'):
                    update_identifier = target_notebook_full['uuid']
                # Start with the full notebook from target, then update with our changes
                update_payload = target_notebook_full.copy() if isinstance(target_notebook_full, dict) else {}
            else:
                raise export_error  # Re-raise if we couldn't find it
        except Exception as get_error:
            # If both methods fail, fall back to using source payload
            logger_setup.logger.debug(f"{logger_prefix}Could not fetch full notebook from target, using source payload: {str(export_error)}")
            update_payload = notebook_payload.copy() if isinstance(notebook_payload, dict) else {}
    
    # Update the ID to match source notebook ID
    update_payload['id'] = notebook_id
    # Ensure UUID in payload matches the UUID in the URL path (required per working curl)
    if update_identifier and not update_identifier.startswith('/'):
        # If update_identifier is a UUID (not a path), use it
        update_payload['uuid'] = update_identifier
    elif isinstance(target_notebook_full, dict) and target_notebook_full.get('uuid'):
        update_payload['uuid'] = target_notebook_full['uuid']
    elif created_notebook_uuid:
        update_payload['uuid'] = created_notebook_uuid
    # Also update codePath if it contains the old ID
    if 'codePath' in update_payload and created_notebook_id in str(update_payload.get('codePath', '')):
        update_payload['codePath'] = update_payload['codePath'].replace(created_notebook_id, notebook_id)
    
    # Update the notebook ID
    try:
        api_clients.target_api.update_notebook(update_identifier, update_payload)
        logger_setup.logger.info(f"{logger_prefix}Updated notebook ID from '{created_notebook_id}' to '{notebook_id}'")
    except SisenseRestAPIError as update_id_error:
        # Handle 401 errors during ID update - notebook was created, update may have failed
        status_code = update_id_error.result.status_code if hasattr(update_id_error, 'result') and hasattr(update_id_error.result, 'status_code') else None
        if status_code == 401:
            logger_setup.logger.warning(f"{logger_prefix}Got 401 error updating notebook ID (notebook was created with ID '{created_notebook_id}')")
        else:
            logger_setup.logger.warning(f"{logger_prefix}Failed to update notebook ID: {str(update_id_error)}")
            return False, update_identifier
    except Exception as update_id_error:
        logger_setup.logger.warning(f"{logger_prefix}Failed to update notebook ID: {str(update_id_error)}")
        return False, update_identifier
    
    # After updating the notebook ID, also update the codePath in the new notebook
    # Use the full payload (same as ID update) to avoid 401 errors
    try:
        # Only proceed if codePath is present in notebook_payload
        original_code_path = notebook_payload.get('codePath')
        if original_code_path and created_notebook_id:
            # Replace all instances of previous notebook_id with the created_notebook_id
            # For example: "/notebooks/custom_code_notebooks/notebooks/{notebook_id}/{notebook_id}.ipynb"
            new_code_path = original_code_path.replace(str(notebook_id), str(created_notebook_id))
            if new_code_path != original_code_path:
                # Use the full payload from the ID update, just update the codePath
                update_code_path_payload = update_payload.copy()
                update_code_path_payload['codePath'] = new_code_path
                # Ensure UUID is still in the payload
                if update_identifier and not update_identifier.startswith('/'):
                    update_code_path_payload['uuid'] = update_identifier
                # Remove notebookCode from codePath update - it causes 500 errors when updating just the path
                if 'notebookCode' in update_code_path_payload:
                    del update_code_path_payload['notebookCode']
                try:
                    api_clients.target_api.update_notebook(update_identifier, update_code_path_payload)
                    logger_setup.logger.info(f"{logger_prefix}Updated notebook 'codePath' from '{original_code_path}' to '{new_code_path}'")
                except SisenseRestAPIError as update_code_path_error:
                    # Handle 401 errors during codePath update
                    status_code = update_code_path_error.result.status_code if hasattr(update_code_path_error, 'result') and hasattr(update_code_path_error.result, 'status_code') else None
                    if status_code == 401:
                        logger_setup.logger.warning(f"{logger_prefix}Got 401 error updating notebook codePath (notebook was created)")
                    else:
                        logger_setup.logger.warning(f"{logger_prefix}Failed to update notebook codePath: {str(update_code_path_error)}")
                except Exception as update_code_path_error:
                    logger_setup.logger.warning(f"{logger_prefix}Failed to update notebook codePath: {str(update_code_path_error)}")
    except Exception as update_code_path_error:
        logger_setup.logger.warning(f"{logger_prefix}Failed to update notebook codePath: {str(update_code_path_error)}")
    
    return True, update_identifier


def find_matching_notebook(notebook_id, target_notebooks):
    if not notebook_id or not target_notebooks:
        return None
    
    for target_nb in target_notebooks:
        target_id = target_nb.get('id', '')
        target_code_path = target_nb.get('codePath', '')
        
        # Check 1: notebook_id matches the beginning of target notebook's id
        id_matches = target_id and target_id.startswith(notebook_id)
        
        # Check 2: notebook_id matches the prefix of the ipynb file in codePath
        code_path_matches = False
        if target_code_path:
            # Check if notebook_id appears as a directory in the path (e.g., "/work/.../newNotebook49/newNotebook49.ipynb")
            if f'/{notebook_id}/' in target_code_path:
                code_path_matches = True
            # Check if the path ends with notebook_id.ipynb
            elif target_code_path.endswith(f'/{notebook_id}.ipynb'):
                code_path_matches = True
        
        # Return match if:
        # 1. Exact ID match (always match, even without codePath)
        # 2. OR both ID prefix match AND codePath match (for prefix matches like newNotebook40 -> newNotebook401)
        if id_matches:
            if target_id == notebook_id:
                # Exact ID match - always return, even if codePath doesn't match
                return target_nb
            elif code_path_matches:
                # Prefix match with codePath confirmation
                return target_nb
    
    return None


def migrate_datamodels(logger=None):
    if logger is None:
        logger = logger_setup.get_logger_for_migration('datamodels')
    error_list = []

    logger.info(f"Collecting datamodels to migrate.")
    res = api_clients.src_api.get_datamodels_metadata()

    total_datamodels = len(res['data']['elasticubesMetadata'])
    logger.info(f"Found {total_datamodels} total datamodels available.")

    # Filter datamodels based on include/exclude settings
    filtered_datamodels = get_filtered_datamodels(res["data"]["elasticubesMetadata"])
    
    logger.info(f"After filtering, {len(filtered_datamodels)} datamodel(s) will be migrated.")
    
    # Now process the filtered datamodels
    for dm in filtered_datamodels:
        # If the data model has never been opened in the source server it will not
            # have an oid and a data model without an oid can't be imported.
            # To make sure all data models are imported you have to open them at least once in the web application's "Data" tab.
            if dm["oid"] is None:

                try:
                    logger.info(f"{dm['title']} has no oid. Loading datamodel to get oid")

                    res = api_clients.src_api.load_datamodel(dm['title'])
                    logger.debug(f"\t{res}")
                    if res.get("errors", None) is not None:
                        error_list.append(
                            f"Error loading data model {dm['title']}. Reason: {res['errors'][0]['error']['message']}")
                        continue
                    else:
                        dm["oid"] = res["data"]['elasticubeByTitle']['oid']



                except SisenseRestAPIError as e:

                    error_list.append(f"Error importing data model {dm['title']}. Reason: {e.result.reason}")
                    continue

            smodel = api_clients.src_api.export_datamodel_by_id(dm["oid"])

            if "oid" in smodel:
                smodel.pop("oid")

            logger.debug(f"smodel={smodel}")

            # --- Datamodel Schema, Connections, and Shares Migration ---
            if config_loader.settings.get("migrate_datamodels", False):
                logger.info(f"Migrating datamodel schema for {dm['title']}")
                if config_loader.settings.get("enable_update_connections", False):
                    logger.info(f"\tUpdate connections is enabled. Checking for connection parameters to update...")
                    if "datasets" in smodel and smodel["datasets"] is not None:
                        for ds in smodel["datasets"]:
                            if "connection" not in ds or ds["connection"] is None:
                                continue
                            connection_provider = ds["connection"].get("provider")
                            if not connection_provider:
                                continue
                            update_rule = next((rule for rule in config_loader.settings.get("update_connections", []) if connection_provider in rule.get("provider", "")), None)
                            if not update_rule:
                                continue
                            logger.info(f"\tFound rule for provider '{connection_provider}'. Updating connection {ds['connection'].get('id', 'N/A')}")
                            if "function" in update_rule and update_rule.get("function"):
                                module_name = "connection_update_functions"
                                try:
                                    if config_loader.application_path not in sys.path:
                                        sys.path.insert(0, config_loader.application_path)
                                    external_module = importlib.import_module(module_name)
                                    importlib.reload(external_module)
                                    function_name = update_rule["function"]
                                    function_to_call = getattr(external_module, function_name, None)
                                    if callable(function_to_call):
                                        ds["connection"] = function_to_call(ds["connection"], api_clients.target_api, smodel, logger)
                                    else:
                                        logger.error(f"Function '{function_name}' not found or not callable in module '{module_name}'")
                                except Exception as e:
                                    logger.error(f"An error occurred while calling function '{update_rule['function']}': {e}")
                                finally:
                                    if config_loader.application_path in sys.path:
                                        sys.path.remove(config_loader.application_path)
                try:

                    if config_loader.settings.get("migrate_remote_datamodel", False) and dm.get("server") != "LocalHost":
                        logger.info(f"\tmigrate_remote_datamodel is enabled and server is not already 'LocalHost'. Setting smodel['server'] to 'LocalHost'.")
                        dm["server"] = "LocalHost"

                    is_exists = api_clients.target_api.check_elasticube_exists(smodel["title"])
                    if is_exists["data"]["elasticubeExists"] and config_loader.settings["datamodel_overwrite"]:
                        logger.info(f"\tDatamodel exists on target. Overwrite is enabled. Deleting existing datamodel...")
                        del_res = api_clients.target_api.delete_datamodel(dm["title"], dm["server"])
                        if del_res["data"]["removeElasticube"]:
                            logger.info(f"\tDatamodel deleted successfully.")
                        is_exists = api_clients.target_api.check_elasticube_exists(smodel["title"])

                    # Scan smodel for custom_code tables before migration
                    logger.info(f"\tScanning datamodel for custom_code tables...")
                    custom_code_info = []  # List of dicts with dataset_idx, table_idx, table_id, notebook_id
                    skip_migration = False  # Flag to skip migration if required notebooks are missing
                    if "datasets" in smodel and smodel["datasets"]:
                        for dataset_idx, dataset in enumerate(smodel["datasets"]):
                            if "schema" in dataset and dataset["schema"]:
                                schema = dataset["schema"]
                                if "tables" in schema and schema["tables"]:
                                    for table_idx, table in enumerate(schema["tables"]):
                                        if table.get("type") == "custom_code":
                                            table_id = table.get("id")
                                            custom_code = table.get("customCode", {})
                                            notebook_id = custom_code.get("noteBookId") if custom_code else None
                                            if notebook_id:
                                                custom_code_info.append({
                                                    "dataset_idx": dataset_idx,
                                                    "table_idx": table_idx,
                                                    "table_id": table_id,
                                                    "notebook_id": notebook_id
                                                })
                                                logger.info(f"\t\tFound custom_code table with id: {table_id}, noteBookId: {notebook_id}")
                                            else:
                                                logger.warning(f"\t\tFound custom_code table with id: {table_id}, but no noteBookId in customCode field")
                    
                    if custom_code_info:
                        logger.info(f"\tFound {len(custom_code_info)} custom_code table(s) with noteBookId(s)")
                        
                        # Check if notebooks with these IDs exist on target server
                        logger.info(f"\tChecking if notebooks exist on target server for custom_code noteBookIds...")
                        missing_notebooks = []
                        skip_migration = False
                        target_notebooks = []
                        target_notebook_ids = set()
                        custom_code_enabled = None  # Will be set in try block
                        
                        # First, check if custom code is enabled on the target server
                        try:
                            custom_code_enabled = api_clients.target_api.is_custom_code_enabled()
                            
                            if not custom_code_enabled:
                                logger.warning(f"\tCustom code feature is not enabled on the target server.")
                                logger.warning(f"\tSkipping notebook existence check and proceeding with datamodel migration.")
                                logger.warning(f"\tIf required notebooks are missing, the migration will fail when attempting to use them.")
                                # Set empty lists to skip the notebook check logic
                                target_notebooks = []
                                target_notebook_ids = set()
                                # Skip the notebook processing loop since we can't check notebooks
                            else:
                                # Custom code is enabled, proceed with notebook check
                                target_notebooks_response = api_clients.target_api.get_notebooks()
                                target_notebooks = target_notebooks_response if isinstance(target_notebooks_response, list) else target_notebooks_response.get('data', []) if isinstance(target_notebooks_response, dict) else []
                                target_notebook_ids = {nb.get('id') for nb in target_notebooks if nb.get('id')}
                                
                                # Log found notebooks
                                if target_notebooks:
                                    found_notebooks = [(nb.get('id'), nb.get('displayName', nb.get('name', 'N/A'))) for nb in target_notebooks if nb.get('id')]
                                    logger.info(f"\tFound {len(found_notebooks)} notebook(s) on target server:")
                                    for nb_id, nb_name in sorted(found_notebooks, key=lambda x: x[0] or ''):
                                        logger.info(f"\t\t- {nb_id} ({nb_name})")
                                else:
                                    logger.info(f"\tNo notebooks found on target server")
                        except Exception as e:
                            # If we can't check custom code status, log and continue
                            logger.warning(f"\tCould not check if custom code is enabled on target server: {e}")
                            logger.warning(f"\tAttempting to check notebooks anyway...")
                            try:
                                target_notebooks_response = api_clients.target_api.get_notebooks()
                                target_notebooks = target_notebooks_response if isinstance(target_notebooks_response, list) else target_notebooks_response.get('data', []) if isinstance(target_notebooks_response, dict) else []
                                target_notebook_ids = {nb.get('id') for nb in target_notebooks if nb.get('id')}
                                
                                # Log found notebooks
                                if target_notebooks:
                                    found_notebooks = [(nb.get('id'), nb.get('displayName', nb.get('name', 'N/A'))) for nb in target_notebooks if nb.get('id')]
                                    logger.info(f"\tFound {len(found_notebooks)} notebook(s) on target server:")
                                    for nb_id, nb_name in sorted(found_notebooks, key=lambda x: x[0] or ''):
                                        logger.info(f"\t\t- {nb_id} ({nb_name})")
                                else:
                                    logger.info(f"\tNo notebooks found on target server")
                            except SisenseRestAPIError as api_error:
                                # Check if this is the specific 403 error about CUSTOM_CODE_TABLE being forbidden
                                if hasattr(api_error, 'result') and api_error.result.status_code == 403:
                                    try:
                                        error_text = api_error.result.text if hasattr(api_error.result, 'text') else str(api_error)
                                        if 'CUSTOM_CODE_TABLE' in error_text or 'Custom code feature is forbidden' in error_text:
                                            logger.warning(f"\tTarget server does not allow querying CUSTOM_CODE_TABLE notebooks via /v1/notebooks endpoint (403 Forbidden).")
                                            logger.warning(f"\tSkipping notebook existence check and proceeding with datamodel migration.")
                                            logger.warning(f"\tIf required notebooks are missing, the migration will fail when attempting to use them.")
                                            # Set empty lists to skip the notebook check logic
                                            target_notebooks = []
                                            target_notebook_ids = set()
                                            # Continue with migration - don't set skip_migration = True
                                            # Skip the notebook processing loop since we can't check notebooks
                                        else:
                                            # Different 403 error - re-raise it
                                            logger.error(f"\tError checking notebooks on target server: {api_error}")
                                            logger.debug(f"Exception encountered while checking notebooks on target server: {api_error}", exc_info=True)
                                            skip_migration = True
                                    except Exception as parse_error:
                                        # If we can't parse the error, treat it as a regular error
                                        logger.error(f"\tError checking notebooks on target server: {api_error}")
                                        logger.debug(f"Exception encountered while checking notebooks on target server: {api_error}", exc_info=True)
                                        skip_migration = True
                                else:
                                    # Not a 403, or no result attribute - re-raise as regular error
                                    logger.error(f"\tError checking notebooks on target server: {api_error}")
                                    logger.debug(f"Exception encountered while checking notebooks on target server: {api_error}", exc_info=True)
                                    skip_migration = True
                            except Exception as e:
                                logger.error(f"\tError checking notebooks on target server: {e}")
                                logger.debug(f"Exception encountered while checking notebooks on target server: {e}", exc_info=True)
                                skip_migration = True
                        
                        # Process custom_code_info to check notebooks and update codePath
                        # Only run if:
                        # 1. Migration is not skipped
                        # 2. Custom code is enabled (not False) - meaning we can check notebooks
                        # 3. We have notebooks to check (target_notebook_ids is not empty)
                        if not skip_migration and custom_code_enabled is not False and target_notebook_ids:
                            for info in custom_code_info:
                                notebook_id = info["notebook_id"]
                                dataset_idx = info["dataset_idx"]
                                table_idx = info["table_idx"]
                                table_id = info["table_id"]
                                
                                # Use prefix matching instead of exact matching
                                notebook = find_matching_notebook(notebook_id, target_notebooks)
                                if notebook:
                                    notebook_name = notebook.get('displayName', notebook.get('name', 'N/A')) if notebook else 'N/A'
                                    target_notebook_id = notebook.get('id')  # Get the actual target notebook ID
                                    notebook_code_path = notebook.get('codePath') if notebook else None
                                    
                                    logger.info(f"\t\t✓ Notebook with id '{notebook_id}' exists on target server (name: '{notebook_name}', target ID: '{target_notebook_id}')")
                                    
                                    # Update codePath in smodel if notebook has codePath
                                    if notebook_code_path:
                                        try:
                                            # Ensure customCode exists
                                            if "customCode" not in smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]:
                                                smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"] = {}
                                            
                                            old_code_path = smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"].get("codePath")
                                            
                                            # Update codePath to use the actual target notebook ID instead of the datamodel notebook ID
                                            # Replace the datamodel notebook_id with the target notebook_id in the codePath
                                            if target_notebook_id and target_notebook_id != notebook_id:
                                                # Replace all occurrences of the datamodel notebook_id with the target notebook_id
                                                updated_code_path = notebook_code_path.replace(notebook_id, target_notebook_id)
                                            else:
                                                updated_code_path = notebook_code_path
                                            
                                            smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"]["codePath"] = updated_code_path
                                            logger.info(f"\t\t  Updated codePath for table '{table_id}': '{old_code_path}' -> '{updated_code_path}'")
                                        except (IndexError, KeyError) as e:
                                            logger.warning(f"\t\t  Failed to update codePath for table '{table_id}': {e}")
                                    else:
                                        logger.warning(f"\t\t  Notebook '{notebook_id}' found but has no codePath to update")
                                else:
                                    missing_notebooks.append(notebook_id)
                                    logger.info(f"\t\t✗ Notebook with id '{notebook_id}' NOT found on target server")
                            
                            # If any notebooks are missing, check if auto-migration is enabled
                            if not skip_migration and missing_notebooks:
                                auto_migrate = config_loader.settings.get("auto_migrate_missing_custom_code_notebooks", False)
                                if auto_migrate:
                                    logger.info(f"\tAuto-migrate missing notebooks is enabled. Attempting to migrate {len(missing_notebooks)} missing notebook(s): {missing_notebooks}")
                                    
                                    # Get source notebooks for the missing IDs
                                    try:
                                        src_notebooks_response = api_clients.src_api.get_notebooks()
                                        src_notebooks = src_notebooks_response if isinstance(src_notebooks_response, list) else src_notebooks_response.get('data', []) if isinstance(src_notebooks_response, dict) else []
                                        
                                        # Filter to only the missing notebooks
                                        notebooks_to_migrate = [nb for nb in src_notebooks if nb.get('id') in missing_notebooks]
                                        
                                        if notebooks_to_migrate:
                                            logger.info(f"\tFound {len(notebooks_to_migrate)} notebook(s) on source server to migrate")
                                            
                                            # Track notebooks that we know exist (either in target_notebook_ids or from "already exists" errors)
                                            known_existing_notebooks = set(target_notebook_ids)
                                            
                                            # Migrate each notebook
                                            import_mode = config_loader.settings.get("notebook_import_mode", "skip")
                                            for notebook in notebooks_to_migrate:
                                                notebook_id = notebook.get('id')
                                                notebook_uuid = notebook.get('uuid') or notebook.get('oid')
                                                notebook_name = notebook.get('displayName', notebook_id or 'Unknown')
                                                
                                                try:
                                                    logger.info(f"\t\tMigrating notebook: '{notebook_name}' (ID: {notebook_id})")
                                                    
                                                    # Export notebook from source
                                                    export_id = notebook_uuid if notebook_uuid else notebook_id
                                                    exported_notebook = api_clients.src_api.export_notebook(export_id)
                                                    
                                                    if isinstance(exported_notebook, dict) and 'data' in exported_notebook:
                                                        notebook_payload = exported_notebook['data']
                                                    else:
                                                        notebook_payload = exported_notebook
                                                    
                                                    if not notebook_payload:
                                                        logger.error(f"\t\tFailed to export notebook '{notebook_name}' - empty response")
                                                        continue
                                                    
                                                    # Prepare payload
                                                    if 'oid' in notebook_payload:
                                                        del notebook_payload['oid']
                                                    fields_to_remove = ['_id', '_rev', 'createdAt', 'updatedAt', 'createdBy', 'updatedBy']
                                                    for field in fields_to_remove:
                                                        if field in notebook_payload:
                                                            del notebook_payload[field]
                                                    
                                                    if 'id' not in notebook_payload:
                                                        notebook_payload['id'] = notebook_id
                                                    if 'displayName' not in notebook_payload and notebook_name:
                                                        notebook_payload['displayName'] = notebook_name
                                                    
                                                    # Check if notebook already exists on target (using prefix matching)
                                                    target_notebook = find_matching_notebook(notebook_id, target_notebooks)
                                                    if target_notebook:
                                                        if import_mode == "overwrite":
                                                            # Update existing notebook using PATCH
                                                            success, updated_id, updated_uuid = update_existing_notebook(
                                                                api_clients, notebook_id, notebook_payload, target_notebook, logger_prefix="\t\t"
                                                            )
                                                            if success:
                                                                logger.info(f"\t\t✓ Successfully updated notebook '{notebook_name}' using PATCH")
                                                                continue  # Skip creation, already updated
                                                            else:
                                                                logger.warning(f"\t\tFailed to update notebook '{notebook_name}' using PATCH, will attempt to create")
                                                        else:
                                                            # Default to skip if notebook exists and mode is not overwrite
                                                            logger.info(f"\t\tNotebook '{notebook_name}' already exists, skipping (import_mode: {import_mode})")
                                                            continue
                                                    
                                                    # Re-check if notebook exists right before creating (in case it was created between checks)
                                                    notebook_exists_now = False
                                                    quick_check_notebooks = []
                                                    try:
                                                        # Quick check right before create (using prefix matching)
                                                        quick_check_response = api_clients.target_api.get_notebooks()
                                                        quick_check_notebooks = quick_check_response if isinstance(quick_check_response, list) else quick_check_response.get('data', []) if isinstance(quick_check_response, dict) else []
                                                        quick_check_notebook = find_matching_notebook(notebook_id, quick_check_notebooks)
                                                        notebook_exists_now = quick_check_notebook is not None
                                                    except: pass
                                                    
                                                    # If notebook exists now, handle according to import_mode
                                                    if notebook_exists_now:
                                                        if import_mode == "overwrite":
                                                            # Update existing notebook using PATCH (using prefix matching)
                                                            target_notebook = find_matching_notebook(notebook_id, quick_check_notebooks)
                                                            if target_notebook:
                                                                success, updated_id, updated_uuid = update_existing_notebook(
                                                                    api_clients, notebook_id, notebook_payload, target_notebook, logger_prefix="\t\t"
                                                                )
                                                                if success:
                                                                    logger.info(f"\t\t✓ Successfully updated notebook '{notebook_name}' using PATCH")
                                                                    continue  # Skip creation, already updated
                                                                else:
                                                                    logger.warning(f"\t\tFailed to update notebook '{notebook_name}' using PATCH, will attempt to create")
                                                        else:
                                                            # Default to skip if notebook exists and mode is not overwrite
                                                            logger.info(f"\t\tNotebook '{notebook_name}' already exists, skipping (import_mode: {import_mode})")
                                                            continue
                                                    
                                                    # Create notebook on target
                                                    try:
                                                        create_response = api_clients.target_api.create_notebook(notebook_payload)
                                                    except SisenseRestAPIError as create_error:
                                                        # Handle "Notebook ID already exists" error
                                                        if (hasattr(create_error, 'result') and 
                                                            create_error.result.status_code == 400 and
                                                            hasattr(create_error.result, 'text') and
                                                            'Notebook ID already exists' in create_error.result.text):
                                                            
                                                            logger.info(f"\t\tNotebook '{notebook_name}' (ID: {notebook_id}) already exists on target server")
                                                            
                                                            if import_mode == "overwrite":
                                                                # Update existing notebook using PATCH
                                                                logger.info(f"\t\tOverwrite mode: updating existing notebook using PATCH...")
                                                                try:
                                                                    # Get the target notebook using prefix matching
                                                                    target_notebooks_response = api_clients.target_api.get_notebooks()
                                                                    target_notebooks = target_notebooks_response if isinstance(target_notebooks_response, list) else target_notebooks_response.get('data', []) if isinstance(target_notebooks_response, dict) else []
                                                                    target_notebook = find_matching_notebook(notebook_id, target_notebooks)
                                                                    if target_notebook:
                                                                        success, updated_id, updated_uuid = update_existing_notebook(
                                                                            api_clients, notebook_id, notebook_payload, target_notebook, logger_prefix="\t\t"
                                                                        )
                                                                        if success:
                                                                            logger.info(f"\t\t✓ Successfully updated notebook '{notebook_name}' using PATCH")
                                                                            # Set create_response to indicate success
                                                                            create_response = {"data": {"id": updated_id or notebook_id, "uuid": updated_uuid}}
                                                                            # Mark as existing
                                                                            known_existing_notebooks.add(notebook_id)
                                                                        else:
                                                                            raise Exception("Failed to update notebook using PATCH")
                                                                    else:
                                                                        # Notebook exists but not found in list - try update with ID directly
                                                                        try:
                                                                            api_clients.target_api.update_notebook(notebook_id, notebook_payload)
                                                                            logger.info(f"\t\tUpdated existing notebook '{notebook_name}' using ID")
                                                                            create_response = {"data": {"id": notebook_id}}
                                                                            known_existing_notebooks.add(notebook_id)
                                                                        except Exception as update_error:
                                                                            raise Exception(f"Failed to update notebook: {str(update_error)}")
                                                                except Exception as update_error:
                                                                    logger.warning(f"\t\tFailed to update existing notebook '{notebook_name}': {update_error}")
                                                                    # Re-raise the original create_error to continue with error handling
                                                                    raise create_error
                                                            else:
                                                                # Default to skip if mode is not overwrite
                                                                logger.info(f"\t\tSkip mode: skipping notebook '{notebook_name}' (already exists, import_mode: {import_mode})")
                                                                # Mark as existing even though it's not in target_notebook_ids
                                                                known_existing_notebooks.add(notebook_id)
                                                                
                                                                # Still need to update codePath in smodel even when skipping
                                                                try:
                                                                    # Try to get the notebook from target to get its codePath
                                                                    # First try using get_notebooks, but if it's not there, try direct fetch
                                                                    target_notebook = None
                                                                    try:
                                                                        # Refresh target notebooks list
                                                                        target_notebooks_response = api_clients.target_api.get_notebooks()
                                                                        target_notebooks_list = target_notebooks_response if isinstance(target_notebooks_response, list) else target_notebooks_response.get('data', []) if isinstance(target_notebooks_response, dict) else []
                                                                        target_notebook = find_matching_notebook(notebook_id, target_notebooks_list)
                                                                    except Exception:
                                                                        # If get_notebooks fails or doesn't return it, try to export the notebook directly
                                                                        try:
                                                                            target_notebook = api_clients.target_api.export_notebook(notebook_id)
                                                                            if isinstance(target_notebook, dict) and 'data' in target_notebook:
                                                                                target_notebook = target_notebook['data']
                                                                        except Exception:
                                                                            pass
                                                                    
                                                                    if target_notebook:
                                                                        target_notebook_id = target_notebook.get('id')  # Get the actual target notebook ID
                                                                        notebook_code_path = target_notebook.get('codePath')
                                                                        if notebook_code_path:
                                                                            # Find the corresponding entry in custom_code_info and update codePath
                                                                            for info in custom_code_info:
                                                                                if info.get("notebook_id") == notebook_id:
                                                                                    dataset_idx = info["dataset_idx"]
                                                                                    table_idx = info["table_idx"]
                                                                                    table_id = info["table_id"]
                                                                                    
                                                                                    try:
                                                                                        # Ensure customCode exists
                                                                                        if "customCode" not in smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]:
                                                                                            smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"] = {}
                                                                                        
                                                                                        old_code_path = smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"].get("codePath")
                                                                                        
                                                                                        # Update codePath to use the actual target notebook ID instead of the datamodel notebook ID
                                                                                        if target_notebook_id and target_notebook_id != notebook_id:
                                                                                            # Replace all occurrences of the datamodel notebook_id with the target notebook_id
                                                                                            updated_code_path = notebook_code_path.replace(notebook_id, target_notebook_id)
                                                                                        else:
                                                                                            updated_code_path = notebook_code_path
                                                                                        
                                                                                        smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"]["codePath"] = updated_code_path
                                                                                        logger.info(f"\t\t  Updated codePath for table '{table_id}': '{old_code_path}' -> '{updated_code_path}'")
                                                                                    except (IndexError, KeyError) as e:
                                                                                        logger.warning(f"\t\t  Failed to update codePath for table '{table_id}': {e}")
                                                                                    break
                                                                    else:
                                                                        logger.warning(f"\t\t  Could not fetch notebook '{notebook_id}' to update codePath")
                                                                except Exception as code_path_error:
                                                                    logger.warning(f"\t\t  Error updating codePath for skipped notebook '{notebook_name}': {code_path_error}")
                                                                
                                                                continue
                                                        else:
                                                            # Different error, re-raise it
                                                            raise create_error
                                                    
                                                    # Update ID if needed
                                                    created_notebook_id = None
                                                    created_notebook_uuid = None
                                                    if isinstance(create_response, dict):
                                                        created_notebook_data = create_response.get('data', create_response)
                                                        created_notebook_id = created_notebook_data.get('id')
                                                        created_notebook_uuid = created_notebook_data.get('uuid') or created_notebook_data.get('oid')
                                                    elif isinstance(create_response, list) and len(create_response) > 0:
                                                        created_notebook_data = create_response[0] if isinstance(create_response[0], dict) else None
                                                        if created_notebook_data:
                                                            created_notebook_id = created_notebook_data.get('id')
                                                            created_notebook_uuid = created_notebook_data.get('uuid') or created_notebook_data.get('oid')
                                                    
                                                    if created_notebook_id and created_notebook_id != notebook_id:
                                                        update_notebook_id_and_path(
                                                            api_clients, notebook_id, created_notebook_id, 
                                                            created_notebook_uuid, notebook_payload, logger_prefix="\t\t"
                                                        )
                                                    
                                                    logger.info(f"\t\t✓ Successfully migrated notebook '{notebook_name}' (ID: {notebook_id})")
                                                    
                                                    # Mark as existing
                                                    known_existing_notebooks.add(notebook_id)
                                                    
                                                    # Refresh target notebooks list
                                                    target_notebooks_response = api_clients.target_api.get_notebooks()
                                                    target_notebooks = target_notebooks_response if isinstance(target_notebooks_response, list) else target_notebooks_response.get('data', []) if isinstance(target_notebooks_response, dict) else []
                                                    target_notebook_ids = {nb.get('id') for nb in target_notebooks if nb.get('id')}
                                                    # Update known_existing with any new notebooks found
                                                    known_existing_notebooks.update(target_notebook_ids)
                                                    
                                                except Exception as e:
                                                    # Check if this is a 401 error and if notebook was actually created
                                                    is_401_error = False
                                                    status_code = None
                                                    if isinstance(e, SisenseRestAPIError) and hasattr(e, 'result'):
                                                        status_code = e.result.status_code if hasattr(e.result, 'status_code') else None
                                                        is_401_error = (status_code == 401)
                                                    
                                                    if is_401_error:
                                                        logger.warning(f"\t\tGot 401 error for notebook '{notebook_name}' (ID: {notebook_id}), checking if notebook was created...")
                                                        # Refresh target notebooks list and check if this notebook exists
                                                        try:
                                                            refreshed_target_notebooks_response = api_clients.target_api.get_notebooks()
                                                            refreshed_target_notebooks = refreshed_target_notebooks_response if isinstance(refreshed_target_notebooks_response, list) else refreshed_target_notebooks_response.get('data', []) if isinstance(refreshed_target_notebooks_response, dict) else []
                                                            
                                                            matching_target = find_matching_notebook(notebook_id, refreshed_target_notebooks)
                                                            if matching_target:
                                                                target_notebook_id = matching_target.get('id', 'N/A')
                                                                logger.info(f"\t\t✓ Notebook '{notebook_name}' (ID: {notebook_id}) was successfully created despite 401 error (target ID: {target_notebook_id})")
                                                                # Mark as existing
                                                                known_existing_notebooks.add(notebook_id)
                                                                # Update codePath in smodel if needed
                                                                try:
                                                                    notebook_code_path = matching_target.get('codePath')
                                                                    if notebook_code_path:
                                                                        for info in custom_code_info:
                                                                            if info.get("notebook_id") == notebook_id:
                                                                                dataset_idx = info["dataset_idx"]
                                                                                table_idx = info["table_idx"]
                                                                                table_id = info["table_id"]
                                                                                
                                                                                try:
                                                                                    if "customCode" not in smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]:
                                                                                        smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"] = {}
                                                                                    
                                                                                    old_code_path = smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"].get("codePath")
                                                                                    
                                                                                    if target_notebook_id and target_notebook_id != notebook_id:
                                                                                        updated_code_path = notebook_code_path.replace(notebook_id, target_notebook_id)
                                                                                    else:
                                                                                        updated_code_path = notebook_code_path
                                                                                    
                                                                                    smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"]["codePath"] = updated_code_path
                                                                                    logger.info(f"\t\t  Updated codePath for table '{table_id}': '{old_code_path}' -> '{updated_code_path}'")
                                                                                except (IndexError, KeyError) as code_path_err:
                                                                                    logger.warning(f"\t\t  Failed to update codePath for table '{table_id}': {code_path_err}")
                                                                                break
                                                                except Exception as code_path_error:
                                                                    logger.warning(f"\t\t  Error updating codePath for notebook '{notebook_name}': {code_path_error}")
                                                                continue
                                                            else:
                                                                logger.error(f"\t\tNotebook '{notebook_name}' (ID: {notebook_id}) not found on target after 401 error")
                                                                logger.error(f"\t\tFailed to migrate notebook '{notebook_name}' (ID: {notebook_id}): {e}")
                                                        except Exception as check_error:
                                                            logger.warning(f"\t\tFailed to verify notebook existence after 401 error: {str(check_error)}")
                                                            logger.error(f"\t\tFailed to migrate notebook '{notebook_name}' (ID: {notebook_id}): {e}")
                                                    else:
                                                        logger.error(f"\t\tFailed to migrate notebook '{notebook_name}' (ID: {notebook_id}): {e}")
                                            
                                            # Re-check if all notebooks are now available
                                            # Use known_existing_notebooks instead of just target_notebook_ids
                                            # because get_notebooks() might not return all notebooks
                                            still_missing = [nb_id for nb_id in missing_notebooks if nb_id not in known_existing_notebooks]
                                            if still_missing:
                                                skip_migration = True
                                                logger.error(f"\tAfter auto-migration, {len(still_missing)} notebook(s) still missing on target server: {still_missing}")
                                            else:
                                                logger.info(f"\t✓ All required notebooks have been successfully migrated. Continuing with datamodel migration.")
                                                # Re-fetch target notebooks to get updated codePath values
                                                target_notebooks_response = api_clients.target_api.get_notebooks()
                                                target_notebooks = target_notebooks_response if isinstance(target_notebooks_response, list) else target_notebooks_response.get('data', []) if isinstance(target_notebooks_response, dict) else []
                                                target_notebook_ids = {nb.get('id') for nb in target_notebooks if nb.get('id')}
                                                
                                                # Re-update codePath for all custom_code tables
                                                for info in custom_code_info:
                                                    notebook_id = info["notebook_id"]
                                                    # Use prefix matching instead of exact matching
                                                    notebook = find_matching_notebook(notebook_id, target_notebooks)
                                                    if notebook:
                                                        target_notebook_id = notebook.get('id')  # Get the actual target notebook ID
                                                        notebook_code_path = notebook.get('codePath')
                                                        if notebook_code_path:
                                                            dataset_idx = info["dataset_idx"]
                                                            table_idx = info["table_idx"]
                                                            table_id = info["table_id"]
                                                            try:
                                                                if "customCode" not in smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]:
                                                                    smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"] = {}
                                                                old_code_path = smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"].get("codePath")
                                                                
                                                                # Update codePath to use the actual target notebook ID instead of the datamodel notebook ID
                                                                if target_notebook_id and target_notebook_id != notebook_id:
                                                                    # Replace all occurrences of the datamodel notebook_id with the target notebook_id
                                                                    updated_code_path = notebook_code_path.replace(notebook_id, target_notebook_id)
                                                                else:
                                                                    updated_code_path = notebook_code_path
                                                                
                                                                smodel["datasets"][dataset_idx]["schema"]["tables"][table_idx]["customCode"]["codePath"] = updated_code_path
                                                                logger.info(f"\t\tUpdated codePath for table '{table_id}': '{old_code_path}' -> '{updated_code_path}'")
                                                            except (IndexError, KeyError) as e:
                                                                logger.warning(f"\t\tFailed to update codePath for table '{table_id}': {e}")
                                        else:
                                            skip_migration = True
                                            logger.error(f"\tCould not find {len(missing_notebooks)} required notebook(s) on source server: {missing_notebooks}")
                                    except Exception as e:
                                        skip_migration = True
                                        logger.error(f"\tError during auto-migration of missing notebooks: {e}")
                                else:
                                    skip_migration = True
                                    logger.error(f"\tSkipping datamodel migration: {len(missing_notebooks)} required notebook(s) not found on target server: {missing_notebooks}")
                    else:
                        logger.info(f"\tNo custom_code tables found in datamodel.")

                    if not skip_migration:
                        # Decrypt and re-encrypt connection parameters before import
                        # This is necessary because source and target servers may have different encryption keys
                        logger.info(f"\tRe-encrypting connection parameters for target server...")
                        if "datasets" in smodel and smodel["datasets"] is not None:
                            for ds_idx, ds in enumerate(smodel["datasets"]):
                                if "connection" not in ds or ds["connection"] is None:
                                    continue
                                connection = ds["connection"]
                                if "parameters" in connection and connection["parameters"]:
                                    try:
                                        # Check if parameters are encrypted (string format)
                                        params = connection["parameters"]
                                        if isinstance(params, str):
                                            # Decrypt using source server
                                            logger.debug(f"\t\tDecrypting parameters for dataset {ds_idx} using source server...")
                                            decrypted_params = api_clients.src_api.decrypt(params)
                                            
                                            # Re-encrypt using target server
                                            logger.debug(f"\t\tRe-encrypting parameters for dataset {ds_idx} using target server...")
                                            re_encrypted_params = api_clients.target_api.encrypt(decrypted_params)
                                            
                                            # Update connection parameters
                                            connection["parameters"] = re_encrypted_params
                                            logger.info(f"\t\t✓ Re-encrypted connection parameters for dataset {ds_idx}")
                                        elif isinstance(params, dict):
                                            # Parameters are already decrypted (dict format), just re-encrypt
                                            logger.debug(f"\t\tRe-encrypting parameters for dataset {ds_idx} using target server...")
                                            re_encrypted_params = api_clients.target_api.encrypt(params)
                                            connection["parameters"] = re_encrypted_params
                                            logger.info(f"\t\t✓ Re-encrypted connection parameters for dataset {ds_idx}")
                                    except Exception as e:
                                        logger.warning(f"\t\tFailed to re-encrypt connection parameters for dataset {ds_idx}: {e}")
                                        # Continue with migration - the import might still work or fail with a clearer error
                        
                        # Recheck if datamodel exists before importing (may have been created by another process during custom_code scanning)
                        is_exists = api_clients.target_api.check_elasticube_exists(smodel["title"])
                        if not is_exists["data"]["elasticubeExists"]:
                            logger.info(f"\tImporting datamodel...")
                            new_dm = api_clients.target_api.import_datamodel(json.dumps(smodel))
                            logger.info(f"\tDatamodel imported successfully.")
                        else:
                            logger.info(f"\tDatamodel schema already exists. Overwrite is disabled. Skipping schema migration.")

                        # Share logic
                        updated_shares = []
                        for s in dm["shares"]:
                            party_id = s.get("partyId")
                            if s["type"] == "user":
                                u = next((user for user in users_map if user["src_id"] == party_id), None)
                                if u: s["partyId"] = u["target_id"]
                                else: logger.info(f"User with source ID {party_id} not found in target. Skipping share."); continue
                            elif s["type"] == "group":
                                g = next((group for group in groups_map if group["src_id"] == party_id), None)
                                if g: s["partyId"] = g["target_id"]
                                else: logger.warning(f"Group with source ID '{party_id}' not found in groups_map. Skipping share."); continue
                            updated_shares.append({"partyId": s["partyId"], "permission": s["permission"], "type": s["type"]})

                        logger.info(f"\tUpdating datamodel shares...")
                        api_clients.target_api.share_datamodel(dm["server"], dm["title"], updated_shares)
                        logger.info("\tShare update done.")
                    else:
                        logger.info(f"\tSkipping datamodel migration due to missing notebooks.")

                except SisenseRestAPIError as e:
                    logger.error(f"\tError during datamodel schema migration: status_code={e.result.status_code}, {e.result.text}")
                except Exception as e:
                    logger.error(f"An unexpected error occurred during datamodel schema migration: {e}", exc_info=True)

            # --- Saved Formulas Migration ---
            if config_loader.settings.get("migrate_saved_formulas", False):
                logger.info(f"\tChecking for saved formulas for datamodel {dm['title']}")
                try:
                    target_dm_exists_check = api_clients.target_api.check_elasticube_exists(dm["title"])
                    if target_dm_exists_check["data"]["elasticubeExists"]:
                        src_formulas = api_clients.src_api.get_datasource_measures(dm["title"], f'{dm["server"]}/{dm["title"]}')
                        if src_formulas:
                            logger.info(f"\tMigrating {len(src_formulas)} saved formulas for datamodel {dm['title']}")
                            for formula in src_formulas:
                                for key in ["_id", "created", "lastUpdated", "oid"]:
                                    formula.pop(key, None)
                                if formula.get("formula"):
                                    logger.info(f"\t\tAdding formula: {formula.get('title', '<no title>')}")
                                    try:
                                        api_clients.target_api.add_datasource_measure(json.dumps(formula))
                                    except Exception as e:
                                        logger.error(f"\t\tFailed to add formula {formula.get('title', '<no title>')}: {e}")
                        else:
                            logger.info(f"\tNo saved formulas found for {dm['title']}.")
                    else:
                        logger.warning(f"\tSkipping formula migration for '{dm['title']}' because the datamodel does not exist on the target server.")
                except Exception as e:
                    logger.error(f"An unexpected error occurred during formula migration for {dm['title']}: {e}", exc_info=True)

            # --- Saved Filter Migration ---
            if config_loader.settings.get("migrate_saved_filters", False):
                logger.info(f"\tChecking for saved filters for datamodel {dm['title']}")
                try:
                    target_dm_exists_check = api_clients.target_api.check_elasticube_exists(dm["title"])
                    if target_dm_exists_check["data"]["elasticubeExists"]:

                        src_saved_filters = api_clients.src_api.get_datasource_dimensions(dm["title"], f'{dm["server"]}/{dm["title"]}')
                        if src_saved_filters:
                            logger.info(f"Migrating {len(src_saved_filters)} saved filters for datamodel {dm['title']}")
                            for filter in src_saved_filters:
                                for key in ["_id", "created", "lastUpdated", "oid"]:
                                    filter.pop(key, None)
                                if filter.get("filter"):
                                    logger.info(f"\t\tAdding filter: {filter.get('title', '<no title>')}")
                                    try:
                                        api_clients.target_api.post_metadata_query(json.dumps(filter))
                                    except Exception as e:
                                        logger.error(f"\t\tFailed to add filter {filter.get('title', '<no title>')}: {e}")
                        else:
                            logger.info(f"\tNo saved filters found for {dm['title']}.")
                    else:
                        logger.warning(f"\tSkipping filter migration for '{dm['title']}' because the datamodel does not exist on the target server.")
                except Exception as e:
                    logger.error(f"An unexpected error occurred during filter migration for {dm['title']}: {e}", exc_info=True)

    # Run post import connection update function
    if config_loader.settings.get("post_import_update_connection_function"):

        try:
            # Import the module where the function is defined (replace with actual module)
            module_name = "connection_update_functions"  # Adjust this to the module name
            external_module = importlib.import_module(module_name)

            # Get the function object from the imported module
            function_name = config_loader.settings['post_import_update_connection_function']  # e.g., "my_function_name"
            function_to_call = getattr(external_module, function_name, None)
            if callable(function_to_call):
                # Call the function with the specific argument
                ret = function_to_call(api_clients.target_api, logger)

            else:
                logger.error(
                    f"Function '{function_name}' not found or not callable in module '{module_name}'")


        except ModuleNotFoundError as e:
            logger.error(f"Failed to load module '{module_name}': {e}")
        except AttributeError as e:
            logger.error(
                f"Function '{config_loader.settings['post_import_update_connection_function']}' not found in module '{module_name}': {e}")
        except Exception as e:
            logger.error(
                f"An error occurred while calling function '{config_loader.settings['post_import_update_connection_function']}': {e}")
        except SisenseRestAPIError as e:
            logger.error(f"\tstatus_code={e.result.status_code}, {e.result.text}")

        except Exception as e:
            logger.error(f"Error occurred: {e}")


def migrate_datasecurity(logger=None):
    if logger is None:
        logger = logger_setup.get_logger_for_migration('datasecurity')
    logger.info("Collecting datamodels for data security migration.")
    data_models = api_clients.src_api.get_datamodels_metadata()
    
    total_datamodels = len(data_models["data"]["elasticubesMetadata"])
    logger.info(f"Found {total_datamodels} total datamodels available.")
    
    # Filter datamodels based on include_datamodels_datasecurity and exclude_datamodels_datasecurity settings
    # Process include_datamodels_datasecurity - handle both string and list types
    include_datasecurity_raw = config_loader.settings.get("include_datamodels_datasecurity", "")
    if isinstance(include_datasecurity_raw, list):
        include_datasecurity_str = ", ".join(str(x) for x in include_datasecurity_raw if x)
    else:
        include_datasecurity_str = str(include_datasecurity_raw).strip() if include_datasecurity_raw else ""
    
    # Process exclude_datamodels_datasecurity - handle both string and list types
    exclude_datasecurity_raw = config_loader.settings.get("exclude_datamodels_datasecurity", "")
    if isinstance(exclude_datasecurity_raw, list):
        exclude_datasecurity_list = [str(x).strip() for x in exclude_datasecurity_raw if x]
    else:
        exclude_datasecurity_str = str(exclude_datasecurity_raw).strip() if exclude_datasecurity_raw else ""
        exclude_datasecurity_list = [title.strip() for title in exclude_datasecurity_str.split(",") if title.strip()] if exclude_datasecurity_str else []
    
    # Determine include list
    include_all = False
    include_list = []
    if not include_datasecurity_str or include_datasecurity_str == "ALL":
        include_all = True
        logger.info("include_datamodels_datasecurity is empty or 'ALL' - will migrate data security for all datamodels (except excluded ones).")
    else:
        include_list = [title.strip() for title in include_datasecurity_str.split(",") if title.strip()]
        logger.info(f"include_datamodels_datasecurity filter active - will only migrate data security for: {include_list}")
    
    if exclude_datasecurity_list:
        logger.info(f"exclude_datamodels_datasecurity filter active - will exclude: {exclude_datasecurity_list}")

    # Filter datamodels for data security migration
    filtered_datamodels = []
    for dm in data_models["data"]["elasticubesMetadata"]:
        # Determine if this datamodel should be included
        should_include = False
        if include_all:
            should_include = True
        else:
            should_include = dm['title'] in include_list
        
        # Check exclusion list
        if should_include and dm['title'] not in exclude_datasecurity_list:
            filtered_datamodels.append(dm)
    
    logger.info(f"After filtering, {len(filtered_datamodels)} datamodel(s) will have data security migrated.")

    for e in filtered_datamodels:
        try:
            logger.info(f"Migrating data security for elasticube {e['title']}")
            # Check if elasticube exists in target
            # target_datamodels = api_clients.target_api.get_datamodels_metadata()
            target_datamodels = api_clients.target_api.get_datamodels_metadata()['data']['elasticubesMetadata']

            is_exists = api_clients.target_api.check_elasticube_exists(e["title"])

            if (
                    is_exists["data"]["elasticubeExists"]
                    and e["title"] != "Usage Analytics Model"
            ):
                dm_status = next(
                    (
                        dm
                        for dm in target_datamodels
                        if dm["title"] == e["title"]
                    ),
                    None,
                )

                if (
                    dm_status.get("lastSuccessfulManualBuildStartTime", None)
                    or (
                        dm_status.get("type") == "live"
                        and dm_status['lastPublishTime'] is not None
                    )
                ):
                    # Get source data security
                    if e["type"] == "live":
                        src_ds = api_clients.src_api.get_live_datasecurity(e["title"])
                    else:
                        src_ds = api_clients.src_api.get_datasecurity(e["title"])

                    logger.debug(f"src_datasecurity = {src_ds}")

                    if src_ds:
                        logger.info(
                            f"\tFound {len(src_ds)} datasecurity rules to migrate."
                        )

                        ds_to_post = []
                        for rule in src_ds:
                            new_rule = {
                                "table": rule["table"],
                                "column": rule["column"],
                                "datatype": rule["datatype"],
                                "members": rule["members"],
                                "allMembers": rule.get("allMembers", None),
                                "elasticube": e["title"],
                                "server": "LocalHost",
                                "shares": [],

                            }
                            if e["type"] == "live":
                                new_rule["live"] = True
                                new_rule["fullname"] = "live:" + e["title"]

                            if "exclusionary" in rule:
                                new_rule["exclusionary"] = rule["exclusionary"]

                            else:
                                new_rule["exclusionary"] = False

                            # Update shares with target user/group ids
                            for s in rule["shares"]:
                                if s["type"] == "user":
                                    res = next(
                                        (
                                            user
                                            for user in users_map
                                            if user["src_id"] == s["partyId"]
                                        ),
                                        None,
                                    )

                                    if res:
                                        if e["type"] == "live":
                                            new_rule["shares"].append(
                                            {"type": "user", "partyId": res["target_id"]}
                                        )
                                        else:
                                            new_rule["shares"].append(
                                                {"type": "user", "party": res["target_id"]}
                                            )

                                if s["type"] == "group":
                                    res = next(
                                        (
                                            group
                                            for group in groups_map
                                            if group["src_id"] == s["partyId"]
                                        ),
                                        None,
                                    )

                                    if res:
                                        if e["type"] == "live":
                                            new_rule["shares"].append(
                                                {"type": "group", "partyId": res["target_id"]}
                                            )
                                        else:
                                            new_rule["shares"].append(
                                            {"type": "group", "party": res["target_id"]}
                                            )

                                if s["type"] == "default":
                                    new_rule["shares"].append(
                                        {"type": "default"}
                                    )
                            ds_to_post.append(new_rule)

                        logger.debug(f"ds_to_post={ds_to_post}")

                        if config_loader.settings["migrate_datasecurity_chunk_size"] != -1:
                            step = config_loader.settings["migrate_datasecurity_chunk_size"]
                        else:
                            step = len(ds_to_post)

                        total = 0
                        for i in range(0, len(ds_to_post), step):
                            x = i
                            chunk = ds_to_post[x: x + step]

                            retries_s = 0
                            retries_e = 1
                            retry = True
                            while retry:
                                try:
                                    if retries_s == 0:
                                        logger.info(
                                            f"\tMigrating next chunk (chunk size = {len(chunk)}.)"
                                        )

                                    if e["type"] == "live":
                                        api_clients.target_api.set_live_datasecurity_add_many(e["title"], chunk)
                                    else:
                                        api_clients.target_api.update_datasecurity(e["title"], chunk)

                                    # Successful update
                                    retry = False
                                    total += len(chunk)

                                    if total != len(ds_to_post):
                                        logger.info(
                                            f"\t>> Done ({total} rules of {len(ds_to_post)} migrated.)"
                                        )

                                    if (
                                            len(chunk)
                                            >= config_loader.settings["wait_chunk_size_threshold"]
                                    ):
                                        logger.info(
                                            f"Waiting {config_loader.settings['wait_between_chunks']} sec..."
                                        )
                                        time.sleep(config_loader.settings["wait_between_chunks"])

                                except SisenseRestAPIError as e:
                                    logger.error(
                                        f"\tError migrating datasecurity  {e.result.text}, {e.result.status_code}, {e.result.reason}"
                                    )

                                    retries_s += 1

                                    if retries_s <= retries_e:
                                        logger.info(
                                            f"\tRetrying ({retries_s}/{retries_e})..."
                                        )
                                    else:
                                        retry = False
                                        logger.debug(
                                            f"\tThe following rules were not migrated: {chunk}"
                                        )

                                    logger.info(
                                        f"Waiting {config_loader.settings['wait_between_retries']} sec..."
                                    )
                                    time.sleep(config_loader.settings["wait_between_retries"])

                                except Exception as e:
                                    logger.error(f"\tError migrating datasecurity.")
                                    retries_s += 1

                                    if retries_s <= retries_e:
                                        logger.info(
                                            f"\tRetrying ({retries_s}/{retries_e})..."
                                        )
                                    else:
                                        retry = False
                                        logger.debug(
                                            f"\tThe following error occurred migrating users: {e}"
                                        )

                                    logger.info(
                                        f"Waiting {config_loader.settings['wait_between_retries']} sec..."
                                    )
                                    time.sleep(config_loader.settings["wait_between_retries"])

                        logger.info(f"\tDone.")

                    else:
                        logger.info(
                            f"\tNo datasecurity rules found for elasticube {e['title']}. Nothing to migrate."
                        )
                else:
                    logger.warning(
                        f"Elasticube {e['title']} is in 'draft' state. The elasticube must be built before "
                        f"migrating datasecurity rules Skipping..."
                    )

            else:
                logger.error(
                    f"Elasticube {e['title']} does not exists in target. Skipping..."
                )

        except Exception as e:
            logger.error(e)
            logger.exception(f"Error occurred: {e}")
            logger.info("Skipping...")


def migrate_folders(src_api: SisenseRestApiClient, target_api: SisenseRestApiClient, logger=None):
    if logger is None:
        logger = logger_setup.get_logger_for_migration('folders')
    logger.info("Migrating folders")
    global folders_map
    if not folders_map:
        folders_map = load_folders_map(settings=config_loader.settings, logger=logger)
    # Get existing folder
    # get_existing_folders(api_clients.target_api)

    if config_loader.settings["folders"].get("share_source_dashboards_with_migration_user", False) is True:
        asyncio.run(share_source_dashboards_with_migration_user_async(src_api=api_clients.src_api, logger=logger))

    res = api_clients.src_api.get_folders("flat")
    num_of_folders = len(res) - 1  # remove the root folder from the count
    if json.dumps(res).count("Usage Analytics") != 0:
        num_of_folders -= 1  # remove "Usage Analytics" because we are not migration that

    src_folders = api_clients.src_api.get_folders("tree")

    logger.debug("src_folders={}".format(src_folders))

    # Progress spinner wrapper
    with alive_bar(num_of_folders, title="Migrating folder ", force_tty=True, spinner="arrow", monitor=False, bar=None,
                   refresh_secs=1, spinner_length=25, elapsed="elapsed: {elapsed}", stats=False) as bar:

        add_folders2(
            src_folders, [{"name": "rootFolder", "src_oid": '""', "target_oid": '""'}],
            bar, logger
        )

    logger.debug(f"folders_map before deduplication={folders_map}")

    # --- FIX: De-duplicate folders_map in memory ---
    # A folder is considered unique by its source OID and source host.
    unique_folders = {}
    for folder in folders_map:
        # Create a unique key for each folder
        folder_key = (folder.get('src_oid'), folder.get('src_host'))
        if folder_key not in unique_folders:
            unique_folders[folder_key] = folder

    deduplicated_folders_map = list(unique_folders.values())
    if len(folders_map) != len(deduplicated_folders_map):
        logger.info(
            f"Deduplicated folders map. Original: {len(folders_map)} entries, Final: {len(deduplicated_folders_map)} entries.")

    # Re-assign the de-duplicated list to the global variable
    folders_map = deduplicated_folders_map
    # --- End De-duplication ---

    logger.debug(f"folders_map after deduplication={folders_map}")

    # Save the now de-duplicated folders map.
    save_folders_map(folders_map, settings=config_loader.settings, logger=logger,
                     backup=False)

    # --- FIX: Removed the call to the broken function ---
    # remove_duplicate_objects_from_json(folders_map_file_path, folders_map_file_path)

    # Save report
    report_folder_owner_update_errors.save_report_to_excel("reports/folder_owner_update_errors_report.xlsx")
    report_add_folder_errors.save_report_to_excel("reports/add_folder_errors_report.xlsx")


def add_folders2(src_tree, folder_ancestors, bar, logger=None):
    if logger is None:
        logger = logger_setup.get_logger_for_migration('folders')
    global report_folder_owner_update_errors

    if "folders" in src_tree:
        for folder in src_tree["folders"]:
            bar()  # update spinner

            if folder["name"] != "Usage Analytics":
                u = next(
                    (
                        user
                        for user in users_map
                        if user["src_id"] == folder["owner"]
                    ),
                    None,
                )

                f = {
                    "name": folder["name"],
                    "src_oid": folder["oid"],
                    "target_oid": "",
                    "src_owner_id": folder["owner"],
                    # "owner_email": folder["owner"]["email"],
                }

                if u:

                    f["target_owner_id"] = u["target_id"]
                    f["owner_email"] = u["email"]

                else:

                    logger.warning(
                        f"Could not find user in user map. Try re-migrating the users and group to "
                        f"make sure they are up to date.")
                    report_folder_owner_update_errors.add_report_entry({
                        "name": f['name'],
                        "error": f"Could not find user{folder['owner']} in user map",
                    })

                folder_ancestors.append(f)

                add_folders2(folder, folder_ancestors, bar, logger)

                if config_loader.settings['folders']['update_target_folders_owner']:
                    # Update folders owner
                    new_folder = next((f for f in folders_map if f['src_oid'] == folder['oid'] and
                                       f['src_host'] == config_loader.settings['src_host']), None)
                    if new_folder:
                        try:
                            if "target_owner_id" in new_folder:
                                payload = {
                                    "owner": new_folder["target_owner_id"]
                                }
                                payload_str = json.dumps(payload)
                                # Debug: log exact request so 500s can be reproduced (e.g. with curl)
                                logger.debug(
                                    f"Folder owner update: name={f['name']!r} target_oid={new_folder['target_oid']!r} "
                                    f"target_owner_id={new_folder['target_owner_id']!r} payload={payload_str}"
                                )
                                res = api_clients.target_api.update_folder(new_folder["target_oid"],
                                                                           payload_str)

                                logger.info(
                                    f"\tUpdate folder owner {f['name']} to {new_folder['target_owner_id']}")
                                logger.debug(f"\t\t{res}")

                            else:

                                logger.error(
                                    f"New owner id is missing. Could not update folder owner. Source owner id is"
                                    f"{f['src_owner_id']}")
                                report_folder_owner_update_errors.add_report_entry({
                                    "name": f['name'],
                                    "error": f"New owner id is missing. Could not update folder owner. Source owner id is"
                                             f"{f['src_owner_id']}",
                                })

                        except SisenseRestAPIError as e:
                            req_url = getattr(e.result, 'url', None)
                            status = getattr(e.result, 'status_code', None)
                            logger.error(
                                f"Could not update folder owner {f['name']} to {f['target_owner_id']}: {e.result.text}. Skipping...")
                            logger.debug(
                                f"Folder owner update failed: name={f['name']!r} target_oid={new_folder['target_oid']!r} "
                                f"target_owner_id={new_folder.get('target_owner_id')!r} status={status} url={req_url}"
                            )
                            report_folder_owner_update_errors.add_report_entry({
                                "name": f['name'],
                                "error": f"Could not update folder owner {f['name']} to {f['target_owner_id']}: {e.result.text}",
                            })
                            continue

                folder_ancestors.pop()


    else:
        logger.info(f'Migrating folder')
        logger.debug(f'\tFolder ancestors = {[x["name"] for x in folder_ancestors]}')
        expr_str = ""

        for idx, f in enumerate(folder_ancestors):

            if f['name'] != "rootFolder":

                tabs = "\t" * idx
                if idx in range(1, len(folder_ancestors)):
                    if idx == len(folder_ancestors) - 1:
                        # logger_setup.logger.info(f'{tabs}⎹⎽⎽ {f["name"]}')
                        logger_setup.logger.info(f'{f["name"]}')

                    else:
                        # logger_setup.logger.info(f'{tabs}⎸⎼⎼ {f["name"]}')
                        logger_setup.logger.info(f'{f["name"]}')

                in_folders_map = next((fm for fm in folders_map if
                                       fm['src_oid'] == f['src_oid'] and fm['src_host'] == config_loader.settings[
                                           'src_host']),
                                      None)

                # TODO: Maybe check fm['src_host'] == settings['src_host'] separately in case it doesn't exist by accident
                if not in_folders_map:

                    # if len(match) == 0:
                    logger_setup.logger.debug(f'\tAdding folder {f["name"]} to target')
                    payload = {
                        "name": f["name"],

                    }

                    if folder_ancestors[idx - 1]["target_oid"] != '""' and folder_ancestors[idx - 1][
                        "target_oid"] is not None:
                        payload["parentId"] = folder_ancestors[idx - 1]["target_oid"]

                    try:
                        res = api_clients.target_api.add_folder(json.dumps(payload))
                    except SisenseRestAPIError as e:
                        logger_setup.logger.error(f"Could not add folder {f['name']}. Error: {e}. Skipping...")
                        report_add_folder_errors.add_report_entry({
                            'name': f['name'],
                            "error": e.result.text,
                        })
                        continue

                    f["parentId"] = folder_ancestors[idx - 1]["target_oid"]
                    f["target_oid"] = res["oid"]

                    new_folder = {
                        "name": f["name"],
                        "src_oid": f["src_oid"],
                        "target_oid": f["target_oid"],
                        "src_host": config_loader.settings['src_host'],
                    }

                    if "target_owner_id" in f:
                        new_folder["target_owner_id"] = f["target_owner_id"]

                    if "owner_email" in f:
                        new_folder["owner_email"] = f["owner_email"]

                    if "parentId" in f:
                        new_folder["parent_id"] = f["parentId"]

                    folders_map.append(new_folder)
                    save_folders_map(folders_map, settings=config_loader.settings, logger=logger,
                                     backup=False)

                else:
                    logger.info(f'\tFolder {f["name"]} already in target')
                    f["target_oid"] = in_folders_map["target_oid"]
                    # Refresh folders_map entry with current owner mapping so owner update step finds target_owner_id
                    if "target_owner_id" in f:
                        in_folders_map["target_owner_id"] = f["target_owner_id"]
                    if "owner_email" in f:
                        in_folders_map["owner_email"] = f["owner_email"]


def transform_blox_action(source_action):
    """
    Transform blox action from source format to target format.
    
    Args:
        source_action (dict): Source action with keys like '_id', 'type', 'body', 'code'
        
    Returns:
        dict: Transformed action with keys 'type', 'body', 'snippet', 'step'
    """
    action_type = source_action.get("type", "Unknown")
    logger_setup.logger.debug(f"Transforming BloX action '{action_type}' from source format")
    
    transformed_action = {
        "type": action_type,
        "body": source_action.get("body", ""),
        "snippet": {
            "type": action_type,
            "title": "title" 
        },
        "step": "2"
    }
    
    # If there's a title field in source, use it
    if "title" in source_action:
        transformed_action["snippet"]["title"] = source_action["title"]
        logger_setup.logger.debug(f"Using custom title '{source_action['title']}' for action '{action_type}'")
    else:
        logger_setup.logger.debug(f"No custom title found for action '{action_type}', using default")
    
    # Log transformation details
    body_length = len(transformed_action.get("body", ""))
    logger_setup.logger.debug(f"Action '{action_type}' transformed - Body length: {body_length} chars")
    
    return transformed_action


def migrate_blox_actions(logger=None):
    if logger is None:
        logger = logger_setup.get_logger_for_migration('blox')
    logger.info("=== Starting BloX Actions Migration ===")
    
    try:
        # Fetch existing actions from source, and from target to avoid overwrite, if enabled in settings
        logger.info("Fetching BloX actions from source server...")
        srcActions = api_clients.src_api.get_blox_actions()
        logger.info(f"Found {len(srcActions)} BloX actions on source server")
        
        logger.info("Fetching BloX actions from target server...")
        targetActions = api_clients.target_api.get_blox_actions()
        logger.info(f"Found {len(targetActions)} BloX actions on target server")

        # Remove actions already present
        srcBloxActionTitles = [action["type"] for action in srcActions]
        targetBloxActionTitles = [action["type"] for action in targetActions]

        targetActionsWithSameTitle = [act for act in targetBloxActionTitles if act in srcBloxActionTitles]
        logger.info(f"Found {len(targetActionsWithSameTitle)} duplicate action titles: {targetActionsWithSameTitle}")

        # If overwriting actions, then ignore duplicates
        if (config_loader.settings["overwrite_existing_blox_actions"]):
            logger.info("Overwrite mode enabled - will replace existing actions")
            targetActionsWithSameTitle = [];
        else:
            logger.info("Overwrite mode disabled - will skip existing actions")

        # Transfer actions individually via loop
        actions_to_migrate = [action for action in srcActions if action["type"] not in targetActionsWithSameTitle]
        logger.info(f"Will migrate {len(actions_to_migrate)} BloX actions")
        
        migrated_count = 0
        skipped_count = 0
        
        for action in srcActions:
            action_type = action.get("type", "Unknown")
            
            # If not duplicates
            if (action_type not in targetActionsWithSameTitle):
                try:
                    logger.info(f"Migrating BloX action: '{action_type}'")
                    
                    # Transform the action to target format
                    transformed_action = transform_blox_action(action)
                    logger.debug(f"Transformed action '{action_type}' - Original keys: {list(action.keys())}, Transformed keys: {list(transformed_action.keys())}")
                    
                    bloxTransferResponse = api_clients.target_api.import_blox_action(transformed_action)
                    logger.info(f"✓ Successfully migrated BloX action '{action_type}' - Response: {bloxTransferResponse}")
                    migrated_count += 1
                    
                except Exception as e:
                    logger.error(f"✗ Failed to migrate BloX action '{action_type}': {str(e)}")
                    continue
            else:
                logger.info(f"Skipping BloX action '{action_type}' - already exists on target")
                skipped_count += 1

        logger.info(f"=== BloX Actions Migration Complete ===")
        logger.info(f"Total actions processed: {len(srcActions)}")
        logger.info(f"Successfully migrated: {migrated_count}")
        logger.info(f"Skipped (duplicates): {skipped_count}")
        
    except Exception as e:
        logger.error(f"BloX Actions migration failed: {str(e)}")
        raise


def migrate_custom_code(logger=None):
    """
    Migrate custom code (notebooks) from source to target server.
    Exports notebooks from source and creates/updates them on target.
    """
    if logger is None:
        logger = logger_setup.get_logger_for_migration('custom_code')
    logger.info("=== Starting Custom Code (Notebooks) Migration ===")
    
    try:
        # Step 1: Fetch notebooks from source server
        logger.info("Fetching notebooks from source server...")
        src_notebooks_response = api_clients.src_api.get_notebooks()
        src_notebooks = src_notebooks_response if isinstance(src_notebooks_response, list) else src_notebooks_response.get('data', []) if isinstance(src_notebooks_response, dict) else []
        logger.info(f"Found {len(src_notebooks)} notebooks on source server")
        
        if not src_notebooks:
            logger.info("No notebooks found on source server. Skipping migration.")
            return
        
        # Step 1.5: Filter notebooks based on notebook_include_list
        notebook_include_list = config_loader.settings.get("notebook_include_list", "ALL")
        if notebook_include_list and str(notebook_include_list).strip().upper() != "ALL":
            # Parse the include list - can be comma-separated string or newline-separated
            if isinstance(notebook_include_list, list):
                include_list = [str(x).strip() for x in notebook_include_list if x]
            else:
                include_str = str(notebook_include_list).strip()
                # Try splitting by newline first, then by comma
                if '\n' in include_str:
                    include_list = [x.strip() for x in include_str.split('\n') if x.strip()]
                else:
                    include_list = [x.strip() for x in include_str.split(',') if x.strip()]
            
            logger.info(f"Filtering notebooks - will only migrate: {include_list}")
            
            # Filter notebooks by ID or displayName
            filtered_notebooks = []
            for nb in src_notebooks:
                notebook_id = nb.get('id', '')
                notebook_name = nb.get('displayName', nb.get('name', ''))
                
                # Check if notebook matches any item in include list (by ID or name)
                if notebook_id in include_list or notebook_name in include_list:
                    filtered_notebooks.append(nb)
            
            src_notebooks = filtered_notebooks
            logger.info(f"After filtering, {len(src_notebooks)} notebooks will be migrated")
        
        if not src_notebooks:
            logger.info("No notebooks match the include filter. Skipping migration.")
            return
        
        # Step 2: Fetch notebooks from target server (for duplicate checking)
        logger.info("Fetching notebooks from target server...")
        target_notebooks_response = api_clients.target_api.get_notebooks()
        target_notebooks = target_notebooks_response if isinstance(target_notebooks_response, list) else target_notebooks_response.get('data', []) if isinstance(target_notebooks_response, dict) else []
        logger.info(f"Found {len(target_notebooks)} notebooks on target server")
        
        # Print list of notebooks on target server
        if target_notebooks:
            logger.info("Target server notebooks:")
            for nb in target_notebooks:
                nb_id = nb.get('id', 'N/A')
                nb_name = nb.get('displayName', nb.get('name', 'N/A'))
                logger.info(f"  - {nb_name} (ID: {nb_id})")
        else:
            logger.info("No notebooks found on target server")
        
        # Step 3: Identify duplicates using prefix matching (same logic as datamodel migration)
        # Use prefix matching instead of exact matching due to system limitations
        existing_notebooks_map = {}  # Maps source notebook_id to matching target notebook
        for src_nb in src_notebooks:
            src_notebook_id = src_nb.get('id')
            if src_notebook_id:
                matching_target = find_matching_notebook(src_notebook_id, target_notebooks)
                if matching_target:
                    existing_notebooks_map[src_notebook_id] = matching_target
        
        existing_notebook_ids = set(existing_notebooks_map.keys())
        logger_setup.logger.info(f"Found {len(existing_notebook_ids)} duplicate notebooks (using prefix matching)")
        
        # Step 4: Handle import mode setting
        import_mode = config_loader.settings.get("notebook_import_mode", "skip")
        if import_mode not in ["skip", "overwrite", "duplicate"]:
            logger_setup.logger.warning(f"Invalid notebook_import_mode '{import_mode}', defaulting to 'skip'")
            import_mode = "skip"
        
        if import_mode == "overwrite":
            logger_setup.logger.info("Import mode: 'overwrite' - will update existing notebooks")
        elif import_mode == "duplicate":
            logger_setup.logger.info("Import mode: 'duplicate' - will create duplicates with modified IDs")
        else:
            logger_setup.logger.info("Import mode: 'skip' - will skip existing notebooks")
        
        # Step 5: Filter notebooks to migrate
        if import_mode == "overwrite":
            # In overwrite mode, migrate all notebooks (will update existing ones)
            notebooks_to_migrate = src_notebooks
        elif import_mode == "duplicate":
            # In duplicate mode, migrate all notebooks (will create with new IDs)
            notebooks_to_migrate = src_notebooks
        else:
            # In skip mode, only migrate notebooks that don't exist on target
            notebooks_to_migrate = [nb for nb in src_notebooks if nb.get('id') not in existing_notebook_ids]
        logger_setup.logger.info(f"Will migrate {len(notebooks_to_migrate)} notebooks")
        
        migrated_count = 0
        skipped_count = 0
        error_count = 0
        
        # Step 6: Migration loop
        for notebook in notebooks_to_migrate:
            notebook_id = notebook.get('id')
            notebook_uuid = notebook.get('uuid') or notebook.get('oid')  # Try uuid first, fallback to oid
            notebook_name = notebook.get('displayName', notebook_id or 'Unknown')
            
            if not notebook_id:
                logger_setup.logger.warning(f"Skipping notebook '{notebook_name}' - missing ID")
                skipped_count += 1
                continue
            
            # Check if notebook exists on target using prefix matching (same logic as datamodel migration)
            matching_target = find_matching_notebook(notebook_id, target_notebooks)
            
            # Check if should skip (duplicate exists and mode is skip)
            if matching_target and import_mode == "skip":
                target_notebook_id = matching_target.get('id', 'N/A')
                logger_setup.logger.info(f"Skipping notebook '{notebook_name}' (ID: {notebook_id}) - already exists on target (matched with target ID: {target_notebook_id})")
                skipped_count += 1
                continue
            
            try:
                logger_setup.logger.info(f"Migrating notebook: '{notebook_name}' (ID: {notebook_id})")
                
                # Step 6a: Export notebook from source using UUID if available, otherwise use ID
                export_id = notebook_uuid if notebook_uuid else notebook_id
                logger_setup.logger.debug(f"Exporting notebook '{notebook_name}' from source using {'UUID' if notebook_uuid else 'ID'}: {export_id}")
                exported_notebook = api_clients.src_api.export_notebook(export_id)
                
                # Handle response format (could be dict with 'data' or direct dict)
                if isinstance(exported_notebook, dict) and 'data' in exported_notebook:
                    notebook_payload = exported_notebook['data']
                else:
                    notebook_payload = exported_notebook
                
                if not notebook_payload:
                    logger_setup.logger.error(f"Failed to export notebook '{notebook_name}' - empty response")
                    error_count += 1
                    continue
                
                # Step 6b: Prepare payload - remove/modify fields
                # Remove oid if present (target will assign new)
                if 'oid' in notebook_payload:
                    del notebook_payload['oid']
                
                # Remove any read-only or system fields that shouldn't be sent during creation
                fields_to_remove = ['_id', '_rev', 'createdAt', 'updatedAt', 'createdBy', 'updatedBy']
                for field in fields_to_remove:
                    if field in notebook_payload:
                        del notebook_payload[field]
                
                # Handle duplicate mode - modify ID to create a duplicate
                # Use the matching_target found earlier in the loop (line 3588)
                if import_mode == "duplicate" and matching_target:
                    # Generate a new ID for duplicate (append timestamp or suffix)
                    new_id = f"{notebook_id}_duplicate_{int(time.time())}"
                    notebook_payload['id'] = new_id
                    logger_setup.logger.debug(f"Duplicate mode: Changed notebook ID from '{notebook_id}' to '{new_id}'")
                
                # Ensure required fields are present
                if 'id' not in notebook_payload:
                    notebook_payload['id'] = notebook_id
                if 'displayName' not in notebook_payload and notebook_name:
                    notebook_payload['displayName'] = notebook_name
                
                # Keep essential fields for migration
                # The exported notebook should already have all necessary fields
                logger_setup.logger.debug(f"Prepared payload for notebook '{notebook_name}' with keys: {list(notebook_payload.keys())}")
                
                # Step 6c: Create or update on target
                # Use the matching_target found earlier in the loop
                if matching_target and import_mode == "overwrite":
                    # Update existing notebook using PATCH
                    success, updated_id, updated_uuid = update_existing_notebook(
                        api_clients, notebook_id, notebook_payload, matching_target, logger_prefix=""
                    )
                    if success:
                        logger_setup.logger.info(f"✓ Successfully updated notebook '{notebook_name}' using PATCH")
                    else:
                        logger_setup.logger.warning(f"Failed to update notebook '{notebook_name}' using PATCH, will attempt to create")
                        # Fall through to create logic below as fallback
                        matching_target = None  # Clear matching_target so it goes to create path
                else:
                    # Create new notebook (either new or duplicate)
                    final_id = notebook_payload.get('id', notebook_id)
                    logger_setup.logger.info(f"Creating new notebook '{notebook_name}' (ID: {final_id}) on target...")
                    create_response = api_clients.target_api.create_notebook(notebook_payload)
                    logger_setup.logger.info(f"✓ Successfully created notebook '{notebook_name}' - Response: {create_response}")
                    
                    # Step 6d: Compare IDs and update if they don't match
                    # Extract the ID and UUID from the create response
                    created_notebook_id = None
                    created_notebook_uuid = None
                    created_notebook_data = None
                    
                    if isinstance(create_response, dict):
                        created_notebook_data = create_response.get('data', create_response)
                        created_notebook_id = created_notebook_data.get('id')
                        created_notebook_uuid = created_notebook_data.get('uuid') or created_notebook_data.get('oid')
                    elif isinstance(create_response, list) and len(create_response) > 0:
                        created_notebook_data = create_response[0] if isinstance(create_response[0], dict) else None
                        if created_notebook_data:
                            created_notebook_id = created_notebook_data.get('id')
                            created_notebook_uuid = created_notebook_data.get('uuid') or created_notebook_data.get('oid')
                    
                    # Compare source ID with created notebook ID
                    if created_notebook_id and created_notebook_id != notebook_id:
                        update_notebook_id_and_path(
                            api_clients, notebook_id, created_notebook_id, 
                            created_notebook_uuid, notebook_payload, logger_prefix=""
                        )
                    elif created_notebook_id == notebook_id:
                        logger_setup.logger.debug(f"Notebook ID matches source ID ({notebook_id}) - no update needed")
                    else:
                        logger_setup.logger.warning(f"Could not extract ID from create response to compare with source ID ({notebook_id})")
                
                migrated_count += 1
                
            except SisenseRestAPIError as e:
                # Get detailed error message
                error_msg = str(e)
                error_data = None
                status_code = None
                if hasattr(e, 'result'):
                    status_code = e.result.status_code if hasattr(e.result, 'status_code') else None
                    if hasattr(e.result, 'text'):
                        try:
                            error_data = e.result.json()
                            error_msg = error_data.get('message', error_data.get('error', e.result.reason))
                        except:
                            error_msg = e.result.text[:500] if e.result.text else e.result.reason
                    else:
                        error_msg = e.result.reason if hasattr(e.result, 'reason') else str(e)
                
                # Debug: Log the error details
                logger_setup.logger.debug(f"API error details - status_code: {status_code}, error_msg: {error_msg}, error_data: {error_data}")
                
                
                
                # Check if the error is "notebook already exists"
                # Check both error_data dict and error_msg string
                is_already_exists = False
                error_message_to_check = ''
                
                if error_data:
                    # error_data is a dict, check the 'message' field
                    error_message_to_check = str(error_data.get('message', '')).lower()
                    if not error_message_to_check:
                        error_message_to_check = str(error_data.get('error', '')).lower()
                else:
                    # Use error_msg string
                    error_message_to_check = str(error_msg).lower()
                
                is_already_exists = 'already exists' in error_message_to_check or 'duplicate' in error_message_to_check
                logger_setup.logger.debug(f"Checking error message: '{error_message_to_check}', is_already_exists: {is_already_exists}")
                
                if is_already_exists:
                    logger_setup.logger.info(f"Detected 'already exists' error for notebook '{notebook_name}' (ID: {notebook_id}), handling with import_mode: {import_mode}")
                    # Notebook already exists - handle based on import mode
                    if import_mode == "skip":
                        logger_setup.logger.info(f"Skipping notebook '{notebook_name}' (ID: {notebook_id}) - already exists on target (detected via API error)")
                        skipped_count += 1
                    elif import_mode == "overwrite":
                        # Delete existing notebook first, then create new one (using prefix matching)
                        target_notebook = find_matching_notebook(notebook_id, target_notebooks)
                        target_notebook_uuid = None
                        if target_notebook:
                            target_notebook_uuid = target_notebook.get('uuid') or target_notebook.get('oid')
                        else:
                            # Notebook exists (per API error) but not in our target list - try to get it by ID
                            logger_setup.logger.debug(f"Notebook '{notebook_id}' not found in target list, will try to delete using ID directly")
                        
                        # Use UUID if available, otherwise use ID for deletion
                        delete_identifier = target_notebook_uuid if target_notebook_uuid else notebook_id
                        logger_setup.logger.info(f"Notebook '{notebook_name}' already exists, deleting and recreating using {'UUID' if target_notebook_uuid else 'ID'} '{delete_identifier}'...")
                        try:
                            # Delete the existing notebook
                            delete_response = api_clients.target_api.delete_notebook(delete_identifier)
                            logger_setup.logger.info(f"✓ Successfully deleted existing notebook '{notebook_name}' - Response: {delete_response}")
                            
                            # Now create the new notebook
                            create_response = api_clients.target_api.create_notebook(notebook_payload)
                            logger_setup.logger.info(f"✓ Successfully created notebook '{notebook_name}' - Response: {create_response}")
                            
                            # Check if ID needs to be updated (same logic as normal create flow)
                            created_notebook_id = None
                            created_notebook_uuid = None
                            if isinstance(create_response, dict):
                                created_notebook_data = create_response.get('data', create_response)
                                created_notebook_id = created_notebook_data.get('id')
                                created_notebook_uuid = created_notebook_data.get('uuid') or created_notebook_data.get('oid')
                            elif isinstance(create_response, list) and len(create_response) > 0:
                                created_notebook_data = create_response[0] if isinstance(create_response[0], dict) else None
                                if created_notebook_data:
                                    created_notebook_id = created_notebook_data.get('id')
                                    created_notebook_uuid = created_notebook_data.get('uuid') or created_notebook_data.get('oid')
                            
                            if created_notebook_id and created_notebook_id != notebook_id:
                                update_notebook_id_and_path(
                                    api_clients, notebook_id, created_notebook_id, 
                                    created_notebook_uuid, notebook_payload, logger_prefix=""
                                )
                            
                            migrated_count += 1
                        except Exception as delete_error:
                            logger_setup.logger.error(f"✗ Failed to delete/recreate notebook '{notebook_name}': {str(delete_error)}")
                            error_count += 1
                    elif import_mode == "duplicate":
                        # Generate a new ID and try again
                        new_id = f"{notebook_id}_duplicate_{int(time.time())}"
                        notebook_payload['id'] = new_id
                        logger_setup.logger.info(f"Notebook '{notebook_name}' already exists, creating duplicate with ID '{new_id}'...")
                        try:
                            create_response = api_clients.target_api.create_notebook(notebook_payload)
                            logger_setup.logger.info(f"✓ Successfully created duplicate notebook '{notebook_name}' (ID: {new_id}) - Response: {create_response}")
                            migrated_count += 1
                        except Exception as dup_error:
                            logger_setup.logger.error(f"✗ Failed to create duplicate notebook '{notebook_name}': {str(dup_error)}")
                            error_count += 1
                    continue
                else:
                    # Other API error (including 401)
                    # For 401 errors, check if notebook was actually created before marking as failed
                    if status_code == 401:
                        logger_setup.logger.warning(f"Got 401 error for notebook '{notebook_name}' (ID: {notebook_id}), checking if notebook was created...")
                        
                        
                        # Refresh target notebooks list and check if this notebook exists
                        try:
                            refreshed_target_notebooks = api_clients.target_api.get_notebooks()
                            if isinstance(refreshed_target_notebooks, dict):
                                refreshed_target_notebooks = refreshed_target_notebooks.get('data', [])
                            elif not isinstance(refreshed_target_notebooks, list):
                                refreshed_target_notebooks = []
                            
                            matching_target = find_matching_notebook(notebook_id, refreshed_target_notebooks)
                            if matching_target:
                                target_notebook_id = matching_target.get('id', 'N/A')
                                logger_setup.logger.info(f"✓ Notebook '{notebook_name}' (ID: {notebook_id}) was successfully created despite 401 error (target ID: {target_notebook_id})")
                                
                                migrated_count += 1
                                continue
                            else:
                                logger_setup.logger.error(f"✗ Notebook '{notebook_name}' (ID: {notebook_id}) not found on target after 401 error")
                                
                                error_count += 1
                                continue
                        except Exception as check_error:
                            logger_setup.logger.warning(f"Failed to verify notebook existence after 401 error: {str(check_error)}")
                            
                            error_count += 1
                            continue
                    else:
                        # Other API error (not 401)
                        logger_setup.logger.error(f"✗ API error migrating notebook '{notebook_name}' (ID: {notebook_id}): {error_msg}")
                        logger_setup.logger.debug(f"Payload keys sent: {list(notebook_payload.keys()) if 'notebook_payload' in locals() else 'N/A'}")
                        error_count += 1
                        continue
            except Exception as e:
                logger_setup.logger.error(f"✗ Failed to migrate notebook '{notebook_name}' (ID: {notebook_id}): {str(e)}")
                logger_setup.logger.debug(f"Exception details: {e}", exc_info=True)
                
                error_count += 1
                continue
        
        # Step 8: Summary logging
        # Calculate total skipped count (including duplicates filtered out in skip mode)
        total_skipped = skipped_count
        if import_mode == "skip":
            # Add notebooks that were filtered out due to duplicates
            duplicates_filtered_out = len(src_notebooks) - len(notebooks_to_migrate)
            total_skipped += duplicates_filtered_out
        
        logger_setup.logger.info(f"=== Custom Code (Notebooks) Migration Complete ===")
        logger_setup.logger.info(f"Total notebooks on source: {len(src_notebooks)}")
        logger_setup.logger.info(f"Successfully migrated: {migrated_count}")
        logger_setup.logger.info(f"Skipped (duplicates or missing ID): {total_skipped}")
        logger_setup.logger.info(f"Errors: {error_count}")
        
    except Exception as e:
        logger_setup.logger.error(f"Custom Code (Notebooks) migration failed: {str(e)}")
        logger_setup.logger.debug(f"Exception details: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sisense Migration & Merge Tool")
    parser.add_argument('--settings-file', type=str, help='Path to the settings YAML file', default='settings.yaml')
    parser.add_argument('--export-settings', nargs='?', const='.', default=None,
                        help='Export the current settings file to the specified directory (or current directory if not specified)')
    parser.add_argument('--export-servers', nargs='?', const='.', default=None,
                        help='Export the current servers.yaml file to the specified directory (or current directory if not specified)')
    args, unknown = parser.parse_known_args()
    if args.export_settings is not None:
        from config_loader import application_path

        settings_file_name = args.settings_file
        src_path = os.path.join(application_path, settings_file_name) if not os.path.isabs(
            settings_file_name) else settings_file_name
        if not os.path.exists(src_path):
            print(f"Settings file '{src_path}' not found.")
            sys.exit(1)
        dest_dir = args.export_settings or '.'
        os.makedirs(dest_dir, exist_ok=True)
        dest_path = os.path.join(dest_dir, os.path.basename(src_path))
        shutil.copy2(src_path, dest_path)
        print(f"Exported settings file to: {dest_path}")
        sys.exit(0)
    if args.export_servers is not None:
        from config_loader import application_path, SERVERS_FILE

        servers_file_name = SERVERS_FILE if 'SERVERS_FILE' in globals() else 'servers.yaml'
        src_path = os.path.join(application_path, servers_file_name) if not os.path.isabs(
            servers_file_name) else servers_file_name
        if not os.path.exists(src_path):
            print(f"Servers file '{src_path}' not found.")
            sys.exit(1)
        dest_dir = args.export_servers or '.'
        os.makedirs(dest_dir, exist_ok=True)
        dest_path = os.path.join(dest_dir, os.path.basename(src_path))
        shutil.copy2(src_path, dest_path)
        print(f"Exported servers file to: {dest_path}")
        sys.exit(0)
    try:
        main()
    finally:
        print(">>> About to call logging.shutdown()")
        logging.shutdown()
        print(">>> Finished logging.shutdown()")