import asyncio # Added asyncio
import json
import logging
import os
import sys
from deepdiff import DeepDiff
from alive_progress import alive_bar

import SisenseRESTAPIClientClass
from MigrationReportClass import MigrationReport
from SisenseRESTAPIClientClass import SisenseRestAPIError
from utils.utils import fetch_all_dashboards

if getattr(sys, 'frozen', False):
    # --- Case 1: Application is frozen ---
    application_path = os.path.dirname(sys.executable)
else:
    # --- Case 2: Application is a standard script ---
    try:

        application_path = os.path.dirname(os.path.abspath(__file__))
    except NameError:

        application_path = os.getcwd()  # Use current working directory as fallback

# --- Helper Function for DeepDiff ---
def is_subset_dict(subset_dict, main_dict, mig_user_email, logger: logging.Logger = logging.getLogger(__name__)):
    exclude_paths = []
    for i in range(len(main_dict.get('sharesTo', []))):
        exclude_paths.append(f"root['sharesTo'][{i}]['usersCount']")
    exclude_paths.append("root['title']")
    exclude_paths.append("root['subscription']")
    exclude_paths.append("root['owner']['lastActivity']")
    exclude_paths.append("root['owner']['lastLogin']")
    exclude_paths.append("root['owner']['lastUpdated']")
    exclude_paths.extend([f"root['sharesTo'][{i}]['email']" for i in range(len(main_dict.get('sharesTo',[])))])

    # Add this exclusion because we add it temporarily in the worker
    exclude_paths.append("root['_mig_user_id_temp']")

    diff = DeepDiff(subset_dict, main_dict,
                    exclude_paths=exclude_paths,
                    ignore_order=True,
                    report_repetition=True)

    if diff:
        added_items = diff.get('iterable_item_added', {})
        if len(added_items) == 1 and any(key.startswith("root['sharesTo']") for key in added_items):
             added_share = list(added_items.values())[0]
             if added_share.get('shareId') == subset_dict.get('_mig_user_id_temp'): # Use temp ID for check
                 # logger.debug(f"Validation Diff OK for {subset_dict.get('title', 'N/A')}: Only migration user added.")
                 return True
        # Log details only if validation actually fails
        logger.warning(f"Validation Diff found differences beyond adding migration user for {subset_dict.get('title', 'N/A')}: {diff}")
        return False
    return True


# --- Synchronous Worker Function (Keep as is) ---
# This function contains the core blocking logic for a single dashboard
def _process_single_dashboard(dash, mig_user, src_api, logger):
    """
    Processes a single dashboard: gets shares, updates if necessary, verifies.
    This is SYNCHRONOUS and potentially BLOCKING.
    Returns a dictionary with results for aggregation.
    """
    oid = dash.get('oid')
    title = dash.get('title')
    mig_user_id = mig_user.get('_id')
    mig_user_email = mig_user.get('email')
    logger.debug(f"Worker processing dashboard: {title} ({oid})") # Keep debug for worker start

    original_shares = None
    updated_shares = None
    status = "skipped"
    error_type = None
    error_message = None
    report_data = None

    try:
        # 1. Get current shares (BLOCKING CALL)
        # original_shares = src_api.get_dashboard_shares(oid)

        original_shares = {'sharesTo': dash.get('shares'), 'title': title, '_mig_user_id_temp': mig_user_id}

        # 2. Check if migration user already has access
        has_access = any(
            share.get('shareId') == mig_user_id
            for share in original_shares.get('sharesTo', [])
        )

        if has_access:
            logger.debug(f"Migration user already has access to {title} ({oid}). Skipping.")
            status = "skipped_has_access"

            logger.info(f"Migration user already has access to {title} ({oid}). Re-publishing.")
            src_api.publish_dashboard(oid)
        else:
            # 3. Prepare new shares payload
            new_share = {
                'rule': 'view', 'shareId': mig_user_id, 'subscribe': False, 'type': 'user'
            }
            new_sharesTo = []
            for dash_share in original_shares.get('sharesTo', []):
                shareTo = {
                    'shareId': dash_share.get('shareId'), 'type': dash_share.get('type'),
                    'subscribe': dash_share.get('subscribe', False)
                }
                if 'rule' in dash_share: shareTo['rule'] = dash_share.get('rule')
                new_sharesTo.append(shareTo)
            new_sharesTo.append(new_share)

            new_payload = {
                "sharesTo": new_sharesTo, "sharesToNew": [],
                "allowChangeSubscription": False, "subscription": original_shares.get('subscription', {}),
            }

            # 4. Update shares via API
            logger.debug(f"Attempting to share {title} ({oid})")
            status = "update_attempted"
            src_api.share_dashboard(oid, json.dumps(new_payload))
            status = "success"
            logger.info(f"Successfully shared {title} ({oid})")

            # 5. Verify update (BLOCKING CALL)
            # time.sleep(0.1) # Optional delay
            # updated_shares = src_api.get_dashboard_shares(oid)
            # updated_shares['title'] = title

            # Perform validation (CPU-bound, but happens within the thread)
            # is_valid = is_subset_dict(original_shares, updated_shares, mig_user_email)

            # if is_valid:
            #     logger.debug(f"Successfully shared and verified {title} ({oid})")
            #     status = "success"
            # else:
            #     # Simulate the case where the API call returned different shares than expected after update
            #     if oid == 'dash9':
            #          updated_shares = {'sharesTo': [{'shareId': 'userG', 'type':'user'}, {'shareId':'someone_else', 'type':'user'}] , 'title':'Dashboard Nine - Validation Fail'} # Simulate unexpected change
            #          is_valid = is_subset_dict(original_shares, updated_shares, mig_user_email) # Recheck for logging
            #
            #     logger.error(f"Validation failed for dashboard {title} ({oid}) after update. Shares might be corrupted.")
            #     status = "update_failed_validation"
            #     error_type = "update"
            #     error_message = f"Validation failed. Original: {original_shares.get('sharesTo')}, Updated: {updated_shares.get('sharesTo')}"
            #     report_data = {"oid": oid, "title": title, "sharesTo_original": original_shares.get('sharesTo'), "sharesTo_updated": updated_shares.get('sharesTo')}

    except SisenseRestAPIError as e:
        error_text = e.result.text if hasattr(e, 'result') and hasattr(e.result, 'text') else str(e)
        if status == "update_attempted":
            logger.error(f"API Error updating/verifying shares for {title} ({oid}): {error_text}.")
            status = "update_failed_api"
            error_type = "update"
            error_message = error_text
            report_data = {"oid": oid, "title": title, "error": error_text}
        else:
            error_text = e.result.text if hasattr(e, 'result') and hasattr(e.result, 'text') else str(e)
            error_message = f"API Error getting shares for {title} ({oid}): {error_text}. Skipping."
            logger.error(f"API Error getting shares for {title} ({oid}): {error_text}. Skipping.")
            status = "get_failed_api"
            error_type = "get"
            report_data = {"oid": oid, "title": title, "error": error_message}

    except Exception as e: # Catch unexpected errors within the sync function
        logger.exception(f"Unexpected synchronous error processing dashboard {title} ({oid}): {e}")
        status = "failed_unexpected"
        error_type = "unknown"
        error_message = str(e)
        report_data = {"oid": oid, "title": title, "error": f"Unexpected sync error: {error_message}"}

    # Prepare final result dict
    final_original_shares = None
    if status != "get_failed_api" and original_shares:
         final_original_shares = original_shares

    return {
        "oid": oid, "title": title, "status": status,
        "original_shares": final_original_shares,
        "error_type": error_type, "error_message": error_message,
        "report_data": report_data
    }


# --- Async Wrapper for the Synchronous Worker ---
async def _process_single_dashboard_async(semaphore: asyncio.Semaphore, dash, mig_user, src_api, logger):
    """
    Async wrapper that acquires a semaphore and runs the synchronous
    _process_single_dashboard function in a separate thread using asyncio.to_thread.
    """
    async with semaphore:
        # logger.debug(f"Semaphore acquired for {dash.get('title')}. Running sync worker in thread.")
        try:
            # Run the blocking synchronous function in a thread pool
            result = await asyncio.to_thread(
                _process_single_dashboard, # The function to run
                dash,                      # Arguments to the function
                mig_user,
                src_api,
                logger
            )
            # logger.debug(f"Sync worker finished for {dash.get('title')}")
            return result
        except Exception as e:
            # Catch potential errors *during the execution* within the thread
            # This supplements the error handling inside _process_single_dashboard
            title = dash.get('title', 'Unknown Title')
            oid = dash.get('oid', 'Unknown OID')
            logger.exception(f"Error executing sync worker via to_thread for {title} ({oid}): {e}")
            return {
                "oid": oid, "title": title, "status": "failed_to_thread_execution",
                "original_shares": None, "error_type": "infra",
                "error_message": f"Error in to_thread execution: {e}",
                "report_data": {"oid": oid, "title": title, "error": f"Failed during to_thread: {e}"}
            }


# --- Main Async Function ---
async def share_source_dashboards_with_migration_user_async(src_api: SisenseRESTAPIClientClass.SisenseRestApiClient,
                                                            logger: logging.Logger = logging.getLogger(__name__),
                                                            concurrency_limit: int = 10):
    """
    Ensures the migration user has 'view' access to all dashboards,
    performing updates concurrently using asyncio, semaphores, and asyncio.to_thread.

    Args:
        concurrency_limit (int): Max number of dashboards to process concurrently.
        :param src_api:
        :param concurrency_limit:
        :param logger:
    """
    logger.info(f"Making sure migration user has access (Concurrency: {concurrency_limit}).")
    report_share_src_dashboards_with_migration_user = MigrationReport("Share src dashboards with migration user")

    # Get migration user (sync call, okay at start)
    try:
        mig_user = src_api.get_my_user()
        logger.info(f"Migration User: {mig_user.get('email')} ({mig_user.get('_id')})")
        if not mig_user or '_id' not in mig_user:
             logger.error("Could not retrieve valid migration user information. Aborting.")
             return
    except Exception as e:
        logger.exception(f"Failed to get migration user: {e}")
        return

    # Get *all* dashboards (sync call, okay at start)
    try:
        limit = 1000  # Define how many dashboards to fetch per API call
        all_dashboards_raw = fetch_all_dashboards(src_api, logger, limit)
        dashboards_list = list({'oid': d['oid'], 'title': d['title'], 'shares': d['shares']} for d in all_dashboards_raw if 'oid' in d and 'title' in d)
        total_tasks = len(dashboards_list)
        logger.info(f"Found {total_tasks} total dashboards.")
        if not dashboards_list:
            logger.info("No dashboards found to process.")
            return
    except Exception as e:
        logger.exception(f"Failed to get the list of all dashboards: {e}")
        return

    # --- Async Processing Setup ---
    semaphore = asyncio.Semaphore(concurrency_limit)
    tasks = []

    # Data collection lists/dicts
    shares_backup = {}
    get_shares_errors = []
    update_shares_errors = []
    validation_errors = []
    unexpected_errors = [] # Covers sync unexpected and to_thread execution errors
    success_count = 0
    skipped_count = 0

    logger.info(f"Submitting {total_tasks} dashboard sharing tasks...")
    # Create tasks using the async wrapper
    for dash in dashboards_list:
        tasks.append(
            asyncio.create_task(
                _process_single_dashboard_async(semaphore, dash, mig_user, src_api, logger),
                name=f"Task-{dash.get('title')}"
            )
        )

    logger.info("Waiting for dashboard sharing to complete...")
    # alive_bar is disabled globally when not in a TTY, so this check is for printing progress
    is_interactive = sys.stdout.isatty()

    if not is_interactive:
        # For non-interactive mode, print initial progress state for the web UI
        print(f"SHARE_PROGRESS::{json.dumps({'current': 0, 'total': total_tasks})}", flush=True)

    completed_count = 0
    # Process results as they complete using alive_bar
    with alive_bar(total_tasks, title="Sharing Dashboards with Migration User", bar="smooth", spinner="waves") as bar:
        for future in asyncio.as_completed(tasks):
            try:
                result = await future # Wait for the task (which ran the sync code) to complete

                # --- Aggregate results (same logic as before) ---
                oid = result.get('oid', '(Unknown OID)')

                # Backup original shares if successfully retrieved
                if result.get('original_shares'):
                    # Remove internal temp field before backup
                    result['original_shares'].pop('_mig_user_id_temp', None)
                    shares_backup[oid] = result['original_shares']

                # Categorize results and errors
                status = result.get('status', 'unknown_status') # Default if status missing
                title = result.get('title', '(Unknown Title)')
                error_msg = result.get('error_message', 'N/A')
                report_data = result.get('report_data')

                if status == "success":
                    success_count += 1
                elif status == "skipped_has_access":
                    skipped_count += 1
                elif status == "get_failed_api":
                    get_shares_errors.append((title, oid, error_msg))
                    if report_data: report_share_src_dashboards_with_migration_user.add_report_entry(report_data)
                elif status == "update_failed_api":
                    update_shares_errors.append((title, oid, error_msg))
                    if report_data: report_share_src_dashboards_with_migration_user.add_report_entry(report_data)
                elif status == "update_failed_validation":
                     validation_errors.append((title, oid, error_msg))
                     if report_data: report_share_src_dashboards_with_migration_user.add_report_entry(report_data)
                elif status == "failed_unexpected":
                    unexpected_errors.append((title, oid, f"SyncWorkerError: {error_msg}"))
                    if report_data: report_share_src_dashboards_with_migration_user.add_report_entry(report_data)
                elif status == "failed_to_thread_execution":
                     unexpected_errors.append((title, oid, f"ToThreadError: {error_msg}"))
                     if report_data: report_share_src_dashboards_with_migration_user.add_report_entry(report_data)
                else:
                    logger.warning(f"Unknown status '{status}' received for {title} ({oid})")
                    unexpected_errors.append((title, oid, f"UnknownStatus: {status} - {error_msg}"))
                    if report_data: report_share_src_dashboards_with_migration_user.add_report_entry(report_data)
                 # --- End Aggregation ---

            except Exception as exc: # Catch errors *retrieving* the result (less likely now)
                logger.error(f"Error retrieving result from asyncio task: {exc}", exc_info=True)
                # Attempt to get task name if possible for context
                task_name = "Unknown Task"
                if hasattr(future, 'get_name'): task_name = future.get_name()
                unexpected_errors.append((task_name, "N/A", f"Error awaiting task result: {exc}"))
            finally:
                # Ensure the bar progresses for every task future completed
                completed_count += 1
                bar()
                if not is_interactive:
                    # Print structured progress for the server to parse
                    print(f"SHARE_PROGRESS::{json.dumps({'current': completed_count, 'total': total_tasks})}", flush=True)


    # --- Post-Processing and Reporting (Identical to previous versions) ---
    logger.info("Dashboard sharing complete.")
    logger.info(f"Summary: {success_count} updated, {skipped_count} skipped (already shared), "
                f"{len(get_shares_errors)} get errors, {len(update_shares_errors)} update errors, "
                f"{len(validation_errors)} validation errors, {len(unexpected_errors)} unexpected errors.")

    # Write backup file
    backup_file_path = os.path.join(application_path, ".dashboard_shares_backup_asyncio.json")
    try:
        logger.info(f"Writing backup of {len(shares_backup)} original shares to {backup_file_path}")
        with open(backup_file_path, "w") as backup_file:
            json.dump(shares_backup, backup_file, indent=4)
        logger.info("Backup complete.")
    except Exception as e:
        logger.exception(f"Failed to write backup file {backup_file_path}: {e}")

    # Log detailed errors
    if get_shares_errors:
        err_list = '\n\t'.join([f"{t} ({o}): {m}" for t, o, m in get_shares_errors])
        logger.error("Errors occurred while *getting* shares (skipped update):\n\t" + err_list)
    if update_shares_errors:
        err_list = '\n\t'.join([f"{t} ({o}): {m}" for t, o, m in update_shares_errors])
        logger.error("Errors occurred while *updating* shares (check manually, see report):\n\t" + err_list)
    if validation_errors:
        err_list = '\n\t'.join([f"{t} ({o}): {m}" for t, o, m in validation_errors])
        logger.error("Validation *failed* after updating shares (check manually, see report):\n\t" + err_list)
    if unexpected_errors:
        err_list = '\n\t'.join([f"{t} ({o}): {m}" for t, o, m in unexpected_errors])
        logger.error("Unexpected errors occurred during processing (check logs and report):\n\t" + err_list)

    # Save the report
    try:
        report_path = "reports/share_source_dashboards_with_migration_user.xlsx"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        report_share_src_dashboards_with_migration_user.save_report_to_excel(report_path)
        logger.info(f"Failure report saved to {report_path}")
    except Exception as e:
        logger.exception(f"Failed to save failure report: {e}")


# --- Main Execution Block ---
if __name__ == '__main__':
    # Make sure the reports directory exists
    if not os.path.exists("reports"):
        os.makedirs("reports")
