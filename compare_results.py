#!/usr/bin/env python3
"""
Temporary utility script to compare orphaned_files.json with SQL database results.
This script helps verify that the migration to SQL hasn't missed any items.
"""
import json
import sqlite3
import argparse
import os
import sys
import logging
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_JSON_PATH = "orphaned_files.json"

# Load environment variables from .env file
load_dotenv()

# Get the local torrent folder path from environment variable
LOCAL_TORRENT_BASE_LOCAL_FOLDER = os.getenv("LOCAL_TORRENT_BASE_LOCAL_FOLDER")
if LOCAL_TORRENT_BASE_LOCAL_FOLDER:
    logger.info(
        (
            f"Found LOCAL_TORRENT_BASE_LOCAL_FOLDER in .env: "
            f"{LOCAL_TORRENT_BASE_LOCAL_FOLDER}"
        )
    )
else:
    logger.warning("LOCAL_TORRENT_BASE_LOCAL_FOLDER not found in environment variables")

# Set the database path based on the environment variable
if LOCAL_TORRENT_BASE_LOCAL_FOLDER:
    DEFAULT_SQLITE_PATH = os.path.join(
        os.path.dirname(LOCAL_TORRENT_BASE_LOCAL_FOLDER), ".file_cache.sqlite"
    )
    logger.info(f"Using database path from .env: {DEFAULT_SQLITE_PATH}")
else:
    # Fallback to a default location
    DEFAULT_SQLITE_PATH = ".file_cache.sqlite"
    logger.warning(
        (
            f"LOCAL_TORRENT_BASE_LOCAL_FOLDER not found in .env, "
            f"using default database path: {DEFAULT_SQLITE_PATH}"
        )
    )


def load_json_results(json_path):
    """Load results from the JSON file."""
    try:
        with open(json_path, "r") as f:
            data = json.load(f)
        logger.info(f"Successfully loaded JSON data from {json_path}")
        return data
    except FileNotFoundError:
        logger.error(f"JSON file not found: {json_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON format in file: {json_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading JSON file: {e}")
        sys.exit(1)


def get_sql_results(db_path):
    """Get results from the SQL database, using the vw_latest_scan_report view."""
    try:
        if not os.path.exists(db_path):
            logger.error(f"Database file not found: {db_path}")
            sys.exit(1)

        conn = sqlite3.connect(db_path)
        # Use row_factory to access columns by name, useful for fetching metadata
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='view' AND name='vw_latest_scan_report'"
        )
        if not cursor.fetchone():
            logger.error(
                "'vw_latest_scan_report' view not found. "
                "Run the main script to initialize/update the DB."
            )
            sys.exit(1)

        # Get all data from the latest scan using the view
        cursor.execute(
            """
        SELECT scan_id, scan_host, scan_base_path, scan_start, scan_end,
               file_path, file_label, file_size, file_size_human,
               scan_context_file_source
        FROM vw_latest_scan_report
        """
        )

        all_rows_from_view = cursor.fetchall()
        conn.close()

        if not all_rows_from_view:
            logger.warning(
                "vw_latest_scan_report is empty. "
                "This might mean no scans exist or the latest scan had no files."
            )
            # Return an empty structure consistent with what compare_results expects
            return {
                "host": "N/A",
                "base_path": "N/A",
                "scan_start": "N/A",
                "scan_end": "N/A",
                "in_local_torrent_folder_but_not_deluge": [],
                "files_only_in_torrents": [],
                "files_only_in_media": [],
            }

        # Extract scan metadata from the first row (same for all rows from view)
        first_row = all_rows_from_view[0]
        host = first_row["scan_host"]
        base_path = first_row["scan_base_path"]
        scan_start = first_row["scan_start"]
        scan_end = first_row["scan_end"]
        latest_scan_id = first_row["scan_id"]  # For logging purposes

        logger.info(
            f"Successfully retrieved {len(all_rows_from_view)} files "
            f"from the latest scan (ID: {latest_scan_id}) "
            f"using vw_latest_scan_report"
        )

        result_dict = {
            "host": host,
            "base_path": base_path,
            "scan_start": scan_start,
            "scan_end": scan_end,
            "in_local_torrent_folder_but_not_deluge": [],
            "files_only_in_torrents": [],
            "files_only_in_media": [],
        }

        for row in all_rows_from_view:
            file_info = {
                "path": row["file_path"],
                "size": row["file_size"],
                "size_human": row["file_size_human"],
            }
            if row["file_label"]:
                file_info["label"] = row["file_label"]

            source_from_view = row["scan_context_file_source"]
            if source_from_view == "local_torrent_folder":
                result_dict["in_local_torrent_folder_but_not_deluge"].append(file_info)
            elif source_from_view == "torrents":
                result_dict["files_only_in_torrents"].append(file_info)
            elif source_from_view == "media":
                result_dict["files_only_in_media"].append(file_info)

        return result_dict
    except sqlite3.OperationalError as e:
        logger.error(f"SQLite operational error: {e}")
        logger.error(
            "This might indicate an issue with 'vw_latest_scan_report' "
            "or the underlying tables."
        )
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error getting SQL results: {e}")
        sys.exit(1)


def compare_results(json_data, sql_data, output_file=None):
    """Compare JSON and SQL results and report differences with detailed logging."""
    output = []
    output.append("\n=== COMPARISON RESULTS ===\n")
    output.append(
        f"JSON scan time: {json_data.get('scan_start', 'N/A')} "
        f"to {json_data.get('scan_end', 'N/A')}"
    )
    output.append(
        f"SQL scan time: {sql_data.get('scan_start', 'N/A')} "
        f"to {sql_data.get('scan_end', 'N/A')}"
    )
    output.append(f"JSON host: {json_data.get('host', 'N/A')}")
    output.append(f"SQL host: {sql_data.get('host', 'N/A')}")
    output.append(f"JSON base path: {json_data.get('base_path', 'N/A')}")
    output.append(f"SQL base path: {sql_data.get('base_path', 'N/A')}")

    categories = [
        (
            "in_local_torrent_folder_but_not_deluge",
            "Files in local torrent folder but not in Deluge",
        ),
        ("files_only_in_torrents", "Files only in torrents"),
        ("files_only_in_media", "Files only in media"),
    ]

    overall_summary = {
        "total_json_files": 0,
        "total_sql_files": 0,
        "total_missing_in_sql": 0,
        "total_missing_in_json": 0,
        "total_attribute_mismatches": 0,
        "total_common_files_checked": 0,
    }
    any_differences_found = False

    for category_key, description in categories:
        output.append(f"\n--- Category: {description} ---")

        json_items = json_data.get(category_key, [])
        sql_items = sql_data.get(category_key, [])

        json_files_dict = {item["path"]: item for item in json_items}
        sql_files_dict = {item["path"]: item for item in sql_items}

        output.append(f"  JSON files: {len(json_items)}")
        output.append(f"  SQL files: {len(sql_items)}")

        overall_summary["total_json_files"] += len(json_items)
        overall_summary["total_sql_files"] += len(sql_items)

        json_paths = set(json_files_dict.keys())
        sql_paths = set(sql_files_dict.keys())

        missing_in_sql_paths = json_paths - sql_paths
        missing_in_json_paths = sql_paths - json_paths
        common_paths = json_paths.intersection(sql_paths)

        overall_summary["total_missing_in_sql"] += len(missing_in_sql_paths)
        overall_summary["total_missing_in_json"] += len(missing_in_json_paths)
        overall_summary["total_common_files_checked"] += len(common_paths)

        category_has_differences = False

        if missing_in_sql_paths:
            category_has_differences = True
            any_differences_found = True
            output.append(
                f"  Files in JSON but not in SQL ({len(missing_in_sql_paths)}):"
            )
            for path in sorted(list(missing_in_sql_paths)):
                output.append(f"    - {path}")

        if missing_in_json_paths:
            category_has_differences = True
            any_differences_found = True
            output.append(
                f"  Files in SQL but not in JSON ({len(missing_in_json_paths)}):"
            )
            for path in sorted(list(missing_in_json_paths)):
                output.append(f"    - {path}")

        attribute_mismatches_details = []
        category_attribute_mismatches = 0
        for path in sorted(list(common_paths)):
            json_file = json_files_dict[path]
            sql_file = sql_files_dict[path]
            mismatches = []

            # Compare size
            json_size = json_file.get("size")
            sql_size = sql_file.get("size")
            if json_size != sql_size:
                mismatches.append(f"size (JSON: {json_size}, SQL: {sql_size})")

            # Compare label
            json_label = json_file.get("label")
            sql_label = sql_file.get("label")
            if json_label != sql_label:
                mismatches.append(f"label (JSON: '{json_label}', SQL: '{sql_label}')")

            if mismatches:
                category_has_differences = True
                any_differences_found = True
                category_attribute_mismatches += 1
                attribute_mismatches_details.append(f"    - Path: {path}")
                for mismatch_detail in mismatches:
                    attribute_mismatches_details.append(f"      - {mismatch_detail}")

        if attribute_mismatches_details:
            output.append(
                f"  Attribute mismatches for common files "
                f"({category_attribute_mismatches}):"
            )
            output.extend(attribute_mismatches_details)
            overall_summary[
                "total_attribute_mismatches"
            ] += category_attribute_mismatches

        if not category_has_differences:
            output.append("  No differences found in this category.")

    # Overall Summary Section
    output.append("\n\n=== OVERALL SUMMARY ===\n")
    output.append(f"Total files in JSON: {overall_summary['total_json_files']}")
    output.append(f"Total files in SQL: {overall_summary['total_sql_files']}")
    output.append(
        f"Total files in JSON but not in SQL: {overall_summary['total_missing_in_sql']}"
    )
    output.append(
        f"Total files in SQL but not in JSON: "
        f"{overall_summary['total_missing_in_json']}"
    )
    output.append(
        f"Total common files checked for attributes: "
        f"{overall_summary['total_common_files_checked']}"
    )
    output.append(
        f"Total attribute mismatches in common files: "
        f"{overall_summary['total_attribute_mismatches']}"
    )

    if not any_differences_found:
        output.append("\nCONCLUSION: JSON and SQL results are IDENTICAL.")
        logger.info("Comparison complete: JSON and SQL results are identical.")
    else:
        output.append("\nCONCLUSION: Differences found between JSON and SQL results.")
        logger.warning(
            "Comparison complete: Differences found. Check the output for details."
        )

    if output_file:
        try:
            with open(output_file, "w") as f:
                for line in output:
                    f.write(line + "\n")
            logger.info(f"Comparison results written to {output_file}")
        except Exception as e:
            logger.error(f"Error writing to output file {output_file}: {e}")
            # Fallback to console if file write fails
            for line in output:
                print(line)
    else:
        for line in output:
            print(line)


def main():
    parser = argparse.ArgumentParser(
        description="Compare orphaned_files.json with SQL database results"
    )
    parser.add_argument(
        "--json_path", default=DEFAULT_JSON_PATH, help="Path to the JSON results file"
    )
    parser.add_argument(
        "--db_path",
        default=DEFAULT_SQLITE_PATH,
        help="Path to the SQLite database file",
    )
    parser.add_argument(
        "--output",
        default="comparison_results.txt",
        help="File to save the comparison results",
    )

    args = parser.parse_args()

    abs_json_path = os.path.abspath(args.json_path)
    abs_db_path = os.path.abspath(args.db_path)

    logger.info("Starting comparison with the following files:")
    logger.info("  JSON file (absolute): %s", abs_json_path)
    logger.info(f"  SQLite DB (absolute): {abs_db_path} (using vw_latest_scan_report)")

    logger.info(f"Loading JSON results from: {abs_json_path}")
    json_data = load_json_results(abs_json_path)

    logger.info(f"Loading SQL results from: {abs_db_path} (via vw_latest_scan_report)")
    sql_data = get_sql_results(abs_db_path)  # No scan_id passed anymore

    compare_results(json_data, sql_data, args.output)


if __name__ == "__main__":
    main()
