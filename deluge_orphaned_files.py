import os
import json
from pathlib import Path
from datetime import datetime
from deluge_client import DelugeRPCClient
from dotenv import load_dotenv
import hashlib
from tqdm import tqdm
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Deluge connection settings
DELUGE_HOST = os.getenv("DELUGE_HOST")
DELUGE_PORT = int(os.getenv("DELUGE_PORT"))
DELUGE_USERNAME = os.getenv("DELUGE_USERNAME")
DELUGE_PASSWORD = os.getenv("DELUGE_PASSWORD")

# Path to your torrent folder
DELUGE_TORRENT_BASE_REMOTE_FOLDER = os.getenv("DELUGE_TORRENT_BASE_REMOTE_FOLDER")
LOCAL_TORRENT_BASE_LOCAL_FOLDER = os.getenv("LOCAL_TORRENT_BASE_LOCAL_FOLDER")
LOCAL_MEDIA_BASE_LOCAL_FOLDER = os.getenv("LOCAL_MEDIA_BASE_LOCAL_FOLDER")

# Output file path
OUTPUT_FILE = os.getenv("OUTPUT_FILE")

# List of file extensions and folders to ignore (comma-separated in .env file)
EXTENSIONS_BLACKLIST = os.getenv('EXTENSIONS_BLACKLIST', '.nfo,.srt,.jpg').split(',')
LOCAL_SUBFOLDERS_BLACKLIST = os.getenv("LOCAL_SUBFOLDERS_BLACKLIST", "music,ebooks,courses").split(',')
CACHE_SAVE_INTERVAL = int(os.getenv("CACHE_SAVE_INTERVAL", 25))

__version__ = "1.1.1"

def print_version_info():
    logger.info(f"Deluge Orphaned Files Checker v{__version__}")

def verify_paths():
    """Verify that all required paths exist and are accessible."""
    paths_to_check = [
        ('LOCAL_TORRENT_BASE_LOCAL_FOLDER', LOCAL_TORRENT_BASE_LOCAL_FOLDER),
        ('LOCAL_MEDIA_BASE_LOCAL_FOLDER', LOCAL_MEDIA_BASE_LOCAL_FOLDER)
    ]

    for name, path in paths_to_check:
        if not path:
            logger.error(f"{name} environment variable is not set")
            return False

        if not os.path.exists(path):
            logger.error(f"{name} path does not exist: {path}")
            return False

        if not os.path.isdir(path):
            logger.error(f"{name} is not a directory: {path}")
            return False

        # Try to list directory contents
        try:
            next(os.scandir(path))
            logger.info(f"Successfully accessed {name}: {path}")
        except StopIteration:
            logger.warning(f"{name} directory is empty: {path}")
        except PermissionError:
            logger.error(f"Permission denied accessing {name}: {path}")
            return False
        except Exception as e:
            logger.error(f"Error accessing {name} ({path}): {str(e)}")
            return False

    return True

def get_deluge_files():
    client = DelugeRPCClient(DELUGE_HOST, DELUGE_PORT, DELUGE_USERNAME, DELUGE_PASSWORD)
    client.connect()

    logger.info(f"Fetching torrent list from {client.username}@{client.host}:{client.port}...")
    torrent_list = client.call('core.get_torrents_status', {}, ['files', 'save_path', 'label'])

    all_files = set()
    file_labels = {}
    for torrent_id, torrent_data in torrent_list.items():
        save_path = torrent_data[b'save_path'].decode()
        label = torrent_data.get(b'label', b'').decode() or 'No Label'
        for file in torrent_data[b'files']:
            file_path = file[b'path'].decode()
            full_path = os.path.join(save_path, file_path)
            relative_path = full_path[len(DELUGE_TORRENT_BASE_REMOTE_FOLDER):].lstrip('/')
            all_files.add(relative_path)
            file_labels[relative_path] = label

    return all_files, file_labels

def load_hash_cache(cache_file):
    logger.info(f"Loading cache from: {cache_file}")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                cache = json.load(f)
            logger.info(f"Loaded {len(cache)} cache entries")
            logger.debug(f"Cache: {str(cache)[:400]}...")
            return cache
        except Exception as e:
            logger.error(f"Error loading cache: {str(e)}")
    else:
        logger.warning("Cache file not found")
    return {}

def save_hash_cache(cache_file, hash_cache):
    logger.debug(f"Saving {len(hash_cache)} entries to hash cache")
    try:
        with open(cache_file, 'w') as f:
            json.dump(hash_cache, f)
    except Exception as e:
        logger.error(f"Error saving cache: {str(e)}")

def get_local_files(folder):
    local_files = {}
    cache_file = Path(folder) / '.hash_cache.json'
    hash_cache = load_hash_cache(cache_file)

    # First, count total files for the progress bar
    total_files = 0
    for root, _, files in os.walk(folder):
        # Check if this is a blacklisted first-level subdirectory
        relative_root = Path(root).relative_to(folder)
        if relative_root.parts and relative_root.parts[0] in LOCAL_SUBFOLDERS_BLACKLIST:
            continue
        total_files += sum(1 for f in files if Path(f).suffix.lower() not in EXTENSIONS_BLACKLIST)

    files_since_last_save = 0
    with tqdm(total=total_files, desc=f"Scanning {Path(folder).name}") as pbar:
        for root, dirs, files in os.walk(folder):
            # Check if this is a blacklisted first-level subdirectory
            relative_root = Path(root).relative_to(folder)
            if relative_root.parts and relative_root.parts[0] in LOCAL_SUBFOLDERS_BLACKLIST:
                continue

            for file in files:
                if Path(file).suffix.lower() not in EXTENSIONS_BLACKLIST:
                    full_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_path, folder)

                    mtime = os.path.getmtime(full_path)
                    cache_key = relative_path
                    cache_hit = False

                    if cache_key in hash_cache:
                        cached_mtime = float(hash_cache[cache_key]['mtime'])
                        # Allow for 2 second difference
                        if abs(cached_mtime - mtime) <= 2:
                            cache_hit = True
                        else:
                            logger.debug(f"Cache miss for {cache_key}: cached_mtime={cached_mtime}, current_mtime={mtime}")

                    if cache_hit:
                        file_hash = hash_cache[cache_key]['hash']
                        logger.debug(f"Cache hit for {cache_key}")
                    else:
                        logger.debug(f"Cache miss for {cache_key}")
                        file_hash = get_file_hash(full_path)
                        hash_cache[cache_key] = {
                            'hash': file_hash,
                            'mtime': mtime
                        }
                        files_since_last_save += 1

                    # Save cache every interval of new/modified files
                    if files_since_last_save >= CACHE_SAVE_INTERVAL:
                        save_hash_cache(cache_file, hash_cache)
                        files_since_last_save = 0
                        logger.debug(f"Cache write, {len(hash_cache)} total cache entries")

                    local_files[relative_path] = file_hash
                pbar.update(1)

    # Final save of the cache
    if files_since_last_save > 0:
        save_hash_cache(cache_file, hash_cache)
    return local_files

def get_file_hash(file_path):
    md5_hash = hashlib.md5()
    file_size = os.path.getsize(file_path)

    # Use a larger chunk size for better performance with large files
    chunk_size = 1024 * 1024  # 1MB chunks instead of 8KB

    with open(file_path, "rb") as f:
        with tqdm(total=file_size, unit='B', unit_scale=True,
                 desc=f"Hashing {Path(file_path).name}",
                 leave=False) as pbar:
            while chunk := f.read(chunk_size):
                md5_hash.update(chunk)
                pbar.update(len(chunk))
    return md5_hash.hexdigest()

def find_orphaned_files(skip_media_check=False):
    scan_start_time = datetime.now()
    try:
        logger.info("Connecting to Deluge and getting file list...")
        deluge_files, file_labels = get_deluge_files()
        logger.info(f"Found {len(deluge_files)} files in Deluge.")

        logger.info("Scanning local torrent folder...")
        local_torrent_files = get_local_files(LOCAL_TORRENT_BASE_LOCAL_FOLDER)
        logger.info(f"Found {len(local_torrent_files)} files in local torrent folder.")

        logger.info("Comparing files in deluge against files in the local torrent folder...")
        orphaned_torrent_files = sorted(list(set(local_torrent_files.keys()) - deluge_files))
        logger.info(f"Found {len(orphaned_torrent_files)} files in the local torrent folder that are not in Deluge.")

        if skip_media_check:
            if orphaned_torrent_files:
                logger.info(f"\nFound {len(orphaned_torrent_files)} orphans")
                save_scan_results(orphaned_torrent_files, [], [])
            else:
                logger.info("\nNo orphaned files found.")
            return

        logger.info("Scanning local media folder...")
        local_media_files = get_local_files(LOCAL_MEDIA_BASE_LOCAL_FOLDER)
        logger.info(f"Found {len(local_media_files)} files in local media folder.")

        #logger.info("Comparing files in deluge against files in the local torrent folder...")
        #orphaned_torrent_files = sorted(list(local_torrent_files - deluge_files))
        #logger.info(f"Found {len(orphaned_torrent_files)} files in the local torrent folder that are not in Deluge.")

        # Compare files based on their hashes
        # Exclude files in blacklisted subfolders and with blacklisted extensions
        torrent_hashes = {hash: name for name, hash in local_torrent_files.items()
                          if not any(name.startswith(subfolder + '/') for subfolder in LOCAL_SUBFOLDERS_BLACKLIST)
                          and Path(name).suffix.lower() not in EXTENSIONS_BLACKLIST}
        media_hashes = {hash: name for name, hash in local_media_files.items()
                        if not any(name.startswith(subfolder + '/') for subfolder in LOCAL_SUBFOLDERS_BLACKLIST)
                        and Path(name).suffix.lower() not in EXTENSIONS_BLACKLIST}

        # Pre-filter collections before set operations
        torrent_set = frozenset(torrent_hashes.keys())
        media_set = frozenset(media_hashes.keys())

        only_in_torrents = sorted([torrent_hashes[hash] for hash in torrent_set - media_set])
        only_in_media = sorted([media_hashes[hash] for hash in media_set - torrent_set])

        logger.info(f"Found {len(only_in_torrents)} files only in torrents folder")
        logger.info(f"Found {len(only_in_media)} files only in media folder")

        # If you want to see which files correspond to each other despite different names:
        renamed_files = []
        for hash in torrent_set.intersection(media_set):
            if torrent_hashes[hash] != media_hashes[hash]:
                renamed_files.append((torrent_hashes[hash], media_hashes[hash]))

        logger.info(f"Found {len(renamed_files)} files that were renamed")

        # Save results regardless of whether orphans were found
        logger.info(f"\nScan complete. Found {len(orphaned_torrent_files)} orphans, {len(only_in_torrents)} files only in torrents, {len(only_in_media)} files only in media")
        save_scan_results(orphaned_torrent_files, only_in_torrents, only_in_media, scan_start_time)

    except KeyboardInterrupt:
        logger.warning("\nOperation cancelled by user. Progress has been saved in the cache.")
        return

def save_scan_results(
    orphaned_torrent_files: list[str],
    only_in_torrents: list[str],
    only_in_media: list[str],
    scan_start_time: datetime
) -> None:
    output_data = {
        "host": f"{DELUGE_USERNAME}@{DELUGE_HOST}:{DELUGE_PORT}",
        "base_path": str(DELUGE_TORRENT_BASE_REMOTE_FOLDER),
        "scan_start": scan_start_time.isoformat(),
        "scan_end": datetime.now().isoformat(),
        "in_local_torrent_folder_but_not_deluge": orphaned_torrent_files,
        "files_only_in_torrents": only_in_torrents,
        "files_only_in_media": only_in_media
    }

    try:
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(output_data, f, indent=2)
        logger.info(f"Scan results saved to {OUTPUT_FILE}")
    except IOError as e:
        logger.error(f"Failed to save scan results to {OUTPUT_FILE}: {e}")

def clean_hash_cache(folder: Path) -> None:
    cache_file = folder / '.hash_cache.json'
    hash_cache = load_hash_cache(cache_file)

    current_files = set()
    for root, _, files in os.walk(folder):
        for file in files:
            if Path(file).suffix.lower() not in EXTENSIONS_BLACKLIST:
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, folder)
                current_files.add(relative_path)

    # Remove entries for files that no longer exist
    updated_cache = {k: v for k, v in hash_cache.items() if k in current_files}

    # Save cleaned cache
    save_hash_cache(cache_file, updated_cache)

    removed = len(hash_cache) - len(updated_cache)
    if removed > 0:
        logger.info(f"Removed {removed} stale entries from hash cache")

def main():
    print_version_info()

    parser = argparse.ArgumentParser()
    parser.add_argument('--clean-cache', action='store_true',
                       help='Clean stale entries from hash cache')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--skip-media-check', action='store_true',
                       help='Only check Deluge vs local torrent files (skip media folder comparison)')
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)


    # Verify paths before proceeding
    if not verify_paths():
        logger.error("Path verification failed, exiting")
        return

    if args.clean_cache:
        clean_hash_cache(Path(LOCAL_TORRENT_BASE_LOCAL_FOLDER))
        clean_hash_cache(Path(LOCAL_MEDIA_BASE_LOCAL_FOLDER))

    find_orphaned_files(skip_media_check=args.skip_media_check)

if __name__ == "__main__":
    main()
