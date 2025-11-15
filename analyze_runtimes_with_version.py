#!/usr/bin/env python3
"""Analyze runtime trends from exported email reports with version correlation."""

import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional
import statistics
from collections import defaultdict


def parse_email_file(filepath: Path) -> Optional[Tuple[str, datetime, datetime, float, Optional[str]]]:
    """Parse an email file and extract scan information."""
    with open(filepath, "r") as f:
        content = f.read()

    # Extract date from filename
    filename_date = filepath.name[:8]  # YYYYMMDD

    # Extract version if present
    version_match = re.search(r"Version:\s+(.+)", content)
    version = version_match.group(1).strip() if version_match else None

    # Extract scan start and end times
    start_match = re.search(r"Scan Start: (.+)", content)
    end_match = re.search(r"Scan End: (.+)", content)

    if not start_match or not end_match:
        return None

    start_time = datetime.fromisoformat(start_match.group(1))
    end_time = datetime.fromisoformat(end_match.group(1))

    # Calculate runtime in minutes
    runtime = (end_time - start_time).total_seconds() / 60

    return filename_date, start_time, end_time, runtime, version


def main():
    emails_dir = Path("/home/five/Code/deluge-orphaned-files/emails")

    # Parse all email files
    results = []
    for email_file in sorted(emails_dir.glob("*.txt")):
        try:
            result = parse_email_file(email_file)
            if result:
                results.append(result)
        except Exception as e:
            print(f"Error parsing {email_file.name}: {e}")

    if not results:
        print("No valid email files found!")
        return

    # Sort by date
    results.sort(key=lambda x: x[0])

    # Print detailed results with version
    print("=" * 95)
    print("DELUGE ORPHANED FILES - RUNTIME ANALYSIS WITH VERSION CORRELATION")
    print("=" * 95)
    print(f"\n{'Date':<12} {'Version':<10} {'Start Time':<20} {'End Time':<20} {'Runtime (min)':<15}")
    print("-" * 95)

    for date, start, end, runtime, version in results:
        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        version_str = version if version else "N/A"
        print(f"{formatted_date:<12} {version_str:<10} {start.strftime('%H:%M:%S'):<20} " f"{end.strftime('%H:%M:%S'):<20} {runtime:>14.2f}")

    # Group by version
    print("\n" + "=" * 95)
    print("STATISTICS BY VERSION")
    print("=" * 95)

    version_data = defaultdict(list)
    for date, start, end, runtime, version in results:
        version_key = version if version else "Unknown (pre-version)"
        version_data[version_key].append((date, runtime))

    print(f"\n{'Version':<20} {'Count':<8} {'Avg Runtime':<15} {'Min':<12} {'Max':<12} {'Date Range'}")
    print("-" * 95)

    for version in sorted(version_data.keys(), key=lambda v: (v == "Unknown (pre-version)", v)):
        runtimes = [rt for _, rt in version_data[version]]
        dates = [dt for dt, _ in version_data[version]]

        avg_rt = statistics.mean(runtimes)
        min_rt = min(runtimes)
        max_rt = max(runtimes)
        count = len(runtimes)

        # Format date range
        first_date = f"{dates[0][:4]}-{dates[0][4:6]}-{dates[0][6:8]}"
        last_date = f"{dates[-1][:4]}-{dates[-1][4:6]}-{dates[-1][6:8]}"
        date_range = f"{first_date} to {last_date}" if first_date != last_date else first_date

        print(f"{version:<20} {count:<8} {avg_rt:>14.2f} {min_rt:>11.2f} {max_rt:>11.2f} {date_range}")

    # Calculate statistics
    runtimes = [r[3] for r in results]

    print("\n" + "=" * 95)
    print("OVERALL STATISTICS")
    print("=" * 95)
    print(f"Total scans analyzed: {len(results)}")
    print(f"Average runtime: {statistics.mean(runtimes):.2f} minutes")
    print(f"Median runtime: {statistics.median(runtimes):.2f} minutes")
    print(f"Minimum runtime: {min(runtimes):.2f} minutes")
    print(f"Maximum runtime: {max(runtimes):.2f} minutes")
    print(f"Std deviation: {statistics.stdev(runtimes):.2f} minutes")

    # Check for increasing trend
    print("\n" + "=" * 95)
    print("TREND DETECTION")
    print("=" * 95)

    # Compare first half vs second half
    mid_point = len(runtimes) // 2
    first_half_avg = statistics.mean(runtimes[:mid_point])
    second_half_avg = statistics.mean(runtimes[mid_point:])

    print(f"\nFirst half average (earliest {mid_point} scans): {first_half_avg:.2f} minutes")
    print(f"Second half average (latest {len(runtimes) - mid_point} scans): {second_half_avg:.2f} minutes")

    if second_half_avg > first_half_avg:
        increase_pct = ((second_half_avg - first_half_avg) / first_half_avg) * 100
        print(f"\n⚠️  WARNING: Runtime has INCREASED by {increase_pct:.1f}%")
    else:
        decrease_pct = ((first_half_avg - second_half_avg) / first_half_avg) * 100
        print(f"\n✓ Runtime has decreased by {decrease_pct:.1f}%")

    # Find when version first appears
    print("\n" + "=" * 95)
    print("VERSION ADOPTION TIMELINE")
    print("=" * 95)

    first_with_version = None
    for date, start, end, runtime, version in results:
        if version:
            first_with_version = date
            break

    if first_with_version:
        formatted_date = f"{first_with_version[:4]}-{first_with_version[4:6]}-{first_with_version[6:8]}"
        print(f"\nFirst email with version info: {formatted_date}")

        # Compare pre-version vs post-version runtimes
        pre_version_runtimes = [rt for _, _, _, rt, v in results if not v]
        post_version_runtimes = [rt for _, _, _, rt, v in results if v]

        if pre_version_runtimes and post_version_runtimes:
            pre_avg = statistics.mean(pre_version_runtimes)
            post_avg = statistics.mean(post_version_runtimes)
            print(f"\nAverage runtime BEFORE version tracking: {pre_avg:.2f} minutes ({len(pre_version_runtimes)} scans)")
            print(f"Average runtime AFTER version tracking: {post_avg:.2f} minutes ({len(post_version_runtimes)} scans)")

            if post_avg > pre_avg:
                increase_pct = ((post_avg - pre_avg) / pre_avg) * 100
                print(f"⚠️  Runtime INCREASED by {increase_pct:.1f}% after version tracking was added")


if __name__ == "__main__":
    main()
