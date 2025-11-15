#!/usr/bin/env python3
"""Analyze runtime trends from exported email reports."""

import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
import statistics


def parse_email_file(filepath: Path) -> Tuple[str, datetime, datetime, float]:
    """Parse an email file and extract scan information."""
    with open(filepath, "r") as f:
        content = f.read()

    # Extract date from filename
    filename_date = filepath.name[:8]  # YYYYMMDD

    # Extract scan start and end times
    start_match = re.search(r"Scan Start: (.+)", content)
    end_match = re.search(r"Scan End: (.+)", content)

    if not start_match or not end_match:
        return None

    start_time = datetime.fromisoformat(start_match.group(1))
    end_time = datetime.fromisoformat(end_match.group(1))

    # Calculate runtime in minutes
    runtime = (end_time - start_time).total_seconds() / 60

    return filename_date, start_time, end_time, runtime


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

    # Print detailed results
    print("=" * 80)
    print("DELUGE ORPHANED FILES - RUNTIME ANALYSIS")
    print("=" * 80)
    print(f"\n{'Date':<12} {'Start Time':<20} {'End Time':<20} {'Runtime (min)':<15}")
    print("-" * 80)

    for date, start, end, runtime in results:
        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        print(f"{formatted_date:<12} {start.strftime('%H:%M:%S'):<20} " f"{end.strftime('%H:%M:%S'):<20} {runtime:>14.2f}")

    # Calculate statistics
    runtimes = [r[3] for r in results]

    print("\n" + "=" * 80)
    print("STATISTICS")
    print("=" * 80)
    print(f"Total scans analyzed: {len(results)}")
    print(f"Average runtime: {statistics.mean(runtimes):.2f} minutes")
    print(f"Median runtime: {statistics.median(runtimes):.2f} minutes")
    print(f"Minimum runtime: {min(runtimes):.2f} minutes (on {results[runtimes.index(min(runtimes))][0]})")
    print(f"Maximum runtime: {max(runtimes):.2f} minutes (on {results[runtimes.index(max(runtimes))][0]})")
    print(f"Std deviation: {statistics.stdev(runtimes):.2f} minutes")

    # Analyze trend by week
    print("\n" + "=" * 80)
    print("WEEKLY TREND ANALYSIS")
    print("=" * 80)

    # Group by week
    from collections import defaultdict

    weekly_data = defaultdict(list)

    for date, start, end, runtime in results:
        # Parse date string to get week number
        dt = datetime.strptime(date, "%Y%m%d")
        week_key = f"{dt.year}-W{dt.isocalendar()[1]:02d}"
        weekly_data[week_key].append(runtime)

    print(f"\n{'Week':<12} {'Avg Runtime (min)':<20} {'Min':<10} {'Max':<10} {'Count':<8}")
    print("-" * 80)

    for week in sorted(weekly_data.keys()):
        week_runtimes = weekly_data[week]
        avg = statistics.mean(week_runtimes)
        min_rt = min(week_runtimes)
        max_rt = max(week_runtimes)
        count = len(week_runtimes)
        print(f"{week:<12} {avg:>18.2f} {min_rt:>9.2f} {max_rt:>9.2f} {count:>7}")

    # Check for increasing trend
    print("\n" + "=" * 80)
    print("TREND DETECTION")
    print("=" * 80)

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

    # Find longest runs
    print("\n" + "=" * 80)
    print("TOP 10 LONGEST RUNS")
    print("=" * 80)

    sorted_by_runtime = sorted(results, key=lambda x: x[3], reverse=True)[:10]

    print(f"\n{'Date':<12} {'Runtime (min)':<15} {'Start Time':<12}")
    print("-" * 50)
    for date, start, end, runtime in sorted_by_runtime:
        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        print(f"{formatted_date:<12} {runtime:>14.2f} {start.strftime('%H:%M:%S'):<12}")


if __name__ == "__main__":
    main()
