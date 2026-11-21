"""Standalone read-only sidecar plan, independent of the scan and repair CLI."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .logic.sidecar_metainfo import PlanError
from .logic.sidecar_repair import plan_sidecars, read_manifest, torrent_id


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plan v1 cross-seed sidecar recovery without changing files or torrents.")
    parser.add_argument("--manifest", required=True, type=Path, help="JSON map of exact torrent IDs to sidecars and media_sources")
    parser.add_argument("--metainfo-dir", required=True, type=Path, help="Trusted directory containing <torrent-id>.torrent files")
    parser.add_argument("--torrent", help="Plan only this exact 40-digit v1 torrent ID from the manifest")
    args = parser.parse_args(argv)

    from deluge_client import DelugeRPCClient
    from deluge_client.client import DelugeClientException
    from dotenv import dotenv_values

    try:
        requests = read_manifest(args.manifest)
        if args.torrent is not None:
            tid = torrent_id(args.torrent)
            if tid not in requests:
                raise PlanError(f"exact torrent ID is not in the manifest: {tid}")
            requests = {tid: requests[tid]}
        # The scan settings create output/cache directories during validation.
        # Read only the existing connection and path mapping inputs for this command.
        dotenv = dotenv_values(".env")

        def setting(name: str) -> str:
            value = os.environ.get(name, dotenv.get(name))
            if value is None:
                raise PlanError(f"missing connection or path setting: {name}")
            return value

        local_base = Path(setting("LOCAL_TORRENT_BASE_LOCAL_FOLDER"))
        remote_base = setting("DELUGE_TORRENT_BASE_REMOTE_FOLDER")
        client = DelugeRPCClient(
            host=setting("DELUGE_HOST"),
            port=int(setting("DELUGE_PORT")),
            username=setting("DELUGE_USERNAME"),
            password=setting("DELUGE_PASSWORD"),
            decode_utf8=True,
            automatic_reconnect=False,
        )
        try:
            client.connect()
            result = plan_sidecars(client, local_base=local_base, remote_base=remote_base, metadata=args.metainfo_dir, requests=requests)
        finally:
            client.disconnect()
    except (OSError, ValueError, DelugeClientException) as exc:
        print(f"Cannot plan sidecar recovery: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return int(any(plan["status"] == "blocked" for plan in result["plans"]))


if __name__ == "__main__":
    raise SystemExit(main())
