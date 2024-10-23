#!/usr/bin/env python3
# pylint: disable=C0301

# Copyright 2024 Alibaba Cloud

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Main module for the OSS Point-in-Time Recovery application."""

import sys
import datetime
import argparse
import logging
from typing import Dict, Set, Any

import oss2

logger = logging.getLogger(__name__)

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments for OSS Point-in-Time Recovery Script."""
    parser = argparse.ArgumentParser(description='OSS Point-in-Time Recovery Script')
    parser.add_argument('--access-key-id', required=True, help='Your Alibaba Cloud Access Key ID')
    parser.add_argument('--access-key-secret', required=True, help='Your Alibaba Cloud Access Key Secret')
    parser.add_argument('--endpoint', required=True, help='Your OSS endpoint (e.g., oss-region.aliyuncs.com)')
    parser.add_argument('--bucket-name', required=True, help='Your OSS bucket name')
    parser.add_argument('--folder-prefix', required=True, help='The folder prefix to recover (e.g., "my-folder/")')
    parser.add_argument('--recovery-time', required=True, help='Recovery time in UTC (e.g., "2023-10-07T14:24:00Z")')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without making changes')
    parser.add_argument('--delete-newer-objects', action='store_true',
                        help='Delete objects if their earliest version is after the recovery timestamp')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    return parser.parse_args()

def parse_recovery_time(recovery_time_str: str) -> datetime.datetime:
    """Parse the recovery time from the argument."""
    try:
        recovery_time = datetime.datetime.strptime(recovery_time_str, '%Y-%m-%dT%H:%M:%SZ')
        return recovery_time.replace(tzinfo=datetime.timezone.utc)
    except ValueError:
        logger.error("Invalid recovery time format. Use UTC time in format: YYYY-MM-DDTHH:MM:SSZ")
        sys.exit(1)

def initialize_oss_client(access_key_id: str, access_key_secret: str, endpoint: str, bucket_name: str) -> oss2.Bucket:
    """Initialize the OSS client."""
    auth = oss2.Auth(access_key_id, access_key_secret)
    return oss2.Bucket(auth, endpoint, bucket_name)

def recover_objects(bucket: oss2.Bucket, prefix: str, recovery_time: datetime.datetime, dry_run: bool = False, delete_newer: bool = False) -> None:
    """Recover objects from OSS bucket with specified prefix up to given time."""
    logger.info("Starting recovery process for prefix '%s' up to time %sZ", prefix, recovery_time.isoformat())

    latest_versions, keys_without_older_versions = get_object_versions(bucket, prefix, recovery_time)
    restore_latest_versions(bucket, latest_versions, dry_run)

    if delete_newer:
        delete_newer_versions(bucket, keys_without_older_versions, dry_run)

    logger.info("Recovery process completed.")

def get_object_versions(bucket: oss2.Bucket, prefix: str, recovery_time: datetime.datetime) -> tuple[Dict[str, Dict[str, Any]], Set[str]]:
    """Get the latest acceptable versions of objects and keys without older versions."""
    logger.debug("Starting to get object versions for prefix: %s", prefix)
    latest_versions = {}
    keys_without_older_versions = set()
    next_key_marker = ''
    next_versionid_marker = ''

    while True:
        logger.debug("Listing object versions with key_marker: %s, versionid_marker: %s", next_key_marker, next_versionid_marker)
        result = bucket.list_object_versions(
            prefix=prefix,
            key_marker=next_key_marker,
            versionid_marker=next_versionid_marker,
            max_keys=999)

        for obj_version in result.versions:
            key = obj_version.key
            version_time = datetime.datetime.fromtimestamp(obj_version.last_modified, datetime.timezone.utc)
            logger.debug("Processing version for key: %s, version time: %s", key, version_time)

            if version_time <= recovery_time:
                if key not in latest_versions or version_time > latest_versions[key]['version_time']:
                    logger.debug("Updating latest version for key: %s", key)
                    latest_versions[key] = {
                        'versionid': obj_version.versionid,
                        'version_time': version_time,
                        'last_modified_str': obj_version.last_modified
                    }
                keys_without_older_versions.discard(key)
            elif key not in latest_versions:
                logger.debug("Adding key to keys_without_older_versions: %s", key)
                keys_without_older_versions.add(key)

        if not result.is_truncated:
            break
        next_key_marker = result.next_key_marker
        next_versionid_marker = result.next_versionid_marker

    logger.debug("Finished getting object versions. Found %d latest versions and %d keys without older versions",
                 len(latest_versions), len(keys_without_older_versions))
    return latest_versions, keys_without_older_versions

def restore_latest_versions(bucket: oss2.Bucket, latest_versions: Dict[str, Dict[str, Any]], dry_run: bool) -> None:
    """Restore the latest versions of objects in the specified bucket."""
    logger.debug("Starting to restore latest versions for %d objects", len(latest_versions))
    for key, version_info in latest_versions.items():
        versionid = version_info['versionid']
        version_time_str = version_info['version_time'].strftime('%Y-%m-%dT%H:%M:%SZ')
        action = f"Restoring '{key}' to version '{versionid}' modified at {version_time_str}"

        if dry_run:
            logger.info("[Dry Run] Would %s", action)
        else:
            try:
                logger.info(action)
                logger.debug("Copying object: bucket=%s, key=%s, versionId=%s", bucket.bucket_name, key, versionid)
                bucket.copy_object(
                    source_bucket_name=bucket.bucket_name,
                    source_key=key,
                    target_key=key,
                    params={'versionId': versionid}
                )
                logger.debug("Successfully restored object: %s", key)
            except Exception as e:
                logger.error("Error restoring '%s' version '%s': %s", key, versionid, e)
                logger.debug("Exception details:", exc_info=True)

def delete_newer_versions(bucket: oss2.Bucket, keys_without_older_versions: Set[str], dry_run: bool) -> None:
    """Delete newer versions of objects in the specified bucket."""
    logger.debug("Starting to delete %d newer versions", len(keys_without_older_versions))
    for key in keys_without_older_versions:
        action = f"Deleting '{key}'"
        if dry_run:
            logger.info("[Dry Run] Would %s", action)
        else:
            try:
                logger.info(action)
                logger.debug("Deleting object: bucket=%s, key=%s", bucket.bucket_name, key)
                bucket.delete_object(key)
                logger.debug("Successfully deleted object: %s", key)
            except Exception as e:
                logger.error("Error deleting '%s': %s", key, e)
                logger.debug("Exception details:", exc_info=True)

def main() -> None:
    """Main function to run the OSS Point-in-Time Recovery Script."""
    args = parse_arguments()

    # Set up logging based on debug flag
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    # Adjust the logger level for our script
    logger.setLevel(log_level)

    logger.debug("Arguments parsed: %s", vars(args))
    recovery_time = parse_recovery_time(args.recovery_time)
    logger.debug("Parsed recovery time: %s", recovery_time)

    bucket = initialize_oss_client(args.access_key_id, args.access_key_secret, args.endpoint, args.bucket_name)
    logger.debug("OSS client initialized for bucket: %s", args.bucket_name)

    recover_objects(
        bucket,
        args.folder_prefix,
        recovery_time,
        dry_run=args.dry_run,
        delete_newer=args.delete_newer_objects
    )

if __name__ == "__main__":
    main()
