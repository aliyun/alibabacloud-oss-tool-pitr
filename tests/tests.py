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

"""Unit tests for the application."""

from unittest.mock import Mock, patch
from datetime import datetime, timezone
import sys
import os

import unittest
from main import parse_recovery_time, get_object_versions, restore_latest_versions, delete_newer_versions

# Add the parent directory to the Python path to import the main script
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestOSSRecovery(unittest.TestCase):
    """Tests for OSS recovery functionality."""

    def test_parse_recovery_time(self):
        """Test the parsing of recovery time. expects a iso8601 formatted time string."""
        valid_time = "2023-10-07T14:24:00Z"
        expected = datetime(2023, 10, 7, 14, 24, 0, tzinfo=timezone.utc)
        self.assertEqual(parse_recovery_time(valid_time), expected)

        with self.assertRaises(SystemExit):
            parse_recovery_time("invalid_time_format")

    @patch('oss2.Bucket')
    def test_get_object_versions(self, mock_bucket):
        """Test the retrieval of object versions and assigning them to latest_versions and keys_without_older_versions."""
        mock_bucket.list_object_versions.return_value = Mock(
            versions=[
                Mock(key='object1', versionid='v1', last_modified=datetime(2021, 10, 7, 14, 0, 0, tzinfo=timezone.utc).timestamp()),
                Mock(key='object1', versionid='v2', last_modified=datetime(2021, 10, 7, 15, 25, 0, tzinfo=timezone.utc).timestamp()),
                Mock(key='object1', versionid='v3', last_modified=datetime(2021, 10, 7, 16, 0, 0, tzinfo=timezone.utc).timestamp()),
                Mock(key='object2', versionid='v1', last_modified=datetime(2021, 10, 7, 15, 15, 0, tzinfo=timezone.utc).timestamp()),
                Mock(key='object2', versionid='v2', last_modified=datetime(2021, 10, 7, 16, 0, 0, tzinfo=timezone.utc).timestamp()),
                Mock(key='object3', versionid='v1', last_modified=datetime(2021, 10, 7, 16, 0, 0, tzinfo=timezone.utc).timestamp()),
            ],
            is_truncated=False
        )

        recovery_time = datetime(2021, 10, 7, 15, 30, 0, tzinfo=timezone.utc)
        latest_versions, keys_without_older_versions = get_object_versions(mock_bucket, 'prefix/', recovery_time)

        self.assertEqual(len(latest_versions), 2)
        self.assertEqual(latest_versions['object1']['versionid'], 'v2')
        self.assertEqual(latest_versions['object2']['versionid'], 'v1')
        self.assertEqual(keys_without_older_versions, {'object3'})

        recovery_time = datetime(2021, 10, 7, 14, 0, 0, tzinfo=timezone.utc)
        latest_versions, keys_without_older_versions = get_object_versions(mock_bucket, 'prefix/', recovery_time)

        self.assertEqual(len(latest_versions), 1)
        self.assertEqual(latest_versions['object1']['versionid'], 'v1')
        self.assertEqual(keys_without_older_versions, {'object2', 'object3'})

        # delete all versions
        recovery_time = datetime(2021, 10, 7, 13, 0, 0, tzinfo=timezone.utc)
        latest_versions, keys_without_older_versions = get_object_versions(mock_bucket, 'prefix/', recovery_time)

        self.assertEqual(len(latest_versions), 0)
        self.assertEqual(keys_without_older_versions, {'object1', 'object2', 'object3'})

        # restore all versions
        recovery_time = datetime(2021, 10, 7, 17, 0, 0, tzinfo=timezone.utc)
        latest_versions, keys_without_older_versions = get_object_versions(mock_bucket, 'prefix/', recovery_time)

        self.assertEqual(len(latest_versions), 3)
        self.assertEqual(latest_versions['object1']['versionid'], 'v3')
        self.assertEqual(latest_versions['object2']['versionid'], 'v2')
        self.assertEqual(latest_versions['object3']['versionid'], 'v1')
        self.assertEqual(len(keys_without_older_versions), 0)

    @patch('oss2.Bucket')
    def test_restore_latest_versions(self, mock_bucket):
        """Test the restoration of latest versions."""
        latest_versions = {
            'object1': {'versionid': 'v2', 'version_time': datetime(2021, 10, 7, 15, 0, 0, tzinfo=timezone.utc)}
        }

        restore_latest_versions(mock_bucket, latest_versions, dry_run=False)

        mock_bucket.copy_object.assert_called_once_with(
            source_bucket_name=mock_bucket.bucket_name,
            source_key='object1',
            target_key='object1',
            params={'versionId': 'v2'}
        )

    @patch('oss2.Bucket')
    def test_delete_newer_versions(self, mock_bucket):
        """Test the deletion of newer versions."""
        keys_without_older_versions = {'object2', 'object3'}

        delete_newer_versions(mock_bucket, keys_without_older_versions, dry_run=False)

        self.assertEqual(mock_bucket.delete_object.call_count, 2)
        mock_bucket.delete_object.assert_any_call('object2')
        mock_bucket.delete_object.assert_any_call('object3')

if __name__ == '__main__':
    unittest.main()
