"""Tests for the Iceberg metadata helpers in utils/migrations/shared.py.

These helpers (_list_iceberg_tables, _read_iceberg_metadata, _extract_schema,
etc.) are consumed by migration_dag_iceberg_catalog.py to read Iceberg table
metadata directly off S3 without a metastore.
"""

from unittest.mock import MagicMock

import pytest
from utils.migrations import shared as sh


class TestMapIcebergType:

    @pytest.mark.parametrize("iceberg, spark", [
        ('boolean', 'BOOLEAN'),
        ('int', 'INT'),
        ('long', 'BIGINT'),
        ('float', 'FLOAT'),
        ('double', 'DOUBLE'),
        ('date', 'DATE'),
        ('time', 'STRING'),
        ('timestamp', 'TIMESTAMP'),
        ('timestamptz', 'TIMESTAMP'),
        ('string', 'STRING'),
        ('binary', 'BINARY'),
        ('uuid', 'STRING'),
    ])
    def test_primitive_types(self, iceberg, spark):
        assert sh._map_iceberg_type(iceberg) == spark

    def test_decimal_preserves_precision_and_scale(self):
        assert sh._map_iceberg_type('decimal(10, 2)') == 'DECIMAL(10, 2)'

    def test_fixed_maps_to_binary(self):
        assert sh._map_iceberg_type('fixed[16]') == 'BINARY'

    def test_unknown_type_falls_back_to_string(self):
        assert sh._map_iceberg_type('some_future_type') == 'STRING'

    def test_struct_renders_nested_fields(self):
        struct = {
            'type': 'struct',
            'fields': [
                {'name': 'a', 'type': 'int'},
                {'name': 'b', 'type': 'string'},
            ],
        }
        assert sh._map_iceberg_type(struct) == 'STRUCT<a:INT,b:STRING>'

    def test_list_renders_element_type(self):
        assert sh._map_iceberg_type({'type': 'list', 'element': 'int'}) == 'ARRAY<INT>'

    def test_map_renders_key_and_value_types(self):
        mp = {'type': 'map', 'key': 'string', 'value': 'long'}
        assert sh._map_iceberg_type(mp) == 'MAP<STRING,BIGINT>'

    def test_list_of_structs_renders_recursively(self):
        nested = {
            'type': 'list',
            'element': {
                'type': 'struct',
                'fields': [{'name': 'x', 'type': 'double'}],
            },
        }
        assert sh._map_iceberg_type(nested) == 'ARRAY<STRUCT<x:DOUBLE>>'

    def test_unknown_complex_kind_falls_back_to_string(self):
        assert sh._map_iceberg_type({'type': 'variant'}) == 'STRING'


class TestMatchTokens:

    NAMES = ['orders', 'order_items', 'customers', 'sales_2024']

    def test_star_returns_all_names(self):
        assert sh._match_tokens(self.NAMES, ['*']) == self.NAMES

    def test_exact_token_matches_single_table(self):
        assert sh._match_tokens(self.NAMES, ['customers']) == ['customers']

    def test_wildcard_token_matches_prefix(self):
        assert sh._match_tokens(self.NAMES, ['order*']) == ['orders', 'order_items']

    def test_no_match_returns_empty(self):
        assert sh._match_tokens(self.NAMES, ['missing']) == []

    def test_overlapping_tokens_do_not_duplicate(self):
        result = sh._match_tokens(self.NAMES, ['order*', 'orders'])
        assert result == ['orders', 'order_items']


class TestParseTransform:

    def test_identity_has_no_param(self):
        assert sh._parse_transform('identity') == ('identity', None)

    def test_bucket_extracts_param(self):
        assert sh._parse_transform('bucket[16]') == ('bucket', 16)

    def test_truncate_extracts_param(self):
        assert sh._parse_transform('truncate[4]') == ('truncate', 4)


class TestExtractPartitionSpec:

    def test_no_specs_returns_unpartitioned(self):
        assert sh._extract_partition_spec({}) == ([], False)

    def test_empty_fields_returns_unpartitioned(self):
        metadata = {'partition-specs': [{'spec-id': 0, 'fields': []}]}
        assert sh._extract_partition_spec(metadata) == ([], False)

    def test_resolves_source_column_from_schema(self):
        metadata = {
            'default-spec-id': 0,
            'current-schema-id': 0,
            'schemas': [{'schema-id': 0, 'fields': [{'id': 1, 'name': 'event_ts'}]}],
            'partition-specs': [{
                'spec-id': 0,
                'fields': [{'source-id': 1, 'name': 'event_day', 'transform': 'day'}],
            }],
        }
        fields, is_partitioned = sh._extract_partition_spec(metadata)
        assert is_partitioned is True
        assert fields == [{
            'source_column': 'event_ts',
            'transform': 'day',
            'name': 'event_day',
            'param': None,
        }]

    def test_parses_bucket_transform_param(self):
        metadata = {
            'default-spec-id': 0,
            'current-schema-id': 0,
            'schemas': [{'schema-id': 0, 'fields': [{'id': 7, 'name': 'user_id'}]}],
            'partition-specs': [{
                'spec-id': 0,
                'fields': [{'source-id': 7, 'name': 'user_bucket', 'transform': 'bucket[16]'}],
            }],
        }
        fields, _ = sh._extract_partition_spec(metadata)
        assert fields[0]['transform'] == 'bucket'
        assert fields[0]['param'] == 16

    def test_unknown_source_id_falls_back_to_field_name(self):
        metadata = {
            'partition-specs': [{
                'spec-id': 0,
                'fields': [{'source-id': 99, 'name': 'p', 'transform': 'identity'}],
            }],
        }
        fields, _ = sh._extract_partition_spec(metadata)
        assert fields[0]['source_column'] == 'field_99'

    def test_falls_back_to_last_spec_when_default_missing(self):
        metadata = {
            'default-spec-id': 5,
            'schemas': [{'schema-id': 0, 'fields': [{'id': 1, 'name': 'c'}]}],
            'partition-specs': [{
                'spec-id': 0,
                'fields': [{'source-id': 1, 'name': 'c', 'transform': 'identity'}],
            }],
        }
        fields, is_partitioned = sh._extract_partition_spec(metadata)
        assert is_partitioned is True
        assert fields[0]['source_column'] == 'c'


class TestExtractRowCount:

    def test_no_current_snapshot_returns_zero(self):
        assert sh._extract_row_count({}) == 0

    def test_returns_total_records_for_current_snapshot(self):
        metadata = {
            'current-snapshot-id': 42,
            'snapshots': [
                {'snapshot-id': 1, 'summary': {'total-records': '10'}},
                {'snapshot-id': 42, 'summary': {'total-records': '999'}},
            ],
        }
        assert sh._extract_row_count(metadata) == 999

    def test_missing_snapshot_returns_zero(self):
        metadata = {'current-snapshot-id': 42, 'snapshots': []}
        assert sh._extract_row_count(metadata) == 0


class TestExtractSchema:

    def test_no_schemas_returns_empty(self):
        assert sh._extract_schema({}) == []

    def test_maps_each_field_type(self):
        metadata = {
            'current-schema-id': 0,
            'schemas': [{'schema-id': 0, 'fields': [
                {'name': 'id', 'type': 'long'},
                {'name': 'amount', 'type': 'double'},
            ]}],
        }
        assert sh._extract_schema(metadata) == [
            {'name': 'id', 'type': 'BIGINT'},
            {'name': 'amount', 'type': 'DOUBLE'},
        ]

    def test_falls_back_to_last_schema_when_current_missing(self):
        metadata = {
            'current-schema-id': 9,
            'schemas': [{'schema-id': 0, 'fields': [{'name': 'x', 'type': 'string'}]}],
        }
        assert sh._extract_schema(metadata) == [{'name': 'x', 'type': 'STRING'}]


class TestRebaseTablePath:

    def test_replaces_prefix(self):
        result = sh._rebase_table_path(
            's3a://src/data/db/t1', 's3a://src/data', 's3a://dst/data'
        )
        assert result == 's3a://dst/data/db/t1'

    def test_normalizes_trailing_slashes(self):
        result = sh._rebase_table_path(
            's3a://src/data/db/t1', 's3a://src/data/', 's3a://dst/data/'
        )
        assert result == 's3a://dst/data/db/t1'

    def test_raises_when_prefix_mismatch(self):
        with pytest.raises(ValueError, match='does not start with prefix'):
            sh._rebase_table_path('s3a://other/db/t1', 's3a://src/data', 's3a://dst/data')


# ---------------------------------------------------------------------------
# Filesystem-backed helpers (use a fake Hadoop FileSystem via spark._jvm)
# ---------------------------------------------------------------------------
def _make_fs_spark(fs):
    """Build a spark stub whose _jvm wires Path/URI/FileSystem to `fs`."""
    spark = MagicMock(name='SparkSession')
    jvm = spark._jvm
    jvm.java.net.URI.side_effect = lambda uri: uri
    jvm.org.apache.hadoop.fs.Path.side_effect = lambda p: p
    jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs
    return spark


def _file_status(name, is_dir):
    status = MagicMock()
    status.isDirectory.return_value = is_dir
    status.getPath.return_value.getName.return_value = name
    return status


class TestListIcebergTables:

    def test_returns_sorted_dirs_with_metadata_folder(self):
        fs = MagicMock()
        # base exists; t_b and t_a are iceberg tables, "_stage" is a plain dir
        fs.exists.side_effect = lambda p: not str(p).endswith('/_stage/metadata')
        fs.listStatus.return_value = [
            _file_status('t_b', True),
            _file_status('t_a', True),
            _file_status('_stage', True),
            _file_status('readme.txt', False),
        ]
        spark = _make_fs_spark(fs)
        assert sh._list_iceberg_tables(spark, 's3a://bucket/db') == ['t_a', 't_b']

    def test_missing_base_path_returns_empty(self):
        fs = MagicMock()
        fs.exists.return_value = False
        spark = _make_fs_spark(fs)
        assert sh._list_iceberg_tables(spark, 's3a://bucket/db') == []

    def test_swallows_errors_and_returns_empty(self):
        fs = MagicMock()
        fs.exists.return_value = True
        fs.listStatus.side_effect = RuntimeError('boom')
        spark = _make_fs_spark(fs)
        assert sh._list_iceberg_tables(spark, 's3a://bucket/db') == []


class TestResolveMetadataFile:

    def test_uses_version_hint_when_present(self):
        fs = MagicMock()
        fs.exists.return_value = True
        spark = _make_fs_spark(fs)
        reader = spark._jvm.java.io.BufferedReader.return_value
        reader.readLine.return_value = '7'
        path = sh._resolve_metadata_file(spark, 's3a://bucket/db/t1')
        assert path == 's3a://bucket/db/t1/metadata/v7.metadata.json'

    def test_picks_highest_version_when_no_hint(self):
        fs = MagicMock()
        fs.exists.return_value = False
        fs.listStatus.return_value = [
            _file_status('v1.metadata.json', False),
            _file_status('v10.metadata.json', False),
            _file_status('v2.metadata.json', False),
            _file_status('snap-data.avro', False),
        ]
        spark = _make_fs_spark(fs)
        path = sh._resolve_metadata_file(spark, 's3a://bucket/db/t1')
        assert path == 's3a://bucket/db/t1/metadata/v10.metadata.json'

    def test_raises_when_no_metadata_files(self):
        fs = MagicMock()
        fs.exists.return_value = False
        fs.listStatus.return_value = [_file_status('snap-data.avro', False)]
        spark = _make_fs_spark(fs)
        with pytest.raises(FileNotFoundError):
            sh._resolve_metadata_file(spark, 's3a://bucket/db/t1')


class TestReadIcebergMetadata:

    def test_parses_json_from_resolved_metadata_file(self):
        fs = MagicMock()
        fs.exists.return_value = True
        spark = _make_fs_spark(fs)
        reader = spark._jvm.java.io.BufferedReader.return_value
        # First read resolves version-hint ('3'); the read loop returns the JSON
        # body line-by-line, then None to terminate.
        reader.readLine.side_effect = ['3', '{"format-version":', '2}', None]
        metadata = sh._read_iceberg_metadata(spark, 's3a://bucket/db/t1')
        assert metadata == {'format-version': 2}
