"""
Tests for shared utility functions:
  - get_config()
  - track_duration decorator
  - execute_with_iceberg_retry()
"""

import time
from unittest.mock import MagicMock, patch

import migrator_utils.migrations.shared as m
import pytest


class TestGetConfig:

    def test_returns_expected_values_from_variables(self):
        cfg = m.get_config()
        assert cfg['ssh_conn_id'] == 'cluster_edge_ssh'
        assert cfg['tracking_database'] == 'migration_tracking'
        assert cfg['default_s3_bucket'] == 's3a://test-bucket'
        assert cfg['distcp_mappers'] == '10'
        assert cfg['distcp_preserve_delete'] is True
        for key in ['s3_endpoint', 's3_access_key', 's3_secret_key',
                     'spark_conn_id', 'tracking_location', 'report_output_location',
                     'auth_method', 'smtp_conn_id', 'email_recipients']:
            assert key in cfg, f"Missing key: {key}"

    def test_falls_back_to_default_when_variable_missing(self):
        with patch('airflow.models.Variable.get', return_value='fallback'):
            cfg = m.get_config()
        assert cfg['ssh_conn_id'] == 'fallback'


class TestVarPrecedence:
    """Resolution order in get_config._var.

    Portal-triggered run: run-scoped → nx1_ → plain → env → default.
    Hand-launched run:     run-scoped → plain → env → default (nx1_ is skipped).
    """

    @staticmethod
    def _variables(mapping: dict):
        """Patch Variable.get so only *mapping* keys exist."""
        return patch(
            'airflow.models.Variable.get',
            side_effect=lambda key, default_var=None, **kw: mapping.get(key, default_var),
        )

    @staticmethod
    def _context(run_id='data_migration_1', triggered_by=None):
        """Patch the task context, optionally stamped as a portal-triggered run."""
        conf = {'triggered_by': triggered_by} if triggered_by else {}
        dag_run = type('DagRun', (), {'conf': conf})()
        return patch(
            'airflow.operators.python.get_current_context',
            return_value={'run_id': run_id, 'dag_run': dag_run},
        )

    def _portal(self, mapping, run_id='data_migration_1'):
        return self._context(run_id, m.PORTAL_TRIGGER), self._variables(mapping)

    # -- portal-triggered runs may read the nx1_ namespace -------------------

    def test_nx1_variable_wins_over_plain_variable(self):
        ctx, vars_ = self._portal({
            'nx1_s3_access_key': 'FROM_PORTAL',
            's3_access_key': 'FROM_AIRFLOW_UI',
        })
        with ctx, vars_:
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'FROM_PORTAL'

    def test_nx1_variable_wins_over_env_file(self):
        ctx, vars_ = self._portal({'nx1_s3_access_key': 'FROM_PORTAL'})
        with ctx, vars_, patch.dict('os.environ', {'S3_ACCESS_KEY': 'FROM_ENV'}):
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'FROM_PORTAL'

    def test_empty_nx1_variable_falls_to_default_not_to_plain(self):
        """Cleared in the portal means unset for portal runs, not "use the manual value"."""
        ctx, vars_ = self._portal({
            'nx1_s3_access_key': '',
            's3_access_key': 'FROM_AIRFLOW_UI',
        })
        with ctx, vars_:
            cfg = m.get_config()
        assert cfg['s3_access_key'] == ''

    def test_run_scoped_variable_wins_over_nx1_variable(self):
        ctx, vars_ = self._portal({
            'nx1_s3_access_key__data_migration_1': 'PER_RUN',
            'nx1_s3_access_key': 'FROM_PORTAL',
        })
        with ctx, vars_:
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'PER_RUN'

    def test_absent_run_scoped_variable_falls_through_to_nx1(self):
        """A blank S3 override is not written at all, so the global applies."""
        ctx, vars_ = self._portal({'nx1_s3_access_key': 'FROM_PORTAL'})
        with ctx, vars_:
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'FROM_PORTAL'

    def test_empty_run_scoped_variable_is_authoritative(self):
        """Tier 1 resolves on presence: an operator who cleared a field meant it.

        The portal writes a run-scoped Variable for every field the user filled
        in, and skips only the S3 credentials when blank (where blank means
        "inherit"). So a run-scoped '' elsewhere is deliberate — here, send no
        report email — and must not fall through to the tenant mailing list.
        """
        ctx, vars_ = self._portal({
            'nx1_migration_email_recipients__data_migration_1': '',
            'nx1_migration_email_recipients': 'tenant@example.com',
        })
        with ctx, vars_:
            cfg = m.get_config()
        assert cfg['email_recipients'] == ''

    # -- hand-launched runs must not see the portal's namespace --------------

    def test_manual_run_ignores_nx1_variable_and_uses_env_file(self):
        """A DAG deployed by hand keeps its env file even when the portal has globals."""
        with self._context(triggered_by=None), \
                self._variables({'nx1_s3_access_key': 'FROM_PORTAL'}), \
                patch.dict('os.environ', {'S3_ACCESS_KEY': 'FROM_ENV'}):
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'FROM_ENV'

    def test_manual_run_ignores_nx1_variable_and_uses_plain_variable(self):
        with self._context(triggered_by=None), self._variables({
            'nx1_s3_access_key': 'FROM_PORTAL',
            's3_access_key': 'FROM_AIRFLOW_UI',
        }):
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'FROM_AIRFLOW_UI'

    def test_unrecognised_trigger_marker_is_treated_as_manual(self):
        with self._context(triggered_by='cron'), \
                self._variables({'nx1_s3_access_key': 'FROM_PORTAL'}), \
                patch.dict('os.environ', {'S3_ACCESS_KEY': 'FROM_ENV'}):
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'FROM_ENV'

    # -- lower tiers, unaffected by the marker ------------------------------

    def test_plain_variable_used_when_no_nx1_variable(self):
        with self._variables({'s3_access_key': 'FROM_AIRFLOW_UI'}):
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'FROM_AIRFLOW_UI'

    def test_env_file_used_when_no_variable_set(self):
        with self._variables({}), patch.dict('os.environ', {'S3_ACCESS_KEY': 'FROM_ENV'}):
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'FROM_ENV'

    def test_empty_plain_variable_falls_through_to_env_file(self):
        """A Variable cleared in the Airflow UI must not mask the env file."""
        with self._variables({'s3_access_key': ''}), \
                patch.dict('os.environ', {'S3_ACCESS_KEY': 'FROM_ENV'}):
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'FROM_ENV'

    def test_empty_env_var_falls_through_to_default(self):
        with self._variables({}), \
                patch.dict('os.environ', {'MIGRATION_DEFAULT_S3_BUCKET': ''}):
            cfg = m.get_config()
        assert cfg['default_s3_bucket'] == 's3a://data-lake'

    # -- portal runs are sealed off from the manual namespace ----------------

    def test_portal_run_ignores_plain_variable_for_an_owned_key(self):
        """The page is the whole truth: no plain value can be in effect behind it."""
        ctx, vars_ = self._portal({'s3_access_key': 'FROM_AIRFLOW_UI'})
        with ctx, vars_:
            cfg = m.get_config()
        assert cfg['s3_access_key'] == ''

    def test_portal_run_ignores_env_file_for_an_owned_key(self):
        ctx, vars_ = self._portal({})
        with ctx, vars_, patch.dict('os.environ', {'S3_ACCESS_KEY': 'FROM_ENV'}):
            cfg = m.get_config()
        assert cfg['s3_access_key'] == ''

    def test_portal_run_falls_to_hardcoded_default_for_an_owned_key(self):
        """Nothing in the nx1_ namespace means unset for a portal run — even
        with a plain Variable AND an env value both sitting there ready to
        catch it, an owned key must not fall through to either.
        """
        ctx, vars_ = self._portal({'cluster_ssh_conn_id': 'FROM_AIRFLOW_UI'})
        with ctx, vars_, patch.dict('os.environ', {'CLUSTER_SSH_CONN_ID': 'FROM_ENV'}):
            cfg = m.get_config()
        assert cfg['ssh_conn_id'] == 'cluster_edge_ssh'

    def test_portal_run_reads_env_file_for_a_key_nobody_owns(self):
        """Unowned keys are deployment baseline, shared by both origins — a
        portal run must ignore an nx1_ value sitting right there for one.
        """
        ctx, vars_ = self._portal({'nx1_cluster_type': 'FROM_PORTAL'})
        with ctx, vars_, patch.dict('os.environ', {'CLUSTER_TYPE': 'HDP'}):
            cfg = m.get_config()
        assert cfg['cluster_type'] == 'HDP'

    def test_portal_run_reads_plain_variable_for_a_key_nobody_owns(self):
        ctx, vars_ = self._portal({
            'nx1_hdfs_nameservice': 'FROM_PORTAL',
            'hdfs_nameservice': 'ns1',
        })
        with ctx, vars_:
            cfg = m.get_config()
        assert cfg['hdfs_nameservice'] == 'ns1'

    def test_manual_run_ignores_the_prefixed_run_scoped_variable(self):
        with self._context(triggered_by=None), self._variables({
            'nx1_s3_access_key__data_migration_1': 'PER_RUN',
            's3_access_key': 'FROM_AIRFLOW_UI',
        }):
            cfg = m.get_config()
        assert cfg['s3_access_key'] == 'FROM_AIRFLOW_UI'

    # The portal writes these; no DAG in either repo reads them. auth_method
    # 'kinit' only checks for an existing TGT. They stay in PORTAL_OWNED_KEYS so
    # that isolation is already correct if a DAG ever starts reading one.
    # mapr_user lost its reader on main ("fix: remove mapr_user variable") —
    # migration_dag_mapr_to_s3 now reads service_account_user_id instead. The
    # portal still has a MapR User field writing nx1_mapr_user, so the key stays
    # owned; nothing consumes it until that field is repointed.
    _WRITTEN_BUT_NEVER_READ = frozenset({
        'kinit_keytab', 'kinit_password', 'kinit_principal', 'mapr_user',
    })

    def test_every_owned_key_is_read_by_get_config(self):
        """A key in the frozenset that no _var call reads is dead weight.

        One key at a time: service_account_user_id and mapr_user resolve through
        one `or` chain, so seeding both at once hides whichever loses.
        """
        for key in sorted(m.PORTAL_OWNED_KEYS - self._WRITTEN_BUT_NEVER_READ):
            ctx, vars_ = self._portal({f'nx1_{key}': f'value_of_{key}'})
            with ctx, vars_:
                cfg = m.get_config()
            assert f'value_of_{key}' in {str(v) for v in cfg.values()}, \
                f'in PORTAL_OWNED_KEYS but never read by get_config: {key}'

    def test_the_unread_keys_are_still_unread(self):
        """Fails when a DAG starts reading one, as a prompt to drop the exemption."""
        for key in sorted(self._WRITTEN_BUT_NEVER_READ):
            ctx, vars_ = self._portal({f'nx1_{key}': f'value_of_{key}'})
            with ctx, vars_:
                cfg = m.get_config()
            assert f'value_of_{key}' not in {str(v) for v in cfg.values()}, (
                f'{key} is now read — remove it from _WRITTEN_BUT_NEVER_READ'
            )

    def test_s3_listing_tool_empty_variable_falls_through_to_env(self):
        """When Variable is set to '', _var treats it as unset and consults env."""
        ctx, vars_ = self._portal({'s3_listing_tool': ''})
        with ctx, vars_, patch.dict('os.environ', {'S3_LISTING_TOOL': 'aws'}):
            cfg = m.get_config()
        assert cfg['s3_listing_tool'] == 'aws'

    def test_folder_copy_allow_delete_empty_variable_falls_through_to_env(self):
        """When Variable is set to '', _var treats it as unset and consults env.

        This is the destructive-delete flag: old code treated empty as authoritative,
        new code treats empty as unset. This is consistent with _var semantics
        (empty means the operator cleared it, don't use manual-run values).
        """
        ctx, vars_ = self._portal({'folder_copy_allow_delete': ''})
        with ctx, vars_, patch.dict('os.environ', {'FOLDER_COPY_ALLOW_DELETE': 'true'}):
            cfg = m.get_config()
        assert cfg['folder_copy_allow_delete'] == 'true'

    def test_folder_copy_allow_delete_defaults_to_false(self):
        """When neither Variable nor env are set, default is 'false'."""
        with self._variables({}):
            cfg = m.get_config()
        assert cfg['folder_copy_allow_delete'] == 'false'


class TestTrackDuration:

    def test_adds_duration_and_preserves_result(self):
        @m.track_duration
        def sample_task(x, y=10):
            time.sleep(0.02)
            return {'status': 'COMPLETED', 'sum': x + y}

        result = sample_task(5, y=3)
        assert result['status'] == 'COMPLETED'
        assert result['sum'] == 8
        assert isinstance(result['_task_duration'], float)
        assert result['_task_duration'] >= 0.01


class TestExecuteWithIcebergRetry:

    def test_succeeds_immediately_or_after_retries(self, mock_spark):
        # Immediate success
        m.execute_with_iceberg_retry(mock_spark, "SELECT 1")
        mock_spark.sql.assert_called_once_with("SELECT 1")

        # Success after retries — requires an actual commit-conflict marker
        mock_spark.sql.reset_mock()
        mock_spark.sql.side_effect = [
            Exception("CommitFailedException"),
            Exception("CommitFailedException"),
            None,
        ]
        with patch('time.sleep'):
            m.execute_with_iceberg_retry(mock_spark, "MERGE INTO t USING s", max_retries=3)
        assert mock_spark.sql.call_count == 3

    def test_raises_immediately_on_non_retryable_error(self, mock_spark):
        """Errors without a commit-conflict marker raise immediately without retrying."""
        mock_spark.sql.side_effect = Exception("persistent error")
        with pytest.raises(Exception, match="persistent error"):
            m.execute_with_iceberg_retry(mock_spark, "BAD SQL")
        assert mock_spark.sql.call_count == 1

    def test_raises_after_exhausting_default_six_retries(self, mock_spark):
        """CommitFailedException is retried up to max_retries (default 6) times."""
        mock_spark.sql.side_effect = Exception("CommitFailedException")
        with patch('time.sleep'), pytest.raises(Exception, match="CommitFailedException"):
            m.execute_with_iceberg_retry(mock_spark, "BAD SQL")
        assert mock_spark.sql.call_count == 6


# ---------------------------------------------------------------------------
# build_s3_opts
# ---------------------------------------------------------------------------
class TestBuildS3Opts:
    """Unit tests for the build_s3_opts credential-builder helper."""

    def _cfg(self, endpoint='', access_key='GLOBALAK', secret_key='GLOBALSK'):
        return {'s3_endpoint': endpoint, 's3_access_key': access_key, 's3_secret_key': secret_key}

    # ------------------------------------------------------------------
    # Case 2: no dest_endpoint — original unscoped global config behaviour
    # ------------------------------------------------------------------

    def test_global_creds_emitted_unscoped(self):
        opts = m.build_s3_opts('s3a://data-lake', self._cfg())
        assert 'fs.s3a.access.key=GLOBALAK' in opts
        assert 'fs.s3a.secret.key=GLOBALSK' in opts
        assert 'fs.s3a.bucket.' not in opts

    def test_global_endpoint_emitted_unscoped(self):
        opts = m.build_s3_opts('s3a://data-lake', self._cfg(endpoint='https://s3.default.example.com'))
        assert 'fs.s3a.endpoint=https://s3.default.example.com' in opts
        assert 'fs.s3a.bucket.' not in opts

    def test_case2_same_output_regardless_of_s3_url_prefix(self):
        """Case 2 emits unscoped props — bucket name in URL does not matter."""
        for url in ('s3://bucket-x', 's3n://bucket-x', 's3a://bucket-x', ''):
            opts = m.build_s3_opts(url, self._cfg())
            assert 'fs.s3a.access.key=GLOBALAK' in opts
            assert 'fs.s3a.secret.key=GLOBALSK' in opts
            assert 'fs.s3a.bucket.' not in opts

    def test_empty_global_creds_produce_empty_string(self):
        opts = m.build_s3_opts('s3a://data-lake', self._cfg(access_key='', secret_key=''))
        assert opts == ''

    def test_no_endpoint_no_creds_produces_empty_string(self):
        opts = m.build_s3_opts('', self._cfg(access_key='', secret_key=''))
        assert opts == ''

    # ------------------------------------------------------------------
    # Case 1: dest_endpoint provided — endpoint-hostname credential lookup
    # ------------------------------------------------------------------

    def test_endpoint_used_directly(self):
        ep = 'https://s3.tenant-a.example.com'
        with patch('airflow.models.Variable.get', return_value=''):
            opts = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep)
        assert f'fs.s3a.bucket.data-lake.endpoint={ep}' in opts

    def test_endpoint_creds_looked_up_by_hostname(self):
        ep = 'https://s3.tenant-a.example.com'
        def fake_var(key, default_var=''):
            return {'s3.tenant-a.example.com_access_key': 'TENANTAAK',
                    's3.tenant-a.example.com_secret_key': 'TENANTASK'}.get(key, default_var)
        with patch('airflow.models.Variable.get', side_effect=fake_var):
            opts = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep)
        assert 'fs.s3a.bucket.data-lake.access.key=TENANTAAK' in opts
        assert 'fs.s3a.bucket.data-lake.secret.key=TENANTASK' in opts

    def test_endpoint_creds_fall_back_to_global_when_variable_absent(self):
        ep = 'https://s3.tenant-a.example.com'
        with patch('airflow.models.Variable.get', return_value=''):
            opts = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep)
        assert 'fs.s3a.bucket.data-lake.access.key=GLOBALAK' in opts
        assert 'fs.s3a.bucket.data-lake.secret.key=GLOBALSK' in opts

    def test_two_different_endpoints_same_bucket_produce_different_opts(self):
        ep_a = 'https://s3.tenant-a.example.com'
        ep_b = 'https://s3.tenant-b.example.com'
        def fake_var(key, default_var=''):
            mapping = {
                's3.tenant-a.example.com_access_key': 'AK_A',
                's3.tenant-b.example.com_access_key': 'AK_B',
            }
            return mapping.get(key, default_var)
        with patch('airflow.models.Variable.get', side_effect=fake_var):
            opts_a = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep_a)
            opts_b = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep_b)
        assert f'endpoint={ep_a}' in opts_a
        assert f'endpoint={ep_b}' in opts_b
        assert 'AK_A' in opts_a
        assert 'AK_B' in opts_b
        assert opts_a != opts_b

    def test_global_creds_not_used_when_endpoint_hostname_variable_present(self):
        """Endpoint-scoped Variable must win over global config key."""
        ep = 'https://s3.tenant-x.example.com'
        def fake_var(key, default_var=''):
            if key == 's3.tenant-x.example.com_access_key':
                return 'TENANT_X_AK'
            if key == 's3.tenant-x.example.com_secret_key':
                return 'TENANT_X_SK'
            return default_var
        with patch('airflow.models.Variable.get', side_effect=fake_var):
            opts = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep)
        assert 'TENANT_X_AK' in opts
        assert 'GLOBALAK' not in opts


# ---------------------------------------------------------------------------
# _endpoint_credentials / config['portal_run']
# ---------------------------------------------------------------------------
class TestEndpointCredentials:
    """Credentials for an Excel row that names its own S3 endpoint."""

    _HOST = 's3.tenant-a.example.com'

    @staticmethod
    def _variables(mapping: dict):
        return patch(
            'airflow.models.Variable.get',
            side_effect=lambda key, default_var=None, **kw: mapping.get(key, default_var),
        )

    def test_manual_run_uses_the_per_host_variable(self):
        cfg = {'portal_run': False, 's3_access_key': 'GLOBAL', 's3_secret_key': 'GLOBALSEC'}
        with self._variables({f'{self._HOST}_access_key': 'PER_HOST'}):
            access, _ = m._endpoint_credentials(self._HOST, cfg)
        assert access == 'PER_HOST'

    def test_manual_run_falls_back_to_the_global(self):
        cfg = {'portal_run': False, 's3_access_key': 'GLOBAL', 's3_secret_key': 'GLOBALSEC'}
        with self._variables({}):
            access, secret = m._endpoint_credentials(self._HOST, cfg)
        assert (access, secret) == ('GLOBAL', 'GLOBALSEC')

    def test_portal_run_ignores_the_plain_per_host_variable(self):
        """A wizard override must not be silently outranked by a hand-set value."""
        cfg = {'portal_run': True, 's3_access_key': 'FROM_WIZARD', 's3_secret_key': 'WIZSEC'}
        with self._variables({f'{self._HOST}_access_key': 'PER_HOST'}):
            access, _ = m._endpoint_credentials(self._HOST, cfg)
        assert access == 'FROM_WIZARD'

    def test_portal_run_uses_the_prefixed_per_host_variable(self):
        cfg = {'portal_run': True, 's3_access_key': 'FROM_WIZARD', 's3_secret_key': 'WIZSEC'}
        with self._variables({f'nx1_{self._HOST}_access_key': 'PER_HOST_PORTAL'}):
            access, _ = m._endpoint_credentials(self._HOST, cfg)
        assert access == 'PER_HOST_PORTAL'

    def test_manual_run_reads_the_per_host_env_var(self):
        cfg = {'portal_run': False, 's3_access_key': '', 's3_secret_key': ''}
        with self._variables({}), patch.dict(
            'os.environ', {'S3_TENANT_A_EXAMPLE_COM_ACCESS_KEY': 'FROM_ENV'}
        ):
            access, _ = m._endpoint_credentials(self._HOST, cfg)
        assert access == 'FROM_ENV'

    def test_portal_run_ignores_the_per_host_env_var(self):
        cfg = {'portal_run': True, 's3_access_key': 'FROM_WIZARD', 's3_secret_key': 'WIZSEC'}
        with self._variables({}), patch.dict(
            'os.environ', {'S3_TENANT_A_EXAMPLE_COM_ACCESS_KEY': 'FROM_ENV'}
        ):
            access, _ = m._endpoint_credentials(self._HOST, cfg)
        assert access == 'FROM_WIZARD'

    def test_build_s3_opts_uses_the_helper(self):
        cfg = {'portal_run': True, 's3_access_key': 'FROM_WIZARD', 's3_secret_key': 'WIZSEC'}
        with self._variables({f'{self._HOST}_access_key': 'PER_HOST'}):
            opts = m.build_s3_opts('s3a://bucket-a', cfg, f'https://{self._HOST}')
        assert 'PER_HOST' not in opts
        assert 'FROM_WIZARD' in opts


class TestPortalRunFlagInConfig:

    def test_config_reports_a_portal_run(self):
        conf = {'triggered_by': m.PORTAL_TRIGGER}
        dag_run = type('DagRun', (), {'conf': conf})()
        with patch('airflow.operators.python.get_current_context',
                   return_value={'run_id': 'data_migration_1', 'dag_run': dag_run}):
            cfg = m.get_config()
        assert cfg['portal_run'] is True

    def test_config_reports_a_manual_run(self):
        dag_run = type('DagRun', (), {'conf': {}})()
        with patch('airflow.operators.python.get_current_context',
                   return_value={'run_id': 'manual__2026', 'dag_run': dag_run}):
            cfg = m.get_config()
        assert cfg['portal_run'] is False


# ---------------------------------------------------------------------------
# configure_spark_s3
# ---------------------------------------------------------------------------
class TestConfigureSparkS3:

    def test_sets_source_and_dest_credentials(self, mock_spark):
        config = {
            's3_source_endpoint': 'https://src.example.com',
            's3_source_access_key': 'SRC_AK',
            's3_source_secret_key': 'SRC_SK',
            's3_dest_endpoint': 'https://dst.example.com',
            's3_dest_access_key': 'DST_AK',
            's3_dest_secret_key': 'DST_SK',
        }
        m.configure_spark_s3(mock_spark, config)
        mock_spark.conf.set.assert_any_call('fs.s3a.endpoint', 'https://src.example.com')
        mock_spark.conf.set.assert_any_call('fs.s3a.access.key', 'SRC_AK')
        mock_spark.conf.set.assert_any_call('fs.s3a.secret.key', 'SRC_SK')
        assert config['_dest_endpoint'] == 'https://dst.example.com'
        assert config['_dest_access_key'] == 'DST_AK'

    def test_falls_back_to_global_keys(self, mock_spark):
        config = {
            's3_endpoint': 'https://global.example.com',
            's3_access_key': 'GLOBAL_AK',
            's3_secret_key': 'GLOBAL_SK',
        }
        m.configure_spark_s3(mock_spark, config)
        mock_spark.conf.set.assert_any_call('fs.s3a.endpoint', 'https://global.example.com')
        assert config['_src_endpoint'] == 'https://global.example.com'
        assert config['_dest_endpoint'] == 'https://global.example.com'

    def test_skips_empty_values(self, mock_spark):
        config = {}
        m.configure_spark_s3(mock_spark, config)
        mock_spark.conf.set.assert_not_called()


# ---------------------------------------------------------------------------
# apply_bucket_credentials
# ---------------------------------------------------------------------------
class TestApplyBucketCredentials:

    def test_sets_per_bucket_credentials(self, mock_spark):
        m.apply_bucket_credentials(mock_spark, 's3a://my-bucket/path', 'https://ep.com', 'AK', 'SK')
        mock_spark.conf.set.assert_any_call('fs.s3a.bucket.my-bucket.endpoint', 'https://ep.com')
        mock_spark.conf.set.assert_any_call('fs.s3a.bucket.my-bucket.access.key', 'AK')
        mock_spark.conf.set.assert_any_call('fs.s3a.bucket.my-bucket.secret.key', 'SK')

    def test_skips_non_s3a_url(self, mock_spark):
        m.apply_bucket_credentials(mock_spark, '/local/path', 'https://ep.com', 'AK', 'SK')
        mock_spark.conf.set.assert_not_called()

    def test_skips_when_no_credentials_or_endpoint(self, mock_spark):
        m.apply_bucket_credentials(mock_spark, 's3a://bucket/path', '', '', '')
        mock_spark.conf.set.assert_not_called()


# ---------------------------------------------------------------------------
# compute_dest_path
# ---------------------------------------------------------------------------
class TestComputeDestPath:

    def test_uses_prefix_mapping_when_matched(self):
        result = m.compute_dest_path(
            source_location='s3a://src-bucket/data/db/tbl',
            dest_database='dest_db',
            table_name='tbl',
            dest_bucket='s3a://dest-bucket',
            source_s3_prefix='s3a://src-bucket/data',
            dest_s3_prefix='s3a://dest-bucket/data',
        )
        assert result == 's3a://dest-bucket/data/db/tbl'

    def test_falls_back_to_bucket_db_table(self):
        result = m.compute_dest_path(
            source_location='s3a://src-bucket/data/db/tbl',
            dest_database='dest_db',
            table_name='tbl',
            dest_bucket='s3a://dest-bucket',
            source_s3_prefix='',
            dest_s3_prefix='',
        )
        assert result == 's3a://dest-bucket/dest_db/tbl'

    def test_falls_back_when_source_doesnt_match_prefix(self):
        result = m.compute_dest_path(
            source_location='s3a://other-bucket/data/db/tbl',
            dest_database='dest_db',
            table_name='tbl',
            dest_bucket='s3a://dest-bucket',
            source_s3_prefix='s3a://src-bucket/data',
            dest_s3_prefix='s3a://dest-bucket/data',
        )
        assert result == 's3a://dest-bucket/dest_db/tbl'

# ---------------------------------------------------------------------------
# cell_str
# ---------------------------------------------------------------------------
class TestCellStr:

    def test_returns_stripped_string(self):
        assert m.cell_str('  hello  ') == 'hello'

    def test_returns_default_for_none(self):
        assert m.cell_str(None) == ''
        assert m.cell_str(None, default='N/A') == 'N/A'

    def test_returns_default_for_nan(self):
        assert m.cell_str(float('nan')) == ''
        assert m.cell_str(float('nan'), default='EMPTY') == 'EMPTY'

    def test_returns_default_for_blank_string(self):
        assert m.cell_str('   ') == ''
        assert m.cell_str('   ', default='EMPTY') == 'EMPTY'

    def test_converts_int_and_float(self):
        assert m.cell_str(42) == '42'
        assert m.cell_str(3.14) == '3.14'


# ---------------------------------------------------------------------------
# normalize_s3
# ---------------------------------------------------------------------------
class TestNormalizeS3:

    def test_s3n_prefix_converted(self):
        assert m.normalize_s3('s3n://bucket/key') == 's3a://bucket/key'

    def test_s3_prefix_converted(self):
        assert m.normalize_s3('s3://bucket/key') == 's3a://bucket/key'

    def test_s3a_prefix_unchanged(self):
        assert m.normalize_s3('s3a://bucket/key') == 's3a://bucket/key'

    def test_no_prefix_gets_s3a_prepended(self):
        assert m.normalize_s3('bucket/key') == 's3a://bucket/key'

    def test_empty_string_returned_as_is(self):
        assert m.normalize_s3('') == ''

    def test_none_returned_as_is(self):
        assert m.normalize_s3(None) is None


# ---------------------------------------------------------------------------
# validate_bucket_endpoint_pairs — boto3-independent paths
# ---------------------------------------------------------------------------
class TestValidateBucketEndpointPairs:

    def _make_grouped(self, bucket='s3a://data-lake', endpoint='https://s3.example.com', src_db='sales'):
        return {(src_db, 'dest_db', bucket, endpoint, None): {'tokens': []}}

    def test_skips_silently_when_boto3_unavailable(self):
        import builtins
        real_import = builtins.__import__
        def mock_import(name, *args, **kwargs):
            if name == 'boto3':
                raise ImportError('no boto3')
            return real_import(name, *args, **kwargs)
        with patch('builtins.__import__', side_effect=mock_import):
            m.validate_bucket_endpoint_pairs(self._make_grouped(), {})  # must not raise

    def test_rows_with_no_endpoint_are_skipped(self):
        grouped = {('db', 'dest_db', 's3a://bucket', '', None): {'tokens': []}}
        import builtins
        real_import = builtins.__import__
        def mock_import(name, *args, **kwargs):
            if name == 'boto3':
                raise ImportError('no boto3')
            return real_import(name, *args, **kwargs)
        with patch('builtins.__import__', side_effect=mock_import):
            m.validate_bucket_endpoint_pairs(grouped, {})  # no endpoint → skipped, no raise

    def _inject_boto3(self, s3_client_mock, fake_client_error_cls):
        import sys
        import types

        boto3_mod = types.ModuleType('boto3')
        boto3_mod.client = MagicMock(return_value=s3_client_mock)

        botocore_mod = types.ModuleType('botocore')
        botocore_exc_mod = types.ModuleType('botocore.exceptions')
        botocore_exc_mod.ClientError = fake_client_error_cls
        botocore_mod.exceptions = botocore_exc_mod

        return patch.dict(sys.modules, {
            'boto3': boto3_mod,
            'botocore': botocore_mod,
            'botocore.exceptions': botocore_exc_mod,
        })

    def test_deduplicates_same_bucket_endpoint_pair(self):
        """Same (bucket, endpoint) pair appearing in two rows is only validated once."""
        s3_client_mock = MagicMock()
        s3_client_mock.head_bucket.return_value = {}

        class _CE(Exception):
            pass

        grouped = {
            ('db1', 'dest1', 's3a://bucket', 'https://ep.com', None): {},
            ('db2', 'dest2', 's3a://bucket', 'https://ep.com', None): {},
        }
        with self._inject_boto3(s3_client_mock, _CE), \
             patch('airflow.models.Variable.get', return_value=''):
            m.validate_bucket_endpoint_pairs(grouped, {})

        assert s3_client_mock.head_bucket.call_count == 1

    def test_raises_on_missing_bucket(self):
        """404/NoSuchBucket causes a validation Exception listing the failure."""

        class FakeClientError(Exception):
            def __init__(self, *a, **kw):
                self.response = {'Error': {'Code': 'NoSuchBucket'}}

        s3_client_mock = MagicMock()
        s3_client_mock.head_bucket.side_effect = FakeClientError()

        with self._inject_boto3(s3_client_mock, FakeClientError), \
             patch('airflow.models.Variable.get', return_value=''), \
             pytest.raises(Exception, match='validation failed'):
            m.validate_bucket_endpoint_pairs(self._make_grouped(), {})

    def test_403_access_denied_does_not_raise(self):
        """403 means bucket exists but creds lack full access — should warn and continue."""

        class FakeClientError(Exception):
            def __init__(self, *a, **kw):
                self.response = {'Error': {'Code': '403'}}

        s3_client_mock = MagicMock()
        s3_client_mock.head_bucket.side_effect = FakeClientError()

        with self._inject_boto3(s3_client_mock, FakeClientError), \
             patch('airflow.models.Variable.get', return_value=''):
            m.validate_bucket_endpoint_pairs(self._make_grouped(), {})

    def test_general_connection_error_raises(self):
        """Any non-ClientError (e.g. network timeout) is collected and raised."""
        class _CE(Exception):
            pass

        s3_client_mock = MagicMock()
        s3_client_mock.head_bucket.side_effect = ConnectionError('timeout')

        with self._inject_boto3(s3_client_mock, _CE), \
             patch('airflow.models.Variable.get', return_value=''), \
             pytest.raises(Exception, match='validation failed'):
            m.validate_bucket_endpoint_pairs(self._make_grouped(), {})


# ---------------------------------------------------------------------------
# hive_type_to_spark_ddl
# ---------------------------------------------------------------------------
class TestHiveTypeToSparkDdl:

    def _conv(self, t):
        return m.hive_type_to_spark_ddl(t)

    # Primitives — no colons present, must be returned unchanged
    def test_primitives_are_no_ops(self):
        for t in ("string", "bigint", "timestamp", "boolean", "double"):
            assert self._conv(t) == t

    def test_decimal_and_varchar_unchanged(self):
        assert self._conv("decimal(18,4)") == "decimal(18,4)"
        assert self._conv("varchar(255)") == "varchar(255)"

    # Simple structs
    def test_single_field_struct(self):
        assert self._conv("struct<a:int>") == "struct<a int>"

    def test_multi_field_struct(self):
        assert self._conv("struct<a:int,b:string>") == "struct<a int,b string>"

    # Arrays and maps
    def test_array_of_primitives_unchanged(self):
        assert self._conv("array<string>") == "array<string>"

    def test_array_of_struct(self):
        assert self._conv("array<struct<a:int>>") == "array<struct<a int>>"

    def test_map_of_primitives_unchanged(self):
        assert self._conv("map<string,int>") == "map<string,int>"

    def test_map_with_struct_value(self):
        assert self._conv("map<string,struct<a:int>>") == "map<string,struct<a int>>"

    # Deep nesting
    def test_deeply_nested(self):
        assert self._conv("struct<a:array<struct<b:map<string,int>>>>") == \
            "struct<a array<struct<b map<string,int>>>>"

    def test_array_of_array_of_struct(self):
        assert self._conv("array<array<struct<x:int>>>") == "array<array<struct<x int>>>"

    # Parameterised types inside structs (decimal/varchar — no colons → no-op on the type part)
    def test_decimal_inside_struct(self):
        assert self._conv("struct<amount:decimal(18,4),qty:int>") == \
            "struct<amount decimal(18,4),qty int>"

    def test_varchar_inside_struct(self):
        assert self._conv("struct<name:varchar(255),code:char(10)>") == \
            "struct<name varchar(255),code char(10)>"

    # Edge cases
    def test_idempotent_already_converted(self):
        already = "struct<a int,b string>"
        assert self._conv(already) == already

    def test_empty_string(self):
        assert self._conv("") == ""

    def test_mixed_case(self):
        assert self._conv("STRUCT<A:INT>") == "STRUCT<A INT>"

    def test_no_colon_struct_field_returned_unchanged(self):
        # Malformed but must not crash
        assert self._conv("struct<malformed>") == "struct<malformed>"
