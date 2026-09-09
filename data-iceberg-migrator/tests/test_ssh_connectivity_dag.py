"""DAG integrity tests for the ssh_connectivity diagnostic DAG."""

from datetime import timedelta

import ssh_connectivity as s1


class TestSshConnectivityDagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert s1.dag.dag_id == 'ssh_connectivity'

    def test_tags(self):
        assert s1.dag.tags == ['migration', 'mapr', 's3', 'hive']

    def test_owner(self):
        assert s1.default_args['owner'] == 'data-migration'

    def test_retries(self):
        assert s1.default_args['retries'] == 1

    def test_retry_delay(self):
        assert s1.default_args['retry_delay'] == timedelta(minutes=5)


class TestSshConnectivityTask:

    def _task(self):
        assert len(s1.dag.tasks) == 1
        return s1.dag.tasks[0]

    def test_task_id(self):
        assert self._task().task_id == 'test_ssh_remotely'

    def test_ssh_conn_id(self):
        assert self._task().ssh_conn_id == 'ssh_test_edl'

    def test_command(self):
        assert 'Testing SSH connectivity' in self._task().command
