"""Unit tests for ranger_utils (KeycloakRoleManager, RangerPolicyManager).

The DAG-layer tests (test_tasks.py) mock ranger_utils entirely.
These tests import the real module with external deps stubbed out.
"""

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_RANGER_DIR = Path(__file__).resolve().parent.parent


def _apache_ranger_stubs():
    stubs = {}
    for mod in [
        "apache_ranger",
        "apache_ranger.client",
        "apache_ranger.client.ranger_client",
        "apache_ranger.client.ranger_user_mgmt_client",
        "apache_ranger.model",
        "apache_ranger.model.ranger_policy",
        "apache_ranger.model.ranger_user_mgmt",
    ]:
        stubs[mod] = MagicMock()
    return stubs


@pytest.fixture()
def keycloak_mgr():
    """Import the real ranger_utils and build a KeycloakRoleManager with a mocked admin."""
    stubs = _apache_ranger_stubs()
    saved = {}
    for mod_name, fake in stubs.items():
        saved[mod_name] = sys.modules.get(mod_name)
        sys.modules[mod_name] = fake

    saved_ru = sys.modules.get("ranger_utils")
    sys.modules.pop("ranger_utils", None)

    str_dir = str(_RANGER_DIR)
    added = str_dir not in sys.path
    if added:
        sys.path.insert(0, str_dir)

    try:
        mod = importlib.import_module("ranger_utils")
        manager = object.__new__(mod.KeycloakRoleManager)
        mock_admin = MagicMock()
        manager.keycloak_admin = mock_admin
        manager.server_url = "http://keycloak:8080"
        manager.realm_name = "test"
        manager.max_retries = 1
        manager.connection_timeout = 1
        yield manager, mock_admin
    finally:
        sys.modules.pop("ranger_utils", None)
        if saved_ru is not None:
            sys.modules["ranger_utils"] = saved_ru
        for mod_name, original in saved.items():
            if original is None:
                sys.modules.pop(mod_name, None)
            else:
                sys.modules[mod_name] = original
        if added and str_dir in sys.path:
            sys.path.remove(str_dir)


class TestEnsureGroupExists:
    def test_raises_when_group_missing(self, keycloak_mgr):
        manager, mock_admin = keycloak_mgr
        mock_admin.get_groups.return_value = []

        with pytest.raises(ValueError, match="does not exist in Keycloak"):
            manager.ensure_group_exists("nonexistent_grp")

    def test_returns_id_when_group_exists(self, keycloak_mgr):
        manager, mock_admin = keycloak_mgr
        mock_admin.get_groups.return_value = [{"name": "grp1", "id": "uuid-123"}]

        group_id, created = manager.ensure_group_exists("grp1")

        assert group_id == "uuid-123"
        assert created is False
        mock_admin.create_group.assert_not_called()


class TestSyncRolesAndPrincipals:
    def test_missing_group_skips_mapping(self, keycloak_mgr):
        manager, mock_admin = keycloak_mgr
        mock_admin.get_groups.return_value = []
        mock_admin.get_realm_roles.return_value = []

        result = manager.sync_roles_and_principals({
            "role_a": {"groups": ["missing_grp"], "users": []},
        })

        assert result["missing_groups"] == ["missing_grp"]
        assert result["created_mappings"] == []
        assert any(
            f["principal"] == "missing_grp" and f["type"] == "group"
            for f in result["failed"]
        )

    def test_same_missing_group_looked_up_once(self, keycloak_mgr):
        """Two roles reference the same missing group: one lookup, one missing entry, two failures."""
        manager, mock_admin = keycloak_mgr
        mock_admin.get_groups.return_value = []
        mock_admin.get_realm_roles.return_value = []

        result = manager.sync_roles_and_principals({
            "role_a": {"groups": ["shared_grp"], "users": []},
            "role_b": {"groups": ["shared_grp"], "users": []},
        })

        assert mock_admin.get_groups.call_count == 1
        assert result["missing_groups"] == ["shared_grp"]
        failed = [f for f in result["failed"] if f["principal"] == "shared_grp"]
        assert len(failed) == 2
        assert {f["role"] for f in failed} == {"role_a", "role_b"}

    def test_existing_group_mapped_missing_group_fails(self, keycloak_mgr):
        """One group exists, one doesn't: good group gets a mapping, bad group is tracked as failed."""
        manager, mock_admin = keycloak_mgr
        mock_admin.get_groups.return_value = [{"name": "good_grp", "id": "uuid-good"}]
        mock_admin.get_realm_roles.return_value = []
        mock_admin.get_realm_role.return_value = {"name": "role_a"}
        mock_admin.get_group_realm_roles.return_value = []

        result = manager.sync_roles_and_principals({
            "role_a": {"groups": ["good_grp", "bad_grp"], "users": []},
        })

        assert "good_grp" in result["existing_groups"]
        assert "bad_grp" in result["missing_groups"]
        assert any(m["principal"] == "good_grp" for m in result["created_mappings"])
        assert any(f["principal"] == "bad_grp" for f in result["failed"])
