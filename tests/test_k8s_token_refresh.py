"""Regression test: the node-SA k8s client must auto-refresh its bearer token.

Without refresh_api_key_hook the GCP token fetched at client creation is frozen
into Configuration.api_key, so a long-lived client (e.g. the precovery-v2
sharded poll loop) returns 401 once that token's TTL elapses.
"""
import base64

import google.auth.transport.requests  # noqa: F401 (ensure submodule importable)
import adam_dagster_shared.k8s as k8s


class _FakeCreds:
    def __init__(self):
        self.token = "tok-init"
        self._valid = True
        self.refreshes = 0

    @property
    def valid(self):
        return self._valid

    def refresh(self, _req):
        self.refreshes += 1
        self.token = f"tok-refreshed-{self.refreshes}"
        self._valid = True


class _FakeMasterAuth:
    cluster_ca_certificate = base64.b64encode(b"FAKE-CA").decode()


class _FakeClusterResp:
    endpoint = "10.0.0.1"
    master_auth = _FakeMasterAuth()


class _FakeClusterManagerClient:
    def get_cluster(self, project_id, zone, cluster_id):
        return _FakeClusterResp()


def test_refresh_api_key_hook_refreshes_on_expiry():
    fake_creds = _FakeCreds()
    orig_cmc = k8s.container_v1.ClusterManagerClient
    orig_default = k8s.google.auth.default
    k8s.container_v1.ClusterManagerClient = lambda *a, **k: _FakeClusterManagerClient()
    k8s.google.auth.default = lambda *a, **k: (fake_creds, "proj")
    try:
        api = k8s.get_node_sa_kubernetes_client("p", "z", "c")
        cfg = api.configuration

        # hook is wired
        assert cfg.refresh_api_key_hook is not None
        # one refresh happened at construction; token snapshot reflects it
        assert fake_creds.refreshes == 1, fake_creds.refreshes
        assert cfg.api_key["authorization"] == "tok-refreshed-1"

        # token expires -> hook re-fetches and updates api_key
        fake_creds._valid = False
        cfg.refresh_api_key_hook(cfg)
        assert fake_creds.refreshes == 2, fake_creds.refreshes
        assert cfg.api_key["authorization"] == "tok-refreshed-2"

        # still valid -> hook is a no-op (no extra token-endpoint calls)
        cfg.refresh_api_key_hook(cfg)
        assert fake_creds.refreshes == 2, fake_creds.refreshes
    finally:
        k8s.container_v1.ClusterManagerClient = orig_cmc
        k8s.google.auth.default = orig_default


if __name__ == "__main__":
    test_refresh_api_key_hook_refreshes_on_expiry()
    print("PASS: refresh_api_key_hook refreshes on expiry, skips while valid")
