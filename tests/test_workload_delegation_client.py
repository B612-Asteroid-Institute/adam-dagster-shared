"""Workload-credential and job-delegation client support (personal-rqe.2/.3)."""

from __future__ import annotations

from unittest import mock

from adam_dagster_shared import adam_api_client as client_module
from adam_dagster_shared.adam_api_client import ADAMAPIClient, send_job_update


class _Response:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


def _client() -> ADAMAPIClient:
    return ADAMAPIClient("api.example", 80)


def test_workload_credential_absent_by_default(monkeypatch) -> None:
    monkeypatch.delenv("ADAM_API_WORKLOAD_CREDENTIAL", raising=False)
    assert ADAMAPIClient.workload_credential() is None
    assert _client().post_with_workload("/api/x", {}) is None


def test_request_job_delegation_uses_workload_scheme(monkeypatch) -> None:
    monkeypatch.setenv("ADAM_API_WORKLOAD_CREDENTIAL", "wl-secret")
    client = _client()
    captured: dict = {}

    def _request(method, url, json=None, headers=None):
        captured.update(method=method, url=url, json=json, headers=headers)
        return _Response(200, {"token": "dgt_token"})

    monkeypatch.setattr(client.session, "request", _request)
    token = client.request_job_delegation("parent-1", ["jobs:create:cutout"])
    assert token == "dgt_token"
    assert captured["headers"]["Authorization"] == "Workload wl-secret"
    assert captured["url"].endswith("/api/internal/jobs/parent-1/delegations/")
    assert captured["json"] == {
        "scopes": ["jobs:create:cutout"],
        "ttl_seconds": 600,
    }


def test_request_job_delegation_failure_returns_none(monkeypatch) -> None:
    monkeypatch.setenv("ADAM_API_WORKLOAD_CREDENTIAL", "wl-secret")
    client = _client()
    monkeypatch.setattr(
        client.session,
        "request",
        lambda **kwargs: _Response(403, {}, "forbidden"),
    )
    assert client.request_job_delegation("parent-1", ["jobs:create:cutout"]) is None


def test_post_with_delegation_sets_delegation_scheme(monkeypatch) -> None:
    client = _client()
    captured: dict = {}

    def _request(method, url, json=None, headers=None):
        captured.update(headers=headers, url=url)
        return _Response(200, {"id": "child"})

    monkeypatch.setattr(client.session, "request", _request)
    response = client.post_with_delegation("/api/cutouts/", {"a": 1}, "dgt_x")
    assert response.status_code == 200
    assert captured["headers"]["Authorization"] == "Delegation dgt_x"


def test_send_job_update_prefers_workload_credential(monkeypatch) -> None:
    monkeypatch.setenv("ADAM_API_WORKLOAD_CREDENTIAL", "wl-secret")
    client = _client()
    monkeypatch.setattr(client_module, "get_adam_client", lambda: client)
    captured: dict = {}

    def _post(url, json=None, headers=None):
        captured.update(url=url, headers=headers)
        return _Response(200)

    with mock.patch.object(client_module.requests, "post", side_effect=_post):
        assert send_job_update(job_id="job-1", status="running") is True
    assert captured["headers"]["Authorization"] == "Workload wl-secret"


def test_send_job_update_falls_back_to_legacy_bearer(monkeypatch) -> None:
    monkeypatch.delenv("ADAM_API_WORKLOAD_CREDENTIAL", raising=False)
    client = ADAMAPIClient("api.example", 80, refresh_token="refresh")
    client._access_token = "jwt-token"
    monkeypatch.setattr(client_module, "get_adam_client", lambda: client)
    captured: dict = {}

    def _post(url, json=None, headers=None):
        captured.update(headers=headers)
        return _Response(200)

    with mock.patch.object(client_module.requests, "post", side_effect=_post):
        assert send_job_update(job_id="job-1", status="running") is True
    assert captured["headers"]["Authorization"] == "Bearer jwt-token"
