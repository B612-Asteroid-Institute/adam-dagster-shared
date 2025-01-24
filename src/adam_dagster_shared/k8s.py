import base64
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Optional

import google.auth
from google.cloud import container_v1
from kubernetes import client


def get_node_sa_kubernetes_client(project_id, zone, cluster_id) -> client.ApiClient:
    print("Attempting to init k8s client from cluster response.")
    container_client = container_v1.ClusterManagerClient()
    response = container_client.get_cluster(
        project_id=project_id, zone=zone, cluster_id=cluster_id
    )
    creds, projects = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    configuration = client.Configuration()
    configuration.host = f"https://{response.endpoint}"
    with NamedTemporaryFile(delete=False) as ca_cert:
        ca_cert.write(base64.b64decode(response.master_auth.cluster_ca_certificate))
    configuration.ssl_ca_cert = ca_cert.name
    configuration.api_key_prefix["authorization"] = "Bearer"
    configuration.api_key["authorization"] = creds.token
    configuration.client_side_validation = False

    # Return the ApiClient while the temporary file is still open
    return client.ApiClient(configuration)


def get_current_namespace() -> str:
    """
    Get namespace to avoid collisions with shared resources
    """
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
            namespace = f.read()
    except FileNotFoundError:
        namespace = "nonamespace"
    return namespace


def create_k8s_config(
    cpu: int = 1000,  # m
    memory: int = 2000,  # MiB
    tmp_volume: int = 0,  # MiB
    shm_volume: int = 0,  # MiB
    allow_spot: Optional[bool] = False,
    allow_private: Optional[bool] = False,
    use_spot: Optional[bool] = False,
) -> Dict[str, Any]:
    """
    Generates the contents of op tags 'dagster-k8s/config' for a given asset

    Args:
        cpu: CPU request in millicores
        memory: Memory request in MiB
        tmp_volume: Size of the temporary volume in GiB.
        allow_spot: Whether the pod can be scheduled on spot nodes
        allow_private: Whether the pod can be scheduled on private nodes

    Returns:
        dict: The pod spec configuration. E.g.

        {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "1000m", "memory": "2Gi", "ephemeral-storage": "22Gi"},
                },
                "volume_mounts": [
                    {
                        "name": "run-volume",
                        "mountPath": "/tmp",
                    }
                ],
            },
            "pod_spec_config": {
                "volumes": [{"name": "run-volume", "empty_dir": {"size_limit": "20Gi"}}],
                "affinity": {
                    "node_affinity": {
                        "required_during_scheduling_ignored_during_execution": {
                            "node_selector_terms": [
                                {
                                    "match_expressions": [
                                        {
                                            "key": "cloud.google.com/gke-provisioning",
                                            "operator": "In",
                                            "values": ["spot"],
                                        }
                                    ]
                                }
                            ]
                        },
                    }
                },
                "tolerations": [
                    {
                        "key": "allow-spot",
                        "operator": "Equal",
                        "value": "true",
                        "effect": "NoSchedule",
                    },
                ],
            },
        }

    """
    if use_spot:
        allow_spot = True

    spec = {
        "container_config": {
            "resources": {
                "requests": {"cpu": f"{cpu}m", "memory": f"{memory}Mi"},
            },
        },
        "pod_spec_config": {},
    }

    if tmp_volume > 0:
        # Add a buffer of 2Gi to the ephemeral storage request
        spec["container_config"]["resources"]["requests"][
            "ephemeral-storage"
        ] = f"{tmp_volume + 2000}Mi"
        spec["container_config"]["volume_mounts"] = [
            {
                "name": "run-volume",
                "mountPath": "/tmp",
            }
        ]
        spec["pod_spec_config"]["volumes"] = [
            {"name": "run-volume", "empty_dir": {"size_limit": f"{tmp_volume}Mi"}}
        ]

    if shm_volume > 0:
        # Ensure we have enough memory for the shm volume
        assert memory > shm_volume, "Not enough memory for shm volume"
        spec["container_config"]["volume_mounts"] = [
            {"name": "shm-volume", "mountPath": "/dev/shm", "read_only": False}
        ]
        spec["pod_spec_config"]["volumes"] = [
            {
                "name": "shm-volume",
                "empty_dir": {"medium": "Memory", "size_limit": f"{shm_volume}Mi"},
            }
        ]

    if allow_spot:
        spec["pod_spec_config"].setdefault("tolerations", []).append(
            {
                "key": "allow-spot",
                "operator": "Equal",
                "value": "true",
                "effect": "NoSchedule",
            }
        )

    if allow_private:
        spec["pod_spec_config"].setdefault("tolerations", []).append(
            {
                "key": "allow-private",
                "operator": "Equal",
                "value": "true",
                "effect": "NoSchedule",
            }
        )

    if use_spot:
        spec["pod_spec_config"].setdefault("affinity", {}).setdefault(
            "node_affinity", {}
        ).setdefault(
            "required_during_scheduling_ignored_during_execution", {}
        ).setdefault(
            "node_selector_terms", []
        ).append(
            {
                "match_expressions": [
                    {
                        "key": "cloud.google.com/gke-provisioning",
                        "operator": "In",
                        "values": ["spot"],
                    }
                ]
            }
        )

    return spec
