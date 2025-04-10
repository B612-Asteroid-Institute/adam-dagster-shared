import pytest

from src.adam_dagster_shared.k8s import create_k8s_config


def test_create_k8s_config_basic():
    """Test basic configuration with default values"""
    config = create_k8s_config()
    
    assert config["container_config"]["resources"]["requests"] == {
        "cpu": "1000m",
        "memory": "2000Mi"
    }
    assert "volumes" not in config["pod_spec_config"]


def test_create_k8s_config_with_tmp_volume():
    """Test configuration with temporary volume"""
    config = create_k8s_config(tmp_volume=1000)
    
    assert config["container_config"]["resources"]["requests"]["ephemeral-storage"] == "3000Mi"
    assert config["container_config"]["volume_mounts"] == [
        {
            "name": "run-volume",
            "mountPath": "/tmp",
        }
    ]
    assert config["pod_spec_config"]["volumes"] == [
        {"name": "run-volume", "empty_dir": {"size_limit": "1000Mi"}}
    ]


def test_create_k8s_config_with_shm_volume():
    """Test configuration with shared memory volume"""
    config = create_k8s_config(memory=3000, shm_volume=1000)
    
    assert config["container_config"]["volume_mounts"] == [
        {"name": "shm-volume", "mountPath": "/dev/shm", "read_only": False}
    ]
    assert config["pod_spec_config"]["volumes"] == [
        {"name": "shm-volume", "empty_dir": {"medium": "Memory", "size_limit": "1000Mi"}}
    ]


def test_create_k8s_config_shm_volume_memory_assertion():
    """Test that shm_volume larger than memory raises assertion"""
    with pytest.raises(AssertionError, match="Not enough memory for shm volume"):
        create_k8s_config(memory=500, shm_volume=1000)


def test_create_k8s_config_with_spot():
    """Test configuration with spot instances"""
    config = create_k8s_config(use_spot=True)
    
    assert config["pod_spec_config"]["tolerations"] == [
        {
            "key": "allow-spot",
            "operator": "Equal",
            "value": "true",
            "effect": "NoSchedule",
        }
    ]
    
    assert config["pod_spec_config"]["affinity"]["node_affinity"][
        "required_during_scheduling_ignored_during_execution"
    ]["node_selector_terms"] == [
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


def test_create_k8s_config_with_private():
    """Test configuration with private nodes"""
    config = create_k8s_config(allow_private=True)
    
    assert config["pod_spec_config"]["tolerations"] == [
        {
            "key": "allow-private",
            "operator": "Equal",
            "value": "true",
            "effect": "NoSchedule",
        }
    ]


def test_create_k8s_config_with_custom_resources():
    """Test configuration with custom CPU and memory values"""
    config = create_k8s_config(cpu=2000, memory=4000)
    
    assert config["container_config"]["resources"]["requests"] == {
        "cpu": "2000m",
        "memory": "4000Mi"
    }


def test_create_k8s_config_with_required_labels_and_tolerations():
    """Test configuration with required labels and allowed tolerations"""
    config = create_k8s_config(
        required_labels=["custom-label-1", "custom-label-2"],
        allowed_tolerations=["custom-toleration-1", "custom-toleration-2"]
    )
    
    # Check required labels in node affinity
    node_selector_terms = config["pod_spec_config"]["affinity"]["node_affinity"][
        "required_during_scheduling_ignored_during_execution"
    ]["node_selector_terms"]
    
    assert len(node_selector_terms) == 2
    assert {
        "match_expressions": [{"key": "custom-label-1", "operator": "In", "values": ["true"]}]
    } in node_selector_terms
    assert {
        "match_expressions": [{"key": "custom-label-2", "operator": "In", "values": ["true"]}]
    } in node_selector_terms
    
    # Check tolerations
    tolerations = config["pod_spec_config"]["tolerations"]
    assert len(tolerations) == 2
    assert {
        "key": "custom-toleration-1",
        "operator": "Equal",
        "value": "true",
        "effect": "NoSchedule"
    } in tolerations
    assert {
        "key": "custom-toleration-2",
        "operator": "Equal",
        "value": "true",
        "effect": "NoSchedule"
    } in tolerations


def test_create_k8s_config_with_both_volumes():
    """Test configuration with both temporary and shared memory volumes"""
    config = create_k8s_config(tmp_volume=1000, shm_volume=500, memory=2000)
    
    # Check ephemeral storage request
    assert config["container_config"]["resources"]["requests"]["ephemeral-storage"] == "3000Mi"
    
    # Check volume mounts
    assert len(config["container_config"]["volume_mounts"]) == 2
    assert {
        "name": "run-volume",
        "mountPath": "/tmp",
    } in config["container_config"]["volume_mounts"]
    assert {
        "name": "shm-volume",
        "mountPath": "/dev/shm",
        "read_only": False
    } in config["container_config"]["volume_mounts"]
    
    # Check volumes
    assert len(config["pod_spec_config"]["volumes"]) == 2
    assert {
        "name": "run-volume",
        "empty_dir": {"size_limit": "1000Mi"}
    } in config["pod_spec_config"]["volumes"]
    assert {
        "name": "shm-volume",
        "empty_dir": {"medium": "Memory", "size_limit": "500Mi"}
    } in config["pod_spec_config"]["volumes"] 