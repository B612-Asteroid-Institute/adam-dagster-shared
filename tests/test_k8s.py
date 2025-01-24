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