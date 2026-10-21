"""Run against an installed wheel to exercise native register validation."""

import pytest

from mooncake import _conductor
from mooncake.conductor import ConductorClient


@pytest.fixture(params=[ConductorClient, _conductor.ConductorClient])
def client(request):
    instance = request.param()
    yield instance
    instance.close()


@pytest.mark.parametrize("config", [None, [], "config", 1])
def test_register_requires_dict(client, config):
    with pytest.raises(ValueError, match="dict"):
        client.register(config)


@pytest.mark.parametrize(
    "field",
    [
        "instance_id",
        "endpoint",
        "replay_endpoint",
        "publisher_type",
        "model_name",
        "lora_name",
        "tenant_id",
    ],
)
def test_register_string_fields(client, field):
    with pytest.raises(ValueError, match=field):
        client.register({field: 123})


@pytest.mark.parametrize("field", ["block_size", "dp_rank", "cache_group"])
@pytest.mark.parametrize("value", ["4", 4.5, True, [], 2**100])
def test_register_integer_fields(client, field, value):
    with pytest.raises(ValueError, match=field):
        client.register({field: value})


@pytest.mark.parametrize("profile", [None, [], "profile", 123])
def test_hash_profile_requires_dict(client, profile):
    with pytest.raises(ValueError, match="hash_profile"):
        client.register({"hash_profile": profile})


@pytest.mark.parametrize(
    "field", ["strategy", "algorithm", "python_hash_seed", "index_projection"]
)
def test_hash_profile_field_types(client, field):
    with pytest.raises(ValueError, match=field):
        client.register({"hash_profile": {field: 123}})


@pytest.mark.parametrize("key", ["unknown", "root_digest", 123])
def test_hash_profile_rejects_unknown_keys(client, key):
    with pytest.raises(ValueError, match="hash_profile"):
        client.register({"hash_profile": {key: "value"}})


@pytest.mark.parametrize("key", ["unknown", 123])
def test_register_rejects_unknown_keys(client, key):
    with pytest.raises(ValueError, match="register config"):
        client.register({key: "value"})


def test_valid_types_reach_uninitialized_client(client):
    # None is a valid optional cache_group. No RPC server is needed to prove
    # conversion succeeds: the native client returns an error code instead.
    result = client.register(
        {
            "instance_id": "test",
            "endpoint": "tcp://127.0.0.1:12345",
            "model_name": "model",
            "block_size": 4,
            "dp_rank": 0,
            "cache_group": None,
            "hash_profile": {
                "strategy": "vllm_v1",
                "algorithm": "sha256_cbor",
                "python_hash_seed": "0",
                "index_projection": "low64_be",
            },
        }
    )
    assert isinstance(result, int)
    assert result < 0
