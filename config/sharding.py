from CosmologyConcepts import M_value

replicated_tables = [
    "version",
    "store_tag",
    "redshift",
    "tolerance",
    "beta_value",
    "M_value",
    "Lambda_value",
    "temperature",
    "phi_value",
    "pi_value",
    "ExponentialCoupling",
    "LambdaCDM",
    "QCD_Cosmology",
    "IntegrationSolver",
]

sharded_tables = {
    "InversePowerPotential": "shard_key",
    "ExponentialPotential": "shard_key",
    "ScalarModel": "shard_key",
    "ScalarModelValue": "shard_key",
}

read_table_config = {
    "read_redshift_table": {"class": "redshift", "tables_arg": True},
    "read_beta_table": {"class": "beta_value", "tables_arg": False},
    "read_M_table": {"class": "M_value", "tables_arg": False},
    "read_Lambda_table": {"class": "Lambda_value", "tables_arg": False},
    "read_phi_table": {"class": "phi_value", "tables_arg": False},
    "read_pi_table": {"class": "pi_value", "tables_arg": False},
}


# configure ShardedPool to shard by M_value
shard_key_type = M_value


# get wavenumber store id for a wavenumber object, interpreted as a shard key,
# or a proxy for it
def get_shard_key_store_id(obj):
    if isinstance(obj, M_value):
        return obj.store_id

    raise RuntimeError(
        f'Could not determine wavenumber shard key for object of type "{type(obj)}"'
    )
