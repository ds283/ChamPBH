from CosmologyConcepts import beta_value

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
    "ExponentialPotential",
    "InversePowerPotential",
    "LambdaCDM",
    "QCD_Cosmology",
    "IntegrationSolver",
]

sharded_tables = {
    "ExponentialCoupling": "shard_key",
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


# configure ShardedPool to shard by beta_value
# this seems the best choice, because we know the phenomenology is mostly independent of (M, Lambda), and we won't
# always want to probe a lot of those
ShardKeyType = beta_value


# get wavenumber store id for a wavenumber object, interpreted as a shard key,
# or a proxy for it
def get_shard_key_store_id(obj):
    if isinstance(obj, beta_value):
        return obj.store_id

    raise RuntimeError(
        f'Could not determine shard key for object of type "{type(obj)}"'
    )
