replicated_tables = [
    "version",
    "store_tag",
    "redshift",
    "tolerance",
    "LambdaCDM",
    "QCD_Cosmology",
    "IntegrationSolver",
]

sharded_tables = {
    "ScalarModel": "shard_key",
    "ScalarModelValue": "shard_key",
}
