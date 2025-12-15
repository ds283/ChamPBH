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


shard_key_type = None


# get wavenumber store id for a wavenumber object, interpreted as a shard key,
# or a proxy for it
def get_shard_key_store_id(obj):
    # if isinstance(obj, wavenumber):
    #     return obj.store_id
    #
    # if isinstance(obj, wavenumber_exit_time):
    #     return obj.k.store_id

    raise RuntimeError(
        f'Could not determine wavenumber shard key for object of type "{type(obj)}"'
    )
