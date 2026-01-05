class SQLAFactoryBase:
    def register(self):
        raise NotImplementedError

    def build(self, payload, conn, table, inserter, tables, inserters):
        raise NotImplementedError

    def store(self, obj, conn, table, inserter, tables, inserters):
        raise NotImplementedError

    def validate(self, obj, conn, table, tables):
        raise NotImplementedError

    def validate_on_startup(self, conn, table, tables, prune=False):
        raise NotImplementedError
