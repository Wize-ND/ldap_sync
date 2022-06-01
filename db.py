import logging
from config import DbConfigPg, DbConfigOracle, Config


class Database:
    _registry = {}

    def __init_subclass__(cls, driver: str):
        cls._registry[driver] = cls

    def __new__(cls, driver: str, **kwargs):
        subclass = cls._registry[driver]
        return object.__new__(subclass)

    def save_and_sync(self, groups: dict, persons: list, memberships: list):
        raise NotImplementedError


class PgDatabase(Database, driver='pg_driver'):
    def save_and_sync(self, groups: dict, persons: list, memberships: list):
        pass


class OracleDatabase(Database, driver='oracle_driver'):
    def save_and_sync(self, groups: dict, persons: list, memberships: list):
        pass

