from typing import Literal, Optional, Any, List
from cx_Oracle import makedsn
from pydantic import BaseModel, validator, SecretStr


class LdapConfig(BaseModel):
    host: str
    bind_dn: str
    password: str
    base_user_dn: str
    base_group_dn: str
    filter_users: str
    filter_groups: str
    key: str
    sync_interval: int
    user_attrs: List[str]
    group_attrs: List[str]
    domain: Optional[str]


class DbConfigOracle(BaseModel):
    password: SecretStr
    user: Optional[str]
    host: Optional[str]
    port: Optional[int]
    sid: Optional[str]
    service_name: Optional[str]
    tns_name: Optional[str]
    dsn: Any

    @validator('dsn', always=True)
    def get_oracle_dsn(cls, v, values):
        i = [v for v in values if v in ['sid', 'service_name', 'tns_name'] and values[v]]
        if len(i) != 1:
            raise ValueError(f'either one of sid/service_name/tns_name must be in config for oracle. {len(i)} given')

        if values['tns_name']:
            return values['tns_name']

        if not values['host'] or not values['port']:
            raise ValueError('host/port key is missing for oracle in config')

        if values['service_name']:
            return makedsn(values['host'],
                           values['port'],
                           service_name=values['service_name'])

        return makedsn(values['host'],
                       values['port'],
                       sid=values['sid'])


class DbConfigPg(BaseModel):
    password: SecretStr
    user: str
    host: str
    port: int
    database: str
    dsn: Any

    @validator('dsn', always=True)
    def get_pg_dsn(cls, v, values):
        return f'dbname={values["database"]} ' \
               f'user={values["user"]} ' \
               f'password={values["password"].get_secret_value()} ' \
               f'host={values["host"]} port={values["port"]}'


class Config(BaseModel):
    logging_level: Literal['DEBUG', 'INFO'] = 'DEBUG'
    oracle: Optional[DbConfigOracle]
    pg: Optional[DbConfigPg]
    ldap: LdapConfig
    error_retry_interval: int = 60
    check_db: Any

    @validator('check_db', always=True)
    def check_single_db(cls, v, values):
        i = len([v for v in values if v in ['oracle', 'pg'] and values[v]])
        if i > 1:
            raise ValueError('oracle AND pg keys present in config, simultaneous use not allowed')
        if i < 1:
            raise ValueError('oracle or pg key not found in config')
