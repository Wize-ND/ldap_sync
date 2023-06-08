import logging
import re
import uuid

import cx_Oracle
import psycopg2
import psycopg2.extras
from config import DbConfigPg, DbConfigOracle, Config


class Database:
    log = logging.getLogger()
    _registry = {}

    def __init_subclass__(cls, driver: str):
        cls._registry[driver] = cls

    def __new__(cls, driver: str, **kwargs):
        subclass = cls._registry[driver]
        return object.__new__(subclass)

    def save_and_sync(self, groups: list, persons: list, memberships: list):
        raise NotImplementedError


class PgDatabase(Database, driver='pg_driver'):
    pg_api = 'os_ldap'
    groups_sql = f'SELECT {pg_api}.p_get_group(%s)'
    persons_sql = f'SELECT {pg_api}.p_get_person(%s)'
    memberships_sql = f'SELECT {pg_api}.p_get_memberships(%s, %s)'
    run_sync_sql = f'SELECT {pg_api}.p_run_sync()'

    def __init__(self, cfg: Config, **kwargs):
        psycopg2.extras.register_uuid()
        self.cfg = cfg

    def save_and_sync(self, groups: list, persons: list, memberships: list):
        try:
            with psycopg2.connect(dsn=self.cfg.pg.dsn, client_encoding='UTF8') as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    for xml in groups:
                        cur.execute(self.groups_sql, (xml,))
                        result = cur.fetchone()[0]
                        if result != '(0,Success)':
                            self.log.error(f'save group {xml} error: {result=}')

                    for xml in persons:
                        cur.execute(self.persons_sql, (xml,))
                        result = cur.fetchone()[0]
                        if result != '(0,Success)':
                            self.log.error(f'save person {xml} error: {result=}')

                    for person_guid, group_guid in memberships:
                        cur.execute(self.memberships_sql, (person_guid, group_guid,))
                        result = cur.fetchone()[0]
                        if result != '(0,Success)':
                            self.log.error(f'save membership {person_guid=} - {group_guid=} error: {result=}')

                    cur.execute(self.run_sync_sql)
                    result = cur.fetchone()[0]
                    if result != '(0,Success)':
                        self.log.error(f'run sync error: {result=}')
                    conn.commit()
                self.log.info('sync success')
        except psycopg2.Error as e:
            logging.exception(e)
            self.log.error(f'save to database error: {e.pgerror} | code {e.pgcode}')


class OracleDatabase(Database, driver='oracle_driver'):
    ora_api = 'os_eqm.ldap_sync_api'
    groups_sql = f'begin {ora_api}.p_get_group(:xml, :code, :msg, :domain); end;'
    persons_sql = f'begin {ora_api}.p_get_person(:xml, :code, :msg, :domain); end;'
    memberships_sql = f'begin {ora_api}.p_get_memberships(:person_guid, :group_guid, :code, :msg, :domain); end;'
    run_sync_sql = f'begin {ora_api}.p_run_sync(:code, :msg, 1, :domain); end;'

    def __init__(self, cfg: Config, **kwargs):
        self.cfg = cfg

    def handle_database_error(self, e: cx_Oracle.DatabaseError, log_str):
        logging.exception(e)
        error, = e.args
        msg = re.search(r'^ORA.\d+:\s(.*)', error.message)
        msg = msg.group(1) if msg else error.message
        self.log.error(f'{log_str} {msg}')

    def save(self, cur: cx_Oracle.Cursor, sql: str, items: list):
        if sql in [self.groups_sql, self.persons_sql]:
            target = 'group' if sql == self.groups_sql else 'person'
            for xml in items:
                code = cur.var(int)
                msg = cur.var(str)
                try:
                    cur.execute(sql, xml=xml, code=code, msg=msg,
                                domain=self.cfg.ldap.domain)
                    self.log.debug(f'save {target} {sql=} {xml=} | msg={msg.getvalue()} code={code.getvalue()}')
                    if code.getvalue() != 0:
                        self.log.error(f'save {target} {xml} error: {msg.getvalue()}')
                except cx_Oracle.DatabaseError as e:
                    self.handle_database_error(e, f'save {target} {xml} error:')
        else:
            for person_guid, group_guid in items:
                code = cur.var(int)
                msg = cur.var(str)
                try:
                    cur.execute(self.memberships_sql, person_guid=person_guid, group_guid=group_guid, code=code, msg=msg,
                                domain=self.cfg.ldap.domain)
                    self.log.debug(f'save membership: {person_guid} <-> {group_guid} | msg={msg.getvalue()} code={code.getvalue()}')
                    if code.getvalue() != 0:
                        self.log.error(
                            f'save membership  {person_guid=} - {group_guid=} error: {msg.getvalue()})')
                except cx_Oracle.DatabaseError as e:
                    self.handle_database_error(e, f'save membership {person_guid=} - {group_guid=} error:')

    def save_and_sync(self, groups: list, persons: list, memberships: list):
        try:
            with cx_Oracle.connect(user=self.cfg.oracle.user, password=self.cfg.oracle.password.get_secret_value(), dsn=self.cfg.oracle.dsn,
                                   encoding='UTF-8') as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    self.log.debug(f'RUN {self.groups_sql}')
                    self.save(cur, self.groups_sql, groups)
                    self.log.debug(f'RUN {self.persons_sql}')
                    self.save(cur, self.persons_sql, persons)
                    self.log.debug(f'RUN {self.memberships_sql}')
                    self.save(cur, self.memberships_sql, memberships)
                    conn.commit()
                    code = cur.var(int)
                    msg = cur.var(str)
                    cur.execute(self.run_sync_sql, code=code, msg=msg, domain=self.cfg.ldap.domain)
                    if code.getvalue() != 0:
                        self.log.error(
                            f'run sync error: {msg.getvalue()} | (code={code.getvalue()})')
                    conn.commit()
                self.log.info('sync success')
        except cx_Oracle.DatabaseError as e:
            self.handle_database_error(e, f'save to database error:')
