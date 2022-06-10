import logging
import re
import uuid

import cx_Oracle
import psycopg2
import psycopg2.extras
from config import DbConfigPg, DbConfigOracle, Config


class Database:
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
                            logging.getLogger().error(
                                f'save group {xml} error: {result=}')

                    for xml in persons:
                        cur.execute(self.persons_sql, (xml,))
                        result = cur.fetchone()[0]
                        if result != '(0,Success)':
                            logging.getLogger().error(
                                f'save person {xml} error: {result=}')

                    for person_guid, group_guid in memberships:
                        cur.execute(self.memberships_sql, (person_guid, group_guid,))
                        result = cur.fetchone()[0]
                        if result != '(0,Success)':
                            logging.getLogger().error(
                                f'save membership {person_guid=} - {group_guid=} error: {result=}')

                    cur.execute(self.run_sync_sql)
                    result = cur.fetchone()[0]
                    if result != '(0,Success)':
                        logging.getLogger().error(f'run sync error: {result=}')
                    conn.commit()
                logging.getLogger().info('sync success')
        except psycopg2.Error as e:
            logging.exception(e)
            logging.getLogger().error(f'save to database error: {e.pgerror} | code {e.pgcode}')


class OracleDatabase(Database, driver='oracle_driver'):
    ora_api = 'os_eqm.ldap_sync_api'
    groups_sql = f'begin {ora_api}.p_get_group(:xml, :code, :msg, :domain); end;'
    persons_sql = f'begin {ora_api}.p_get_person(:xml, :code, :msg, :domain); end;'
    memberships_sql = f'begin {ora_api}.p_get_memberships(:person_guid, :group_guid, :code, :msg, :domain); end;'
    run_sync_sql = f'begin {ora_api}.p_run_sync(:code, :msg, 1, :domain); end;'

    def __init__(self, cfg: Config, **kwargs):
        self.cfg = cfg

    def save(self, cur: cx_Oracle.Cursor, sql: str, items: list):
        if sql in [self.groups_sql, self.persons_sql]:
            target = 'group' if sql == self.groups_sql else 'person'
            for xml in items:
                code = cur.var(int)
                msg = cur.var(str)
                try:
                    cur.execute(self.groups_sql if target == 'groups' else self.persons_sql, xml=xml, code=code, msg=msg,
                                domain=self.cfg.ldap.domain)
                    if code.getvalue() != 0:
                        logging.getLogger().error(f'save {target} {xml} error: {msg.getvalue()}')
                except cx_Oracle.DatabaseError as e:
                    error, = e.args
                    msg = re.search(r'^ORA.\d+:\s(.*)', error.message)
                    msg = msg.group(1) if msg else error.message
                    logging.getLogger().error(f'save {target} {xml} error: {msg}')
        else:
            for person_guid, group_guid in items:
                code = cur.var(int)
                msg = cur.var(str)
                try:
                    cur.execute(self.memberships_sql, person_guid=person_guid, group_guid=group_guid, code=code, msg=msg,
                                domain=self.cfg.ldap.domain)
                    if code.getvalue() != 0:
                        logging.getLogger().error(
                            f'save membership  {person_guid=} - {group_guid=} error: {msg.getvalue()})')
                except cx_Oracle.DatabaseError as e:
                    logging.exception(e)
                    error, = e.args
                    msg = re.search(r'^ORA.\d+:\s(.*)', error.message)
                    msg = msg.group(1) if msg else error.message
                    logging.getLogger().error(f'save membership {person_guid=} - {group_guid=} error: {msg}')

    def save_and_sync(self, groups: list, persons: list, memberships: list):
        try:
            with cx_Oracle.connect(user=self.cfg.oracle.user, password=self.cfg.oracle.password.get_secret_value(), dsn=self.cfg.oracle.dsn,
                                   encoding='UTF-8') as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    self.save(cur, self.groups_sql, groups)
                    self.save(cur, self.persons_sql, persons)
                    self.save(cur, self.memberships_sql, memberships)
                    code = cur.var(int)
                    msg = cur.var(str)
                    cur.execute(self.run_sync_sql, code=code, msg=msg, domain=self.cfg.ldap.domain)
                    if code.getvalue() != 0:
                        logging.getLogger().error(
                            f'run sync error: {msg.getvalue()} | (code={code.getvalue()})')
                    conn.commit()
                logging.getLogger().info('sync success')
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            msg = re.search(r'^ORA.\d+:\s(.*)', error.message)
            msg = msg.group(1) if msg else error.message
            logging.getLogger().error(f'save to database error: {msg}')
