import hashlib
import pprint
import logging
import sys
import time
import uuid

import ldap.resiter
from pydantic import ValidationError
from config import Config
import yaml

from db import Database

common_attrs = ['cn', 'objectGUID', 'objectCategory', 'memberOf']
pp = pprint.PrettyPrinter(sort_dicts=False)


class MyLDAPObject(ldap.ldapobject.LDAPObject, ldap.resiter.ResultProcessor):
    pass


def generate_credentials(ldap_guid: str, key: str) -> tuple:
    def baseN(num, b, numerals='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_'):
        return ((num == 0) and numerals[0]) or (baseN(num // b, b, numerals).lstrip(numerals[0]) + numerals[num % b])

    return f'L0_{baseN(int(ldap_guid, 16), 37)}', f'P0_{baseN(int(hashlib.md5((ldap_guid + key).encode()).hexdigest(), 16), 37)}'


if __name__ == '__main__':
    log = logging.getLogger()
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                        stream=sys.stdout,
                        datefmt="%Y-%m-%d %H:%M:%S")
    log.info('starting sync main loop')
    while True:
        try:
            cfg = Config.parse_obj(yaml.safe_load(open('config.yml')))  # type: Config
        except ValidationError as e:
            log.critical(f'Config load error: \n{e}\n retry in 60 sec.')
            time.sleep(60)
            continue

        log.setLevel(cfg.logging_level)
        log.info('start')
        try:
            ldap_conn = MyLDAPObject(f'ldap://{cfg.ldap.host}')
            ldap_conn.set_option(ldap.OPT_REFERRALS, 0)
            ldap_conn.simple_bind_s(cfg.ldap.bind_dn, cfg.ldap.password)
        except ldap.LDAPError as e:
            err = e
            if len(e.args) > 0 and "desc" in e.args[0]:
                err = e.args[0]["desc"]
            log.critical(f'ldap initialize/bind: {err}, retry in {cfg.error_retry_interval} sec.')
            time.sleep(cfg.error_retry_interval)
            continue

        # Ldap connect successful
        groups_search = ldap_conn.search(base=cfg.ldap.base_group_dn, scope=ldap.SCOPE_SUBTREE,
                                         filterstr=cfg.ldap.filter_groups,
                                         attrlist=common_attrs + cfg.ldap.group_attrs)
        users_search = ldap_conn.search(base=cfg.ldap.base_user_dn, scope=ldap.SCOPE_SUBTREE,
                                        filterstr=cfg.ldap.filter_users.format('*'),
                                        attrlist=common_attrs + cfg.ldap.user_attrs)
        groups = {}
        for res_type, res_data, res_msgid, res_controls in ldap_conn.allresults(groups_search):
            for dn, attrs in res_data:
                guid = uuid.UUID(bytes_le=attrs['objectGUID'][0])
                groups[dn] = dict(dn=dn, objectGUID=str(guid).upper())
                for attr in attrs:
                    if attr in ['member', 'memberOf', 'objectGUID']:
                        continue
                    if attr and isinstance(attrs[attr], list):
                        groups[dn][attr] = attrs[attr][0].decode()
        persons = []
        memberships = []
        for res_type, res_data, res_msgid, res_controls in ldap_conn.allresults(users_search):
            for dn, attrs in res_data:
                if 'memberOf' in attrs and attrs['memberOf']:
                    guid = uuid.UUID(bytes_le=attrs['objectGUID'][0])
                    in_group = False

                    for memberOf in [m.decode() for m in attrs['memberOf']]:
                        if memberOf in groups:
                            memberships.append((str(guid).upper(), groups[memberOf]['objectGUID']))
                            in_group = True

                    if not in_group:
                        log.debug(f'person {dn} does not have membership in groups, skipping')
                        continue

                    person = dict(dn=dn, objectGUID=str(guid).upper())
                    person['login'], person['password'] = generate_credentials(guid.hex.upper(), cfg.ldap.key)
                    for attr in attrs:
                        if attr in ['member', 'memberOf', 'objectGUID']:
                            continue
                        if attr and isinstance(attrs[attr], list):
                            person[attr] = attrs[attr][0].decode()
                    persons.append(person)
                else:
                    log.debug(f'person {dn} does not have memberships at all, skipping')
        ldap_conn.unbind_s()

        log.debug(f'groups:\n{pp.pformat(groups)}')
        log.debug(f'persons:\n{pp.pformat(persons)}')
        log.info(f'search results: groups({len(groups)}), persons({len(persons)}), memberships({len(memberships)})')
        db = Database(driver='pg_driver' if cfg.pg else 'oracle_driver', cfg=cfg)  # type: Database
        # todo save all in database
        break
        time.sleep(cfg.ldap.sync_interval)
