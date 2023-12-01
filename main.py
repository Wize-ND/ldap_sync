import hashlib
import pprint
import logging
import sys
import time
import uuid
from xml.dom import minidom
import ldap, ldap.resiter, ldap.controls
from pydantic import ValidationError
from config import Config
import yaml

from db import Database

common_attrs = ['cn', 'objectGUID', 'objectCategory', 'memberOf']
pp = pprint.PrettyPrinter(sort_dicts=False)


class MyLDAPObject(ldap.ldapobject.LDAPObject, ldap.resiter.ResultProcessor):
    pass


def to_xml(object_dict: dict):
    root = minidom.Document()
    xml = root.createElement('object')
    root.appendChild(xml)
    for k, v in object_dict.items():
        child = root.createElement(k)
        child.appendChild(root.createTextNode(v))
        xml.appendChild(child)
    # return root.toprettyxml(indent="\t", encoding='UTF-8').decode()
    return root.toxml(encoding='UTF-8').decode()


def generate_credentials(ldap_guid: str, key: str) -> tuple:
    def baseN(num, b, numerals='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_'):
        return ((num == 0) and numerals[0]) or (baseN(num // b, b, numerals).lstrip(numerals[0]) + numerals[num % b])

    return f'L0_{baseN(int(ldap_guid, 16), 37)}', f'P0_{baseN(int(hashlib.md5((ldap_guid + key).encode()).hexdigest(), 16), 37)}'


if __name__ == '__main__':
    ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_NEVER)
    log = logging.getLogger()
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                        stream=sys.stdout,
                        datefmt="%Y-%m-%d %H:%M:%S")
    log.info('starting sync main loop')
    while True:
        try:
            cfg = Config.model_validate(yaml.safe_load(open('config.yml')))  # type: Config
        except ValidationError as e:
            log.critical(f'Config load error: \n{e}\n retry in 60 sec.')
            time.sleep(60)
            continue

        log.setLevel(cfg.logging_level)
        log.info('start')
        try:
            ldap_conn = MyLDAPObject(cfg.ldap.host)
            ldap_conn.set_option(ldap.OPT_REFERRALS, 0)
            ldap_conn.simple_bind_s(cfg.ldap.bind_dn, cfg.ldap.password)
        except ldap.LDAPError as e:
            log.debug(e)
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

        groups = {}
        for res_type, res_data, res_msgid, res_controls in ldap_conn.allresults(groups_search):
            for dn, attrs in res_data:
                if not dn:
                    continue  # it's referral, skip
                guid = uuid.UUID(bytes_le=attrs['objectGUID'][0])
                groups[dn] = dict(dn=dn, objectGUID=str(guid).upper())
                for attr in attrs:
                    if attr in ['member', 'memberOf', 'objectGUID']:
                        continue
                    if attr and isinstance(attrs[attr], list):
                        groups[dn][attr] = attrs[attr][0].decode()

        persons = []
        memberships = []
        pages = 0
        page_control = ldap.controls.SimplePagedResultsControl(True, size=cfg.ldap.page_size, cookie='')
        while True:
            pages += 1
            if pages > 999:
                raise Exception('users search pages > 999 infinite loop')

            users_search = ldap_conn.search_ext(base=cfg.ldap.base_user_dn, scope=ldap.SCOPE_SUBTREE,
                                                filterstr=cfg.ldap.filter_users.format('*'),
                                                attrlist=common_attrs + cfg.ldap.user_attrs, serverctrls=[page_control])

            res_type, res_data, res_msgid, serverctrls = ldap_conn.result3(users_search)
            controls = [control for control in serverctrls
                        if control.controlType == ldap.controls.SimplePagedResultsControl.controlType]

            for dn, attrs in res_data:
                if not dn:
                    continue  # it's referral, skip
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

            if not controls:
                log.warning('The server ignores RFC 2696 control')
                break
            if not controls[0].cookie:  # end of pages
                break
            page_control.cookie = controls[0].cookie

        log.debug(f'{pages=}')
        ldap_conn.unbind_s()
        groups_xmls = [to_xml(group) for _, group in groups.items()]
        persons_xmls = [to_xml(person) for person in persons]
        # log.debug(f'groups:\n{pp.pformat(groups)}')
        log.debug(f'groups xml\'s:\n {groups_xmls}')
        # log.debug(f'persons:\n{pp.pformat(persons)}')
        log.debug(f'persons xml\'s:\n {persons_xmls}')
        log.info(f'search results: groups({len(groups)}), persons({len(persons)}), memberships({len(memberships)})')

        db = Database(driver='pg_driver' if cfg.pg else 'oracle_driver', cfg=cfg)  # type: Database
        if cfg.dbg_no_save:
            log.info(f'{cfg.dbg_no_save=} results not saved!')
        else:
            db.save_and_sync(groups=groups_xmls,
                             persons=persons_xmls,
                             memberships=memberships)
        log.debug(f'waiting for {cfg.ldap.sync_interval}sec for next sync cycle')
        time.sleep(cfg.ldap.sync_interval)
