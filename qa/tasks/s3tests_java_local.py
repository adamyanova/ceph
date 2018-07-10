"""
Task for running RGW S3 tests with the AWS Java SDK
"""
from cStringIO import StringIO
import logging

import base64
import os
import random
import string
import yaml
import socket
import getpass

from teuthology import misc as teuthology
from teuthology.exceptions import ConfigError
from teuthology.task import Task
from teuthology.orchestra import run
from teuthology.orchestra.remote import Remote

log = logging.getLogger(__name__)


class S3tests_java_local(Task):
    """
    Download and install S3TestsGo
    This will require golang
    """

    def __init__(self, ctx, config):
        super(S3tests_java_local, self).__init__(ctx, config)
        self.log = log
        log.debug('S3 Tests Java Local: __INIT__ ')
        assert hasattr(ctx, 'rgw'), 'S3tests_java must run after the rgw task'
        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])
        if self.all_clients is None:
            self.all_clients = 'client.0'
        self.users = {'s3main': 'tester',
                      's3alt': 'johndoe', 'tenanted': 'testx$tenanteduser'}

    def setup(self):
        super(S3tests_java_local, self).setup()
        log.debug('S3 Tests Java Local: SETUP')
        for client in self.all_clients:
            self.download_test_suite(client)
            self.install_required_packages(client)

    def begin(self):
        super(S3tests_java_local, self).begin()
        log.debug('S3 Tests Java Local: BEGIN')
        log.debug('S3 Tests Java Local: ctx is: {ctx}'.format(ctx=self.ctx))
        for (host, roles) in self.ctx.cluster.remotes.iteritems():
            log.info(
                'S3 Tests Java Local: Cluster config is: {cfg}'.format(cfg=roles))
            log.info('S3 Tests Java Local: Host is: {host}'.format(host=host))
        self.create_users()
        self.run_tests()

    def teardown(self):
        super(S3tests_java_local, self).teardown()
        log.debug('S3 Tests Java Local: TEARDOWN')
        for client in self.all_clients:
            self.remove_tests(client)
            self.delete_users(client)

    def download_test_suite(self, client):
        log.info("S3 Tests Java Local: Downloading test suite...")
        testdir = teuthology.get_testdir(self.ctx)
        branch = 'master'
        repo = 'https://github.com/ceph/java_s3tests.git'
        if client in self.config and self.config[client] is not None:
            if 'branch' in self.config[client] and self.config[client]['branch'] is not None:
                branch = self.config[client]['branch']
            if 'repo' in self.config[client] and self.config[client]['repo'] is not None:
                repo = self.config[client]['repo']
        self.ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', branch,
                repo,
                '{tdir}/s3-tests-java'.format(tdir=testdir),
            ],
            stdout=StringIO()
        )
        if client in self.config and self.config[client] is not None:
            if 'sha1' in self.config[client] and self.config[client]['sha1'] is not None:
                self.ctx.cluster.only(client).run(
                    args=[
                        'cd', '{tdir}/s3-tests-java'.format(tdir=testdir),
                        run.Raw('&&'),
                        'git', 'reset', '--hard', self.config[client]['sha1'],
                    ],
                )

            if 'debug' in self.config[client]:
                self.ctx.cluster.only(client).run(
                    args=['mkdir', '-p',
                        '{tdir}/s3-tests-java/src/main/resources/'.format(
                            tdir=testdir),
                        run.Raw('&&'),
                        'cp',
                        '{tdir}/s3-tests-java/log4j.properties'.format(
                            tdir=testdir),
                        '{tdir}/s3-tests-java/src/main/resources/'.format(
                            tdir=testdir)
                        ]
                )

    def install_required_packages(self, client):
        log.info("S3 Tests Java Local: Installing required packages...")
        testdir = teuthology.get_testdir(self.ctx)
        self.ctx.cluster.only(client).run(
            args=['{tdir}/s3-tests-java/bootstrap.sh'.format(tdir=testdir)],
            stdout=StringIO()
        )

        # The openssl_keys task generates a self signed certificate for each client
        # It is located in the {testdir}/ca/ and should be added to the java keystore
        for task in self.ctx.config['tasks']:
            if 'openssl_keys' in task:
                endpoint = self.ctx.rgw.role_endpoints.get(client)
                path = 'lib/security/cacerts'
                self.ctx.cluster.only(client).run(
                    args=['sudo',
                          'keytool',
                          '-import', '-alias', '{alias}'.format(
                              alias=endpoint.hostname),
                          '-keystore',
                          run.Raw(
                              '$(readlink -e $(dirname $(readlink -e $(which keytool)))/../{path})'.format(path=path)),
                          '-file', '{tdir}/ca/rgw.{client}.crt'.format(
                              tdir=testdir, client=client),
                          '-storepass', 'changeit',
                          ],
                    stdout=StringIO()
                )

    def create_users(self):
        """
        Create a main and an alternative s3 user.
        """
        log.info("S3 Tests Java Local: Creating users...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            endpoint = self.ctx.rgw.role_endpoints.get(client)
            username = getpass.getuser()
            log.info("S3 Tests Java Local: username is: {username}".format(
                username=username))
            os.system("scp {username}@{host}:{tdir}/s3-tests-java/s3tests.teuth.config.yaml /home/{username}/".format(
                host=endpoint.hostname, tdir=testdir, username=username))
            s3tests_conf = teuthology.config_file(
                '/home/{username}/s3tests.teuth.config.yaml'.format(username=username))
            log.info("S3 Tests Java Local: s3tests_conf is {s3cfg}".format(
                s3cfg=s3tests_conf))
            for section, user in self.users.items():
                if section in s3tests_conf:
                    userid = '{user}.{client}'.format(user=user, client=client)
                    log.debug(
                        'S3 Tests Java Local: Creating user {userid}'.format(userid=userid))
                    self._config_user(s3tests_conf=s3tests_conf,
                                      section=section, user=userid, client=client)
                    cluster_name, daemon_type, client_id = teuthology.split_role(
                        client)
                    client_with_id = daemon_type + '.' + client_id
                    args = [
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'user', 'create',
                        '--uid', s3tests_conf[section]['user_id'],
                        '--display-name', s3tests_conf[section]['display_name'],
                        '--access-key', s3tests_conf[section]['access_key'],
                        '--secret', s3tests_conf[section]['access_secret'],
                        '--email', s3tests_conf[section]['email'],
                        '--cluster', cluster_name,
                    ]
                    log.info('{args}'.format(args=args))
                    self.ctx.cluster.only(client).run(
                        args=args,
                        stdout=StringIO()
                    )
                else:
                    self.users.pop(section)
            self._write_cfg_file(s3tests_conf, client)
            os.system(
                "rm -rf /home/{username}/s3tests.teuth.config.yaml".format(username=username))

    def _config_user(self, s3tests_conf, section, user, client):
        """
        Generate missing users data for this section by stashing away keys, ids, and
        email addresses.
        """

        self._set_cfg_entry(
            s3tests_conf[section], 'user_id', '{user}'.format(user=user))
        self._set_cfg_entry(
            s3tests_conf[section], 'email', '{user}_test@test.test'.format(user=user))
        self._set_cfg_entry(
            s3tests_conf[section], 'display_name', 'Ms. {user}'.format(user=user))
        access_key = ''.join(random.choice(string.ascii_uppercase)
                             for i in range(20))
        secret = base64.b64encode(os.urandom(40))
        self._set_cfg_entry(
            s3tests_conf[section], 'access_key', '{ak}'.format(ak=access_key))
        self._set_cfg_entry(
            s3tests_conf[section], 'access_secret', '{sk}'.format(sk=secret))
        self._set_cfg_entry(s3tests_conf[section], 'region', 'mexico')
        self._set_cfg_entry(s3tests_conf[section], 'bucket', 'bucket1')

        endpoint = self.ctx.rgw.role_endpoints.get(client)
        self._set_cfg_entry(s3tests_conf[section], 'endpoint', '{ip}:{port}'.format(
            ip=endpoint.hostname, port=443))
        self._set_cfg_entry(s3tests_conf[section], 'port', 443)
        self._set_cfg_entry(
            s3tests_conf[section], 'is_secure', True)  # if endpoint.cert else False

        log.info("S3 Tests Java Local: s3tests_conf[{sect}] is {s3cfg}".format(
            sect=section, s3cfg=s3tests_conf[section]))
        log.debug('S3 Tests Java Local: Setion, User = {sect}, {user}'.format(
            sect=section, user=user))

    def _write_cfg_file(self, cfg_dict, client):
        testdir = teuthology.get_testdir(self.ctx)
        (remote,) = self.ctx.cluster.only(client).remotes.keys()
        with open('tmp.yaml', 'w') as outfile:
            yaml.dump(cfg_dict, outfile, default_flow_style=False)

        conf_fp = StringIO()
        with open('tmp.yaml', 'r') as infile:
            for line in infile:
                conf_fp.write(line)

        teuthology.write_file(
            remote=remote,
            path='{tdir}/archive/s3-tests-java.{client}.conf'.format(
                tdir=testdir, client=client),
            data=conf_fp.getvalue(),
        )
        os.remove('tmp.yaml')

    def _set_cfg_entry(self, cfg_dict, key, value):
        if not (key in cfg_dict):
            cfg_dict.setdefault(key, value)
        elif cfg_dict[key] is None:
            cfg_dict[key] = value

    def run_tests(self):
        log.info("S3 Tests Java Local: Running tests...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            self.ctx.cluster.only(client).run(
                args=['cp',
                      '{tdir}/archive/s3-tests-java.{client}.conf'.format(
                          tdir=testdir, client=client),
                      '{tdir}/s3-tests-java/config.properties'.format(
                          tdir=testdir)
                      ],
                stdout=StringIO()
            )
            args = ['cd',
                    '{tdir}/s3-tests-java'.format(tdir=testdir),
                    run.Raw('&&'),
                    '/opt/gradle/gradle-4.7/bin/gradle', 'clean', 'test',
                    '-S', '--console', 'verbose', '--no-build-cache',
                    ]
            if client in self.config and self.config[client] is not None:
                if 'extra_args' in self.config[client]:
                    args.extend(self.config[client]['extra_args'])
                if 'debug' in self.config[client]:
                    args += ['--debug']
                if 'log_fwd' in self.config[client]:
                    log_name = '{tdir}/s3tests_log.txt'.format(tdir=testdir)
                    if self.config[client]['log_fwd'] is not None:
                        log_name = self.config[client]['log_fwd']
                    args += [run.Raw('>>'),
                            log_name]

            self.ctx.cluster.only(client).run(
                args=args,
                stdout=StringIO()
            )

    def remove_tests(self, client):
        log.info('S3 Tests Java Local: Removing s3-tests-java...')
        testdir = teuthology.get_testdir(self.ctx)
        self.ctx.cluster.only(client).run(
            args=[
                'rm',
                '-rf',
                '{tdir}/s3-tests-java'.format(tdir=testdir),
            ],
            stdout=StringIO()
        )

    def delete_users(self, client):
        log.info("S3 Tests Java Local: Deleting users...")
        testdir = teuthology.get_testdir(self.ctx)
        for section, user in self.users.items():
            userid = '{user}.{client}'.format(user=user, client=client)
            self.ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client,
                    'user', 'rm',
                    '--uid', userid,
                    '--purge-data',
                    '--cluster', 'ceph',
                ],
                stdout=StringIO()
            )


task = S3tests_java_local
