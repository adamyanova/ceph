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


class S3tests_java(Task):
    """
    Download and install S3TestsGo
    This will require golang
    """

    def __init__(self, ctx, config):
        super(S3tests_java, self).__init__(ctx, config)
        self.log = log
        log.debug('S3 Tests Java: __INIT__ ')
        assert hasattr(ctx, 'rgw'), 'S3tests_java must run after the rgw task'
        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = [clients[0]]
        self.users = {'s3main': 'tester',
                      's3alt': 'johndoe', 'tenanted': 'tenant'}

    def setup(self):
        super(S3tests_java, self).setup()
        log.debug('S3 Tests Java: SETUP')
        for client in self.all_clients:
            self.download_test_suite(client)
            self.install_required_packages(client)

    def begin(self):
        super(S3tests_java, self).begin()
        log.debug('S3 Tests Java: BEGIN')
        log.debug('S3 Tests Java: ctx is: {ctx}'.format(ctx=self.ctx))
        for (host, roles) in self.ctx.cluster.remotes.iteritems():
            log.info(
                'S3 Tests Java: Cluster config is: {cfg}'.format(cfg=roles))
            log.info('S3 Tests Java: Host is: {host}'.format(host=host))
        self.create_users()
        self.run_tests()

    def teardown(self):
        super(S3tests_java, self).teardown()
        log.debug('S3 Tests Java: TEARDOWN')
        for client in self.all_clients:
            self.remove_tests(client)
            self.delete_users(client)

    def download_test_suite(self, client):
        log.info("S3 Tests Java: Downloading test suite...")
        testdir = teuthology.get_testdir(self.ctx)
        branch = None
        repo = None
        if 's3tests_branch' in self.ctx.config:
            branch = self.ctx.config['s3tests_branch']
        if 's3tests_repo' in self.ctx.config:
            repo = self.ctx.config['s3tests_repo']
        if branch is None:
            branch = 'master'
        if repo is None:
            repo = 'https://github.com/adamyanova/java_s3tests.git'
        self.ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', branch,
                repo,
                '{tdir}/s3-tests-java'.format(tdir=testdir),
            ],
            stdout=StringIO()
        )
        if 'debug' in self.ctx.config:
            self.ctx.cluster.only(client).run(
                args=['cp',
                      '{tdir}/s3-tests-java/log4j.properties'.format(tdir=testdir),
                      '{tdir}/s3-tests-java/src/main/resources/'.format(tdir=testdir)
                      ]
            )

    def install_required_packages(self, client):
        log.info("S3 Tests Java: Installing required packages...")
        testdir = teuthology.get_testdir(self.ctx)
        self.ctx.cluster.only(client).run(
            args=['{tdir}/s3-tests-java/bootstrap.sh'.format(tdir=testdir)],
            stdout=StringIO()
        )
        username = getpass.getuser()
        endpoint = self.ctx.rgw.role_endpoints.get(client)
        self.ctx.cluster.only(client).run(
            args=['sudo',
                  'keytool',
                  '-import', '-alias', '{alias}'.format(
                      alias=endpoint.hostname),
                  '-keystore', '/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.171-8.b10.el7_5.x86_64/jre/lib/security/cacerts',
                  '-file', '/home/ubuntu/cephtest/ca/rgw.{client}.crt'.format(
                      client=client),
                  '-storepass', 'changeit',
                  ],
            stdout=StringIO()
        )

    def create_users(self):
        """
        Create a main and an alternative s3 user.
        """
        log.info("S3 Tests Java: Creating users...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            endpoint = self.ctx.rgw.role_endpoints.get(client)
            username = getpass.getuser()
            log.info("S3 Tests Java: username is: {username}".format(
                username=username))
            os.system("scp {username}@{host}:{tdir}/s3-tests-java/s3tests.teuth.config.yaml /home/{username}/".format(
                host=endpoint.hostname, tdir=testdir, username=username))
            s3tests_conf = teuthology.config_file(
                '/home/{username}/s3tests.teuth.config.yaml'.format(username=username))
            log.info("S3 Tests Java: s3tests_conf is {s3cfg}".format(
                s3cfg=s3tests_conf))
            self._s3tests_cfg_default_section(
                client=client, cfg_dict=s3tests_conf)
            for section, user in self.users.items():
                if section in s3tests_conf:
                    userid = '{user}.{client}'.format(user=user, client=client)
                    log.debug(
                        'S3 Tests Java: Creating user {userid}'.format(userid=userid))
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

    def _s3tests_cfg_default_section(self, client, cfg_dict):
        log.info("S3 Tests Java: Add DEFAULT section")
        endpoint = self.ctx.rgw.role_endpoints.get(client)
        assert endpoint, 'S3 Tests Java: No RGW endpoint for {clt}'.format(
            clt=client)

        cfg_dict['DEFAULT']['host'] = socket.gethostbyname(endpoint.hostname)
        cfg_dict['DEFAULT']['port'] = endpoint.port
        cfg_dict['DEFAULT']['is_secure'] = True if endpoint.cert else False

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
        self._set_cfg_entry(
            s3tests_conf[section], 'kmskeyid', 'barbican_key_id')
        self._set_cfg_entry(s3tests_conf[section], 'SSE', 'AES256')
        self._set_cfg_entry(s3tests_conf[section], 'region', 'mexico')
        self._set_cfg_entry(s3tests_conf[section], 'bucket', 'bucket1')

        endpoint = self.ctx.rgw.role_endpoints.get(client)
        self._set_cfg_entry(s3tests_conf[section], 'endpoint', '{ip}:{port}'.format(
            ip=socket.gethostbyname(endpoint.hostname), port=endpoint.port))
        self._set_cfg_entry(
            s3tests_conf[section], 'host', socket.gethostbyname(endpoint.hostname))
        self._set_cfg_entry(s3tests_conf[section], 'port', endpoint.port)
        self._set_cfg_entry(
            s3tests_conf[section], 'is_secure', True if endpoint.cert else False)

        log.info("S3 Tests Java: s3tests_conf[{sect}] is {s3cfg}".format(
            sect=section, s3cfg=s3tests_conf[section]))
        log.debug('S3 Tests Java: Setion, User = {sect}, {user}'.format(
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
        log.info("S3 Tests Java: Running tests...")
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
            self.ctx.cluster.only(client).run(
                args=['cd',
                      '{tdir}/s3-tests-java'.format(tdir=testdir),
                      run.Raw('&&'),
                      '/opt/gradle/gradle-4.7/bin/gradle', 'clean', 'test',
                      '-S', '--console', 'verbose', '--rerun-tasks', '--no-build-cache',
                      '--tests', 'ObjectTest.testEncryptionKeySSECNoKey',
                      run.Raw('>>'),
                      'log.txt'
                      ],
                stdout=StringIO()
            )

    def remove_tests(self, client):
        log.info('S3 Tests Java: Removing s3-tests-java...')
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
        log.info("S3 Tests Java: Deleting users...")
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


task = S3tests_java
