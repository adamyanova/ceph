"""
Task for running RGW S3 tests with the AWS GO SDK
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


class S3tests_go_local(Task):
    """
    Download and install S3TestsGo
    This will require golang
    """

    def __init__(self, ctx, config):
        super(S3tests_go_local, self).__init__(ctx, config)
        self.log = log
        log.debug('S3 Tests GO Local: __INIT__ ')
        assert hasattr(ctx, 'rgw'), 'S3tests_go must run after the rgw task'
        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])
        if self.all_clients is None:
            self.all_clients = 'client.0'
        self.users = {'s3main': 'tester', 's3alt': 'johndoe'}

    def setup(self):
        super(S3tests_go_local, self).setup()
        log.debug('S3 Tests GO Local: SETUP')
        for client in self.all_clients:
            self.download_test_suite(client)
            self.install_required_packages(client)

    def begin(self):
        super(S3tests_go_local, self).begin()
        log.debug('S3 Tests GO Local: BEGIN')
        log.debug('S3 Tests GO Local: ctx is: {ctx}'.format(ctx=self.ctx))
        for (host, roles) in self.ctx.cluster.remotes.iteritems():
            log.info(
                'S3 Tests GO Local: Cluster config is: {cfg}'.format(cfg=roles))
            log.info('S3 Tests GO Local: Host is: {host}'.format(host=host))
        self.create_users()
        self.run_tests()

    def teardown(self):
        super(S3tests_go_local, self).teardown()
        log.debug('S3 Tests GO Local: TEARDOWN')
        for client in self.all_clients:
            self.remove_tests(client)
            self.delete_users(client)

    def download_test_suite(self, client):
        log.info("S3 Tests GO Local: Downloading test suite...")
        testdir = teuthology.get_testdir(self.ctx)
        branch = 'master'
        repo = 'https://github.com/ceph/go_s3tests.git'
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
                '{tdir}/s3-tests-go'.format(tdir=testdir),
            ],
            stdout=StringIO()
        )
        if client in self.config and self.config[client] is not None:
            if 'sha1' in self.config[client] and self.config[client]['sha1'] is not None:
                self.ctx.cluster.only(client).run(
                    args=[
                        'cd', '{tdir}/s3-tests-go'.format(tdir=testdir),
                        run.Raw('&&'),
                        'git', 'reset', '--hard', self.config[client]['sha1'],
                    ],
                )

    def install_required_packages(self, client):
        log.info("S3 Tests GO Local: Installing required packages...")
        testdir = teuthology.get_testdir(self.ctx)
        self.ctx.cluster.only(client).run(
            args=['{tdir}/s3-tests-go/bootstrap.sh'.format(tdir=testdir)],
            stdout=StringIO()
        )
        # remote_user = teuthology.get_test_user()
        local_user = getpass.getuser()
        log.debug("S3 Tests GO Local: Remote user is {remote}".format(remote=local_user))
        self.gopath = '/home/{remote}/go'.format(remote=local_user)
        self._setup_golang(client)
        self._install_tests_utils(client)

    def _setup_golang(self, client):
        log.info("S3 Tests Go: Setting up golang...")
        self.ctx.cluster.only(client).run(
            args = ['mkdir', '-p',
                self.gopath,
                run.Raw('&&'),
                'GOPATH={path}'.format(path = self.gopath)
            ],
            stdout = StringIO()
            )

    def _install_tests_utils(self, client):
        log.info("S3 Tests Go: Installing tests dependencies...")
        testdir = teuthology.get_testdir(self.ctx)
        # explicit download of stretchr/testify is required
        self.ctx.cluster.only(client).run(
                args = ['cd', 
                    '{tdir}/s3-tests-go'.format(tdir = testdir),
                    run.Raw(';'),
                    'go', 'get', '-v', '-d', './...',
                    run.Raw(';'),
                    'go', 'get', '-v', 'github.com/stretchr/testify',
                ],
                stdout = StringIO()
            )


    def create_users(self):
        """
        Create a main and an alternative s3 user.
        """
        log.info("S3 Tests GO Local: Creating users...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            endpoint = self.ctx.rgw.role_endpoints.get(client)
            local_user = getpass.getuser()
            remote_user = getpass.getuser() #teuthology.get_test_user()
            log.info("S3 Tests GO Local: username is: {username}".format(
                username=local_user))
            os.system("scp {remote}@{host}:{tdir}/s3-tests-go/s3tests.teuth.config.yaml /home/{local}/".format(
                host=endpoint.hostname, tdir=testdir, remote=remote_user, local=local_user))
            s3tests_conf = teuthology.config_file(
                '/home/{local}/s3tests.teuth.config.yaml'.format(local=local_user))
            log.info("S3 Tests GO Local: s3tests_conf is {s3cfg}".format(
                s3cfg=s3tests_conf))
            self._s3tests_cfg_default_section(client = client, cfg_dict = s3tests_conf)
            for section, user in self.users.items():
                if section in s3tests_conf:
                    s3_user_id = '{user}.{client}'.format(user=user, client=client)
                    log.debug(
                        'S3 Tests GO Local: Creating user {s3_user_id}'.format(s3_user_id=s3_user_id))
                    self._config_user(s3tests_conf=s3tests_conf,
                                      section=section, user=s3_user_id, client=client)
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
                "rm -rf /home/{local}/s3tests.teuth.config.yaml".format(local=local_user))

    def _s3tests_cfg_default_section(self, client, cfg_dict):
        log.info("S3 Tests Go: Add DEFAULT section")
        endpoint = self.ctx.rgw.role_endpoints.get(client)
        assert endpoint, 'S3 Tests Go: No RGW endpoint for {clt}'.format(clt = client) 

        cfg_dict['DEFAULT']['host'] = endpoint.hostname
        cfg_dict['DEFAULT']['port'] = 443 #endpoint.port
        cfg_dict['DEFAULT']['is_secure'] = True #if endpoint.cert else False

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
        self._set_cfg_entry(s3tests_conf[section], 'region', 'us-east-1')
        self._set_cfg_entry(s3tests_conf[section], 'bucket', 'bucket1')
        self._set_cfg_entry(s3tests_conf[section], 'SSE', 'AES256')

        endpoint = self.ctx.rgw.role_endpoints.get(client)
        self._set_cfg_entry(s3tests_conf[section], 'endpoint', '{ip}:{port}'.format(
            ip=endpoint.hostname, port=443))
        self._set_cfg_entry(s3tests_conf[section], 'port', 443)
        self._set_cfg_entry(
            s3tests_conf[section], 'is_secure', True)  # if endpoint.cert else False

        log.info("S3 Tests GO Local: s3tests_conf[{sect}] is {s3cfg}".format(
            sect=section, s3cfg=s3tests_conf[section]))
        log.debug('S3 Tests GO Local: Setion, User = {sect}, {user}'.format(
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
            path='{tdir}/archive/s3-tests-go.{client}.conf'.format(
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
        log.info("S3 Tests GO Local: Running tests...")
        testdir = teuthology.get_testdir(self.ctx)
        for client in self.all_clients:
            self.ctx.cluster.only(client).run(
                args=['cp',
                      '{tdir}/archive/s3-tests-go.{client}.conf'.format(
                          tdir=testdir, client=client),
                      '{tdir}/s3-tests-go/config.yaml'.format(
                          tdir=testdir)
                      ],
                stdout=StringIO()
            )
            args = ['cd',
                    '{tdir}/s3-tests-go/s3tests'.format(tdir=testdir),
                    run.Raw('&&'),
                    'go', 'test', '-v'
                    ]
            extra_args = []
            if client in self.config and self.config[client] is not None:
                if 'extra_args' in self.config[client]:
                    extra_args.extend(self.config[client]['extra_args'])
                if 'log_fwd' in self.config[client]:
                    log_name = '{tdir}/s3tests_log.txt'.format(tdir=testdir)
                    if self.config[client]['log_fwd'] is not None:
                        log_name = self.config[client]['log_fwd']
                    extra_args += [run.Raw('>>'),
                            log_name]

            test_groups = ['awsv4', 'bucket', 'object']

            for gr in test_groups:
                self.ctx.cluster.only(client).run(
                    args= args + ['-run'] + [gr] + extra_args,
                    stdout=StringIO()
                )
                self.ctx.cluster.only(client).run(
                    args=['radosgw-admin', 'gc', 'process', '--include-all'],
                    stdout=StringIO()
                )
                self.ctx.cluster.only(client).run(
                    args=['radosgw-admin', 'gc', 'process', '--include-all'],
                    stdout=StringIO()
                )

    def remove_tests(self, client):
        log.info('S3 Tests GO Local: Removing s3-tests-go...')
        testdir = teuthology.get_testdir(self.ctx)
        self.ctx.cluster.only(client).run(
            args=[
                'rm',
                '-rf',
                '{tdir}/s3-tests-go'.format(tdir=testdir),
            ],
            stdout=StringIO()
        )

    def delete_users(self, client):
        log.info("S3 Tests GO Local: Deleting users...")
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


task = S3tests_go_local
