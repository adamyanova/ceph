"""
Task for running RGW S3 tests with the AWS Go SDK
"""
from cStringIO import StringIO
from configobj import ConfigObj
import logging

import base64
import os
import random
import string


from teuthology import misc as teuthology
from teuthology.exceptions import ConfigError
from teuthology.task import Task
from teuthology.orchestra import run
from teuthology.orchestra.remote import Remote

log = logging.getLogger(__name__)


class S3tests_go(Task):
    """
    Download and install S3TestsGo
    This will require golang
    """

    def __init__(self, ctx, config):
        super(S3tests_go, self).__init__(ctx, config)
        self.log = log
        log.info('S3 Tests Go: In __init__ step')
        assert hasattr(ctx, 'rgw'), 'S3tests_go must run after the rgw task'

    def setup(self):
        super(S3tests_go, self).setup()
        config = self.config
        log.info('S3 Tests Go: In setup step')
        log.debug('config is: %r', config)
        self.download()
        self.install_packages()
        self.setup_go()
        self.install_tests_dependencies()

    def begin(self):
        super(S3tests_go, self).begin()
        log.info('S3 Tests Go: In begin step')
        ctx = self.ctx
        log.debug('S3 Tests Go: ctx is: %r', ctx)
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        for (host, roles) in cluster.remotes.iteritems():
            log.info('S3 Tests Go: Client {clt} config is: {cfg}'.format(clt = host, cfg = roles))
        self.create_users()
        self.run_tests()
        
    def teardown(self):
        super(S3tests_go, self).teardown()
        log.info('S3 Tests Go: Teardown step')
        self.delete_users()
        self.remove_tests()
        

    def install_packages(self):
        log.info("S3 Tests Go: Installing required packages...")
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        for (host, roles) in cluster.remotes.iteritems():
            cluster.run(
                args=['{tdir}/s3-tests/bootstrap.sh'.format(tdir=testdir)],
                stdout=StringIO()
            )

    def download(self):
        log.info("S3 Tests Go: Downloading test suite...")
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        s3_branches = ['wip-foo']
        for (host, roles) in cluster.remotes.iteritems():
            cluster.run(
                args=['echo', 'S3 Tests Go: Client is {clt}'.format(clt = host)],
                stdout=StringIO()
            )
            cluster.run(
                args=['echo', 'S3 Tests Go: Cluster config is: {cfg}'.format(cfg = roles)],
                stdout=StringIO()
            )
            cluster.run(
                args=[
                    'git', 'clone',
                    '-b', 'master',
                    'https://github.com/adamyanova/go_s3tests.git',
                    '{tdir}/s3-tests'.format(tdir=testdir),
                    ],
                stdout=StringIO()
                )
            cluster.run(
                args=['echo', '{tdir}/s3-tests'.format(tdir=testdir)],
                stdout=StringIO()
            )
            cluster.run(
                args=['ls','{tdir}/s3-tests'.format(tdir=testdir)], 
                stdout=StringIO()
                )

    def setup_go(self):
        log.info("S3 Tests Go: Setting up Go...")
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        for (host, roles) in cluster.remotes.iteritems():
            cluster.run(
                args=['mkdir', '{tdir}/../go'.format(tdir=testdir)],
                stdout=StringIO()
                )
            cluster.run(
                args=['GOPATH=/home/ubuntu/go'],
                stdout=StringIO()
            )

    def install_tests_dependencies(self):
        log.info("S3 Tests Go: Installing tests dependencies...")
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        # explicit download of stretchr/testify is required
        cluster.run(
                args=['cd', 
                    '{tdir}/s3-tests'.format(tdir=testdir),
                    run.Raw(';'),
                    'go', 'get', '-d', './...',
                    run.Raw(';'),
                    'go', 'get', 'github.com/stretchr/testify',
                    ],
                stdout=StringIO()
            )
        cluster.run(
                args=['ls', '/home/ubuntu/go/src/github.com/'],
                stdout=StringIO()
            )

    def run_tests(self):
        log.info("S3 Tests Go: Running tests...")
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        cluster.run(
                args=['cp', 
                    '{tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client='client.0'),
                    # '{tdir}/s3-tests/config.toml.sample'.format(tdir=testdir),
                    '{tdir}/s3-tests/config.toml'.format(tdir=testdir)
                ],
                stdout=StringIO()
            )
        cluster.run(
                args=['cd', 
                    '{tdir}/s3-tests/s3tests'.format(tdir=testdir),
                    run.Raw(';'),
                    'go', 'test', '-v'],
                stdout=StringIO()
            )

    def remove_tests(self):
        log.info('S3 Tests Go: Removing s3-tests...')
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        for (host, roles) in cluster.remotes.iteritems():
            cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/s3-tests'.format(tdir=testdir),
                    # '{tdir}/../go'.format(tdir=testdir)
                    ],
                stdout=StringIO()
                )

    def _config_user(self, s3tests_conf, section, user):
        """
        Configure users for this section by stashing away keys, ids, and
        email addresses.
        """
        log.info("S3 Tests Go: s3tests_conf[{sect}] is {s3cfg}".format(sect=section, s3cfg = s3tests_conf[section]))
        log.debug('S3 Tests Go: Setion, User = {sect}, {user}'.format(sect=section, user=user))
        # s3tests_conf[section].setdefault('user_id', '{user}'.format(user=user))
        s3tests_conf[section].setdefault('email', '{user}_test@test.test'.format(user=user))
        s3tests_conf[section].setdefault('display_name', 'Ms. {user}'.format(user=user))
        if section is 's3main':
            s3tests_conf[section].setdefault('access_key', '0555b35654ad1656d804')
            s3tests_conf[section].setdefault('secret_key', 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==')
        elif section is 's3alt':
            s3tests_conf[section].setdefault('access_key', 'NOPQRSTUVWXYZABCDEFG')
            s3tests_conf[section].setdefault('secret_key', 'nopqrstuvwxyzabcdefghijklmnabcdefghijklm')
        else:
            s3tests_conf[section].setdefault('access_key', ''.join(random.choice(string.ascii_uppercase) for i in range(20)))
            skey = base64.b64encode(os.urandom(40))
            s3tests_conf[section].setdefault('secret_key', '{key}'.format(key=skey))
        s3tests_conf[section].setdefault('kmskeyid', "barbican_key_id")
        s3tests_conf[section].setdefault('bucket', '\"bucket1\"')
        s3tests_conf[section].setdefault('region', '\"us-east-1\"')
        s3tests_conf[section].setdefault('SSE', '\"AES256\"')

    def create_users(self):
        """
        Create a main and an alternative s3 user.
        """
        log.info("S3 Tests Go: Creating users...")
        ctx = self.ctx
        testdir = teuthology.get_testdir(ctx)
        all_clients = ['client.{id}'.format(id=id_)
                for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        users = {'s3main': 'tester', 's3alt': 'johndoe', 's3tenant' : 'tenanteduser'}
        s3tests_conf = self.s3tests_skelethon_config()
        for client in all_clients:
            # log.info("S3 Tests Go: s3tests_conf is {s3cfg}".format(s3cfg = s3tests_conf))
            for section, user in users.items():
                log.debug('S3 Tests Go: Setion, User = {sect}, {user}'.format(sect=section, user=user))
                self._config_user(s3tests_conf=s3tests_conf[client], section=section, user='{user}.{client}'.format(user=user, client=client))
                log.info("S3 Tests Go: s3tests_conf is {s3cfg}".format(s3cfg=s3tests_conf))
                log.debug('S3 Tests Go: Creating user {user} on {client}'.format(user=user, client=client))
                cluster_name, daemon_type, client_id = teuthology.split_role(client)
                client_with_id = daemon_type + '.' + client_id
                ctx.cluster.run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'user', 'create',
                        '--uid', user,
                        '--display-name', s3tests_conf[client][section]['display_name'],
                        '--access-key', s3tests_conf[client][section]['access_key'],
                        '--secret', s3tests_conf[client][section]['secret_key'],
                        '--email', s3tests_conf[client][section]['email'],
                        '--cluster', cluster_name,
                    ],
                    stdout=StringIO()
                )
            (remote,) = ctx.cluster.only(client).remotes.keys()   
            log.debug("S3 Tests Go: remote is {rmt}".format(rmt=remote)) 
            conf_fp = StringIO()
            s3tests_conf[client].write(conf_fp)
            teuthology.write_file(
                    remote=remote,
                    path='{tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client=client),
                    data=conf_fp.getvalue(),
                )


    def delete_users(self):
        log.info("S3 Tests Go: Deleting users...")
        ctx = self.ctx
        testdir = teuthology.get_testdir(ctx)
        all_clients = ['client.{id}'.format(id=id_)
                for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        users = {'s3main': 'tester', 's3alt': 'johndoe', 's3tenant' : 'tenanteduser'}
        for client in all_clients:
            # log.info("S3 Tests Go: s3tests_conf is {s3cfg}".format(s3cfg = s3tests_conf))
            s3tests_conf = teuthology.config_file('{tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client=client))
            for section, user in users.items():
                client_with_id = 'client.0'
                ctx.cluster.run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'user', 'rm',
                        '--uid', user,
                        '--purge-data',
                        '--cluster', 'ceph',
                        ],
                        stdout=StringIO()
                    )

    def s3tests_skelethon_config(self):
        log.info("S3 Tests Go: Generate skelethon config file")
        all_clients = ['client.{id}'.format(id=id_)
                        for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
                        # the iterator is needed because all_roles_of_type() is a generator
        log.info("S3 Tests Go: List all_clients: {clts}".format(clts=all_clients))
        # clients = {'client.0'}
        s3tests_conf = {}
        for client in all_clients:
            log.info("S3 Tests Go: Config for client {clt}".format(clt=client))
            endpoint = self.ctx.rgw.role_endpoints.get('client.0')
            # assert endpoint, 'S3 Tests Go: no rgw endpoint for {}'.format(client)

            s3tests_conf[client] = ConfigObj(
                indent_type='',
                infile={
                    'DEFAULT':
                        {
                        'host'      : '{ep}'.format(ep=endpoint.hostname),
                        'port'      : '{ep}'.format(ep=endpoint.port),
                        'is_secure' : 'yes' if endpoint.cert else 'no',
                        },
                    'fixtures' : {
                        'bucket_prefix' : '"joannah"' 
                        },
                    's3main'  : {
                        'host'      : '{ep}'.format(ep=endpoint.hostname),
                        'port'      : '{ep}'.format(ep=endpoint.port),
                        'is_secure' : 'yes' if endpoint.cert else 'no',
                    },
                    's3alt'   : {
                        'host'      : '{ep}'.format(ep=endpoint.hostname),
                        'port'      : '{ep}'.format(ep=endpoint.port),
                        'is_secure' : 'yes' if endpoint.cert else 'no',
                    },
                    's3tenant': {
                        'host'      : '{ep}'.format(ep=endpoint.hostname),
                        'port'      : '{ep}'.format(ep=endpoint.port),
                        'is_secure' : 'yes' if endpoint.cert else 'no',
                    },
                    }
                )
        return s3tests_conf


# TODO: 
#   -   write rgw client data to yaml file for configuring the 
#       s3 tests suites
#   -   add cleanup script to uninstall packages installed in bootstrap.sh
#   -   read users data from config file


task = S3tests_go
