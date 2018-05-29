"""
Task for running RGW S3 tests with the AWS Go SDK
"""
from cStringIO import StringIO
import logging

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
        for (client, cconf) in cluster.remotes.iteritems():
            log.info('S3 Tests Go: Client {clt} config is: {cfg}'.format(clt = client, cfg = cconf))
        self.create_users()
        
    def teardown(self):
        log.info('S3 Tests Go: Teardown step')
        self.delete_users()
        self.remove_tests()
        

    def install_packages(self):
        log.info("S3 Tests Go: Installing required packages...")
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        for (client, cconf) in cluster.remotes.iteritems():
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
        for (client, cconf) in cluster.remotes.iteritems():
            cluster.run(
                args=['echo', '"S3 Tests Go: Client is {clt}"'.format(clt = client)],
                stdout=StringIO()
            )
            cluster.run(
                args=['echo', '"S3 Tests Go: Cluster config is: {cfg}"'.format(cfg = cconf)],
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
        for (client, cconf) in cluster.remotes.iteritems():
            cluster.run(
                args=['mkdir', '{tdir}/../go'.format(tdir=testdir)],
                stdout=StringIO()
                )
            cluster.run(
                args=['export', 'GOPATH=$HOME/go'],
                stdout=StringIO()
            )
            cluster.run(
                args=['echo', '$GOPATH'],
                stdout=StringIO()
            )

    def install_tests_dependencies(self):
        log.info("S3 Tests Go: Installing tests dependencies...")
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        cluster.run(
                args=['cd', 
                    '{tdir}/s3-tests'.format(tdir=testdir)
                    ],
                stdout=StringIO()
            )
        cluster.run(
                args=['go', 'get', '-d', './...'],
                stdout=StringIO()
            )

    def remove_tests(self):
        log.info('"S3 Tests Go: Removing s3-tests...')
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        for (client, cconf) in cluster.remotes.iteritems():
            cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/s3-tests'.format(tdir=testdir),
                    '{tdir}/../go'.format(tdir=testdir)
                    ],
                stdout=StringIO()
                )

    def _config_user(s3tests_conf, section, user):
        """
        Configure users for this section by stashing away keys, ids, and
        email addresses.
        """
        s3tests_conf[section].setdefault('user_id', user)
        s3tests_conf[section].setdefault('email', '{user}+test@test.test'.format(user=user))
        s3tests_conf[section].setdefault('display_name', 'Mr. {user}'.format(user=user))
        s3tests_conf[section].setdefault('access_key', ''.join(random.choice(string.uppercase) for i in xrange(20)))
        s3tests_conf[section].setdefault('secret_key', base64.b64encode(os.urandom(40)))
        s3tests_conf[section].setdefault('totp_serial', ''.join(random.choice(string.digits) for i in xrange(10)))
        s3tests_conf[section].setdefault('totp_seed', base64.b32encode(os.urandom(40)))
        s3tests_conf[section].setdefault('totp_seconds', '5')

    def create_users(self):
        """
        Create a main and an alternate s3 user.
        """
        log.info("S3 Tests Go: Creating users...")
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        users = {'s3 main': 'foo', 's3 alt': 'bar'}
        for (client, cconf) in cluster.remotes.iteritems():
            # s3tests_conf = teuthology.config_file()
            # s3tests_conf.setdefault('fixtures', {})
            # s3tests_conf['fixtures'].setdefault('bucket prefix', 'test-' + client + '-{random}-')
            # log.info("S3 Tests Go: s3tests_conf is {s3cfg}".format(s3cfg = s3tests_conf))
            # for (section, user) in users.iteritems():
            #     _config_user(s3tests_conf, section, '{user}.{client}'.format(user=user, client=client))
            #     log.debug('Creating user {user} on {host}'.format(user=s3tests_conf[section]['user_id'], host=client))
            #     cluster_name, daemon_type, client_id = teuthology.split_role(client)
            #     client_with_id = daemon_type + '.' + client_id
            #     ctx.cluster.run(
            #         args=[
            #             'adjust-ulimits',
            #             'ceph-coverage',
            #             '{tdir}/archive/coverage'.format(tdir=testdir),
            #             'radosgw-admin',
            #             '-n', client_with_id,
            #             'user', 'create',
            #             '--uid', s3tests_conf[section]['user_id'],
            #             '--display-name', s3tests_conf[section]['display_name'],
            #             '--access-key', s3tests_conf[section]['access_key'],
            #             '--secret', s3tests_conf[section]['secret_key'],
            #             '--email', s3tests_conf[section]['email'],
            #             '--cluster', cluster_name,
            #         ],
            #     )
            # cluster_name, daemon_type, client_id = teuthology.split_role(cconf)

            # client_with_id = daemon_type + '.' + client_id
            client_with_id = 'client.0'
            ctx.cluster.run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client_with_id,
                    'user', 'create',
                    '--uid', 'testid',
                    '--display-name', 'M. Tester',
                    '--access-key', '0555b35654ad1656d804',
                    '--secret', 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==',
                    '--email', 'tester@ceph.com',
                    '--cluster', 'ceph',
                ],
                stdout=StringIO()
            )

    def delete_users(self):
        log.info("S3 Tests Go: Deleting users...")
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        for (client, cconf) in cluster.remotes.iteritems():
            # cluster_name, daemon_type, client_id = teuthology.split_role(cconf)
            # client_with_id = daemon_type + '.' + client_id
            client_with_id = 'client.0'
            ctx.cluster.run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client_with_id,
                    'user', 'rm',
                    '--uid', 'testid',
                    '--purge-data',
                    '--cluster', 'ceph',
                    ],
                    stdout=StringIO()
                )

task = S3tests_go
