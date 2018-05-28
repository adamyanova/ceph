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

    def begin(self):
        super(S3tests_go, self).begin()
        log.info('S3 Tests Go: In begin step')
        ctx = self.ctx
        log.debug('S3 Tests Go: ctx is: %r', ctx)
        
    def teardown(self):
        log.info('S3 Tests Go: Teardown step')
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


task = S3tests_go
