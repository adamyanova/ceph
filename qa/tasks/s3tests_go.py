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

    def begin(self):
        super(S3tests_go, self).begin()
        log.info('S3 Tests Go: In begin step')
        ctx = self.ctx
        log.debug('ctx is: %r', ctx)
        remote = Remote('ubuntu@smithi085.front.sepia.ceph.com')
        remote.run(
            args=['echo', 'S3 Tests Go: console output test"'], stdout=StringIO())
        remote.run(args=['sleep', '15'], stdout=StringIO())
        
    def teardown(self):
        log.info('S3 Tests Go: Teardown step')
        self.remove_tests()
        

    def install_packages(self):
        log.info("S3 Tests Go: Installing packages...")

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
                args=['echo', '"{tdir}/s3-tests"'.format(tdir=testdir)],
                stdout=StringIO()
            )
            cluster.run(
                args=['echo', '"$(ls {tdir}/s3-tests)"'.format(tdir=testdir)], 
                stdout=StringIO()
                )
            cluster.run(
                args=['{tdir}/s3-tests/bootsrap.sh'.format(tdir=testdir)],
                stdout=StringIO()
            )

    def remove_tests(self):
        log.info('"S3 Tests Go: Removing s3-tests...')
        ctx = self.ctx
        testdir = teuthology.get_testdir(ctx)
        config = self.config
        for client in config:
            ctx.cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/s3-tests'.format(tdir=testdir),
                    ],
                )

task = S3tests_go
