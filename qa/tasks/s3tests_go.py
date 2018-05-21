"""
Task for running RGW S3 tests with the AWS Go SDK
"""
from cStringIO import StringIO
import logging

from teuthology import misc
from teuthology.exceptions import ConfigError
from teuthology.task import Task
from teuthology.orchestra import run
from teuthology.orchestra.remote import Remote

log = logging.getLogger(__name__)


class S3TestsGo(Task):
    """
    Download and install S3TestsGo
    This will require golang
    """

    def __init__(self, ctx, config):
        super(S3TestsGo, self).__init__(ctx, config)
        self.log = log
        log.info('S3 Tests Go: In __init__ step')

    def setup(self):
        super(S3TestsGo, self).setup()
        config = self.config
        log.info('S3 Tests Go: In setup step')
        log.debug('config is: %r', config)

    def begin(self):
        super(S3TestsGo, self).begin()
        log.info('S3 Tests Go: In begin step')
        ctx = self.ctx
        log.debug('ctx is: %r', ctx)
        remote = Remote('ubuntu@smithi166.front.sepia.ceph.com')
        remote.run(
            args=['echo', 'S3 Tests Go: console output test"'], stdout=StringIO())
        remote.run(args=['sleep', '15'], stdout=StringIO())

    def teardown(self):
        log.info('S3 Tests Go: Teardown step')

    def install_packages(self):
        log.info("S3 Tests Go: Installing packages...")

    def download(self):
        log.info("S3 Tests Go: Downloading test suite...")
        ctx = self.ctx
        cluster = ctx.cluster
        testdir = teuthology.get_testdir(ctx)
        s3_branches = ['wip-foo']
        for (client, cconf) in cluster.remotes.iteritems():
            branch = cconf.get('force-branch', None)
            if not branch:
                ceph_branch = ctx.config.get('branch')
                suite_branch = ctx.config.get('suite_branch')
                if suite_branch in s3_branches:
                    branch = cconf.get('branch', suite_branch)
                else:
                    branch = cconf.get('branch', 'ceph-' + suite_branch)
            if not branch:
                raise ValueError(
                    "S3 Tests Go: Could not determine what branch to use for s3tests!")
            else:
                log.info("S3 Tests Go: Using branch '%s' for s3tests", branch)
            sha1 = cconf.get('sha1')
            git_remote = cconf.get('git_remote', None) or teuth_config.ceph_git_base_url
            ctx.cluster.only(client).run(
                args=[
                    'git', 'clone',
                    '-b', branch,
                    git_remote + 's3-tests.git',
                    '{tdir}/s3-tests'.format(tdir=testdir),
                    ],
                )
            
        

    def remove_tests(self):
        log.info('"S3 Tests Go: Removing s3-tests...')
        ctx = self.ctx
        testdir = teuthology.get_testdir(ctx)
        config = self.config
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/s3-tests'.format(tdir=testdir),
                    ],
                )



task = S3TestsGo
