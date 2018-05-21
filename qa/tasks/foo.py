"""
Task for running foo
"""
from cStringIO import StringIO
import logging

from teuthology import misc
from teuthology.exceptions import ConfigError
from teuthology.task import Task
from teuthology.orchestra import run
from teuthology.orchestra.remote import Remote

log = logging.getLogger(__name__)


class Foo(Task):
    """
    Install and mount foo

    This will require foo.

    For example:

    tasks:
    - foo:
        biz:
        bar:

    Possible options for this task are:
        biz:
        bar:
        baz:
    """
    def __init__(self, ctx, config):
        super(Foo, self).__init__(ctx, config)
        self.log = log
        log.info('In __init__ step, hello world')

    def setup(self):
        super(Foo, self).setup()
        config = self.config
        log.info('In setup step, hello world')
        log.debug('config is: %r', config)

    def begin(self):
        super(Foo, self).begin()
        log.info('In begin step, hello world')
	ctx = self.ctx
        log.debug('ctx is: %r', ctx)
        remote = Remote('ubuntu@smithi166.front.sepia.ceph.com')
        remote.run(iargs=['sleep', '15'], stdout=StringIO())

    def teardown(self):
        log.info('Teardown step, hello world')

task = Foo


