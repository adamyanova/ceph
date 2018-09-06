"""
Deploy and configure Keystone v3 for Teuthology
"""
import argparse
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.orchestra.connection import split_user
from teuthology.packaging import install_package
from teuthology.packaging import remove_package

from teuthology.task import Task
from teuthology.orchestra import run
from teuthology.orchestra.remote import Remote

log = logging.getLogger(__name__)


class Keystone_v3(Task):
    """
    Download and install S3 tests in Java
    This will require openjdk and gradle
    """

    def __init__(self, ctx, config):
        super(Keystone_v3, self).__init__(ctx, config)
        ctx.keystone = argparse.Namespace()
        ctx.keystone.public_endpoints = assign_ports(ctx, config, 5000)
        ctx.keystone.admin_endpoints = assign_ports(ctx, config, 35357)

    def setup(self):
        super(Keystone_v3, self).setup()
        for (client, cconfig) in self.config.items():
            self.install_packages(client)
            self.download(client, cconfig)
            self.setup_venv(client)
            self.configure_instance(client)

    def begin(self):
        super(Keystone_v3, self).begin()
        for (client, _) in self.config.items():
            self.run_keystone(client)

    def end(self):
        super(Keystone_v3, self).end()
        log.info('End keystone task...')
        for (client, _) in self.config.items():
            run_in_keystone_venv(self.ctx, client,
                             ['sudo', 'systemctl', 'stop', 'httpd.service'])
            run_in_keystone_venv(self.ctx, client,
                        ['sudo', 'service', 'mariadb', 'stop'])

    def install_packages(self, client):
        """
        Download the packaged dependencies of Keystone.
        Keeps a list of the packages to cleanup at the end
        """
        assert isinstance(self.config, dict)
        log.info('Installing packages for Keystone...')

        self.deps = {
            # TODO: add mariadb packages for debian
            'deb': ['libffi-dev', 'libssl-dev', 'libldap2-dev', 'libsasl2-dev', 'python-dev'],
            'rpm': ['libffi-devel', 'openssl-devel', 'python34-devel', 'mariadb', 'mariadb-server'],
        }
        (remote,) = self.ctx.cluster.only(client).remotes.iterkeys()
        for dep in self.deps[remote.os.package_type]:
            install_package(dep, remote)

    def download(self, client, cconf):
        """
        Download the Keystone from github.
        """
        log.info('Downloading keystone...')
        keystonedir = get_keystone_dir(self.ctx)

        self.ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', cconf.get('force-branch', 'master'),
                'https://github.com/openstack/keystone.git',
                keystonedir,
            ],
        )

        sha1 = cconf.get('sha1')
        if sha1 is not None:
            run_in_keystone_dir(self.ctx, client, [
                'git', 'reset', '--hard', sha1,
            ],
            )

            # # hax for http://tracker.ceph.com/issues/23659
            # run_in_keystone_dir(self.ctx, client, [
            #     'sed', '-i',
            #     's/pysaml2<4.0.3,>=2.4.0/pysaml2>=4.5.0/',
            #     'requirements.txt'
            # ],
            # )

    def setup_venv(self, client):
        """
        Setup the virtualenv for Keystone using tox.
        """
        log.info('Setting up virtualenv for keystone...')
        run_in_keystone_dir(self.ctx, client,
                            ['source',
                                '{tvdir}/bin/activate'.format(
                                    tvdir=get_toxvenv_dir(self.ctx)),
                                run.Raw('&&'),
                                'tox', '-e', 'venv', '--notest'
                             ])

        run_in_keystone_venv(self.ctx, client,
                             ['pip', 'install', 'python-openstackclient'])

    def configure_instance(self, client):
        log.info('Configuring keystone...')

        keyrepo_dir = '{kdir}/etc/fernet-keys'.format(
            kdir=get_keystone_dir(self.ctx))
        # prepare the config file
        run_in_keystone_dir(self.ctx, client,
                            ['source',
                             '{tvdir}/bin/activate'.format(
                                 tvdir=get_toxvenv_dir(self.ctx)),
                                run.Raw('&&'),
                                'tox', '-e', 'genconfig'
                             ])
        run_in_keystone_dir(self.ctx, client,
                            [
                                'cp', '-f',
                                'etc/keystone.conf.sample',
                                'etc/keystone.conf'
                            ])
        # run_in_keystone_dir(self.ctx, client,
        #                     [
        #                         'sed',
        #                         '-e', 's/#admin_token =.*/admin_token = ADMIN/',
        #                         '-i', 'etc/keystone.conf'
        #                     ])
        run_in_keystone_dir(self.ctx, client,
                            [
                                'sed',
                                '-e', 's^#key_repository =.*^key_repository = {kr}^'.format(
                                    kr=keyrepo_dir),
                                '-i', 'etc/keystone.conf'
                            ])

        # setup MariaDB
        run_in_keystone_venv(self.ctx, client,
                             ['sudo', 'service', 'mariadb', 'start'])
        # TODO: make sure mariadb service is running
        mdbargs="CREATE DATABASE keystone; GRANT ALL PRIVILEGES ON keystone.* TO \'keystone\'@\'localhost\' IDENTIFIED BY \'KEYSTONE_DBPASS\'; GRANT ALL PRIVILEGES ON keystone.* TO \'keystone\'@\'%\' IDENTIFIED BY \'KEYSTONE_DBPASS\';"
        run_in_keystone_venv(self.ctx, client,
                             ['mysql', '-u', 'root', '-e',
                              mdbargs
                              ])

        # prepare key repository for Fetnet token authenticator
        run_in_keystone_dir(self.ctx, client, ['mkdir', '-p', keyrepo_dir])
        run_in_keystone_venv(self.ctx, client, [
                             'keystone-manage', 'fernet_setup'])

        # sync database
        run_in_keystone_venv(self.ctx, client, ['keystone-manage', 'db_sync'])

        admin_host, admin_port = self.ctx.keystone.admin_endpoints[client]
        run_in_keystone_venv(self.ctx, client,
                             ['keystone-manage', 'bootstrap',
                              '--bootstrap-password', "ADMIN",
                              '--bootstrap-username', 'admin',
                              '--bootstrap-project-name', 'admin',
                              '--bootstrap-role-name', 'admin',
                              '--bootstrap-service-name', 'keystone',
                              '--bootstrap-region-id', 'RegionOne',
                              '--bootstrap-admin-url', 'http://{host}:35357/v3/'.format(
                                  host=admin_host),
                              '--bootstrap-internal-url', 'http:{host}:5000/v3/'.format(
                                  host=admin_host),
                              '--bootstrap-public-url', 'http://{host}:5000/v3/'.format(
                                  host=admin_host),
                              ])

        # start httpd
        run_in_keystone_venv(self.ctx, client,
                             ['sudo', 'systemctl', 'enable', 'httpd.service'])
        run_in_keystone_venv(self.ctx, client,
                             ['sudo', 'systemctl', 'start', 'httpd.service'])

    def run_keystone(self, client):
        log.info('Run keystone...')

        run_in_keystone_venv(self.ctx, client,
                             ['openstack', 'project', 'create',
                              '--domain', 'default',
                              '--description',
                              "Service Project", 'service'
                              ])
        run_in_keystone_venv(self.ctx, client,
                             ['openstack', 'project', 'create',
                              '--domain', 'default',
                              '--description',
                              "Demo Project", 'demo'
                              ])

def assign_ports(ctx, config, initial_port):
    """
    Assign port numbers starting from @initial_port
    """
    port = initial_port
    role_endpoints = {}
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for role in roles_for_host:
            if role in config:
                role_endpoints[role] = (remote.name.split('@')[1], port)
                port += 1

    return role_endpoints


def get_keystone_dir(ctx):
    return '{tdir}/keystone'.format(tdir=teuthology.get_testdir(ctx))


def run_in_keystone_dir(ctx, client, args):
    ctx.cluster.only(client).run(
        args=['cd', get_keystone_dir(ctx), run.Raw('&&'), ] + args,
    )


def run_in_keystone_venv(ctx, client, args):
    run_in_keystone_dir(ctx, client,
                        ['source',
                            '.tox/venv/bin/activate',
                            run.Raw('&&')
                         ] + args)


def get_keystone_venved_cmd(ctx, cmd, args):
    kbindir = get_keystone_dir(ctx) + '/.tox/venv/bin/'
    return [kbindir + 'python', kbindir + cmd] + args


def get_toxvenv_dir(ctx):
    return ctx.tox.venv_path

task = Keystone_v3