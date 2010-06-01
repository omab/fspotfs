# -*- coding: utf-8 -*-
# setup script
from distutils.command.install_data import install_data

setup_args = {
        'name': 'F-Spot FS',
        'version': '0.1',
        'url': 'http://github.com/omab/fspotfs',
        'description': 'F-Spot FUSE filesystem',
        'long_description': 'A FUSE filesystem to navigate F-Spot tagged photos',
        'author': u'Matías Aguirre',
        'maintainer_email': 'matiasaguirre@gmail.com',
        'license': 'GPLv3',
        'packages': ['fspotfs'],
        'scripts': ['fsfs'],
        'cmdclass': {'install_data': install_data}
}

try:
    from setuptools import setup
    setup_args['install_requires'] = ['fuse-python>=0.2', 'sqlite3>=2.4.1']
except ImportError:
    from distutils.core import setup

setup(**setup_args)
