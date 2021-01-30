# -*- coding: utf-8 -*-
import codecs
import os
from setuptools import setup

here = os.path.dirname(os.path.abspath(__file__))

with open('README.md') as f:
    readme = f.read()

# with open('LICENSE') as f:
#     license = f.read()

# Requirements
with codecs.open(os.path.join(here, 'requirements.txt')) as f:
    install_requirements = [
        line.split('#')[0].strip() for line in f.readlines() if not line.startswith('#')
    ]

setup(
    name='kleine',
    version='0.1.0',
    description='bla bla bla',
    long_description=readme,
    license=license,
    install_requires=install_requirements,
    py_modules=['kleine'],
    entry_points='''
        [console_scripts]
        kleine=kleine:cli
    ''',
)
