"""Setup for lahman_update."""

from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='lahman_updater',
    version='1.0.0',
    description='Update lahman database',
    long_description=readme,
    author='Benjamin Field',
    author_email='benjamin.field@gmail.com',
    url='https://github.com/aitchslash',
    license=license,
    install_requires=['spynner==2.19',
                      'PyMySQL==0.6.7',
                      'beautifulsoup4==4.4.1'],
    packages=find_packages()
)
