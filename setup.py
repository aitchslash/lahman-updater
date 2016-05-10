"""Setup for lahman_update."""

from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='lahman_updater',
    version='0.8.9',
    description='Update lahman database',
    long_description=readme,
    author='Benjamin Field',
    author_email='benjamin.field@gmail.com',
    url='https://github.com/aitchslash',
    license=license,
    packages=find_packages()
)
