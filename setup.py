from setuptools import setup

setup(
    name='MySQLPlugin',
    version='1.0',
    packages=['MySQLPlugin'],
    install_requires=
    [
        'Coronado',
        'PyMySQL',
        'argh'
    ],
    author='Mukul Majmudar',
    author_email='mukul@curecompanion.com',
    description='MySQL plugin for Coronado')
