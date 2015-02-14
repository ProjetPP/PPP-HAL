#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name='ppp_hal',
    version='0.2.1',
    description='HAL backend module for the Projet Pensées Profondes',
    url='https://github.com/ProjetPP',
    author='Valentin Lorentz',
    author_email='valentin.lorentz@ens-lyon.org',
    license='MIT',
    classifiers=[
        'Environment :: No Input/Output (Daemon)',
        'Development Status :: 1 - Planning',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Application',
        'Topic :: Software Development :: Libraries',
    ],
    install_requires=[
        'python3-memcached',
        'ppp_datamodel>=0.6.6',
        'ppp_libmodule>=0.7.3',
    ],
    packages=[
        'ppp_hal',
    ],
)
