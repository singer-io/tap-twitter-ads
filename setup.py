#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-twitter-ads',
      version='0.0.6',
      description='Singer.io tap for extracting data from the Twitter Ads API API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_twitter_ads'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.23.0',
          'singer-python==5.9.0',
          'twitter-ads==8.0.0'
      ],
      extras_require={
          'dev': [
              'pylint',
              'ipdb',
              'nose',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-twitter-ads=tap_twitter_ads:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_twitter_ads': [
              'schemas/*.json',
              'schemas/shared/*.json',
              'tests/*.py'
          ]
      })
