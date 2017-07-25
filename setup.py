from os import environ

from setuptools import find_packages, setup

"""
The Kafka schedule is a python app that provides entirely kafka backed scheduling.
"""

__version__ = '1.0'

__build__ = environ.get('BUILD_SUFFIX', '.dev')

setup(author='Sam McDowell',
      author_email='sam.mcdowell@gmail.com',
      description='A kafka backed scheduler',
      entry_points={
          'console_scripts': [
              'populate_kafka_queues = kafka_scheduler.scheduler:populate_kafka_queues',
              'dev = kafka_scheduler.development:main',
          ]
      },
      extras_require={
      },
      install_requires=[
          'python-json-logger>=0.1.0,<1',
          'kafka-python>=1.3.3<2.0',
          'ujson>=1.30,<2',
      ],
      long_description=__doc__,
      name='kafka_scheduler',
      packages=find_packages(exclude=['*.tests']),
      version=__version__ + __build__,
      zip_safe=False)