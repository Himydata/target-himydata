# target-himydata
# Copyright 2018 Himydata, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at
# himydata, Inc.(https://himydata.com/).

from setuptools import setup

setup(name='target-himydata',
      version='1.0.0',
      description='Singer.io target for the Himydata Platform API',
      author='Himydata',
      url='https://www.himydata.com/',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_himydata'],
      install_requires=[
          'jsonschema>=2.6.0,<3.0a',
          'mock==2.0.0',
          'requests>=2.4.0,<3.0a',
          'strict-rfc3339==0.7',
          'singer-python>=5.1.1,<6.0a',
      ],
      entry_points='''
          [console_scripts]
          target-himydata=target_himydata:main
      ''',
      packages=['target_himydata'],
)
