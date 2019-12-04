# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest

from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.models import SensorInstance
from airflow.operators.sensors import MetastorePartitionSensor as Meta2
from airflow.sensors.metastore_partition_sensor import MetastorePartitionSensor as Meta1


class SensorInstanceTest(unittest.TestCase):

    def test_get_classpath(self):
        # Test the classpath in/out airflow
        obj1 = Meta1(partition_name='test_partition',
                     table='test_table',
                     task_id='meta_partition_test_1')
        obj1_classpath = SensorInstance.get_classpath(obj1)
        obj1_importpath = "airflow.sensors.metastore_partition_sensor.MetastorePartitionSensor"

        self.assertEqual(obj1_classpath, obj1_importpath)

        obj2 = Meta2(partition_name='test_partition',
                     table='test_table',
                     task_id='meta_partition_test_2')
        obj2_classpath = SensorInstance.get_classpath(obj2)
        obj2_importpath = "airflow.operators.sensors.MetastorePartitionSensor"

        self.assertEqual(obj2_classpath, obj2_importpath)

        def test_callable():
            return
        obj3 = PythonSensor(python_callable=test_callable,
                            task_id='python_sensor_test')
        obj3_classpath = SensorInstance.get_classpath(obj3)
        obj3_importpath = "airflow.contrib.sensors.python_sensor.PythonSensor"

        self.assertEqual(obj3_classpath, obj3_importpath)
