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

import datetime
import json

from sqlalchemy import Column, Index, Integer, String, Text, BigInteger, UniqueConstraint

from airflow.exceptions import AirflowException
from airflow.models.base import Base, ID_LEN
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State
from airflow.configuration import conf


class SensorInstance(Base, LoggingMixin):
    """
    SensorInstance support the smart sensor service. It stores the sensor task states
    and context that required for poking include poke context and execution context.
    In sensor_instance table we also save the sensor operator classpath so that inside
    smart sensor there is no need to import the dagbag and create task object for each
    sensor task.

    SensorInstance include another set of columns to support the smart sensor shard on
    large number of sensor instance. By hashcode generated from poke_contex and shardcode
    the distributed smart sensor processes can work on different shards.

    """

    __tablename__ = "sensor_instance"

    id = Column(Integer, primary_key=True)
    task_id = Column(String(ID_LEN), nullable=False)
    dag_id = Column(String(ID_LEN), nullable=False)
    execution_date = Column(UtcDateTime, nullable=False)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    start_date = Column(UtcDateTime)
    operator = Column(String(1000), nullable=False)
    op_classpath = Column(String(1000), nullable=False)
    hashcode = Column(BigInteger, nullable=False)
    shardcode = Column(Integer, nullable=False)
    poke_context = Column(Text, nullable=False)
    execution_context = Column(Text)
    created_at = Column(UtcDateTime, default=timezone.utcnow(), nullable=False)
    updated_at = Column(UtcDateTime,
                        default=timezone.utcnow(),
                        onupdate=timezone.utcnow(),
                        nullable=False)

    __table_args__ = (
        Index('ti_primary_key', dag_id, task_id, execution_date, unique=True),

        Index('si_hashcode', hashcode),
        Index('si_shardcode', shardcode),
        Index('si_state_shard', state, shardcode),
    )

    def __init__(self, ti):
        self.dag_id = ti.dag_id
        self.task_id = ti.task_id
        self.execution_date = ti.execution_date

    @staticmethod
    def get_classpath(obj):
        """
        Get the object dotted class path. Used for getting operator classpath
        :param obj:
        :return: string
        """
        module_name, class_name = obj.__module__, obj.__class__.__name__

        if '.' not in module_name:
            # Fix if module name was broken by airflow importer
            import airflow.operators as builtin_operators
            import airflow.sensors as builtin_sensors
            import airflow.contrib.operators as contrib_operators
            stem = ''
            if hasattr(builtin_operators, class_name):
                stem = 'airflow.operators.'
            elif hasattr(builtin_sensors, class_name):
                stem = 'airflow.sensors.'
            elif hasattr(contrib_operators, class_name):
                stem = 'airflow.contrib.operators.'
            module_name = stem + module_name

        return module_name + "." + class_name

    @classmethod
    @provide_session
    def register(cls, ti, poke_context, execution_context, session=None):
        if poke_context is None:
            raise AirflowException('poke_context should not be None')

        encoded_poke = json.dumps(poke_context)
        encoded_execution_context = json.dumps(execution_context)

        sensor = session.query(SensorInstance).filter(
            SensorInstance.dag_id == ti.dag_id,
            SensorInstance.task_id == ti.task_id,
            SensorInstance.execution_date == ti.execution_date
        ).with_for_update().first()

        if sensor is None:
            sensor = SensorInstance(ti=ti)

        sensor.operator = ti.operator
        sensor.op_classpath = SensorInstance.get_classpath(ti.task)
        sensor.poke_context = encoded_poke
        sensor.execution_context = encoded_execution_context

        sensor.hashcode = hash(encoded_poke)
        sensor.shardcode = sensor.hashcode % conf.getint('smart_sensor', 'shard_code_upper_limit')
        sensor.try_number = ti.try_number

        sensor.state = State.SENSING
        sensor.start_date = timezone.utcnow()
        session.add(sensor)
        session.commit()

        return True

    @property
    def key(self):
        """
        Returns a tuple that identifies the task instance uniquely
        """
        return self.dag_id, self.task_id, self.execution_date

    @property
    def try_number(self):
        """
        Return the try number that this task number will be when it is actually
        run.
        If the TI is currently running, this will match the column in the
        databse, in all othercases this will be incremenetd
        """
        # This is designed so that task logs end up in the right file.
        if self.state in State.running():
            return self._try_number
        return self._try_number + 1

    @try_number.setter
    def try_number(self, value):
        self._try_number = value

    def __repr__(self):
        return "<{self.__class__.__name__}: id: {self.id} poke_context: {self.poke_context} " \
               "execution_context: {self.execution_context} state: {self.state}>".format(
            self=self)
