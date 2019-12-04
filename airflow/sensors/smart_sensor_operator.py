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
import logging
import json
from sqlalchemy import and_, join
import time
import traceback
from time import sleep
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.models import BaseOperator, TaskInstance, SensorInstance
from airflow.models import SkipMixin
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import provide_session
from airflow.utils.email import send_email
from airflow.utils.log.logging_mixin import set_context
from airflow.utils.module_loading import import_string
from airflow.utils.net import get_hostname
from airflow.utils.state import State, PokeState
from airflow.utils.timeout import timeout
from airflow.settings import LOGGING_CLASS_PATH, Stats


config = import_string(LOGGING_CLASS_PATH)
handler_config = config['handlers']['task']
try:
    formatter_config = config['formatters'][handler_config['formatter']]
except:
    formatter_config = None
dictConfigurator = logging.config.DictConfigurator(config)


class SensorWork:
    def __init__(self, ti):
        self.dag_id = ti.dag_id
        self.task_id = ti.task_id
        self.execution_date = ti.execution_date
        self.try_number = ti.try_number

        self.poke_context = json.loads(ti.poke_context) if ti.poke_context else {}
        self.execution_context = json.loads(ti.execution_context) if ti.execution_context else {}
        try:
            self.log = self._get_sensor_logger(ti)
        except:
            self.log = None
        self.hashcode = ti.hashcode
        self.start_date = ti.start_date
        self.operator = ti.operator
        self.op_classpath = ti.op_classpath
        self.encoded_poke_context = ti.poke_context

    @staticmethod
    def create_new_task_handler():
        handler_config_copy = {k: handler_config[k] for k in handler_config}
        formatter_config_copy = {k: formatter_config[k] for k in formatter_config}
        handler = dictConfigurator.configure_handler(handler_config_copy)
        formatter = dictConfigurator.configure_formatter(formatter_config_copy)
        handler.setFormatter(formatter)
        return handler

    def _get_sensor_logger(self, ti):
        # TODO:PING, should be somewhere else, but not this file, has to use LOG_ID_TEMPLATE from es
        # but how about other log file handler?
        ti.raw = False  # Otherwise set_context will fail
        log_id = "-".join([ti.dag_id,
                           ti.task_id,
                           ti.execution_date.strftime("%Y_%m_%dT%H_%M_%S_%f"),
                           str(ti.try_number)])
        logger = logging.getLogger('airflow.task' + '.' + log_id)

        if len(logger.handlers) == 0:
            handler = self.create_new_task_handler()
            logger.addHandler(handler)
            set_context(logger, ti)

            line_break = ("-" * 120)
            logger.info(line_break)
            logger.info("Processing sensor task %s in smart sensor service on host: %s",
                        self.ti_key, get_hostname())
            logger.info(line_break)
        return logger

    def close_sensor_logger(self):
        for handler in self.log.handlers:
            try:
                handler.close()
            except Exception:
                pass

    @property
    def ti_key(self):
        return self.dag_id, self.task_id, self.execution_date

    @property
    def cache_key(self):
        return self.operator, self.encoded_poke_context


class CachedPokeWork:
    """
    Wrapper class for the poke work inside smart sensor. It saves
    the sensor_task used to poke and recent poke result state
    """
    def __init__(self):
        self.state = None
        self.sensor_task = None
        self.last_poke_time = None

    def set_state(self, state):
        self.state = state
        self.last_poke_time = timezone.utcnow()

    def clear_state(self):
        self.state = None

    def is_expired(self):
        """
        The cached task object expires if there is no poke for 20 mins.
        :return: Boolean
        """
        return (timezone.utcnow() - self.last_poke_time).total_seconds() > 1200


class SensorExceptionInfo:
    def __init__(self,
                 exception_info,
                 is_infra_failure=False,
                 infra_failure_retry_window=datetime.timedelta(minutes=130)):
        self._exception_info = exception_info
        self._is_infra_failure = is_infra_failure
        self._infra_failure_retry_window = infra_failure_retry_window

        self._infra_failure_timeout = None
        self.set_infra_failure_timeout()
        self.fail_current_run = self.should_fail_current_run()

    def set_latest_exception(self, exception_info, is_infra_failure=False):
        """
        This function set the latest exception information for sensor exception. If the exception
        implies an infra failure, this function will check the recorded infra failure timeout
        which was set at the first infra failure exception arrives. There is a 6 hours window
        for retry without failing current run.

        :param exception_info:
        :param is_infra_failure:
        :return:
        """
        self._exception_info = exception_info
        self._is_infra_failure = is_infra_failure

        self.set_infra_failure_timeout()
        self.fail_current_run = self.should_fail_current_run()

    def set_infra_failure_timeout(self):
        # Only set the infra_failure_timeout if there is no existing one
        if not self._is_infra_failure:
            self._infra_failure_timeout = None
        elif self._infra_failure_timeout is None:
            self._infra_failure_timeout = timezone.utcnow() + self._infra_failure_retry_window

    def should_fail_current_run(self):
        return not self.is_infra_failure or timezone.utcnow() > self._infra_failure_timeout

    @property
    def exception_info(self):
        return self._exception_info

    @property
    def is_infra_failure(self):
        return self._is_infra_failure

    def is_expired(self):
        if not self._is_infra_failure:
            return True
        return timezone.utcnow() > self._infra_failure_timeout + datetime.timedelta(minutes=30)


class SmartSensorOperator(BaseOperator, SkipMixin):
    """
    Smart sensor operators are derived from this class.

    Smart Sensor operators keep refresh a dictionary by visiting DB.
    Taking qualified active sensor tasks. Different from sensor operator,
    Smart sensor operators poke for all sensor tasks in the dictionary at
    a time interval. When a criteria is met or fail by time out, it update
    all sensor task state in task_instance table

    :param soft_fail: Set to true to mark the task as SKIPPED on failure
    :type soft_fail: bool
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: int
    :type mode: str
    """
    ui_color = '#e6f1f2'

    @apply_defaults
    def __init__(self,
                 poke_interval=180,
                 timeout=60 * 60 * 24 * 7,
                 soft_fail=False,
                 shard_min=0,
                 shard_max=100000,
                 poke_exception_cache_ttl=600,
                 poke_timeout=3,
                 poke_exception_to_fail_task_threshold=3,
                 *args,
                 **kwargs):
        super(SmartSensorOperator, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self._validate_input_values()
        self.hostname = ""

        self.cached_dedup_works = {}
        self.cached_sensor_exceptions = {}

        self.max_tis_per_query = 50
        self.shard_min = shard_min
        self.shard_max = shard_max
        self.poke_exception_cache_ttl = poke_exception_cache_ttl
        self.poke_timeout = poke_timeout
        self._poke_exception_to_fail_task_threshold = poke_exception_to_fail_task_threshold

    def _validate_input_values(self):
        if not isinstance(self.poke_interval, (int, float)) or self.poke_interval < 0:
            raise AirflowException(
                "The poke_interval must be a non-negative number")
        if not isinstance(self.timeout, (int, float)) or self.timeout < 0:
            raise AirflowException(
                "The timeout must be a non-negative number")

    @provide_session
    def _load_sensor_works(self, session=None):
        """
        Refresh sensor instances need to be handled by this operator. Put the context,
        hashcode and sensor instance start_date in a wrapper class
        :param session:
        :return:
        """
        SI = SensorInstance
        start_query_time = time.time()
        query = session.query(SI) \
            .filter(SI.state == State.SENSING)\
            .filter(SI.shardcode < self.shard_max,
                    SI.shardcode >= self.shard_min)
        tis = query.all()
        self.log.info("Performance query %s tis, time: %s", len(tis), time.time() - start_query_time)

        # Query without checking dagrun state might keep some failed dag_run tasks alive.
        # Join with DagRun table will be very slow based on the number of sensor tasks we
        # need to handle. We query all smart tasks in this operator
        # and expect scheduler correct the states in _change_state_for_tis_without_dagrun()

        sensor_works = []
        for ti in tis:
            try:
                sensor_works.append(SensorWork(ti))
            except Exception as e:
                self.log.exception("Exception at creating sensor work for ti %s", ti.key)
                self.log.exception(e, exc_info=True)

        self.log.info("%d tasks detected.", len(sensor_works))

        return sensor_works

    @provide_session
    def _mark_multi_state(self, operator, poke_hash, encoded_poke_context, state, session=None):
        """
        Mark state for multiple tasks that have hashcode=poke_hash.
        :param poke_hash:
        :param state:
        :param session:
        :return:
        """
        # TODO: refine for reduce in chunk and with_for_update
        SI = SensorInstance
        TI = TaskInstance

        count_marked = 0
        try:
            query_result = session.query(TI, SI)\
                .join(TI, and_(TI.dag_id == SI.dag_id,
                               TI.task_id == SI.task_id,
                               TI.execution_date == SI.execution_date)) \
                .filter(SI.state == State.SENSING) \
                .filter(SI.hashcode == poke_hash) \
                .filter(SI.operator == operator) \
                .with_for_update().all()

            end_date = timezone.utcnow()
            for ti, si in query_result:
                if si.poke_context != encoded_poke_context:
                    continue

                ti.hostname = self.hostname
                if ti.state == State.SENSING:
                    ti.state = state
                    si.state = state
                    if state in State.finished():
                        ti.end_date = end_date
                        ti.set_duration()
                    count_marked += 1
                else:
                    # ti.state != State.SENSING
                    si.state = ti.state

            session.commit()

        except Exception:
            self.log.exception("Exception _mark_multi_state in smart sensor for hashcode %s",
                               str(poke_hash))
        self.log.info("Marked %s tasks out of %s to state %s", count_marked, len(query_result), state)

    @provide_session
    def _retry_or_fail_task(self, sensor_work, error, session=None):
        """
        Change single task state for sensor task. For final state, set the end_date.
        Since smart sensor take care all retries in one process. Failed sensor tasks
        logically experienced all retries and the try_nubmer should be set to max_tries.
        :param ti_key:
        :param state:
        :param session:
        :return:
        """
        def email_alert(task_instance, error_info):
            try:
                subject, html_content = task_instance.get_email_subject_content(error_info)
                email = sensor_work.execution_context.get('email')

                send_email(email, subject, html_content)
            except Exception:
                sensor_work.log.exception("Exception alerting email.")

        try:
            dag_id, task_id, execution_date = sensor_work.ti_key
            TI = TaskInstance
            SI = SensorInstance
            si = session.query(SI).filter(
                SI.dag_id == dag_id,
                SI.task_id == task_id,
                SI.execution_date == execution_date) \
                .with_for_update() \
                .first()

            if si.hashcode != sensor_work.hashcode:
                # Return without setting state
                return

            ti = session.query(TI).filter(
                TI.dag_id == dag_id,
                TI.task_id == task_id,
                TI.execution_date == execution_date) \
                .with_for_update() \
                .first()

            if ti:
                if ti.state == State.SENSING:
                    ti.hostname = self.hostname
                    if sensor_work.execution_context.get('retries', None) and \
                            ti.try_number <= ti.max_tries:
                        # retry
                        ti.state = State.UP_FOR_RETRY
                        if sensor_work.execution_context.get('email_on_retry', None) and \
                                sensor_work.execution_context.get('email', None):
                            sensor_work.log.info("%s sending email alert for retry", sensor_work.ti_key)
                            email_alert(ti, error)
                    else:
                        ti.state = State.FAILED
                        if sensor_work.execution_context.get('email_on_failure', None) and \
                                sensor_work.execution_context.get('email', None):
                            sensor_work.log.info("%s sending email alert for failure", sensor_work.ti_key)
                            email_alert(ti, error)

                    si.state = State.FAILED
                    ti.end_date = timezone.utcnow()
                    ti.set_duration()
                else:
                    si.state = ti.state
                session.merge(si)
                session.merge(ti)
                session.commit()

                sensor_work.log.info("Task %s got an error: %s. Set the state to failed. Exit.",
                                     str(sensor_work.ti_key), error)
                sensor_work.close_sensor_logger()

        except Exception:
            sensor_work.log.exception("Exception on failing %s", sensor_work.ti_key)

    def _check_and_handle_ti_timeout(self, sensor_work):
        """
        Check if a sensor task in smart sensor is timeout. Could be either sensor operator timeout
        or general operator execution_timeout.
        :param task: SensorWork
        :return:
        """
        task_timeout = sensor_work.execution_context.get('timeout', self.timeout)
        task_execution_timeout = sensor_work.execution_context.get('execution_timeout', None)
        if task_execution_timeout:
            task_timeout = min(task_timeout, task_execution_timeout)

        if (timezone.utcnow() - sensor_work.start_date).total_seconds() > task_timeout:
            error = "Sensor Timeout"
            sensor_work.log.exception(error)
            self._retry_or_fail_task(sensor_work, error)

    def _handle_poke_exception(self, sensor_work):
        """
        Fail task if accumulated exceptions exceeds retries.
        :param sensor_work: SensorWork
        :return:
        """
        sensor_exception = self.cached_sensor_exceptions.get(sensor_work.cache_key)
        error = sensor_exception.exception_info
        sensor_work.log.exception("Handling poke exception: %s", error)

        if sensor_exception.fail_current_run:
            sensor_work.log.exception("Task %s failed by exceptions.", sensor_work.ti_key)
            self._retry_or_fail_task(sensor_work, error)
        else:
            sensor_work.log.info("Exception detected, retrying without failing current run.")
            self._check_and_handle_ti_timeout(sensor_work)

    def _process_sensor_work_with_cached_state(self, sensor_work, state):
        if state == PokeState.LANDED:
            sensor_work.log.info("Task %s succeeded", str(sensor_work.ti_key))
            sensor_work.close_sensor_logger()

        if state == PokeState.NOT_LANDED:
            # Handle timeout if connection valid but not landed yet
            self._check_and_handle_ti_timeout(sensor_work)
        elif state == PokeState.POKE_EXCEPTION:
            self._handle_poke_exception(sensor_work)

    def _execute_sensor_work(self, sensor_work):
        ti_key = sensor_work.ti_key
        log = sensor_work.log or self.log
        log.info("Sensing ti: %s", str(ti_key))
        log.info("Poking with arguments: %s", sensor_work.encoded_poke_context)

        cache_key = sensor_work.cache_key
        if cache_key not in self.cached_dedup_works:
            # create an empty cached_work for a new cache_key
            self.cached_dedup_works[cache_key] = CachedPokeWork()

        cached_work = self.cached_dedup_works[cache_key]

        if cached_work.state is not None:
            # Have a valid cached state, don't poke twice in certain time interval
            self._process_sensor_work_with_cached_state(sensor_work, cached_work.state)
            return

        try:
            with timeout(seconds=self.poke_timeout):
                if self.poke(sensor_work):
                    # Got a landed signal, mark all tasks waiting for this partition
                    cached_work.set_state(PokeState.LANDED)

                    self._mark_multi_state(sensor_work.operator,
                                           sensor_work.hashcode,
                                           sensor_work.encoded_poke_context,
                                           State.SUCCESS)

                    log.info("Task %s succeeded", str(ti_key))
                    sensor_work.close_sensor_logger()
                else:
                    # Not landed yet. Handle possible timeout
                    cached_work.set_state(PokeState.NOT_LANDED)
                    self._check_and_handle_ti_timeout(sensor_work)

                self.cached_sensor_exceptions.pop(cache_key, None)
        except Exception as e:
            # The retry_infra_failure decorator inside hive_hooks will raise exception with
            # is_infra_failure == True. Long poking timeout here is also considered an infra
            # failure. Other exceptions should fail.
            is_infra_failure = getattr(e, 'is_infra_failure', False) or isinstance(e, AirflowTaskTimeout)
            exception_info = traceback.format_exc()
            cached_work.set_state(PokeState.POKE_EXCEPTION)

            if cache_key in self.cached_sensor_exceptions:
                self.cached_sensor_exceptions[cache_key].set_latest_exception(
                    exception_info,
                    is_infra_failure=is_infra_failure)
            else:
                self.cached_sensor_exceptions[cache_key] = SensorExceptionInfo(
                    exception_info,
                    is_infra_failure=is_infra_failure)

            self._handle_poke_exception(sensor_work)

    def flush_cached_sensor_poke_results(self):
        """
        Flush outdated cached sensor states saved in previous loop.
        :return:
        """
        for key, cached_work in self.cached_dedup_works.items():
            if cached_work.is_expired():
                self.cached_dedup_works.pop(key, None)
            else:
                cached_work.state = None

        for ti_key, sensor_exception in self.cached_sensor_exceptions.items():
            if sensor_exception.fail_current_run or sensor_exception.is_expired():
                self.cached_sensor_exceptions.pop(ti_key, None)

    def poke(self, sensor_work):
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        cached_work = self.cached_dedup_works[sensor_work.cache_key]
        if not cached_work.sensor_task:
            init_args = dict(sensor_work.poke_context.items() + [('task_id', sensor_work.task_id)])
            operator_class = import_string(sensor_work.op_classpath)
            cached_work.sensor_task = operator_class(**init_args)
            # Enable infra failure exception raise without retry
            cached_work.sensor_task.raise_infra_failure_without_retry = True

        return cached_work.sensor_task.poke(sensor_work.poke_context)

    def _emit_loop_stats(self):
        try:
            count_poke = 0
            count_poke_success = 0
            count_poke_exception = 0
            count_exception_failures = 0
            count_infra_failure = 0
            for key, cached_work in self.cached_dedup_works.items():
                if cached_work.state is None:
                    continue
                count_poke += 1
                if cached_work.state == PokeState.LANDED:
                    count_poke_success += 1
                elif cached_work.state == PokeState.POKE_EXCEPTION:
                    count_poke_exception += 1
            for key, cached_exception in self.cached_sensor_exceptions.items():
                if cached_exception.is_infra_failure and cached_exception.fail_current_run:
                    count_infra_failure += 1
                if cached_exception.fail_current_run:
                    count_exception_failures += 1

            Stats.gauge("smart_sensor_operator.poked_tasks", count_poke)
            Stats.gauge("smart_sensor_operator.poked_success", count_poke_success)
            Stats.gauge("smart_sensor_operator.poked_exception", count_poke_exception)
            Stats.gauge("smart_sensor_operator.exception_failures", count_exception_failures)
            Stats.gauge("smart_sensor_operator.infra_failures", count_infra_failure)
        except Exception as e:
            self.log.exception("Exception at getting loop stats %s")
            self.log.exception(e, exc_info=True)

    def execute(self, context):
        started_at = timezone.utcnow()

        self.hostname = get_hostname()
        while True:
            poke_start_time = timezone.utcnow()

            self.flush_cached_sensor_poke_results()

            sensor_works = self._load_sensor_works()
            self.log.info("Loaded %s sensor_works", len(sensor_works))
            Stats.gauge("smart_sensor_operator.loaded_tasks", len(sensor_works))

            for sensor_work in sensor_works:
                self._execute_sensor_work(sensor_work)

            duration = (timezone.utcnow() - poke_start_time).total_seconds()

            self.log.info("Taking %s to execute %s tasks.", duration, len(sensor_works))

            Stats.timing("smart_sensor_operator.loop_duration", duration)
            Stats.gauge("smart_sensor_operator.executed_tasks", len(sensor_works))
            self._emit_loop_stats()

            if duration < self.poke_interval:
                sleep(self.poke_interval - duration)
            if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                self.log.info("Time is out for smart senosr.")
                return

    def on_kill(self):
        pass


if __name__ == '__main__':
    SmartSensorOperator(task_id='test').execute({})
