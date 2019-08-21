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
import getpass
import os
import uuid

from cgroupspy import trees
from retrying import retry
import psutil

from airflow.task.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils.helpers import reap_process_group
from airflow.utils.operator_resources import Resources


class CgroupTaskRunner(BaseTaskRunner):
    """
    Runs the raw Airflow task in a cgroup that has containment for memory and
    cpu. It uses the resource requirements defined in the task to construct
    the settings for the cgroup.

    Cgroup must be mounted first otherwise CgroupTaskRunner
    will not be able to work.

    cgroup-bin Ubuntu package must be installed to use cgexec command.

    Note that this task runner will only work if the Airflow user has root privileges,
    e.g. if the airflow user is called `airflow` then the following entries (or an even
    less restrictive ones) are needed in the sudoers file (replacing
    /CGROUPS_FOLDER with your system's cgroups folder, e.g. '/sys/fs/cgroup/'):
    airflow ALL= (root) NOEXEC: /bin/chown /CGROUPS_FOLDER/memory/airflow/*
    airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/memory/airflow/*..*
    airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/memory/airflow/* *
    airflow ALL= (root) NOEXEC: /bin/chown /CGROUPS_FOLDER/cpu/airflow/*
    airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/cpu/airflow/*..*
    airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/cpu/airflow/* *
    airflow ALL= (root) NOEXEC: /bin/chmod /CGROUPS_FOLDER/memory/airflow/*
    airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/memory/airflow/*..*
    airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/memory/airflow/* *
    airflow ALL= (root) NOEXEC: /bin/chmod /CGROUPS_FOLDER/cpu/airflow/*
    airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/cpu/airflow/*..*
    airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/cpu/airflow/* *
    """

    def __init__(self, local_task_job):
        super(CgroupTaskRunner, self).__init__(local_task_job)
        self.process = None
        self._finished_running = False
        self._cpu_shares = None
        self._mem_mb_limit = None
        self._created_cpu_cgroup = False
        self._created_mem_cgroup = False
        self._cur_user = getpass.getuser()

    @retry(stop_max_attempt_number=3, wait_random_min=500, wait_random_max=1000)
    def _build_tree(self):
        """
        retry 3 times in case of the failure due to the race condition when another
        process is deleting a cgroup.
        :retry: 3 times with 0.5 - 1 second delay
        :return: cgroupspy.nodes.Node
        """
        return trees.Tree().root

    def _create_cgroup(self, path):
        """
        Create the specified cgroup.

        :param path: The path of the cgroup to create.
        E.g. cpu/mygroup/mysubgroup
        :return: the Node associated with the created cgroup.
        :rtype: cgroupspy.nodes.Node
        """
        node = self._build_tree()
        path_split = path.split(os.sep)
        for path_element in path_split:
            # node.name is encoded to bytes:
            # https://github.com/cloudsigma/cgroupspy/blob/e705ac4ccdfe33d8ecc700e9a35a9556084449ca/cgroupspy/nodes.py#L64
            name_to_node = {x.name.decode(): x for x in node.children}
            if path_element not in name_to_node:
                self.log.debug("Creating cgroup %s in %s", path_element, node.path.decode())
                node = node.create_cgroup(path_element)
            else:
                self.log.debug(
                    "Not creating cgroup %s in %s since it already exists",
                    path_element, node.path.decode()
                )
                node = name_to_node[path_element]
        return node

    def _delete_cgroup(self, path):
        """
        Delete the specified cgroup.

        :param path: The path of the cgroup to delete.
        E.g. cpu/mygroup/mysubgroup
        """
        node = self._build_tree()
        path_split = path.split("/")
        for path_element in path_split:
            name_to_node = {x.name.decode(): x for x in node.children}
            if path_element not in name_to_node:
                self.log.warning("Cgroup does not exist: %s", path)
                return
            else:
                node = name_to_node[path_element]
        # node is now the leaf node
        parent = node.parent
        self.log.debug("Deleting cgroup %s/%s", parent, node.name)
        parent.delete_cgroup(node.name.decode())

    def _print_memory_usage(self, mem_cgroup_node):
        def byte_to_gb(num_bytes, precision=2):
            return round(num_bytes / (1024.0 * 1024 * 1024), precision)

        with open(mem_cgroup_node.full_path + '/memory.max_usage_in_bytes') as f:
            max_usage_in_bytes = int(f.read().strip())

        self.log.info(
            "Memory max usage of the task is %s GB, while the memory limit is %s GB",
            byte_to_gb(max_usage_in_bytes),
            byte_to_gb(mem_cgroup_node.controller.limit_in_bytes)
        )

    def start(self):
        # Use bash if it's already in a cgroup
        cgroups = self._get_cgroup_names()
        if ((cgroups.get("cpu") and cgroups.get("cpu") != "/") or
                (cgroups.get("memory") and cgroups.get("memory") != "/")):
            self.log.debug(
                "Already running in a cgroup (cpu: %s memory: %s) so not "
                "creating another one",
                cgroups.get("cpu"), cgroups.get("memory")
            )
            self.process = self.run_command()
            return

        # Create a unique cgroup name
        cgroup_name = "airflow/{}/{}".format(datetime.datetime.utcnow().
                                             strftime("%Y-%m-%d"),
                                             str(uuid.uuid4()))

        self.mem_cgroup_name = "memory/{}".format(cgroup_name)
        self.cpu_cgroup_name = "cpu/{}".format(cgroup_name)

        # Get the resource requirements from the task
        task = self._task_instance.task
        resources = task.resources if task.resources is not None else Resources()
        cpus = resources.cpus.qty
        self._cpu_shares = cpus * 1024
        self._mem_mb_limit = resources.ram.qty

        # Create the memory cgroup
        self.mem_cgroup_node = self._create_cgroup(self.mem_cgroup_name)
        self._created_mem_cgroup = True
        if self._mem_mb_limit > 0:
            self.log.debug(
                "Setting %s with %s MB of memory",
                self.mem_cgroup_name, self._mem_mb_limit
            )
            self.mem_cgroup_node.controller.limit_in_bytes = self._mem_mb_limit * 1024 * 1024

        # Create the CPU cgroup
        cpu_cgroup_node = self._create_cgroup(self.cpu_cgroup_name)
        self._created_cpu_cgroup = True
        if self._cpu_shares > 0:
            self.log.debug(
                "Setting %s with %s CPU shares",
                self.cpu_cgroup_name, self._cpu_shares
            )
            cpu_cgroup_node.controller.shares = self._cpu_shares

        # Start the process w/ cgroups
        self.log.debug(
            "Starting task process with cgroups cpu,memory: %s",
            cgroup_name
        )
        self.process = self.run_command(
            ['cgexec', '-g', 'cpu,memory:{}'.format(cgroup_name)]
        )

    def return_code(self):
        return_code = self.process.poll()
        # TODO(plypaul) Monitoring the the control file in the cgroup fs is better than
        # checking the return code here. The PR to use this is here:
        # https://github.com/plypaul/airflow/blob/e144e4d41996300ffa93947f136eab7785b114ed/airflow/contrib/task_runner/cgroup_task_runner.py#L43
        # but there were some issues installing the python butter package and
        # libseccomp-dev on some hosts for some reason.
        # I wasn't able to track down the root cause of the package install failures, but
        # we might want to revisit that approach at some other point.
        if return_code == 137:
            self.log.warning("Task failed with return code of 137. This may indicate "
                             "that it was killed due to excessive memory usage. "
                             "Please consider optimizing your task or using the "
                             "resources argument to reserve more memory for your task")
        return return_code

    def terminate(self):
        if self.process and psutil.pid_exists(self.process.pid):
            reap_process_group(self.process.pid, self.log)

    def on_finish(self):
        # Let the OOM watcher thread know we're done to avoid false OOM alarms
        self._finished_running = True
        # Clean up the cgroups
        if self._created_mem_cgroup:
            self._print_memory_usage(self.mem_cgroup_node)
            self._delete_cgroup(self.mem_cgroup_name)
        if self._created_cpu_cgroup:
            self._delete_cgroup(self.cpu_cgroup_name)
        super(CgroupTaskRunner, self).on_finish()

    @staticmethod
    def _get_cgroup_names():
        """
        :return: a mapping between the subsystem name to the cgroup name
        :rtype: dict[str, str]
        """
        with open("/proc/self/cgroup") as f:
            lines = f.readlines()
            d = {}
            for line in lines:
                line_split = line.rstrip().split(":")
                subsystem = line_split[1]
                group_name = line_split[2]
                d[subsystem] = group_name
            return d
