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

"""Make TaskInstance.pool not nullable

Revision ID: 6e96a59344a4
Revises: 939bb1e647c8
Create Date: 2019-06-13 21:51:32.878437

"""

from alembic import op
import dill
import sqlalchemy as sa
from sqlalchemy import Column, Float, Integer, PickleType, String
from sqlalchemy.ext.declarative import declarative_base

from airflow.utils.sqlalchemy import UtcDateTime


# revision identifiers, used by Alembic.
revision = '6e96a59344a4'
down_revision = '939bb1e647c8'
branch_labels = None
depends_on = None

def upgrade():
    """
    Make TaskInstance.pool field not nullable.
    """
    pass


def downgrade():
    """
    Make TaskInstance.pool field nullable.
    """
    pass
