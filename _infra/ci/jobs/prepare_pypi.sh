#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"

cp $SCRIPT_DIR/netrc ~/.netrc
echo "password $WORKFLOW_ORC_ARTIFACTORY_TOKEN" >> ~/.netrc

cp $SCRIPT_DIR/pypirc ~/.pypirc
echo "password: $WORKFLOW_ORC_ARTIFACTORY_TOKEN" >> ~/.pypirc
