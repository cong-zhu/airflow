import os
import imp
import subprocess

current_short_sha = subprocess.check_output('git rev-parse --short HEAD'.split(' '))
current_short_sha = current_short_sha.strip()

print('current_short_sha', current_short_sha)

dir_path = os.path.dirname(os.path.realpath(__file__))
verion_file = dir_path + '/' + '../../../airflow/version.py'

version_module = imp.load_source('version', verion_file)
version = version_module.version
new_version = version + '-' + str(current_short_sha)

new_version_str = "version = '{}'".format(new_version)
print('new_version:', new_version)
with open(verion_file, 'w') as f:
    f.write(new_version_str)

