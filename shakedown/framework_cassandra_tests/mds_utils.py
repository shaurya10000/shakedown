import shakedown
import paramiko
import http
import json
from dcos import config

def test_if_node_launched_with_replace(host):
    print('MDS debugging..' + __name__)
    command = "ps -eaf | grep -c \"Dcassandra.replace_address\" "
    print("command = " + command)
    result = shakedown.run_command_on_agent(host, command)
    print("result = " + result)
    if result == 2:
        return True
    else:
        return False
