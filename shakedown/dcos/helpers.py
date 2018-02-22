import os
import paramiko
import scp

from dcos import http

import itertools
import shakedown


def mds_get_transport(host):
    """ For use from inside AWS VPC
        Create a transport object

        :param host: the hostname to connect to
        :type host: str

        :return: a transport object
        :rtype: paramiko.Transport
    """

    transport = paramiko.Transport(host)
    return transport


def get_transport(host, username, key):
    """ Create a transport object

        :param host: the hostname to connect to
        :type host: str
        :param username: SSH username
        :type username: str
        :param key: key object used for authentication
        :type key: paramiko.RSAKey

        :return: a transport object
        :rtype: paramiko.Transport
    """

    if host == '34.217.31.163':  # shakedown.master_ip():
        transport = paramiko.Transport("34.217.31.163")
    else:
        # transport_master = paramiko.Transport("shakedown.master_ip())
        print("MDS Debugging.. 34.217.31.163, " + username)
        transport_master = paramiko.Transport("34.217.31.163")
        print("MDS Debugging1.. 34.217.31.163, " + username)
        transport_master = start_transport(transport_master, username, key)
        print("MDS Debugging2.. 34.217.31.163, " + username)

        if not transport_master.is_authenticated():
            print("error: unable to authenticate {}@{} with key {}".format(username, shakedown.master_ip(), key_path))
            return False

        try:
            channel = transport_master.open_channel('direct-tcpip', (host, 22), ('10.0.0.205', 34567))
        except paramiko.SSHException as e:
            print("error: unable to connect to {}".format(host))
            print("Printing the exception..")
            str(e)
            return False

        transport = paramiko.Transport(channel)

    return transport


def start_transport(transport, username, key):
    """ Begin a transport client and authenticate it

        :param transport: the transport object to start
        :type transport: paramiko.Transport
        :param username: SSH username
        :type username: str
        :param key: key object used for authentication
        :type key: paramiko.RSAKey

        :return: the transport object passed
        :rtype: paramiko.Transport
    """

    transport.start_client()

    agent = paramiko.agent.Agent()
    keys = itertools.chain((key,) if key else (), agent.get_keys())
    for test_key in keys:
        try:
            transport.auth_publickey(username, test_key)
            break
        except paramiko.AuthenticationException as e:
            print("Execption.." + __name__)
            print(str(e))
            pass
    else:
        raise ValueError('No valid key supplied')

    return transport


# SSH connection will be auto-terminated at the conclusion of this operation, causing
# a race condition; the try/except block attempts to close the channel and/or transport
# but does not issue a failure if it has already been closed.
def try_close(obj):
    try:
        obj.close()
    except:
        pass


def validate_key(key_path):
    """ Validate a key

        :param key_path: path to a key to use for authentication
        :type key_path: str

        :return: key object used for authentication
        :rtype: paramiko.RSAKey
    """

    key_path = os.path.expanduser(key_path)

    if not os.path.isfile(key_path):
        return False

    return paramiko.RSAKey.from_private_key_file(key_path)
