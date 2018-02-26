import time

import pytest
import requests
from dcos.errors import DCOSException, DCOSAuthenticationException, DCOSHTTPException
import sys
import logging

import dcos
import shakedown
from tests.command import (
    cassandra_api_url,
    check_health,
    get_cassandra_config,
    marathon_api_url,
    unit_health_url,
    request,
    spin,
    unset_ssl_verification,
    install,
    uninstall,
)
from tests.defaults import DEFAULT_NODE_COUNT, PACKAGE_NAME, TASK_RUNNING_STATE, SERVICE_NAME
from tests import infinity_commons

HEALTH_WAIT_TIME = 300
log = logging.getLogger(__name__)


def bump_cpu_count_config(cpu_change=0.1):
    print("MDs bump_cpu_count_config")
    config = get_cassandra_config()
    config['env']['CASSANDRA_CPUS'] = str(
        float(config['env']['CASSANDRA_CPUS']) + cpu_change
    )
    print("mds..chaging cpu now.. " + config['env']['CASSANDRA_CPUS'])
    # This count is not getting changed actualy!
    # In cassandra plugins code we are actually calling install request for the package is upgrade cluster
    response = request(
        dcos.http.put,
        marathon_api_url('apps/' + PACKAGE_NAME),
        json=config,
        is_success=request_success
    )

    return response


counter = 0


def get_and_verify_plan(predicate=lambda r: True, assert_success=True):
    print("mds inside get_and_verify_plan")
    global counter

    def fn():
        str = cassandra_api_url('plans/deploy')
        print("get_and_verify_plan: " + str)
        return dcos.http.get(str, is_success=request_success)

    def success_predicate(result):
        global counter
        message = 'Request to /plan failed'

        try:
            body = result.json()
        except Exception:
            return False, message

        if counter < 3:
            counter += 1

        if predicate(body):
            counter = 0

        return predicate(body), message

    return spin(fn, success_predicate, wait_time=HEALTH_WAIT_TIME, assert_success=assert_success).json()


def request_success(status_code):
    return 200 <= status_code < 300 or 500 <= status_code <= 503 or status_code == 409 or status_code == 401


def get_node_host():
    def fn():
        try:
            print("MDS get_node_host")
            return shakedown.get_service_ips(PACKAGE_NAME)
        except (IndexError, DCOSHTTPException):
            return set()

    def success_predicate(result):
        return len(result) == DEFAULT_NODE_COUNT, 'Nodes failed to return'

    return spin(fn, success_predicate).pop()


def get_scheduler_host():
    return shakedown.get_service_ips('marathon').pop()


def kill_task_with_pattern(pattern, host=None):
    log.info("Killing task with pattern: " + pattern)
    command = (
        "sudo kill -9 "
        "$(ps ax | grep {} | grep -v grep | tr -s ' ' | sed 's/^ *//g' | "
        "cut -d ' ' -f 1)".format(pattern)
    )
    if host is None:
        result = shakedown.run_command_on_master(command)
    else:
        result = shakedown.run_command_on_agent(host, command)

    if not result:
        raise RuntimeError(
            'Failed to kill task with pattern "{}"'.format(pattern)
        )
    else:
        log.warning("Killed task with pattern: " + pattern)


def kill_cassandra_daemon_executor(pattern, host=None):
    command = (
        "sudo kill -9 "
        "$(ps -ef | grep {} | grep mds | tr -s ' ' | sed 's/^ *//g' | cut -d ' ' -f 3)".format(pattern)
    )
    if host is None:
        result = shakedown.run_command_on_master(command)
    else:
        result = shakedown.run_command_on_agent(host, command)

    if not result:
        raise RuntimeError(
            'Failed to kill Cassandra Daemon Executor'
        )


def run_cleanup():
    payload = {'nodes': '*'}
    str = cassandra_api_url('/plans/cleanup/start')
    print(str)
    #dcos.http.post(str, data=None, json=payload)
    request(
        dcos.http.post,
        str,
        data=None,
        json=payload#,
        #is_success=request_success
    )

def verify_plan(plan):
    if (plan['phases'][1]['id'] != plan['phases'][1]['id'] or len(plan['phases']) < len(plan['phases']) or
                plan['status'] == infinity_commons.PlanState.IN_PROGRESS.value or
                plan['status'] == infinity_commons.PlanState.COMPLETE.value ):
        print("Came up again")
        return True
    return False


def run_planned_operation(operation, failure=lambda: None, recovery=lambda: None):
    #plan = get_and_verify_plan()
    print("Running planned operation")
    operation()
    print("MDS12.. run_planned_operation")
    # Give scheduler time to come up again

    print("MDS.. run_planned_operation")
    time.sleep(240)

    # get_and_verify_plan(
    #     lambda p: (
    #         plan['phases'][1]['id'] != p['phases'][1]['id'] or
    #         len(plan['phases']) < len(p['phases']) or
    #         p['status'] == infinity_commons.PlanState.IN_PROGRESS.value
    #     )
    #)
    print("sleeping for 120 sec..let nodes come up again..run_planned_operation")
    #time.sleep(120)
    print("Mds.. Run failure operation")
    failure()
    print("sleeping for 120 sec..let nodes come up again..run_planned_operation")
    time.sleep(120)
    print("Run recovery operation")
    recovery()
    print("Verify plan after failure")
    print("sleeping for 120 sec..let nodes come up again..run_planned_operation")
    time.sleep(120)
    get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)


def run_repair():
    payload = {'nodes': '*'}
    str = cassandra_api_url('/plans/repair/start')
    print(str)
    #dcos.http.post(str, data=None, json=payload)
    request(
        dcos.http.post,
        str,
        data=None,
        json=payload#,
        #is_success=request_success
    )


def _block_on_adminrouter(master_ip):
    headers = {'Authorization': "token={}".format(shakedown.dcos_acs_token())}
    metadata_url = "http://{}/metadata".format(master_ip)

    def get_metadata():
        response = requests.get(metadata_url, headers=headers)
        return response

    def success(response):
        error_message = "Failed to parse json"
        try:
            is_healthy = response.json()['PUBLIC_IPV4'] == master_ip
            return is_healthy, "Master is not healthy yet"
        except Exception:
            return False, error_message

    spin(get_metadata, success, HEALTH_WAIT_TIME)
    log.info("Master is up again.  Master IP: {}".format(master_ip))


def verify_leader_changed(old_leader_ip):

    log.info(sys._getframe().f_code.co_name)

    def fn():
        try:
            return shakedown.master_leader_ip()
        except DCOSAuthenticationException:
            log.error("Got exception while fetching leader")
        return old_leader_ip

    def success_predicate(new_leader_ip):
        is_success = old_leader_ip != new_leader_ip
        if is_success :
            return is_success, "(MDS) Success and leader has changed!"
        is_success = old_leader_ip == new_leader_ip
        if is_success :
            return is_success, "(MDS) Success and leader has not changed!"


    result = spin(fn, success_predicate)
    log.info("Leader IP {}".format(result))


# Check if mesos agent / spartan is not running. Restart spartan to see if it is fixed
def recover_host_from_partitioning(host):
    # if is_dns_healthy_for_node(host):
    log.info("Restarting erlang and mesos on {}".format(host))
    restart_erlang_on_host(host)
    shakedown.restart_agent(host)


def is_dns_healthy_for_node(host):
    unit_and_node = "dcos-spartan.service/nodes/{}".format(host)
    health_check_url = unit_health_url(unit_and_node)
    try:
        response = dcos.http.get(health_check_url)
        return response.json()['health'] == 0
    except DCOSException:
        print("DNS call not responding")
    return False


def restart_erlang_on_host(host):
    command = "sudo systemctl restart dcos-epmd.service"
    print("Restarting erlang daemon")
    result = shakedown.run_command_on_agent(host, command)
    if not result:
        raise RuntimeError(
            'Failed to run command {} on {}'.format(command, host)
        )


ONE_MINUTE = 60
# Restart mesos agent if service is stuck or not running on some nodes
def recover_failed_agents(hosts):
    log.info("Recover failed agents- {}".format(str(hosts)))
    tasks = {}
    try:
        tasks = check_health(wait_time=ONE_MINUTE, assert_success=False)
        log.info("Failed_tasks- " + str(tasks))
        failed_hosts = find_failed_hosts(hosts, tasks)
        log.info("Failed_hosts- " + failed_hosts)
        for h in failed_hosts:
            log.info("Restarting mesos agent on {}".format(h))
            shakedown.restart_agent(h)
    except Exception as e:
        log.error("error in recover_failed_agents")
        log.error(str(e))


def find_failed_hosts(hosts, tasks):
    failed_hosts = set(hosts)
    for t in tasks:
        if t['state'] == TASK_RUNNING_STATE:
            host = t['labels'][2]['value']
            failed_hosts.discard(host)
    return failed_hosts


def recover_agents(hosts):
    get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value, assert_success=False)
    for h in hosts:
        print("Restarting mesos agent on {}".format(h))
        shakedown.restart_agent(h)


def setup_module():
    unset_ssl_verification()
    #uninstall()
    #install()
    log.info("Starting Health Check " + sys._getframe().f_code.co_name)
    log.info("service_name = " + SERVICE_NAME)
    check_health()


def teardown_module():
    print("Not killing the cluster, do all tests on the same cluster only")
    #uninstall()

'''
@pytest.mark.recovery
def test_kill_task_in_node():
    kill_task_with_pattern('CassandraDaemon', get_node_host())
    check_health()
    log.info("Exit " + sys._getframe().f_code.co_name)


@pytest.mark.recovery
def test_kill_all_task_in_node():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)
    for host in hosts:
        kill_task_with_pattern('CassandraDaemon', host)

    recover_failed_agents(hosts)
    check_health()


@pytest.mark.recovery
def test_scheduler_died():
    kill_task_with_pattern('cassandra.scheduler.Main', get_scheduler_host())
    check_health()


@pytest.mark.recovery
def test_executor_killed():
    host = get_node_host()
    log.info("host- " + str(host))
    kill_cassandra_daemon_executor('CassandraDaemon', host)

    recover_failed_agents([host])
    check_health()


@pytest.mark.recovery
def test_all_executors_killed():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)
    for host in hosts:
        kill_cassandra_daemon_executor('CassandraDaemon', host)

    recover_failed_agents(hosts)
    check_health()


@pytest.mark.recovery
def test_master_killed_block_on_admin_router():
    print("Starting.....")
    master_leader_ip = shakedown.master_leader_ip()
    log.info(sys._getframe().f_code.co_name)

    kill_task_with_pattern('mesos-master', master_leader_ip)

    # For us this is to verify that the leader/ mesos-master has come up again
    verify_leader_changed(master_leader_ip)
    check_health()
'''

# FAiled and made cluster unusable but started working after changing the dcos_auth_token in cli
@pytest.mark.recovery
def test_zk_killed_recovery():
    time.sleep(60)
    log.info("Starting {}".format(sys._getframe().f_code.co_name))

    master_leader_ip = shakedown.master_leader_ip()
    log.info("master leader ip- " + master_leader_ip)

    kill_task_with_pattern('zookeeper', master_leader_ip)

    _block_on_adminrouter(master_leader_ip)
    time.sleep(60)
    log.info("Taking a health check")
    check_health()
    print("Sleeping for 120 sec")
    time.sleep(120)


# FAiled
# Failed again when run after the above test
@pytest.mark.recovery
def test_zk_killed():
    time.sleep(60)
    log.info("Starting {}".format(sys._getframe().f_code.co_name))

    master_leader_ip = shakedown.master_leader_ip()
    log.info("master leader ip- " + master_leader_ip)

    kill_task_with_pattern('zookeeper', master_leader_ip)

    time.sleep(60)
    #_block_on_adminrouter(master_leader_ip)
    verify_leader_changed(master_leader_ip)
    time.sleep(60)
    check_health()
#'''

# Set 2
'''
#Passed
@pytest.mark.recovery
def test_partition():
    host = get_node_host()

    shakedown.partition_agent(host)
    time.sleep(20)
    shakedown.reconnect_agent(host)
    recover_host_from_partitioning(host)

    check_health()


#Passed
@pytest.mark.recovery
def test_partition_master_both_ways():
    master_leader_ip = shakedown.master_leader_ip()
    shakedown.partition_master(master_leader_ip)
    time.sleep(20)
    shakedown.reconnect_master(master_leader_ip)

    check_health()
    time.sleep(60)

#Passed
@pytest.mark.recovery
def test_partition_master_incoming():
    master_leader_ip = shakedown.master_leader_ip()
    shakedown.partition_master(master_leader_ip, incoming=True, outgoing=False)
    time.sleep(20)
    shakedown.reconnect_master(master_leader_ip)

    check_health()
    time.sleep(60)

#Passed
@pytest.mark.recovery
def test_partition_master_outgoing():
    master_leader_ip = shakedown.master_leader_ip()
    shakedown.partition_master(master_leader_ip, incoming=False, outgoing=True)
    time.sleep(20)
    shakedown.reconnect_master(master_leader_ip)

    check_health()
    time.sleep(60)

#Passed
@pytest.mark.recovery
def test_all_partition():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)

    for host in hosts:
        shakedown.partition_agent(host)
    time.sleep(20)
    for host in hosts:
        shakedown.reconnect_agent(host)
    for host in hosts:
        recover_host_from_partitioning(host)

    check_health()
    time.sleep(60)
    check_health()
'''

'''
#Passes otherwise but started failing when it runs continuously after the above test
#Managed this by restarting mesos-master and mesos-epmd service
@pytest.mark.recovery
def test_config_update_then_kill_task_in_node():
    print("here test_config_update_then_kill_task_in_node")
    hosts = shakedown.get_service_ips(PACKAGE_NAME)
    host = get_node_host()
    if hosts is not None:
        print("mds hosts " + str(hosts))

    if host is not None:
        print("mds hosts " + str(host))

    run_planned_operation(
        bump_cpu_count_config,
        lambda: kill_task_with_pattern('CassandraDaemon', host),
        lambda: recover_failed_agents(hosts)
    )
    check_health()
    time.sleep(60)

#bump_cpu_count_config does not evaluates
@pytest.mark.recovery
def test_config_update_then_kill_all_task_in_node():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)

    run_planned_operation(
        lambda: bump_cpu_count_config(-0.1),
        lambda: [kill_task_with_pattern('CassandraDaemon', h) for h in hosts],
        lambda: recover_failed_agents(hosts)
    )
    check_health()
    time.sleep(60)


#passed
# When running continuously the first node didn't come up, mesos-service and mesos-epmd restart
# also didn't work.
# Scheduler restart did the work
@pytest.mark.recovery
def test_config_update_then_scheduler_died():
    host = get_scheduler_host()

    run_planned_operation(
        bump_cpu_count_config,
        lambda: kill_task_with_pattern('cassandra.scheduler.Main', host)
    )

    time.sleep(60)
    check_health()


#Passed
@pytest.mark.recovery
def test_config_update_then_executor_killed():
    host = get_node_host()

    print("test_config_update_then_executor_killed " + str(host))
    run_planned_operation(
        lambda: bump_cpu_count_config(-0.1),
        lambda: kill_cassandra_daemon_executor('CassandraDaemon', host),
        lambda: recover_failed_agents([host])
    )
    time.sleep(60)
    check_health()


# passed
# All tasks start staging/ running simultaneously when killed by this test
@pytest.mark.recovery
def test_config_update_then_all_executors_killed():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)

    run_planned_operation(
        bump_cpu_count_config,
        lambda: [kill_cassandra_daemon_executor('CassandraDaemon', h) for h in hosts],
        lambda: recover_failed_agents(hosts)
    )
    time.sleep(60)
    check_health()

#passed
@pytest.mark.recovery
def test_config_update_then_master_killed():
    master_leader_ip = shakedown.master_leader_ip()
    print("master_leader_ip: " + master_leader_ip)
    run_planned_operation(
        lambda: bump_cpu_count_config(-0.1), lambda: kill_task_with_pattern('mesos-master', master_leader_ip)
    )
    #verify_leader_changed(master_leader_ip)
    # if check health succeds then that by itself means that master is up
    # as otherwise taks would not have communicated, this could have delayed effects
    # find out while testing
    print("Will do health check now..")
    time.sleep(60)
    check_health()
    
    

#Passed
@pytest.mark.recovery
def test_config_update_then_zk_killed():
    master_leader_ip = shakedown.master_leader_ip()
    print("master_leader_ip: " + master_leader_ip)
    run_planned_operation(
        bump_cpu_count_config,
        lambda: kill_task_with_pattern('zookeeper', master_leader_ip)#,
        #lambda: verify_leader_changed(master_leader_ip)
    )
    time.sleep(60)
    check_health()




#Passed
@pytest.mark.recovery
def test_config_update_then_partition():
    host = get_node_host()
    print("test_config_update_then_executor_killed " + str(host))

    def partition():
        shakedown.partition_agent(host)
        print("partitioned the host- " + host)
        time.sleep(20)
        shakedown.reconnect_agent(host)
        print("Reconnected the agent- " + host)

    run_planned_operation(
        lambda: bump_cpu_count_config(-0.1), partition, lambda: recover_host_from_partitioning(host))

    time.sleep(60)
    check_health()


#Passed
#Failed in contiguous run
@pytest.mark.recovery
def test_config_update_then_all_partition():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)

    def partition():
        for host in hosts:
            shakedown.partition_agent(host)
        time.sleep(20)
        for host in hosts:
            shakedown.reconnect_agent(host)

    def recovery():
        for host in hosts:
            recover_host_from_partitioning(host)

    run_planned_operation(bump_cpu_count_config, partition, recovery)
    time.sleep(60)
    check_health()


#passed
@pytest.mark.recovery
def test_cleanup_then_kill_task_in_node():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)
    print(str(hosts))

    host = get_node_host()
    print("host = " + str(host))

    run_planned_operation(
        run_cleanup,
        lambda: kill_task_with_pattern('CassandraDaemon', host),
        lambda: recover_failed_agents(hosts)
    )

    check_health()

#passed
@pytest.mark.recovery
def test_cleanup_then_kill_all_task_in_node():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)

    run_planned_operation(
        run_cleanup,
        lambda: [kill_task_with_pattern('CassandraDaemon', h) for h in hosts],
        lambda: recover_failed_agents(hosts)
    )

    check_health()



#node-0 got lost after scheduler started again
#Passed
@pytest.mark.recovery
def test_cleanup_then_scheduler_died():
    host = get_scheduler_host()
    run_planned_operation(run_cleanup, lambda: kill_task_with_pattern('cassandra.scheduler.Main', host))

    check_health()


#Passed
@pytest.mark.recovery
def test_cleanup_then_executor_killed():
    host = get_node_host()

    run_planned_operation(
        run_cleanup,
        lambda: kill_cassandra_daemon_executor('CassandraDaemon', host),
        lambda: recover_failed_agents([host])
    )

    check_health()


#passed
@pytest.mark.recovery
def test_cleanup_then_all_executors_killed():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)

    run_planned_operation(
        run_cleanup,
        lambda: [kill_cassandra_daemon_executor('CassandraDaemon', h) for h in hosts],
        lambda: recover_failed_agents(hosts)
    )

    check_health()

#passed
@pytest.mark.recovery
def test_cleanup_then_master_killed():
    master_leader_ip = shakedown.master_leader_ip()
    run_planned_operation(run_cleanup, lambda: kill_task_with_pattern('mesos-master', master_leader_ip))

    verify_leader_changed(master_leader_ip)
    check_health()



#passed
@pytest.mark.recovery
def test_cleanup_then_zk_killed():
    master_leader_ip = shakedown.master_leader_ip()
    run_planned_operation(
        run_cleanup,
        lambda: kill_task_with_pattern('zookeeper', master_leader_ip),
        lambda: verify_leader_changed(master_leader_ip))

    check_health()
'''

'''
#Passed but all cleanup tasks got launched simultaneously
#Passed
@pytest.mark.recovery
def test_cleanup_then_partition():
    host = get_node_host()

    def partition():
        shakedown.partition_agent(host)
        time.sleep(20)
        shakedown.reconnect_agent(host)

    run_planned_operation(run_cleanup, partition, lambda: recover_host_from_partitioning(host))

    check_health()

#Passed
# After this test the tasks keep getting killed and restarted!
# Also the time of the task update keeps on changing
#Passed
@pytest.mark.recovery
def test_cleanup_then_all_partition():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)

    def partition():
        for host in hosts:
            shakedown.partition_agent(host)
        time.sleep(20)
        for host in hosts:
            shakedown.reconnect_agent(host)

    def recovery():
        for host in hosts:
            recover_host_from_partitioning(host)

    run_planned_operation(run_cleanup, partition, recovery)
    check_health()


#Passed
#Passed
@pytest.mark.recovery
def test_repair_then_kill_task_in_node():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)
    host = get_node_host()

    run_planned_operation(
        run_repair,
        lambda: kill_task_with_pattern('CassandraDaemon', host),
        lambda: recover_failed_agents(hosts)
    )

    check_health()

#flaky
#Passed
@pytest.mark.recovery
def test_repair_then_kill_all_task_in_node():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)

    run_planned_operation(
        run_repair,
        lambda: [kill_task_with_pattern('CassandraDaemon', h) for h in hosts],
        lambda: recover_failed_agents(hosts)
    )

    check_health()



#Passed
#Passed
@pytest.mark.recovery
def test_repair_then_scheduler_died():
    host = get_scheduler_host()
    run_planned_operation(run_repair, lambda: kill_task_with_pattern('cassandra.scheduler.Main', host))

    check_health()


#Passed
#Passed
@pytest.mark.recovery
def test_repair_then_executor_killed():
    host = get_node_host()

    run_planned_operation(
        run_repair,
        lambda: kill_cassandra_daemon_executor('CassandraDaemon', host),
        lambda: recover_failed_agents([host])
    )

    check_health()


#Passed
#Passed
@pytest.mark.recovery
def test_repair_then_all_executors_killed():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)

    run_planned_operation(
        run_repair,
        lambda: [kill_cassandra_daemon_executor('CassandraDaemon', h) for h in hosts],
        lambda: recover_failed_agents(hosts)
    )

    check_health()


#Passed
#Passed
@pytest.mark.recovery
def test_repair_then_master_killed():
    master_leader_ip = shakedown.master_leader_ip()
    run_planned_operation(run_repair, lambda: kill_task_with_pattern('mesos-master', master_leader_ip))

    verify_leader_changed(master_leader_ip)
    check_health()

#Passed
#Passed
@pytest.mark.recovery
def test_repair_then_zk_killed():
    master_leader_ip = shakedown.master_leader_ip()
    run_planned_operation(
        run_repair,
        lambda: kill_task_with_pattern('zookeeper', master_leader_ip),
        lambda: verify_leader_changed(master_leader_ip)
    )

    check_health()

#Passed
#Passed
@pytest.mark.recovery
def test_repair_then_partition():
    host = get_node_host()

    def partition():
        shakedown.partition_agent(host)
        time.sleep(20)
        shakedown.reconnect_agent(host)

    run_planned_operation(run_repair, partition, lambda: recover_host_from_partitioning(host))

    check_health()


#Not sure it passed
#Passed
@pytest.mark.recovery
def test_repair_then_all_partition():
    hosts = shakedown.get_service_ips(PACKAGE_NAME)

    def partition():
        for host in hosts:
            shakedown.partition_agent(host)
        time.sleep(20)
        for host in hosts:
            shakedown.reconnect_agent(host)

    def recovery():
        for host in hosts:
            recover_host_from_partitioning(host)

    run_planned_operation(run_repair, partition, recovery)

    check_health()
'''