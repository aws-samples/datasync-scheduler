"""
ChangeLog:
    - 2023.03.14: add check_final_task_status() to gather the status of latest task execution of specific task
    - 2023.03.07: update available_agents not to use same agent
    - 2023.02.23: update_location instead of creating new task
"""
import boto3
import time
import argparse
import logging
import common
from urllib.parse import urlparse
import random

parser = argparse.ArgumentParser()
parser.add_argument('--timeout_sec', help='specify sleeping timeout seconds ex) 300', default='300', action='store', required=False)
parser.add_argument('--arn_file', help='specify task arn list file ex) arn_file.txt', default='arn_file.txt', action='store', required=False)

args = parser.parse_args()
timeout_sec = int(args.timeout_sec)
arn_file = args.arn_file

# Global variables
ds_ha_logfile = "log/ds_ha.log"
ds_client = boto3.client('datasync')

# Logger configuration
## create log dir
try:
    os.makedirs('log')
except: pass

## Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

## logging for console log
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

## logging for file log
file_handler = logging.FileHandler(ds_ha_logfile)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

#Define functions
def get_exec_arns(arn_file):
    """ read arn_file.txt file, and return TaskExecutionArns list
    """
    f = open(arn_file,'r')
    exec_arns_list = [odir.rstrip() for odir in f.readlines() ]
    for exec_arn in exec_arns_list:
        logger.info("monitoring task execution: %s", exec_arn)
    return exec_arns_list

def check_final_task_status(task_exec_arn):
    """ check status of latest task executions to determine retry or not
    """
    task_arn = task_exec_arn.split("/execution")[0]
    res = ds_client.list_task_executions(TaskArn=task_arn)
    if res['TaskExecutions'][-1]['Status'] == 'ERROR':
        return task_exec_arn
    else:
        return None
    #print("list of task execution:", res['TaskExecutions'][-1]['Status'])


def check_failed_task(exec_arns_list):
    failed_arn_list = []
    for exec_arn in exec_arns_list:
        failed_exec_arn = check_final_task_status(exec_arn)
        #res = ds_client.describe_task_execution(TaskExecutionArn=exec_arn)
        #if res['Status'] == 'ERROR':
        #    failed_arn_list.append(exec_arn)
        if failed_exec_arn:
            failed_arn_list.append(exec_arn)
    return failed_arn_list

def get_task_info(exec_arn):
    if exec_arn:
        tasks_info = []
        manifest = {}
        task_arn = exec_arn.split("/execution")[0]
        task_res = ds_client.describe_task(TaskArn=task_arn)
        src_location = task_res['SourceLocationArn']
        src_loc_res = ds_client.describe_location_nfs(LocationArn=src_location)
        nfs_url=src_loc_res['LocationUri']
        parts = urlparse(nfs_url)
        server_name = parts.netloc
        subdir = parts.path
        dest_location = task_res['DestinationLocationArn']
        cloud_watch_arn = task_res['CloudWatchLogGroupArn']
        task_name = task_res['Name'] + "_retry"
        if len(task_res['Includes']):
            manifest['incl'] =  task_res['Includes'][0]['Value']
        else:
            manifest['incl'] = ""
        if len(task_res['Excludes']):
            manifest['excl'] =  task_res['Excludes'][0]['Value']
        else:
            manifest['excl'] = ""
        task_info={"task_arn": task_arn, "src_loc": src_location, "dest_loc": dest_location, "cloud_watch_arn": cloud_watch_arn, "task_name": task_name,"manifest": manifest, "subdir": subdir, "server_name": server_name}
    else:
        logger.info("Error: exec arn dose not exist")
    logger.info("failed task info: %s", task_info)
    return task_info
# Commentted out 
#def distribute_failed_task(failed_agents_list):
#    return new_task_list

### starting main()
if __name__ == "__main__":
    retried_t_exec_arn_list = []
    while 1:
        failed_arn_list = []
        available_agents_arn = common.get_available_agents(ds_client, logger)
        exec_arns_list = get_exec_arns(arn_file)
        failed_list = check_failed_task(exec_arns_list)

        #remove already retried task from failed_list
        [ failed_arn_list.append(i) for i in failed_list if i not in retried_t_exec_arn_list ] 

        # test failed arn
        failed_arn_count = len(failed_arn_list)
        if failed_arn_count > 0 and len(available_agents_arn) >= failed_arn_count:
            no = 0
            retry_tasks_arn = []
            # update retry source_location_nfs
            for t_exec_arn in failed_arn_list:
                ti = get_task_info(t_exec_arn)
                print("agent arn: ", available_agents_arn[-1])
                update_src_loc_res = common.update_loc_nfs(ds_client, ti['src_loc'], available_agents_arn[-1], logger)
                available_agents_arn.pop(-1)
                if len(available_agents_arn) == 0:
                    available_agents_arn = common.get_available_agents(ds_client, logger)
                    random.shuffle(available_agents_arn)
                logger.info("updated source location: %s", ti['dest_loc'])
                task_arn = ti['task_arn']
                retry_tasks_arn.append(task_arn)
                retried_t_exec_arn_list.append(t_exec_arn)
                #logger.info("retry task created: %s", create_task_res["TaskArn"])
                no += 1
            # execute retry tasks
            for task_arn in retry_tasks_arn:
                start_retry_task_res = common.start_task(ds_client, task_arn, logger)
                logger.info("retry task executed: %s", start_retry_task_res["TaskExecutionArn"])
                logger.info("There is/are %s retry task execution.....", no)
            # show retry esecution arn
        else:
            logger.info("no failed task")
        logger.info("sleeping %s sec", timeout_sec)
        time.sleep(timeout_sec)
