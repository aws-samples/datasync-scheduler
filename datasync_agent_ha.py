import boto3
import time
import argparse
import logging
import common

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
    logger.info("specified task execution: %s", exec_arns_list)
    return exec_arns_list

def check_failed_task(exec_arns_list):
    failed_arn_list = []
    for exec_arn in exec_arns_list:
        res = ds_client.describe_task_execution(TaskExecutionArn=exec_arn)
        if res['Status'] == 'ERROR':
            failed_arn_list.append(exec_arn)
    logger.info("failed task execution: %s", failed_arn_list)
    return failed_arn_list

def get_tasks_info(exec_arn):
    if exec_arn:
        tasks_info = []
        manifest = {}
        task_arn = exec_arn.split("/execution")[0]
        task_res = ds_client.describe_task(TaskArn=task_arn)
        src_location = task_res['SourceLocationArn']
        src_loc_res = ds_client.describe_location_nfs(LocationArn=src_location)
        server_name = src_loc_res['LocationUri'].split('/')[2]
        subdir = "/" + src_loc_res['LocationUri'].split('/')[3]
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
        task_info={"src_loc": src_location, "dest_loc": dest_location, "cloud_watch_arn": cloud_watch_arn, "task_name": task_name,"manifest": manifest, "subdir": subdir, "server_name": server_name}
    else:
        logger.info("Error: exec arn dose not exist")
    logger.info("failed task info: %s", task_info)
    return task_info
# Commentted out 
#def distribute_failed_task(failed_agents_list):
#    return new_task_list

### starting main()
if __name__ == "__main__":
    retried_task_arn_list = []
    while 1:
        failed_arn_list = []
        online_agents = common.get_online_agents(ds_client, logger)
        exec_arns_list = get_exec_arns(arn_file)
        failed_list = check_failed_task(exec_arns_list)

        #remove already retried task from failed_list
        [ failed_arn_list.append(i) for i in failed_list if i not in retried_task_arn_list ] 

        # test failed arn
        #failed_arn_list = ['arn:aws:datasync:ap-northeast-2:253679086765:task/task-0612c3d1ba3700499/execution/exec-04cbdb7375cf9e61c', 'arn:aws:datasync:ap-northeast-2:253679086765:task/task-0dc8bdbe78c2cf3ad/execution/exec-01ef6c476699454d5']
        failed_arn_count = len(failed_arn_list)
        if failed_arn_count > 0 and len(online_agents) >= failed_arn_count:
            no = 0
            retry_tasks_arn = []
            # create retry tasks
            for task_arn in failed_arn_list:
                ti = get_tasks_info(task_arn)
                create_src_loc_res = common.create_src_loc(ds_client, ti['subdir'], ti['server_name'], online_agents[no], logger)
                create_task_res = common.create_task(ds_client, create_src_loc_res, ti['dest_loc'], ti['cloud_watch_arn'], ti['task_name'], no, ti['manifest'], logger)
                retry_tasks_arn.append(create_task_res['TaskArn'])
                logger.info("retry task created: %s", create_task_res["TaskArn"])
                retried_task_arn_list.append(task_arn)
                no += 1
            exec_arns = []
            # execute retry tasks
            for task_arn in retry_tasks_arn:
                start_retry_task_res = common.start_task(ds_client, task_arn, logger)
                exec_arns.append(start_retry_task_res["TaskExecutionArn"])
                logger.info("retry task executed: %s", start_retry_task_res["TaskExecutionArn"])
                logger.info("There is/are %s retry task execution.....", no)
            # show retry esecution arn
        else:
            logger.info("no failed task")
        logger.info("sleeping %s sec", timeout_sec)
        time.sleep(timeout_sec)
