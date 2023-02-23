import boto3
import os
import glob
import argparse
import logging
import common
import time

"""
ChangeLog
- 2023.02.23: get_available_agents() instead of get_online_agents
"""

# introduction
## 1. set aws configure
## 2. create manifest and divide it
## 3. create location with agents
## 4. create tasks for each agent
## 5. run task
## boto3 should be > 1.26 to support "Includes" Directive

parser = argparse.ArgumentParser()
parser.add_argument('--task_name', help='specify your task name prefix ex) distributed_taskt', action='store', required=True)
parser.add_argument('--nfs_server_name', help='specify on-premise nfs hostname nfs://10.0.0.10/vol1 ex) 10.0.0.10', action='store', required=True)
parser.add_argument('--sub_dir', help='specify sub directory of nfs URI nfs://10.0.0.10/vol1 ex) /vol1', action='store', required=True)
parser.add_argument('--mount_path_dir', help='specify local directory to be mounted nfs volume ex) /vol1', action='store', required=True)
parser.add_argument('--dest_loc', help='specify destination location arn, you can get this arn from aws datasync webconsole', action='store', required=True)
parser.add_argument('--cloudwatch_arn', help='specify cloud watch arn, you can get this arn from aws datasync webconsole', action='store', required=True)
parser.add_argument('--source_file', help='specify include file list ex)source_file.manifest', action='store', required=True)

args = parser.parse_args()
task_name_prefix = args.task_name
nfs_server_name = args.nfs_server_name
sub_dir = args.sub_dir
mount_path_dir = args.mount_path_dir
source_file = args.source_file
dest_loc = args.dest_loc
cloudwatch_arn = args.cloudwatch_arn
# Global variables
"""
task_name_prefix = "Distributed_Tasks_"
nfs_server_name="198.19.255.158"
sub_dir="/vol1"
mount_path_dir="/fsx"
dest_loc = "arn:aws:datasync:ap-northeast-2:XXXXXXXXXXXX:location/loc-042c919ab6996b437"
cloudwatch_arn = "arn:aws:logs:ap-northeast-2:XXXXXXXXXXXX:log-group:/aws/datasync:*"
source_file = "source_file.manifest"
"""
max_depth_size = 5
output_file = "arn_file.txt"
ds_ma_sche_logfile = "log/ds_ma_sche.log"
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
file_handler = logging.FileHandler(ds_ma_sche_logfile)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Define functions
def divide_by_agent_count(lst, n):
    """ split list elements by number
    it's used to spread include list to each agents
    """
    splitted = []
    for i in reversed(range(1, n + 1)):
        split_point = len(lst)//i
        splitted.append(lst[:split_point])
        lst = lst[split_point:]
    return splitted

def check_source_file(source_file):
    f = open(source_file,'r')
    origin_file_manifest = [(mount_path_dir + odir).rstrip() for odir in f.readlines() if odir[0] == '#' ]
    for path in origin_file_manifest:
        if not os.path.exists(path):
            logger.info("non path:: %s", path)
            logger.info("Input correct directory path in source file")
            exit()
def remove_sub_dir(include_list, mount_path_dir):
    new_incl_list = []
    for i in include_list:
        new_item = i.replace(mount_path_dir,'')
        new_incl_list.append(new_item)
    return new_incl_list

def get_source_filelist(source_file):
    """ read source_manifest file, and return directory list
    """
    f = open(source_file,'r')
    origin_file_manifest = [(mount_path_dir + odir).rstrip() for odir in f.readlines() ]
    return origin_file_manifest

def create_include_list(source_file, available_agents):
    """ create include_list to separate directories to ditribute task for each agent.
    """
    agent_count = len(available_agents)
    path_delimeter = "/*"
    depth_del = ""
    include_list = []
    temp_dirs_list = []
    #total_file_manifest = origin_file_manifest
    dirs_list = get_source_filelist(source_file)
    logger.info("Distribute directories to agents")
    if len(dirs_list) >= agent_count:
        include_list = remove_sub_dir(dirs_list, mount_path_dir)
        return include_list
    elif agent_count == 1:
        logger.info("there is only 1 agent")
        include_list = remove_sub_dir(dirs_list, mount_path_dir)
        return include_list
    else:
        for depth_size in range(max_depth_size):
            depth_del += path_delimeter
            for path in dirs_list:
                path_depth = glob.glob(path+depth_del)
                path_dirs_list = [ d for d in path_depth if os.path.isdir(d) ] 
                if path_dirs_list == [] and path_depth != []:
                    dir_name, file_name = os.path.split(path_depth[0])
                    path_dirs_list.append(dir_name)
                temp_dirs_list += path_dirs_list
            if len(temp_dirs_list) >= agent_count:
                include_list = remove_sub_dir(temp_dirs_list, mount_path_dir)
                return include_list
                break
        logger.info("dir scan reached to max_depth_size, but can't find proper directory count. It will return source dir lists." )
        return remove_sub_dir(dirs_list, mount_path_dir)

def allocate_include_to_agent(include_list, available_agents):
    """ allcate separated include_list to each agent
    return example: {'agent1_arn': {'incl': ['/dir1', '/dir2'], 'excl':''}, 'agent2_arn':{'incl': ['dir3', 'dir4'], 'excl': ''}, 'agent3_arn':{'incl':'dir5', 'excl':['dir1', 'dir2','dir3','dir4', 'dir5']}}
    """
    manifest_per_agent = {}
    agent_count = len(available_agents)
    source_list = get_source_filelist(source_file)
    if agent_count > 1:
        x = divide_by_agent_count(include_list, agent_count-1)
        task_manifest = {available_agents[i]: {"incl":x[i], "excl":""}  for i in range(agent_count-1)}
        # add exclude_list into first agent to transfer remaining files
        exclude_list = include_list
        task_manifest[available_agents[-1]] = {"incl": remove_sub_dir(source_list, mount_path_dir), "excl": exclude_list}
        logger.info("agents: %s \n task manifest: %s",agent_count, task_manifest)
        return task_manifest
    elif agent_count == 1:
        task_manifest = {available_agents[i]: {"incl":x[i], "excl":""}  for i in range(agent_count-1)}
        logger.info("agents: %s \n task manifest: %s",agent_count, task_manifest)
        return task_manifest
    else:
        logger.info("there is no available agent")


### starting main()
if __name__ == "__main__":
    available_agents = common.get_available_agents(ds_client, logger)
    #initialize output file
    with open(output_file, 'w'):
        pass

    # check input files or dirs  exist
    check_source_file(source_file)

    # create manifest for each agent
    #agent_manifest = create_manifest(source_file, available_agents)
    include_list = create_include_list(source_file, available_agents)

    # map separated include list to each agent
    agent_manifest = allocate_include_to_agent(include_list, available_agents)

    # create tasks
    no = 0
    tasks_arns = []
    for agent_arn in agent_manifest:
        no += 1
        src_loc = common.create_src_loc(ds_client, sub_dir, nfs_server_name, agent_arn, logger)
        create_task_res = common.create_task(ds_client, src_loc, dest_loc, cloudwatch_arn, task_name_prefix, no, agent_manifest[agent_arn], logger)
        if create_task_res != None:
            tasks_arns.append(create_task_res["TaskArn"])

    # start tasks
    exec_arns = []
    for task_arn in tasks_arns:
        start_task_res = common.start_task(ds_client, task_arn, logger)
        exec_arns.append(start_task_res["TaskExecutionArn"])
    for exec_arn in exec_arns:
        with open(output_file, 'a') as arn_file:
            arn_file.write(exec_arn + "\n")
