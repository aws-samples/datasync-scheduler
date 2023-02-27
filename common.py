## boto3 for datasync: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/datasync.html#DataSync.Client.create_task
"""
ChangeLog:
- 2023.02.23: add get_available_agents() instead of get_online_agents
"""
import time
def get_running_agents(ds_client):
    running_agents=[]
    tasks_res = ds_client.list_tasks(
        MaxResults=100,
    )
    for task in tasks_res['Tasks']:
        if task['Status'] == 'RUNNING':
            task_arn = task['TaskArn']
            desc_task_res = ds_client.describe_task(TaskArn=task_arn)
            src_arn = desc_task_res['SourceLocationArn']
            desc_src_loc_nfs_res = ds_client.describe_location_nfs(LocationArn=src_arn)
            running_agent_arn = desc_src_loc_nfs_res['OnPremConfig']['AgentArns']
            for agent_arn in running_agent_arn:
                running_agents.append(agent_arn)
    return running_agents

def get_online_agents(ds_client, logger):
    """ get online agents list
    max agents: 100
    """
    online_agents = []
    agent_res = ds_client.list_agents(
        MaxResults=100,
        #NextToken='string'
    )
    for agent in agent_res["Agents"]:
        if agent["Status"] == "ONLINE":
            #if 
            online_agents.append(agent["AgentArn"])
    if len(online_agents) == 0:
        logger.info("there is no online agent")
    return online_agents

def get_available_agents(ds_client, logger):
    agent_time_out = 60
    avail_agents = []
    online_agents = get_online_agents(ds_client, logger)
    running_agents = get_running_agents(ds_client)
    available_agents = [ agent for agent in online_agents if agent not in running_agents ]
    while not available_agents:
        logger.info("no more agents, waiting for available agents.")
        time.sleep(agent_time_out)
        online_agents = get_online_agents(ds_client, logger)
        running_agents = get_running_agents(ds_client)
        available_agents = [ agent for agent in online_agents if agent not in running_agents ]
    #logger.info("available agents: %s", available_agents)
    return available_agents

def create_src_loc(ds_client, sub_dir, nfs_server_name, agent_arn, logger):
    response = ds_client.create_location_nfs(
        Subdirectory=sub_dir,
        ServerHostname=nfs_server_name,
        OnPremConfig={
            'AgentArns': [
                agent_arn,
            ]
        },
    )
    logger.info("source location is created: %s", response["LocationArn"])
    return response["LocationArn"]

def create_task(ds_client, src_location, dest_location, cloudwatch_arn, task_name_prefix, task_no, manifest_list, logger):
    include_string = check_manifest_type(manifest_list, "incl")
    exclude_string = check_manifest_type(manifest_list, "excl")
    if (exclude_string and include_string == "/") or (exclude_string and not include_string):
        response = ds_client.create_task(
            SourceLocationArn=src_location,
            DestinationLocationArn=dest_location,
            CloudWatchLogGroupArn=cloudwatch_arn,
            Name=task_name_prefix + str(task_no),
            Options={
                'LogLevel':'BASIC',
                'VerifyMode':'ONLY_FILES_TRANSFERRED',
                },
            Excludes=[{
                'FilterType': 'SIMPLE_PATTERN',
                'Value': exclude_string
            },]
        )
        logger.info("task created: %s", response["TaskArn"])
        return response
    elif exclude_string:
        response = ds_client.create_task(
            SourceLocationArn=src_location,
            DestinationLocationArn=dest_location,
            CloudWatchLogGroupArn=cloudwatch_arn,
            Name=task_name_prefix + str(task_no),
            Options={
                'LogLevel':'BASIC',
                'VerifyMode':'ONLY_FILES_TRANSFERRED',
                },
            Excludes=[{
                'FilterType': 'SIMPLE_PATTERN',
                'Value': exclude_string
            },],
            Includes=[{
                'FilterType': 'SIMPLE_PATTERN',
                'Value': include_string
            },]
        )
        logger.info("task created: %s", response["TaskArn"])
        return response
    elif include_string:
        response = ds_client.create_task(
            SourceLocationArn=src_location,
            DestinationLocationArn=dest_location,
            CloudWatchLogGroupArn=cloudwatch_arn,
            Name=task_name_prefix + str(task_no),
            Options={
                'LogLevel':'BASIC',
                'VerifyMode':'ONLY_FILES_TRANSFERRED',
                },
            Includes=[{
                'FilterType': 'SIMPLE_PATTERN',
                'Value': include_string
            },]
        )
        logger.info("task created: %s", response["TaskArn"])
        return response
    else:
        logger.info("nothing to run the task")

def update_loc_nfs(ds_client, loc_arn, agent_arn, logger):
    response = ds_client.update_location_nfs(
        LocationArn=loc_arn,
        OnPremConfig={
            'AgentArns': [
                agent_arn,
            ]
        },
    )

def start_task(ds_client, task_arn, logger):
    response = ds_client.start_task_execution(
        TaskArn=task_arn
    )
    logger.info("task executed: %s", response["TaskExecutionArn"])
    return response

def check_manifest_type(manifest_list, string):
    if type(manifest_list[string]) == type([]):
        include_string = "|".join(manifest_list[string])
    elif type(manifest_list[string]) == type(""):
        include_string = manifest_list[string]
    else:
        logger.info("Error: manifest['incl|excl'] is not list nor string type")
    return include_string

