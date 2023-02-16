## boto3 for datasync: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/datasync.html#DataSync.Client.create_task
def get_online_agents(ds_client, logger):
    """ get online agents list
    max agents: 100
    """
    online_agents = []
    response = ds_client.list_agents(
        MaxResults=100,
        #NextToken='string'
    )
    for agent in response["Agents"]:
        if agent["Status"] == "ONLINE":
            online_agents.append(agent["AgentArn"])
    if len(online_agents) == 0:
        logger.info("there is no online agent")
    logger.info("online agents: %s", online_agents)
    return online_agents

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

