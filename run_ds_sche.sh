#!/bin/bash
python3 datasync_scheduler.py --only_create_tasks="True" --task_name="Distributed_Tasks_" --nfs_server_name="198.19.255.158" --sub_dir="/vol1" --mount_path_dir="/mnt/nfs/" --dest_loc="arn:aws:datasync:ap-northeast-2:253679086765:location/loc-042c919ab6996b437" --cloudwatch_arn="arn:aws:logs:ap-northeast-2:253679086765:log-group:/aws/datasync:*" --source_file="source_file.manifest"
