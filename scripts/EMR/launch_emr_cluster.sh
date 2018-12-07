#!/usr/bin/env bash

configuration=${1-cluster_params.sh}
source ${configuration}

# Create EMR cluster
cluster_id="$(aws emr create-cluster \
                --auto-scaling-role EMR_AutoScaling_DefaultRole \
                --applications Name=Hadoop Name=Hive Name=Spark Name=Zeppelin \
                --service-role EMR_DefaultRole \
                --ec2-attributes '{"KeyName":"ssh","InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-1956da7f","SubnetId":"subnet-8ebb4de6","EmrManagedSlaveSecurityGroup":"sg-1b56da7d","EmrManagedMasterSecurityGroup":"sg-1a56da7c"}' \
                --release-label emr-5.18.0 \
                --name "${cluster_name}" \
                --enable-debugging \
                --instance-groups \
                    'InstanceGroupType=MASTER,InstanceCount='${MasterInstanceCount}',BidPrice='${MasterBidPrice}',InstanceType='${MasterInstanceType}',EbsConfiguration={EbsOptimized=true,EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=gp2,SizeInGB=300}}]}' \
                    InstanceGroupType=CORE,InstanceCount=${CoreInstanceCount},BidPrice=${CoreBidPrice},InstanceType=${CoreInstanceType} \
                --log-uri "${s3_location}" \
                --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
                --region eu-west-1  | python -c "import sys, json; print(json.load(sys.stdin)['ClusterId'])")"

# Wait until EMR cluster is up and ready, may take several minutes
state="PENDING"
while [ "$state" != "WAITING" -a "$state" != "TERMINATED_WITH_ERRORS" ]
    do
        state="$(aws emr describe-cluster --cluster-id ${cluster_id}  | python -c "import sys, json; print(json.load(sys.stdin)['Cluster']['Status']['State'])")"
        echo "Current state is ${state} still waiting for cluster to be ready"
        sleep 60
    done
if [[ "$state" == "WAITING" ]]
then
    echo "Cluster up and ready!"
else
    echo "There has been some problem with the cluster, try it again"
    exit 1
fi

# Obtain master ip
master_public_dns="$(aws emr describe-cluster --cluster-id ${cluster_id} |  python -c "import sys, json; print(json.load(sys.stdin)['Cluster']['MasterPublicDnsName'])")"
master_ip="$(echo ${master_public_dns} | python -c "import sys;import re; print('.'.join(re.findall(r'\d+', sys.stdin.read().split('.')[0])))")"

# Set Zeppelin configuration
scp zeppelin/zeppelin-site.xml hadoop@${master_ip}:/tmp/zeppelin-site.xml
scp zeppelin/interpreter.json hadoop@${master_ip}:/tmp/interpreter.json
ssh -i ~/.ssh/amazon_ssh.pem hadoop@${master_ip} "sudo mv /tmp/zeppelin-site.xml /etc/zeppelin/conf/zeppelin-site.xml && sudo chown zeppelin:zeppelin /etc/zeppelin/conf/zeppelin-site.xml"
ssh -i ~/.ssh/amazon_ssh.pem hadoop@${master_ip} "sudo mv /tmp/interpreter.json /etc/zeppelin/conf/interpreter.json && sudo chown zeppelin:zeppelin /etc/zeppelin/conf/interpreter.json"
ssh -i ~/.ssh/amazon_ssh.pem hadoop@${master_ip} "sudo /sbin/stop zeppelin && sudo /sbin/start zeppelin"

# Establish ssh tunnel
if [[ ${SOCKS} == true ]]
then
    ssh -i ~/.ssh/amazon_ssh.pem -N -f -D 8157 hadoop@${master_ip}
    echo "ssh tunnel established using SOCKS proxy, you can connect to cluster on ${master_public_dns}:<port>"
else
    ssh -i ~/.ssh/amazon_ssh.pem -N  -f hadoop@${master_ip} -L 8880:localhost:8880
    echo "ssh tunnel established, you can connect to cluster on localhost:8880"
fi

# Infinite sleeping until user wants to end the session
trap ctrl_c SIGINT
function ctrl_c() {
        echo "You are going to shut down the cluster. Continue? [Y/n]"
        read continue
        if [[ ${continue} == "Y" ]]
        then
            # Finish EMR cluster
            aws emr terminate-clusters --cluster-ids ${cluster_id}
            exit 0
        fi
}

echo "To close connection to cluster and terminate it press ctrl-c"
while true; do sleep infinity; done
