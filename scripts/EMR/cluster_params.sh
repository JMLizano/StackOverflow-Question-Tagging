#!/usr/bin/env bash

# S3 destination to store the logs of the EMR cluster
startDate=$(date +%Y/%m/%d)
s3_location="s3n://tmp.brandcrumb.com/chema/emr/$startDate"

## CLUSTER CONF
cluster_name="tmpChema"
# Configuration for the master type instance of the cluster
MasterInstanceType="m5.xlarge"
MasterInstanceCount=1
MasterBidPrice="0.3"
# Configuration for the workers of the cluster
CoreInstanceType="r3.2xlarge"
CoreInstanceCount=3
CoreBidPrice="0.3"

# This variable indicates whether to create a SOCKS proxy when establishing the ssh or not. If you dont know what is
# this leave it to false, to learn more check https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-ssh-tunnel.html
SOCKS=true