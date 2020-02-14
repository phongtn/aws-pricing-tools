from __future__ import print_function

import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError

__location__ = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(__location__, "../"))
sys.path.append(os.path.join(__location__, "../vendored"))

import awspricecalculator.ec2.pricing as ec2pricing
import awspricecalculator.common.models as data

logging.basicConfig()
log = logging.getLogger()
log.setLevel('INFO')

ec2client = None
rdsclient = None
elbclient = None
lambdaclient = None
dddbclient = None
kinesisclient = None
cwclient = None
tagsclient = None

# _/_/_/_/_/_/ default_values - start _/_/_/_/_/_/

# Delay in minutes for metrics collection. We want to make sure that all metrics have arrived for the time period we are evaluating
# Note: Unless you have detailed metrics enabled in CloudWatch, make sure it is >= 10
METRIC_DELAY = 10

# Time window in minutes we will use for metric calculations
# Note: make sure this is at least 5, unless you have detailed metrics enabled in CloudWatch
METRIC_WINDOW = 5

FORECAST_PERIOD_MONTHLY = 'monthly'
FORECAST_PERIOD_HOURLY = 'hourly'
DEFAULT_FORECAST_PERIOD = FORECAST_PERIOD_MONTHLY
HOURS_DICT = {FORECAST_PERIOD_MONTHLY: 720, FORECAST_PERIOD_HOURLY: 1}

CW_NAMESPACE = 'ConcurrencyLabs/Pricing/NearRealTimeForecast'
CW_METRIC_NAME_ESTIMATEDCHARGES = 'EstimatedCharges'
CW_METRIC_DIMENSION_SERVICE_NAME = 'ServiceName'
CW_METRIC_DIMENSION_PERIOD = 'ForecastPeriod'
CW_METRIC_DIMENSION_CURRENCY = 'Currency'
CW_METRIC_DIMENSION_TAG = 'Tag'
CW_METRIC_DIMENSION_SERVICE_NAME_EC2 = 'ec2'
CW_METRIC_DIMENSION_SERVICE_NAME_RDS = 'rds'
CW_METRIC_DIMENSION_SERVICE_NAME_LAMBDA = 'lambda'
CW_METRIC_DIMENSION_SERVICE_NAME_DYNAMODB = 'dynamodb'
CW_METRIC_DIMENSION_SERVICE_NAME_KINESIS = 'kinesis'
CW_METRIC_DIMENSION_SERVICE_NAME_TOTAL = 'total'
CW_METRIC_DIMENSION_CURRENCY_USD = 'USD'

SERVICE_EC2 = 'ec2'
SERVICE_RDS = 'rds'
SERVICE_ELB = 'elasticloadbalancing'
SERVICE_LAMBDA = 'lambda'
SERVICE_DYNAMODB = 'dynamodb'
SERVICE_KINESIS = 'kinesis'

RESOURCE_LAMBDA_FUNCTION = 'function'
RESOURCE_ELB = 'loadbalancer'
RESOURCE_ALB = 'loadbalancer/app'
RESOURCE_NLB = 'loadbalancer/net'
RESOURCE_EC2_INSTANCE = 'instance'
RESOURCE_RDS_DB_INSTANCE = 'db'
RESOURCE_EBS_VOLUME = 'volume'
RESOURCE_EBS_SNAPSHOT = 'snapshot'
RESOURCE_DDB_TABLE = 'table'
RESOURCE_STREAM = 'stream'

# This map is used to specify which services and resource types will be searched using the tag service
SERVICE_RESOURCE_MAP = {SERVICE_EC2: [RESOURCE_EBS_VOLUME, RESOURCE_EBS_SNAPSHOT, RESOURCE_EC2_INSTANCE],
                        SERVICE_RDS: [RESOURCE_RDS_DB_INSTANCE],
                        SERVICE_LAMBDA: [RESOURCE_LAMBDA_FUNCTION],
                        SERVICE_ELB: [RESOURCE_ELB],  # only provide RESOURCE_ELB, even for ALB and NLB
                        SERVICE_DYNAMODB: [RESOURCE_DDB_TABLE],
                        SERVICE_KINESIS: [RESOURCE_STREAM]
                        }

config_regions = ('ap-southeast-1', 'us-east-1')


def init_clients():
    global rdsclient
    global elbclient
    global lambdaclient
    global ddbclient
    global kinesisclient
    global cwclient
    global tagsclient
    global region
    global awsaccount
    global default_sts

    region = 'us-east-1'

    rdsclient = boto3.client('rds', region)
    elbclient = boto3.client('elb', region)  # classic load balancers
    elbclientv2 = boto3.client('elbv2', region)  # application and network load balancers
    lambdaclient = boto3.client('lambda', region)
    ddbclient = boto3.client('dynamodb', region)
    kinesisclient = boto3.client('kinesis', region)
    cwclient = boto3.client('cloudwatch', region)
    tagsclient = boto3.client('resourcegroupstaggingapi', region)
    default_sts = boto3.client('sts')


class ResourceManager():
    def __init__(self, tagkey, tagvalue):
        self.resources = []
        self.init_resources_all_region(tagkey, tagvalue)
        # self.init_resources(tagkey, tagvalue)  

    def init_resources_all_region(self, tagkey, tagvalue):
        for r in config_regions:
            if (self.region_is_available(r)):
                self.init_resources(r, tagkey, tagvalue)
            else:
                log.info('the region %s is not avaiable', r)

        self.print_all_resources()

    def print_all_resources(self):
        for res in self.resources:
            log.info("Tagged resource:{}".format(res.__dict__))

    def init_resources(self, region, tagkey, tagvalue):
        log.info('looking resource in region %s', region)
        # TODO: Implement pagination
        clientByRegion = boto3.client('resourcegroupstaggingapi', region)
        response = clientByRegion.get_resources(
            TagsPerPage=500,
            # TagFilters=[{'Key': tagkey, 'Values': [tagvalue]}],
            ResourceTypeFilters=self.get_resource_type_filters(SERVICE_RESOURCE_MAP)
        )
        if 'ResourceTagMappingList' in response:
            for r in response['ResourceTagMappingList']:
                res = self.extract_resource(r['ResourceARN'])
                if res:
                    self.resources.append(res)
                    # log.info("Tagged resource:{}".format(res.__dict__))

    # Return a service:resource list in the format the ResourceGroupTagging API expects it
    def get_resource_type_filters(self, service_resource_map):
        result = []
        for s in service_resource_map.keys():
            for r in service_resource_map[s]: result.append("{}:{}".format(s, r))
        return result

    def extract_resource(self, arn):
        service = arn.split(":")[2]
        resourceId = ''
        # See http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html for different patterns in ARNs
        for service in (SERVICE_EC2, SERVICE_ELB, SERVICE_DYNAMODB, SERVICE_KINESIS):
            for type in (RESOURCE_ALB, RESOURCE_NLB, RESOURCE_ELB):
                if ':' + service + ':' in arn and ':' + type + '/' in arn:
                    resourceId = arn.split(':' + type + '/')[1]
                    return self.Resource(service, type, resourceId, arn)

            for type in (
                    RESOURCE_EC2_INSTANCE, RESOURCE_EBS_VOLUME, RESOURCE_EBS_SNAPSHOT, RESOURCE_DDB_TABLE,
                    RESOURCE_STREAM):
                if ':' + service + ':' in arn and ':' + type + '/' in arn:
                    resourceId = arn.split(':' + type + '/')[1]
                    return self.Resource(service, type, resourceId, arn)
        for service in (SERVICE_RDS, SERVICE_LAMBDA):
            for type in (RESOURCE_RDS_DB_INSTANCE, RESOURCE_LAMBDA_FUNCTION):
                if ':' + service + ':' in arn and ':' + type + ':' in arn:
                    resourceId = arn.split(':' + type + ':')[1]
                    return self.Resource(service, type, resourceId, arn)

        return None

    def get_resources(self, service, resourceType):
        result = []
        if self.resources:
            for r in self.resources:
                if r.service == service and r.type == resourceType:
                    result.append(r)
        return result

    def get_resource_ids(self, service, resourceType):
        result = []
        if self.resources:
            for r in self.resources:
                if r.service == service and r.type == resourceType:
                    result.append(r.id)
        return result

    def region_is_available(self, region):
        regional_sts = boto3.client('sts', region_name=region)
        try:
            regional_sts.get_caller_identity()
            return True
        except ClientError as e:
            default_sts.get_caller_identity()
            return False

    class Resource():
        def __init__(self, service, type, id, arn):
            self.service = service
            self.type = type
            self.id = id
            self.arn = arn


def get_ec2_instances_by_tag(ec2Client, tagkey, tagvalue):
    result = {}
    response = ec2Client.describe_instances(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    if 'Reservations' in response:
        reservations = response['Reservations']
        for r in reservations:
            if 'Instances' in r:
                for i in r['Instances']:
                    result[i['InstanceId']] = i

    return result

def get_instance_type_count(instance_dict):
    result = {}
    for key in instance_dict:
        instance_type = instance_dict[key]['InstanceType']
        if instance_type in result:
            result[instance_type] = result[instance_type] + 1
        else:
            result[instance_type] = 1
    return result


def get_storage_by_ebs_type(ec2_client, instance_dict):
    result = {}
    iops = 0
    ebs_ids = []
    volume_details = {}

    for key in instance_dict:
        block_mappings = instance_dict[key]['BlockDeviceMappings']
        for bm in block_mappings:
            if 'Ebs' in bm:
                if 'VolumeId' in bm['Ebs']:
                    ebs_ids.append(bm['Ebs']['VolumeId'])

    if ebs_ids: volume_details = ec2_client.describe_volumes(VolumeIds=ebs_ids)  # TODO:add support for pagination
    if 'Volumes' in volume_details:
        for v in volume_details['Volumes']:
            volume_type = v['VolumeType']
            if volume_type in result:
                result[volume_type] = result[volume_type] + int(v['Size'])
            else:
                result[volume_type] = int(v['Size'])
            if volume_type == 'io1': iops = iops + int(v['Iops'])

    return result, iops

def compute_ebs_cost(ec2Client, pricing_records, all_instance_dict):
    ebs_cost= 0
    ebs_storage_dict, piops = get_storage_by_ebs_type(ec2Client, all_instance_dict)
    for k in ebs_storage_dict.keys():
        if k == 'io1':
            pricing_piops = piops
        else:
            pricing_piops = 0
        try:
            ebs_storage_cost = ec2pricing.calculate(
                data.Ec2PriceDimension(region=region, ebsVolumeType=k, ebsStorageGbMonth=ebs_storage_dict[k],
                                       pIops=pricing_piops))
            if 'pricingRecords' in ebs_storage_cost: pricing_records.extend(ebs_storage_cost['pricingRecords'])
            ebs_cost = ebs_cost + ebs_storage_cost['totalCost']
        except Exception as failure:
            log.error('Error processing ebs storage costs: %s', failure)
    return ebs_cost


def main():
    init_clients()
    tagkey = 'monitor'
    tagvalue = 'cost'
    ec2Cost = 0
    ebsCost = 0
    pricing_records = []

    resource_manager = ResourceManager(tagkey, tagvalue)

      # taggedEc2 = resource_manager.get_resource_ids(SERVICE_EC2, RESOURCE_EC2_INSTANCE)
    #   log.info('ec2 {}'.format(taggedEc2))

    all_instance_dict = {}

    for r in config_regions:
        ec2Client = boto3.client('ec2', r)
        ec2_instances = get_ec2_instances_by_tag(ec2Client, tagkey, tagvalue)
        ebsCost = ebsCost + compute_ebs_cost(ec2Client, pricing_records, ec2_instances)

        all_instance_dict.update(ec2_instances)
        all_instance_types = get_instance_type_count(all_instance_dict)
        log.info('regions {}: {}'.format(r, ec2_instances))



    # log.info("All instance types:{}".format(all_instance_types))

    # ebs_storage_dict, piops = get_storage_by_ebs_type(all_instance_dict)

    for instance_type in all_instance_types:
        try:
            log.info("calculate instance type %s", instance_type)
            ec2_compute_cost = ec2pricing.calculate(data.Ec2PriceDimension(region=region, instanceType=instance_type,
                                                                           instanceHours=all_instance_types[
                                                                                             instance_type] * HOURS_DICT[DEFAULT_FORECAST_PERIOD]))
            if 'pricingRecords' in ec2_compute_cost: pricing_records.extend(ec2_compute_cost['pricingRecords'])
            ec2Cost = ec2Cost + ec2_compute_cost['totalCost']
        except Exception as failure:
            log.error('Error processing %s: %s', instance_type, failure)

    log.info('ec2 total cost %s', ec2Cost)
    log.info('ebs storage dict %s', ebsCost)


if __name__ == "__main__":
    main()
