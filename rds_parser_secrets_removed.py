import boto3
import pandas as pd
import datetime
from itertools import chain
from sqlalchemy import create_engine
import constants

available_regions=['us-east-2', 'us-east-1', 'us-west-1', 'us-west-2', 'ap-south-1', 'ap-northeast-2', 'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1', 'ca-central-1', 'eu-central-1', 'eu-west-1', 'eu-west-2', 'eu-west-3', 'eu-north-1', 'sa-east-1']
aws_access_key_id=
aws_secret_access_key=
metrics_end_time=datetime.datetime.utcnow()
metrics_start_time=metrics_end_time - datetime.timedelta(hours=240)
MetricDataQueries_db=MetricDataQueries_db=[
        {
            'Id': 'fetching_data_for_something',
            'Expression': "SEARCH('{AWS/RDS,DBInstanceIdentifier} MetricName=\"DatabaseConnections\"', 'Average', 86400)",
            'ReturnData': True
        },
    ]

MetricDataQueries_cpu=[
        {
            'Id': 'fetching_data_for_something',
            'Expression': "SEARCH('{AWS/RDS,DBInstanceIdentifier} MetricName=\"CPUUtilization\"', 'Average', 86400)",
            'ReturnData': True
        },
    ]
final_metric_list=[]

def create_instance_dictionary(i,region):
    rds_dict={}
    rds_dict['DBInstanceIdentifier']=i['DBInstanceIdentifier']
    rds_dict['DBInstanceClass']=i['DBInstanceClass']
    rds_dict['Engine']=i['Engine']=i['Engine']
    rds_dict['DBInstanceStatus']=i['DBInstanceStatus']
    rds_dict['MasterUsername']=i['MasterUsername']
    rds_dict['AllocatedStorage']=i['AllocatedStorage']
    rds_dict['InstanceCreateTime']=i['InstanceCreateTime']
    rds_dict['AvailabilityZone']=i['AvailabilityZone']
    rds_dict['region'] =region
    rds_dict['type'] = 'RDS'
    rds_dict['userid'] = 'cogscaledev'
    return rds_dict


def rds_parsing():
    ins_arr=[]
    for region in available_regions:
        print(region)
        rds = boto3.client('rds', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,region_name=region)
        db_instances=rds.describe_db_instances()
        for dbinstance in db_instances['DBInstances']:
            ins_dict = create_instance_dictionary(dbinstance,region)
            ins_arr.append(ins_dict)

    return ins_arr

final_list=rds_parsing()
df=pd.DataFrame(final_list)
#print(final_list)

def calculate_metrics(metrics,x,region):
    metrics_list = []
    for i in metrics['MetricDataResults']:
        d = {}
        e = {}
        keys = i['Timestamps']
        values = i['Values']
        e = dict(zip(keys, values))
        d[x] = e
        d['instance'] = i['Label']
        d['region']=region
        metrics_list.append(d)

    return metrics_list

def rds_cloud_watch_parsing():
    final_metric_list_db=[]
    final_metric_list_cpu=[]
    for region in available_regions:
        temp_list_db=[]
        cloud_watch_object = boto3.client('cloudwatch', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region)
        metrics = cloud_watch_object.get_metric_data(MetricDataQueries=MetricDataQueries_db,StartTime=metrics_start_time,EndTime=metrics_end_time,
                                                     ScanBy='TimestampDescending',MaxDatapoints=100000)
        temp_list_db=calculate_metrics(metrics,'DatabaseConnections',region)
        final_metric_list_db.append(temp_list_db)
    for region in available_regions:
        temp_list_cpu=[]
        cloud_watch_object = boto3.client('cloudwatch', aws_access_key_id=aws_access_key_id,
                                          aws_secret_access_key=aws_secret_access_key, region_name=region)
        metrics = cloud_watch_object.get_metric_data(MetricDataQueries=MetricDataQueries_cpu,
                                                     StartTime=metrics_start_time, EndTime=metrics_end_time,
                                                     ScanBy='TimestampDescending', MaxDatapoints=100000)
        temp_list_cpu = calculate_metrics(metrics,'CPUUtilization',region)
        final_metric_list_cpu.append(temp_list_cpu)

    return final_metric_list_db,final_metric_list_cpu





final_metric_list_db,final_metric_list_cpu=rds_cloud_watch_parsing()
final_metric_list_db = list(chain.from_iterable(final_metric_list_db))
final_metric_list_cpu = list(chain.from_iterable(final_metric_list_cpu))

final_metric_list_db=pd.DataFrame(final_metric_list_db)
final_metric_list_cpu=pd.DataFrame(final_metric_list_cpu)

#print(final_metric_list_db)
#print(final_metric_list_cpu)

new_df=pd.merge(pd.merge(df, final_metric_list_db, how='left', left_on=['DBInstanceIdentifier'], right_on=['instance']), final_metric_list_cpu, how='left',left_on=['DBInstanceIdentifier'], right_on=['instance'])
new_df=new_df.drop(['region','instance_x','region_y','instance_y'],axis=1)
new_df=new_df.rename(columns={'region_x':'region'})
print(new_df)
engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(constants.user, constants.password, constants.host, constants.port, constants.db))
resp = new_df.to_sql('aws_reporting', engine, if_exists='replace', index=False, chunksize=1000, method='multi')
print(resp)

