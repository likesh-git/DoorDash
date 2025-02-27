import json
import pandas as pd
import boto3
import io
from datetime import date
import os
from dotenv import load_dotenv
import logging 

logger = logging.getLogger()
logger.setLevel(logging.INFO)
load_dotenv()

def lambda_handler(event, context):

    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_key  = event['Records'][0]['s3']['object']['key']

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket = input_bucket, Key = input_key )
    body = obj['Body'].read()
    json_dicts = body.decode('utf-8').split('\r\n')
    df = pd.DataFrame(columns = ['id','status','amount','date'])
    
    for line in json_dicts:
        py_dict = json.loads(line)
        # print('py_dict - ', py_dict)
        if py_dict['status'] == 'delivered':
            df.loc[py_dict['id']] = py_dict

    logger.info('Converting to csv')
    df.to_csv('/tmp/intermediate.csv',sep = ',')
    logger.info('intermediate.csv file created')

    date_var = str(date.today())
    file_name = 'processed_data/{}_processed_data.csv'.format(date_var)
    logger.info(f'file_name - {file_name}')

    try:
        lambda_path = '/tmp/intermediate.csv'
        bucket_name = os.getenv('output_bucket')
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)

        bucket.upload_file('/tmp/intermediate.csv', file_name)
        logger.info('Uploaded')
    except Exception as e:
        # sns to deliver file processed request
        sns = boto3.client('sns')

        response = sns.publish(
        TopicArn=os.getenv('TopicArn'),
        Message="File {} has been not been processed completely!".format(input_key)
        )
        logger.exception('An error occurred: %s', e)


    # sns to deliver file processed request
    sns = boto3.client('sns')
    response = sns.publish(
    TopicArn=os.getenv('TopicArn'),
    Message="File {} has been formatted and filtered. Its been stored in {} as {}".format(input_key,bucket_name,file_name)
    )

    logger.info('Sucess')

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
    
    
    

