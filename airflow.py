import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
import json
import requests
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# loading the json file to access the zillow api configurations
with open('/home/ubuntu/airflow/config_api.json', 'r') as config_file:
    api_host_key = json.load(config_file)

s3_bucket = 'transformed-zillowdata-bucket'

# getting the current date for the response file name
now = datetime.now()
current_date = now.strftime("%d%m%Y%H%M%S")

def extract_zillow_data(**op_kwargs):    
    # storing the variables required to make the api call; values are received from dic. op_kwargs sent with the function call extract_zillow_data
    url = op_kwargs['url']
    headers = op_kwargs['headers']
    querystring = op_kwargs['querystring']
    current_date = op_kwargs['current_date']
    
    # making the api call and extracting the response
    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()

    # specifying the output file path
    # concat the current date to the file name so we get a format as this: response_data_21072024004027.csv
    file_path = f"/home/ubuntu/response_data_{current_date}.json"
    file_name = f'response_data_{current_date}.csv'

    # saving the JSON response to a file
    with open(file_path, "w") as output_file:
        json.dump(response_data, output_file, indent=4)  # indented for pretty formatting
    output_list = [file_path, file_name]                # for future access to the file path and the file name
    print(output_list)
    return output_list 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 20),
    'email': ['nikkibiradar@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG('zillow_analytics_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:
        
        # task to extract data from the zillow api
        zillow_api_data = PythonOperator(
        task_id= 'task_id_zillow_api_data',     # task ID
        python_callable=extract_zillow_data,    # calling the task function
        op_kwargs={'url' : 'https://zillow56.p.rapidapi.com/search', 'querystring': {"location":"houston, tx","output":"json","status":"forSale","sortSelection":"priorityscore","listing_type":"by_agent","doz":"any"}, 'headers': api_host_key, 'current_date': current_date}     # dictionary parameters of the task funcition
        )

        # loading the extracted data into the S3 bucket
        # mv->move (move data to S3)
        # { ti.xcom_pull("zillow_api_data")[0]} -> helps to get the data that was retured with the task mentioned i.e, zillow_api_data, here its the output_list
        # from output_list we are accessing the 0th index, which is the file_path, where the api extracted data is stored
        # xcom->cross communication, xcom_pull->pull the data that was retured from the previous task (zillow_api_data)
        # i.e., the absolute file path of where the extracted data is stored
        s3_load = BashOperator(
            task_id = 'task_id_s3_load',
            bash_command = 'aws s3 mv {{ ti.xcom_pull("task_id_zillow_api_data")[0]}} s3://zillow-api-analytics-bucket/',
        )

        # task to check if the file is available in the copy-of-raw-zillow-data-bucket before transformation
        file_availabe_check = S3KeySensor(
        task_id='task_id_file_availabe_check',
        bucket_key='{{ti.xcom_pull("task_id_zillow_api_data")[1]}}',     # file name that we want from the s3 bucket
        bucket_name=s3_bucket,          # name of the bucket we want to get the data from, "transformed-zillowdata-bucket"
        aws_conn_id='aws_s3_conn',      # to connect airflow task with aws s3; gives access to aws s3; adding it in xcom
        wildcard_match=False,
        timeout=60,
        poke_interval=5,
        )

        # task to transfer the copy s3 data to redshift to analysis/visualizations
        s3_data_to_redshift = S3ToRedshiftOperator(
        task_id="task_id_s3_data_to_redshift",
        aws_conn_id='aws_s3_conn',              # gives access to aws s3
        redshift_conn_id='aws_redshift_conn_id',    # gives access to redshift
        s3_bucket=s3_bucket,                        # name of the bucket we want to get the data from, "transformed-zillowdata-bucket"
        s3_key='{{ti.xcom_pull("task_id_zillow_api_data")[1]}}',    # file name that we want from the s3 bucket
        schema="PUBLIC",        # schema of the redshift db; mentioned in the redshift cluster; first folder name
        table="zillowdata",     # table name of the table created in the public schema
        copy_options=["csv IGNOREHEADER 1"],    # ignore the first raw as they are the column headers
        )

        # defines the sequence of how the tasks should run; zillow_api_data then s3_load
        zillow_api_data >> s3_load >> file_availabe_check >> s3_data_to_redshift