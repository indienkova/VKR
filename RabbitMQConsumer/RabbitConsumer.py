#! /usr/bin/python

import pika
import pandas as pd
import json
import subprocess

credentials = pika.PlainCredentials(username='admin', password='password')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='127.0.0.1', port=5672, credentials=credentials))

channel = connection.channel()

channel.queue_declare(queue='demo-queue')

count = 0
max_rows = 1000
json_data = []


def write(json_data, file_num):
    get_parquet_from_json(json_data)
    subprocess.run(f"/usr/local/hadoop/bin/hdfs dfs -put -f {'demo' + str(file_num) + '.parquet'} /user/hadoopuser/raw", shell=True)
    print('done:' + str(file_num))

def get_parquet_from_json(list_of_dict):
    df = pd.DataFrame(list_of_dict)
    df.to_parquet('demo' + str(count) + '.parquet', engine='pyarrow')

def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def callback(ch, method, properties, body):
    global count
    global json_data
    global max_rows
    global row_counter

    if len(json_data) <= max_rows:
        json_data.append(json.loads(body.decode("utf-8")))
    else:
        write(json_data, file_num=count)
        json_data.clear()
        count += 1

    print(" [x] Received %r" % body)


channel.basic_consume('demo-queue', callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')

channel.start_consuming()
