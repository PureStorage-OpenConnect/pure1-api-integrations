#! /usr/bin/env python3

import argparse
import time
import datetime
import json

from datetime import timezone

from wavefront_sdk.direct import WavefrontDirectClient
from wavefront_sdk.entities.histogram import histogram_granularity

from pypureclient import pure1

metrics_list = None
WAVEFRONT_SOURCE="pure1-rest-api"
WAVEFRONT_METRICS_NAMESPACE="purestorage.metrics."
#IMPORTANT NOTE: make sure max_resource_count * max_metric_count <=16
MAX_RESOURCES_COUNT = 8 #defines the max number of resources (such as arrays) that should be queried for in one single metrics query
MAX_METRICS_COUNT = 2 #defines the max number of metrics that should be queried for in one single metrics query
queries_count = 1

def get_metrics_list(pure1_api_id, pure1_pk_file,pure1_pk_pwd, resource_type, resolution_ms):
    global metrics_list
    if metrics_list is None:
        client = pure1.Client(private_key_file=pure1_pk_file, private_key_password=pure1_pk_pwd, app_id=pure1_api_id)
        response = client.get_metrics(filter=str.format("resource_types[all]='{}' and availabilities.resolution<={}", resource_type, str(resolution_ms)))
        metrics_list = list(response.items)
        
    return metrics_list

def get_send_data(pureClient, wavefront_sender, metrics_list, arrays, server, token, resolution_ms, start, end):

    arrays_metrics_count = len(metrics_list)
    metrics_loops = -(-arrays_metrics_count // MAX_METRICS_COUNT) # upside-down floor division
    array_count = len(arrays)
    array_loops = -(-array_count // MAX_RESOURCES_COUNT)
    print("array_loops: ", array_loops)
    #time.sleep(5)

    _start = time.time() #uncomment for query count logging
    metric_count = 0
    for i in range(0, array_loops):
        ids_list = []
        names_list = []
        for j in range(0,MAX_RESOURCES_COUNT):
            try:
                ids_list.append(arrays[MAX_RESOURCES_COUNT*i+j].id)
                names_list.append(arrays[MAX_RESOURCES_COUNT*i+j].name)
            except:
                pass

        for i in range(0, metrics_loops):
            _metrics_list = []
            for j in range(0,MAX_METRICS_COUNT):
                try:
                    _metrics_list.append(metrics_list[MAX_METRICS_COUNT*i+j].name)
                except:
                    pass
            #print("metrics list: ", _metrics_list)
            #print("array list: ", ids_list)
            response = pureClient.get_metrics_history(aggregation='avg',names=_metrics_list,resource_ids=ids_list, resolution=resolution_ms, start_time=start, end_time=end)
            global queries_count
            queries_count +=1
            time.sleep(0.5) #included to avoid hitting the API rate limit

            if hasattr(response, 'items'):
                metric_items = list(response.items)
                for metric_item in metric_items:
                    metric_name = metric_item.name
                    arrayName = metric_item.resources[0].name
                    #print(metric_item)
                    #print(arrayName)            
                    #print(metric_name)
                    if metric_item.data:
                        for metric_data in metric_item.data:
                            metric_count+=1
                            #print(metric_count, metric_data)
                            #print(metric_data)
                            wavefront_sender.send_metric(name=WAVEFRONT_METRICS_NAMESPACE + metric_name, value=metric_data[1], timestamp=metric_data[0], source=WAVEFRONT_SOURCE, tags={'arrayName': arrayName})
                    else:
                        pass
                        #print("no " + metric_name + " metric for array: " + arrayName)
            else:
                if response.status_code == 429 or response.status_code == 404:
                    print("API rate limit exceeded for ", names_list)
                    print(response.errors[0].message)
                    if response.errors[0].context is not None:
                        print(response.errors[0].context)
                    print("Remaining requests: " + response.headers.x_ratelimit_limit_minute)
                    _end = time.time()
                    _elapsed_time = _end - _start
                    print(str.format("Performed {} queries in {} seconds", str(queries_count), int(_elapsed_time))) 
                else:     
                    print(str.format("error with metrics: {}: {}", str(metrics_list), response))
        wavefront_sender.close()

def report_metrics(server, token, pure1_api_id, pure1_pk_file,pure1_pk_pwd, resource_type, interval_seconds, start_time, resolution_ms):
    
    pure1Client = pure1.Client(private_key_file=pure1_pk_file, private_key_password=pure1_pk_pwd, app_id=pure1_api_id)

    metrics_list = get_metrics_list(pure1_api_id, pure1_pk_file, pure1_pk_pwd, resource_type, resolution_ms)

    #hardcoding metrics array list for testing purposes
    #testMetric = pure1.Metric(name = 'array_read_iops')
    #metrics_list = [testMetric]
    response = None
    if resource_type == "arrays":
        response = pure1Client.get_arrays()
        #currently only supports array metrics
    resources = []
    if response is not None:
        resources = list(response.items)

    wavefront_sender = WavefrontDirectClient(
        server=server,
        token=token,
        max_queue_size=100000,
        batch_size=40000,
        flush_interval_seconds=5)

    #pull data from Pure1 for the last 7 days (or based on specified start time) in increments of 10 minutes
    days_count = 7
    if interval_seconds == -1:
        interval_seconds = 1800
        if start_time != 0:
            initial_start = start_time
            end = int((datetime.datetime.now() - datetime.timedelta(hours = 2)).timestamp())
            timespan_seconds = end - start_time
        else:
            timespan_seconds = 3600 * (24 * days_count - 2) #querying for `days_count` days of data up to 2 hours from now           
            initial_start = int((datetime.datetime.now() - datetime.timedelta(days = days_count)).timestamp())
        #initial_start = 1560818574
        loops = - (-timespan_seconds // interval_seconds) # number of 360 seconds intervals in days_count days (-2 hours)
        print("loops", loops)
        for i in range(0, loops-1):
            start = initial_start + i*interval_seconds
            end = start + interval_seconds
            print("Start Time:", start, "End Time:", end)
            get_send_data(pure1Client, wavefront_sender, metrics_list, resources, server, token, resolution_ms, start, end)
            print(str.format("Performed {} queries", str(queries_count)))  
    else:
        end = int((datetime.datetime.now() - datetime.timedelta(hours = 2)).timestamp())
        start = int(end - datetime.timedelta(seconds=interval_seconds).total_seconds())
        print("Start Time:", start, "End Time:", end)
        get_send_data(pure1Client, wavefront_sender, metrics_list, resources, server, token, resolution_ms, start, end)
        print(str.format("Performed {} queries", str(queries_count))) 
    #get_send_data(pure1Client, wavefront_sender, metrics_list, arrays, server, token, resolution_ms, start, end)
    

if __name__ == '__main__':

    _ = argparse.ArgumentParser(description='Pure1-Wavefront integration parameters')
    _.add_argument('server', type=str, help='Wavefront server for direct ingestion.')
    _.add_argument('token', type=str, help='Wavefront API token.')
    _.add_argument('pure1_api_id', type=str, help='Pure1 API Client App ID.')
    _.add_argument('pure1_pk_file', type=str, help='Pure1 API Client Private Key File')
    _.add_argument('-r', '--resolution', type=int, help='Resolution in ms of the metrics to be published to Wavefront - available values are 30000 or 86400000 only. Defaults to 30,000 ms', default=30000)
    _.add_argument('-i', '--interval', type=int, help='Interval at which the script should run (also impacts Pure1 queries). Defaults to 3 minutes, specify -1 to run once only', default=180)
    _.add_argument('-s', '--start', type=int, help='Start date of the queries. Only works if --interval is set to -1', default=0)
    _.add_argument('-rt', '--resource_type', type=str, help='Name of the resource type to be queried. Currently only supports and defaults to "arrays"', default="arrays")
    _.add_argument('-p', '--password', type=str, help="use if private key is encrypted (or use keyboard prompt)")
    ARGS = _.parse_args()
    if ARGS.interval != -1:
        print(str.format("Running Pure1-Wavefront sync script every {} seconds with resolution: {} ms", ARGS.interval, ARGS.resolution))
        while True:
            start_time = time.time()
            report_metrics(ARGS.server, ARGS.token, ARGS.pure1_api_id, ARGS.pure1_pk_file, ARGS.password, ARGS.resource_type, ARGS.interval, ARGS.start, ARGS.resolution)
            elapsed_time = time.time() - start_time
            print("elapsed time in seconds: ",int(elapsed_time))
            #print("elapsed time:",time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
            sleep_for = ARGS.interval - int(elapsed_time)
            if sleep_for > 0:
                print("waiting", sleep_for, "seconds for next query")
                time.sleep(sleep_for)
    else:
        print("Running script only once")
        start_time = time.time()
        report_metrics(ARGS.server, ARGS.token, ARGS.pure1_api_id, ARGS.pure1_pk_file, ARGS.password, ARGS.resource_type,  ARGS.interval, ARGS.start, ARGS.resolution)
        elapsed_time = time.time() - start_time
        print("elapsed time in seconds: ",int(elapsed_time))