#! /usr/bin/env python3

import argparse
import time
import datetime
import json

from datetime import timezone

from wavefront_sdk.direct import WavefrontDirectClient
from wavefront_sdk.entities.histogram import histogram_granularity

from pypureclient import pure1

WAVEFRONT_SOURCE="pure1-rest-api"
WAVEFRONT_METRICS_NAMESPACE="purestorage.metrics."
#IMPORTANT NOTE: make sure max_resource_count * max_metric_count <=16
MAX_RESOURCES_COUNT_PER_QUERY = 8 #defines the max number of resources (such as arrays) that should be queried for in one single metrics query
MAX_METRICS_COUNT_PER_QUERY = 2 #defines the max number of metrics that should be queried for in one single metrics query
queries_count = 1
sorted_metrics = None

#Retrieves all the metrics with a resolution 
def get_metrics_list(pure1Client, resource_type, resolution_ms):
    global sorted_metrics
    if sorted_metrics is None:
        response = pure1Client.get_metrics(filter=str.format("resource_types[all]='{}' and availabilities.resolution<={})", resource_type, str(resolution_ms)))
        #and not(contains(name, 'mirrored')
        metrics_list = list(response.items)
        sorted_metrics = sort(metrics_list)
    return sorted_metrics

def sort(metrics_list):
    a = {}
    for metric in metrics_list:
        key = metric.availabilities[0].resolution
        a.setdefault(key, []).append(metric)

    b = []
    for key in a:
        for item in a[key]:
            b.append(item)

    return b

def get_send_data(pureClient, wavefront_sender, metrics_list, arrays, server, token, resolution_ms, start, end):

    metrics_count = len(metrics_list)

    array_count = len(arrays)
    array_loops = -(-array_count // MAX_RESOURCES_COUNT_PER_QUERY) #upside-down floor division

    _start = time.time() #used for query count logging

    for i in range(0, array_loops):
        ids_list = []
        names_list = []
        for j in range(0,MAX_RESOURCES_COUNT_PER_QUERY):
            try:
                ids_list.append(arrays[MAX_RESOURCES_COUNT_PER_QUERY*i+j].id)
                names_list.append(arrays[MAX_RESOURCES_COUNT_PER_QUERY*i+j].name)
            except:
                pass

        _count = 0
        while _count < metrics_count:
            _metrics_list = []
            _metrics_names = []
            _metric_resolution_base = 0
            for j in range(_count,_count + MAX_METRICS_COUNT_PER_QUERY):
                try:
                    _metric_resolution = metrics_list[j].availabilities[0].resolution
                    if j == _count:
                        #getting the first metric resolution and trying to group other metrics with the same resolution
                        _metric_resolution_base = _metric_resolution
                        _temp_count = 0

                    if _metric_resolution == _metric_resolution_base:
                        _metrics_list.append(metrics_list[j])
                        _metrics_names.append(metrics_list[j].name)
                        _temp_count += 1
                    else:    
                        break
                except:
                    pass
            _count += _temp_count
            response = pureClient.get_metrics_history(aggregation='avg',names=_metrics_names,resource_ids=ids_list, resolution=_metric_resolution_base, start_time=start, end_time=end)
            global queries_count
            queries_count +=1
            time.sleep(0.5) #added to avoid hitting the API rate limit

            if hasattr(response, 'items'):
                metric_items = list(response.items)
                for metric_item in metric_items:
                    if metric_item.data:
                        for metric_data in metric_item.data:
                            metric_name = metric_item.name
                            arrayName = metric_item.resources[0].name

                            wavefront_sender.send_metric(name=WAVEFRONT_METRICS_NAMESPACE + metric_name, value=metric_data[1], timestamp=metric_data[0], source=WAVEFRONT_SOURCE, tags={'arrayName': arrayName})
                    else:
                        pass
            else:
                if response.status_code == 429 or response.status_code == 404:
                    print(response.errors[0].message)
                    if response.errors[0].context is not None:
                        print(response.errors[0].context)
                    if response.status_code == 429:
                        print("API rate limit exceeded for ", names_list)
                        print("Remaining requests: " + response.headers.x_ratelimit_limit_minute) 
                else:     
                    print(str.format("error code: {}\n error: {}", response.status_code, response.errors[0].message))
                    print(str.format("arrays: {} - metrics: {}", str(names_list), str(_metrics_names)))

    _end = time.time()
    print(str.format("Performed {} queries in {} seconds", str(queries_count), int(_end - _start)))

def report_metrics(server, token, pure1_api_id, pure1_pk_file,pure1_pk_pwd, resource_type, interval_seconds, start_time, resolution_ms):
    
    pure1Client = pure1.Client(private_key_file=pure1_pk_file, private_key_password=pure1_pk_pwd, app_id=pure1_api_id)

    metrics_list = get_metrics_list(pure1Client, resource_type, resolution_ms)

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
        max_queue_size=10000,
        batch_size=4000,
        flush_interval_seconds=5)

    #Retrieves data from Pure1 for the last 7 days (or based on specified start time) in increments of 30 minutes
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
        
        loops = - (-timespan_seconds // interval_seconds) # number of 360 seconds intervals in days_count days (-2 hours)

        for i in range(0, loops-1):
            start = initial_start + i*interval_seconds
            end = start + interval_seconds
            print("Start Time:", start, "End Time:", end)
            get_send_data(pure1Client, wavefront_sender, metrics_list, resources, server, token, resolution_ms, start, end)
    else:
        end = int((datetime.datetime.now() - datetime.timedelta(hours = 2)).timestamp())
        start = int(end - datetime.timedelta(seconds=interval_seconds).total_seconds())
        print("Start Time:", start, "End Time:", end)
        get_send_data(pure1Client, wavefront_sender, metrics_list, resources, server, token, resolution_ms, start, end)

    wavefront_sender.close()

if __name__ == '__main__':

    _ = argparse.ArgumentParser(description='Pure1-Wavefront integration parameters')
    _.add_argument('server', type=str, help='Wavefront server for direct ingestion.')
    _.add_argument('token', type=str, help='Wavefront API token.')
    _.add_argument('pure1_api_id', type=str, help='Pure1 API Client App ID.')
    _.add_argument('pure1_pk_file', type=str, help='Pure1 API Client Private Key File')
    _.add_argument('-r', '--resolution', type=int, help='Resolution (in milliseconds) of the metrics to be retrieved from Pure1 - available values should be between 30,000 (30 seconds) and 86,400,000 (1 day). Defaults to 1 day', default=86400000)
    _.add_argument('-i', '--interval', type=int, help='Interval at which the script should run, measured in seconds (also impacts Pure1 queries). Defaults to 3 minutes, specify -1 to run once only', default=180)
    _.add_argument('-s', '--start', type=int, help='Start date of the queries. Only works if --interval is set to -1', default=0)
    _.add_argument('-rt', '--resource_type', type=str, help='Name of the resource type to be queried. Currently only supports and defaults to "arrays"', default="arrays")
    _.add_argument('-p', '--password', type=str, help="use if private key is encrypted (or use keyboard prompt)")
    ARGS = _.parse_args()
    if ARGS.interval != -1:
        print(str.format("Running Pure1-Wavefront sync script every {} seconds with max resolution: {} ms", ARGS.interval, ARGS.resolution))
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