#! /usr/bin/env python3

import argparse
import time
import datetime
import json

from datetime import timezone

from wavefront_sdk.direct import WavefrontDirectClient
from wavefront_sdk.entities.histogram import histogram_granularity

from pypureclient import pure1

def report_metrics(server, token, pure1_api_id, pure1_pk_file,pure1_pk_pwd, timedelta_seconds, resolution_ms):
    #IMPORTANT NOTE: make sure max_resource_count * max_metric_count <=16
    max_resource_count = 16
    max_metric_count = 1

    client = pure1.Client(private_key_file=pure1_pk_file, private_key_password=pure1_pk_pwd, app_id=pure1_api_id)

    response = client.get_metrics(filter="resource_types='arrays' and not(resource_types='pods') and availabilities.resolution=" + str(resolution_ms))
    arrays_metrics = list(response.items)

    #hardcoding metrics array list for testing purposes
    #testMetric = pure1.Metric(name = 'array_read_iops')
    #arrays_metrics = [testMetric]

    #print(arrays_metrics)
    arrays_metrics_count = len(arrays_metrics)
    #print("array_metrics_count: " + str(arrays_metrics_count))
    metrics_loops = arrays_metrics_count // max_metric_count
    if(arrays_metrics_count % max_metric_count > 0):
        metrics_loops +=1
    #print("metrics loops: ", metrics_loops)

    response = client.get_arrays(sort=pure1.Array.name.ascending())
    arrays = list(response.items)
    #end = int(datetime.datetime.now().replace(tzinfo=timezone.utc).timestamp())
    end = int((datetime.datetime.now() - datetime.timedelta(hours = 2)).timestamp())
    #print("end:", end)
    #end = int(datetime.datetime.now().timestamp())
    start = int(end - datetime.timedelta(seconds=timedelta_seconds).total_seconds())
    #print("start:", start)

    wavefront_sender = WavefrontDirectClient(
        server=server,
        token=token,
        max_queue_size=50000,
        batch_size=10000,
        flush_interval_seconds=5)
    array_count = len(arrays)

    #print("array_count: "+ str(array_count))
    array_loops = array_count // max_resource_count
    if(array_count % max_resource_count > 0):
        array_loops +=1
    #print("max loop: "+ str(max_loop))

    for i in range(0, array_loops):
        ids_list = []
        names_list = []
        for j in range(0,max_resource_count):
            try:
                ids_list.append(arrays[max_resource_count*i+j].id)
                names_list.append(arrays[max_resource_count*i+j].name)
            except:
                pass

        for i in range(0, metrics_loops):
            metrics_list = []
            for j in range(0,max_metric_count):
                try:
                    metrics_list.append(arrays_metrics[max_metric_count*i+j].name)
                except:
                    pass
            #print("metrics list: ", metrics_list)
            #print("array list: ", ids_list)
            response = client.get_metrics_history(aggregation='avg',names=metrics_list,resource_ids=ids_list, resolution=resolution_ms, start_time=start, end_time=end)
            time.sleep(0.5) #included to avoid hitting the API rate limit
            #print(response)
            if hasattr(response, 'items'):
                metric_items = list(response.items)
                for metric_item in metric_items:
                    metric_name = metric_item.name
                    arrayName = metric_item.resources[0].name
                    #print(metric_item)
                    #print(arrayName)            
                    #print(metric_name)
                    #print(array_data)
                    if metric_item.data:
                        for metric_data in metric_item.data:
                        #metric_data = metric_item.data[0]
                            #print(metric_data)
                            wavefront_sender.send_metric(
                                name="purestorage.metrics." + metric_name, value=metric_data[1], timestamp=metric_data[0],
                                source="pure1-rest-api", tags={'arrayName': arrayName})
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
                else:     
                    print(str.format("error with metrics: {}: {}", str(metrics_list), response))
        wavefront_sender.close()
    
    #wf_direct_reporter = wavefront_reporter.WavefrontDirectReporter(
    #    server=server, token=token, registry=reg,
    #    source='pure-storage-playground', 
    #    tags={'purekey1': 'pure1', 'purekey2': 'pure2'},
    #    prefix='python.direct.').report_minute_distribution()

    # counter
    #c_1 = reg.counter('array_iops', tags={'arrayId': arrayId})
    #c_1.inc(metric1[1])
    #c_1.inc()

    # delta counter
    #d_1 = delta.delta_counter(reg, 'pure_delta_count',  tags={'delta_key': 'delta_val'})
    #d_1.inc()
    #d_1.inc()

    # gauge
    #g_1 = reg.gauge('pure_gauge', tags={'gauge_key': 'gauge_val'})
    #g_1.set_value(2)

    # meter
    #m_1 = reg.meter('pure_meter', tags={'meter_key': 'meter_val'})
    #m_1.mark()

    # timer
    #t_1 = reg.timer('pure_timer', tags={'timer_key': 'timer_val'})
    #timer_ctx = t_1.time()
    #time.sleep(3)
    #timer_ctx.stop()

    # histogram
    #h_1 = reg.histogram('pure_histogram', tags={'hist_key': 'hist_val'})
    #h_1.add(1.0)
    #h_1.add(1.5)

    # Wavefront Histogram
    #h_2 = wavefront_histogram.wavefront_histogram(reg, 'wf_histogram')
    #h_2.add(1.0)
    #h_2.add(2.0)

    #wf_direct_reporter.report_now()
    #wf_direct_reporter.stop()
    #wf_proxy_reporter.report_now()
    #wf_proxy_reporter.stop()


if __name__ == '__main__':
    # python example.py proxy_host server_url server_token
    
    _ = argparse.ArgumentParser()
    _.add_argument('server', help='Wavefront server for direct ingestion.')
    _.add_argument('token', help='Wavefront API token.')
    _.add_argument('pure1_api_id', help='Pure1 API Client App ID.')
    _.add_argument('pure1_pk_file', help='Pure1 API Client Private Key File')
    _.add_argument('-p', '--password', type=str, help="use if private key is encrypted (or use keyboard prompt)")
    #_.add_argument('pure1_pk_pwd', help='Pure1 API Client Private Key Password.')
    ARGS = _.parse_args()

    while True:
        start_time = time.time()
        report_metrics(ARGS.server, ARGS.token, ARGS.pure1_api_id, ARGS.pure1_pk_file, ARGS.password, 180, 30000)
        elapsed_time = time.time() - start_time
        print("elapsed time in seconds: ",int(elapsed_time))
        #print("elapsed time:",time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
        sleep_for = 180 - int(elapsed_time)
        if sleep_for > 0:
            print("waiting", sleep_for, "seconds for next query")
            time.sleep(sleep_for)