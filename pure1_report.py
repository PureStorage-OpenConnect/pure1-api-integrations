#! /usr/bin/env python3

import argparse
import time
import datetime
import json
import csv

from datetime import timezone
from pypureclient import pure1

METRIC_RESOLUTION_DAY=86400000
BYTES_IN_A_TERABYTE=1000000000000
BYTES_IN_A_GIGABYTE=1000000000
BYTES_IN_A_TEBIBYTE=1099511627776
BYTES_IN_A_GIBIBYTE=1073741824
AGGREGATION_TYPE = 'avg' #can be set to either 'avg' or 'max'
REPORTING_INTERVAL_DAYS=30


def generate_fleet_report(pure1_api_id, pure1_pk_file, pure1_pk_pwd):
    
    pure1Client = pure1.Client(private_key_file=pure1_pk_file, private_key_password=pure1_pk_pwd, app_id=pure1_api_id)

    response = None
    response = pure1Client.get_arrays(filter="contains(model,'FA')")

    arrays = []
    if response is not None:
        arrays = list(response.items)
    report_filename = str.format('pure1_capacity_report_last{}_days_{}.csv', REPORTING_INTERVAL_DAYS, AGGREGATION_TYPE)
    with open(report_filename, 'w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(['Array Name', 'Model', 'OS Version', 'Total Capacity (TB)', 'Total Capacity (TiB)','Volumes (TB)', 'Volumes (TiB)','Snapshots (TB)', 'Snapshots (TiB)', 'Shared (GB)', 'Shared (GiB)', str.format('% Used (Last {} days)', REPORTING_INTERVAL_DAYS), str.format('% Used (Prev. {} days)', REPORTING_INTERVAL_DAYS), 'Data Reduction'])

        for array in arrays:
            #print(array.name)
            os_version = str.format("{} {}",array.os, array.version)
            metrics_names = ['array_total_capacity', 'array_volume_space', 'array_snapshot_space', 'array_file_system_space', 'array_system_space', 'array_effective_used_space', 'array_shared_space', 'array_data_reduction']
            start = int((datetime.datetime.now() - datetime.timedelta(days = REPORTING_INTERVAL_DAYS)).timestamp())
            #print(start)
            start_comparison = start - REPORTING_INTERVAL_DAYS * METRIC_RESOLUTION_DAY / 1000
            #print(start_comparison)
            end = int((datetime.datetime.now()).timestamp())
            response = pure1Client.get_metrics_history(aggregation=AGGREGATION_TYPE,names=metrics_names,resource_ids=array.id, resolution=METRIC_RESOLUTION_DAY*REPORTING_INTERVAL_DAYS, start_time=start, end_time=end)
            response_comparison = pure1Client.get_metrics_history(aggregation=AGGREGATION_TYPE,names=metrics_names,resource_ids=array.id, resolution=METRIC_RESOLUTION_DAY*REPORTING_INTERVAL_DAYS, start_time=start_comparison, end_time=start)

            total_capacity = 0
            total_capacity_previous = 0
            volume_space = 0
            snapshot_space = 0
            data_reduction = 0
            shared_space = 0
            total_used = 0
            total_used_previous = 0

            total_capacity_i = 0
            volume_space_i = 0
            snapshot_space_i = 0
            shared_space_i = 0

            if hasattr(response, 'items'):
                metrics_items = list(response.items)
                compared_metrics = iter(list(response_comparison.items))
                for metric_item in metrics_items:
                    compared_metric = next(compared_metrics)
                    if metric_item.data:
                        compared_metric_iter = iter(compared_metric.data)
                        #print(compared_metric)
                        for metric_data in metric_item.data:
                            try:
                                compared_metric_data = next(compared_metric_iter)
                            except StopIteration:
                                compared_metric_data = [0,0]
                            #print(compared_metric_data)
                            #print(metric_data)
                            metric_name = metric_item.name
                            if metric_name == 'array_total_capacity':
                                total_capacity_i = round(metric_data[1]/BYTES_IN_A_TEBIBYTE,2)
                                total_capacity_previous_i = round(compared_metric_data[1]/BYTES_IN_A_TEBIBYTE,2)                                
                                total_capacity = round(metric_data[1]/BYTES_IN_A_TERABYTE,2)
                                total_capacity_previous = round(compared_metric_data[1]/BYTES_IN_A_TERABYTE,2)
                            elif metric_name == 'array_effective_used_space':
                                effective_used_space_i = round(metric_data[1]/BYTES_IN_A_GIBIBYTE, 2)
                                effective_used_space = round(metric_data[1]/BYTES_IN_A_GIGABYTE, 2)
                            elif metric_name == 'array_volume_space':
                                total_used = total_used + metric_data[1]
                                total_used_previous = total_used_previous + compared_metric_data[1]
                                volume_space = round(metric_data[1]/BYTES_IN_A_TERABYTE, 2)
                                volume_space_i = round(metric_data[1]/BYTES_IN_A_TEBIBYTE, 2)
                            elif metric_name == 'array_snapshot_space':
                                total_used = total_used + metric_data[1]
                                total_used_previous = total_used_previous + compared_metric_data[1]
                                snapshot_space = round(metric_data[1]/BYTES_IN_A_TERABYTE, 2)
                                snapshot_space_i = round(metric_data[1]/BYTES_IN_A_TEBIBYTE, 2)
                            elif metric_name == 'array_data_reduction':
                                data_reduction = round(metric_data[1], 2)
                            elif metric_name == 'array_shared_space':
                                total_used = total_used + metric_data[1]
                                total_used_previous = total_used_previous + compared_metric_data[1]
                                shared_space = round(metric_data[1]/BYTES_IN_A_GIGABYTE, 2)
                                shared_space_i = round(metric_data[1]/BYTES_IN_A_GIBIBYTE, 2)
                            elif metric_name == 'array_system_space':
                                total_used = total_used + metric_data[1]
                                total_used_previous = total_used_previous + compared_metric_data[1]
                                system_space = round(metric_data[1]/BYTES_IN_A_GIGABYTE, 2)
                                system_space_i = round(metric_data[1]/BYTES_IN_A_GIBIBYTE, 2)
                    else:
                        effective_used_space = 0 #if we end up here, it's most likely because the array_effective_used_space is absent
                percent_used_previous = 0
                if total_capacity_previous != 0:
                    percent_used_previous = round(total_used_previous*100/total_capacity_previous/BYTES_IN_A_TERABYTE, 1)
                if total_capacity != 0:
                    percent_used = round(total_used*100/total_capacity/BYTES_IN_A_TERABYTE, 1)
                    
                    filewriter.writerow([array.name, array.model, os_version, total_capacity, total_capacity_i, volume_space, volume_space_i, snapshot_space, snapshot_space_i, shared_space, shared_space_i, percent_used, percent_used_previous, data_reduction])
            else:
                if response.status_code == 429 or response.status_code == 404:
                    print(response.errors[0].message)
                    if response.errors[0].context is not None:
                        print(response.errors[0].context)
                    if response.status_code == 429:
                        print("Remaining requests: " + response.headers.x_ratelimit_limit_minute) 
                else:     
                    print(str.format("error code: {}\n error: {}", response.status_code, response.errors[0].message))
                    print(str.format(" metrics: {}", str(metrics_names)))

if __name__ == '__main__':

    _ = argparse.ArgumentParser(description='Pure1 Reporting integration parameters')
    _.add_argument('pure1_api_id', type=str, help='Pure1 API Client App ID.')
    _.add_argument('pure1_pk_file', type=str, help='Pure1 API Client Private Key File') 
    _.add_argument('-p', '--password', type=str, help="use if private key is encrypted (or use keyboard prompt)")
    ARGS = _.parse_args()

    print("Generating Pure1 custom capacity report for FlashArray appliances")
    generate_fleet_report(ARGS.pure1_api_id, ARGS.pure1_pk_file, ARGS.password)
  