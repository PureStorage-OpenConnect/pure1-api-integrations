#! /usr/bin/env python3

import argparse
import time
import datetime
import json
import csv

from datetime import timezone
from pypureclient import pure1

METRIC_RESOLUTION_DAY=86400000
REPORTING_INTERVAL_DAYS=7
BYTES_IN_A_TERABYTE=1099511627776
BYTES_IN_A_GIGABYTE=1073741824
queries_count = 1
sorted_metrics = None


def generate_fleet_report(pure1_api_id, pure1_pk_file, pure1_pk_pwd):
    
    pure1Client = pure1.Client(private_key_file=pure1_pk_file, private_key_password=pure1_pk_pwd, app_id=pure1_api_id)

    response = None
    response = pure1Client.get_arrays()

    arrays = []
    if response is not None:
        arrays = list(response.items)

    with open('pure1_report.csv', 'w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(['Array Name', 'Model', 'OS Version', 'Total Capacity (TB)', 'Effective Used Space (GB)', 'Data Reduction', 'Shared Space (GB)'])
        for array in arrays:
            os_version = str.format("{} {}",array.os, array.version)
            metrics_names = ['array_total_capacity', 'array_effective_used_space', 'array_data_reduction', 'array_shared_space']
            start = int((datetime.datetime.now() - datetime.timedelta(days = REPORTING_INTERVAL_DAYS)).timestamp())
            end = int((datetime.datetime.now()).timestamp())
            response = pure1Client.get_metrics_history(aggregation='avg',names=metrics_names,resource_ids=array.id, resolution=METRIC_RESOLUTION_DAY*REPORTING_INTERVAL_DAYS, start_time=start, end_time=end)

            if hasattr(response, 'items'):
                metrics_items = list(response.items)
                for metric_item in metrics_items:
                    if metric_item.data:
                        for metric_data in metric_item.data:
                            metric_name = metric_item.name
                            if metric_name == 'array_total_capacity':
                                total_capacity = metric_data[1]/BYTES_IN_A_TERABYTE
                            elif metric_name == 'array_effective_used_space':
                                effective_used_space = metric_data[1]/BYTES_IN_A_GIGABYTE
                            elif metric_name == 'array_data_reduction':
                                data_reduction = metric_data[1]
                            elif metric_name == 'array_shared_space':
                                shared_space = metric_data[1]/BYTES_IN_A_GIGABYTE
                    else:
                        effective_used_space = 0 #if we end up here, it's most likely because the array_effective_used_space is absent

                filewriter.writerow([array.name, array.model, os_version, total_capacity, effective_used_space, data_reduction, shared_space])

if __name__ == '__main__':

    _ = argparse.ArgumentParser(description='Pure1 Reporting integrtion parameters')
    _.add_argument('pure1_api_id', type=str, help='Pure1 API Client App ID.')
    _.add_argument('pure1_pk_file', type=str, help='Pure1 API Client Private Key File') 
    _.add_argument('-p', '--password', type=str, help="use if private key is encrypted (or use keyboard prompt)")
    ARGS = _.parse_args()

    print("Generating Pure1 custom report")
    generate_fleet_report(ARGS.pure1_api_id, ARGS.pure1_pk_file, ARGS.password)
  