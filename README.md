# Pure Storage-VMware Wavefront integration sample scripts

## Overview
The goal of this integration is to showcase the integration between Pure1 and Wavefront using the [Pure Storage Unified Python Client](https://pypi.org/project/py-pure-client/) and the Wavefront Python SDK. 
The sample `pure1_wf.py` Python script publishes all the array metrics for all the arrays monitored in your Pure1 account to your Wavefront account.
## Pre-requisites
- You must have a Pure1 organization, the credentials of a Pure1 organization administrator account on hand, as well as a valid Wavefront account.
- You must have Python 3.x installed on your computer (Python 2.x might work but the script was only tested with Python 3.6.5)
## Installation
1. Sign in to https://pure1.purestorage.com as an administrator and generate an API key (using the instructions available at https://blog.purestorage.com/pure1-rest-api-part-2, for instance)
2. Take note of your API Application Id as well as your Wavefront account url (such as `https://longboard.wavefront.com`), you will need them later on to call the `pure1_wf.py` script
3. Connect to your Wavefront account or sign up for a trial account at https://www.wavefront.com/
4. Follow [these instructions](https://docs.wavefront.com/wavefront_api.html#generating-an-api-token) to generate a Wavefront API key
5. Install the Python pre-requisites by running the following command:  
     `pip3 install -r requirements.txt`
## Script Execution
To run the script, simply call the following command line:

`python3 pure1_wf.py <your_wavefront_url> <your_wavefront_api_token>  <your_wavefront_api_token> <path_to_your_rsa_private_key_file> -p <your_private_keyfile_password`

Note: the `-p` (`--password`) argument is optional and only required if you encrypted your private key file with a password.

If everything was properly configured, you should see Pure1 metrics available in Wavefront Metrics page in the `purestorage.metrics` bucket.