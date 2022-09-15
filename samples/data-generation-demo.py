# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

import argparse
import sys, getopt, requests, json, datetime, time, random
import uuid
from concurrent.futures import Future
from awscrt import io
from awscrt.io import LogLevel
from awscrt.mqtt import Connection, Client, QoS
from awsiot.greengrass_discovery import DiscoveryClient, DiscoverResponse
from awsiot import mqtt_connection_builder
import yaml
from pathlib import Path

def get_api_keys():
    """ Function to retreive the API key from YAML file.
    The YAML file needs to be in the same as the code directory.

    Returns: string of API key
    """
    full_file_path = Path(__file__).parent.joinpath('api-key.yaml')
    with open(full_file_path) as settings:
        _key = yaml.load(settings, Loader=yaml.Loader)
    return _key


# Param definition
INTERVAL = 60
API_KEY = get_api_keys()['api_key']
allowed_actions = ['both', 'publish', 'subscribe']

parser = argparse.ArgumentParser()
parser.add_argument('--city', action='store', dest='api_city', default='Manila', help='The city to retrieve the weather data')
parser.add_argument('-r', '--root-ca', action='store', dest='root_ca_path', help='Root CA file path')
parser.add_argument('-c', '--cert', action='store', required=True, dest='certificate_path', help='Certificate file path')
parser.add_argument('-k', '--key', action='store', required=True, dest='private_key_path', help='Private key file path')
parser.add_argument('-n', '--thing-name', action='store', required=True, dest='thing_name', help='Targeted thing name')
parser.add_argument('-t', '--topic', action='store', dest='topic', default='demo/weather', help='Targeted topic')
parser.add_argument('-m', '--mode', action='store', dest='mode', default='both',
                    help='Operation modes: %s'%str(allowed_actions))
parser.add_argument('--region', action='store', dest='region', default='ap-southeast-1')
parser.add_argument('--print-discover-resp-only', action='store_true', dest='print_discover_resp_only', default=False)
parser.add_argument('-v', '--verbosity', choices=[x.name for x in LogLevel], default=LogLevel.NoLogs.name,
                    help='Logging level')

args = parser.parse_args()

io.init_logging(getattr(LogLevel, args.verbosity), 'stderr')
event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

tls_options = io.TlsContextOptions.create_client_with_mtls_from_path(args.certificate_path, args.private_key_path)
if args.root_ca_path:
    tls_options.override_default_trust_store_from_path(None, args.root_ca_path)
tls_context = io.ClientTlsContext(tls_options)

socket_options = io.SocketOptions()

print('Performing greengrass discovery...')
discovery_client = DiscoveryClient(client_bootstrap, socket_options, tls_context, args.region)
resp_future = discovery_client.discover(args.thing_name)
discover_response = resp_future.result()

print(discover_response)
if args.print_discover_resp_only:
    exit(0)


def on_connection_interupted(connection, error, **kwargs):
    print('connection interrupted with error {}'.format(error))


def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print('connection resumed with return code {}, session present {}'.format(return_code, session_present))


# Try IoT endpoints until we find one that works
def try_iot_endpoints():
    for gg_group in discover_response.gg_groups:
        for gg_core in gg_group.cores:
            for connectivity_info in gg_core.connectivity:
                try:
                    # connectivity_info.host_address = "52.221.214.138"#remove after testing
                    print('Trying core {} at host {} port {}'.format(gg_core.thing_arn, connectivity_info.host_address, connectivity_info.port))
                    mqtt_connection = mqtt_connection_builder.mtls_from_path(
                        endpoint=connectivity_info.host_address,
                        port=connectivity_info.port,
                        cert_filepath=args.certificate_path,
                        pri_key_filepath=args.private_key_path,
                        client_bootstrap=client_bootstrap,
                        ca_bytes=gg_group.certificate_authorities[0].encode('utf-8'),
                        on_connection_interrupted=on_connection_interupted,
                        on_connection_resumed=on_connection_resumed,
                        client_id=args.thing_name,
                        clean_session=False,
                        keep_alive_secs=30)

                    connect_future = mqtt_connection.connect()
                    connect_future.result()
                    print('Connected!')
                    return mqtt_connection

                except Exception as e:
                    print('Connection failed with exception {}'.format(e))
                    continue

    exit('All connection attempts failed')

def getWindSpeed(p_arg):
    #the api will return value in kph, hence need to convert it to meter per secs
    temp = p_arg * (5/18)
    return temp

def callWeatherAPI(p_apiKey, p_city):
    apiendpoint = "http://api.weatherapi.com/v1/current.json?key={0}&q={1}&aqi={2}"
    finalEndpoint = apiendpoint.format(p_apiKey, p_city, "yes")
    response = requests.get(finalEndpoint)
    return response.json()

def developPayload(p_data, p_slrfd_w, p_rain_mm_tot, p_bRandom):
    lat = p_data["location"]["lat"]
    lon = p_data["location"]["lon"]
    ws_ms = getWindSpeed(p_data["current"]["wind_kph"])
    windir = p_data["current"]["wind_degree"]
    airt_c = p_data["current"]["temp_c"]
    vp_mbar = p_data["current"]["pressure_in"]
    bp_mbar = p_data["current"]["pressure_mb"]
    rh = p_data["current"]["humidity"]
    loc_name = p_data["location"]["name"]
    timestamp = str(datetime.datetime.now())
    result = constructJson(p_slrfd_w, p_rain_mm_tot, ws_ms, windir, airt_c, vp_mbar, bp_mbar, rh, lat, lon, loc_name, timestamp)
    if (p_bRandom):
        temp1 = generateRandom(ws_ms)
        temp2 = generateRandom(windir)
        temp3 = generateRandom(airt_c)
        temp4 = generateRandom(vp_mbar)
        temp5 = generateRandom(bp_mbar)
        temp6 = generateRandom(rh)
        temp7 = str(datetime.datetime.now())
        result = constructJson(p_slrfd_w, p_rain_mm_tot, temp1, temp2, temp3, temp4, temp5, temp6, lat, lon, loc_name, temp7)

    return result

def constructJson(p_slrfd_w, p_rain_mm_tot, p_ws_ms, p_windir, p_airt_c, p_vp_mbar, p_bp_mbar, p_rh, p_lat, p_lon, p_loc_name, p_timestamp):
    raw = {'SlrFD_W': p_slrfd_w, 'Rain_mm_Tot': p_rain_mm_tot, 'WS_ms': p_ws_ms, 'WindDir': p_windir, 'AirT_C': p_airt_c, 'VP_mbar': p_vp_mbar, 'BP_mbar': p_bp_mbar, 'RH': p_rh, 'lat': p_lat, 'lon': p_lon, 'loc_name': p_loc_name, 'timestamp': p_timestamp}
    temp = json.dumps(raw, indent=4)
    return temp

def generateRandom(p_value):
    lowerBound = p_value - 1.0
    upperBound = p_value + 1.0
    return round(random.uniform(lowerBound, upperBound), 1)

mqtt_connection = try_iot_endpoints()

if args.mode == 'both' or args.mode == 'subscribe':

    def on_publish(topic, payload, dup, qos, retain, **kwargs):
        print('Publish received on topic {}'.format(topic))
        print(payload)

    subscribe_future, _ = mqtt_connection.subscribe(args.topic, QoS.AT_MOST_ONCE, on_publish)
    subscribe_result = subscribe_future.result()

data = callWeatherAPI(API_KEY, args.api_city)
initialPayload = developPayload(data, 0, 0, False)
pub_future, _ = mqtt_connection.publish(args.topic, initialPayload, QoS.AT_MOST_ONCE)
pub_future.result()

print("Hit [ CTRL ] + [ C ] to exit!")

print('Published topic {}: {}\n'.format(args.topic, initialPayload))
time.sleep(INTERVAL)

try:
    while(True):
        subsequentPayload = developPayload(data, 0, 0, True)
        pub_future, _ = mqtt_connection.publish(args.topic, subsequentPayload, QoS.AT_MOST_ONCE)
        pub_future.result()
        print('Published topic {}: {}\n'.format(args.topic, subsequentPayload))
        time.sleep(INTERVAL)
    
except KeyboardInterrupt:
    print('Exiting')
    pass