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
INTERVAL = 300  # call this every 5 minutes
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
parser.add_argument('--generate-mockup-data-only', action='store_true', dest='p_mockup_data', default=False, 
                    help='Get the random data instead of actual weather data')

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

def convertTime(time_epoch: int):
    """ Function to convert epoch time onto human readable format
    The time conversion may impact from timezone of the running machine.

    Return: date time in predefined format
    """
    time_ = datetime.datetime.fromtimestamp(time_epoch).strftime('%Y-%m-%d %H:%M:%S')
    return time_

def callWeatherAPI(p_apiKey, p_city):
    apiendpoint = "http://api.weatherapi.com/v1/current.json?key={0}&q={1}&aqi={2}"
    finalEndpoint = apiendpoint.format(p_apiKey, p_city, "yes")
    response = requests.get(finalEndpoint)
    return response.json()

def developPayload(p_data: dict, p_bRandom: bool=False):
    lat = p_data["location"]["lat"]
    lon = p_data["location"]["lon"]
    loc_name = p_data['location']['name']
    country_nm = p_data['location']['country']
    loc_epoch = p_data['location']['localtime_epoch']
    weather_last_update = p_data['current']['last_updated_epoch']
    condition_txt = p_data['current']['condition']['text']

    wind_spd_ms = getWindSpeed(p_data["current"]["wind_kph"])
    wind_degree = p_data["current"]["wind_degree"]
    gust_spd_ms = getWindSpeed(p_data['current']['gust_kph'])  # gust - brief increase of wind speed
    airt_c = p_data["current"]["temp_c"]
    vp_mbar = p_data["current"]["pressure_in"]
    bp_mbar = p_data["current"]["pressure_mb"]
    rh = p_data["current"]["humidity"]
    cld = p_data['current']['cloud']
    precip_mm = p_data['current']['precip_mm']
    pm2_5 = p_data['current']['air_quality']['pm2_5']
    o3 = p_data['current']['air_quality']['o3']
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if p_bRandom:
        # If the mode is to generate random number, replace all weather data into a mock-up data
        airt_c = generateRandom(airt_c)
        wind_spd_ms = generateRandom(wind_spd_ms)
        wind_degree = generateRandom(wind_degree)
        gust_spd_ms = generateRandom(gust_spd_ms)
        vp_mbar = generateRandom(vp_mbar)
        bp_mbar = generateRandom(bp_mbar)
        rh = generateRandom(rh)
        cld = generateRandom(cld)
        precip_mm = generateRandom(precip_mm)
        pm2_5 = generateRandom(pm2_5)
        o3 = generateRandom(o3)

    result = constructJson(
        airt_c, wind_spd_ms, wind_degree, gust_spd_ms,
        vp_mbar, bp_mbar, rh, cld, precip_mm, pm2_5, o3, condition_txt,
        lat, lon, loc_name, country_nm, 
        loc_epoch, weather_last_update, timestamp
    )
    return result

def constructJson(
        airt_c, wind_spd_ms, wind_degree, gust_spd_ms,
        vp_mbar, bp_mbar, rh, cld, precip_mm, pm2_5, o3, condition_txt,
        lat, lon, loc_name, country_name, 
        loc_epoch, weather_last_update_epoch, timestamp
    ):
    dict_ = {
        'air_temp_c': airt_c,
        'wind_speed_ms': wind_spd_ms,
        'wind_direction_degree': wind_degree,
        'wind_gust_speed_ms': gust_spd_ms,
        'vp_mbar': vp_mbar,
        'bp_mbar': bp_mbar,
        'relative_humidity': rh,
        'cloud_coverage': cld,
        'precipitation': precip_mm,
        'pm2_5': pm2_5,
        'ozone': o3,
        'condition': condition_txt,
        'latitude': lat,
        'longitude': lon,
        'city_name': loc_name,
        'country_name': country_name,
        'localtime_epoch': loc_epoch,
        'weather_last_updated_epoch': weather_last_update_epoch,
        'create_timestamp': timestamp
    }
    return json.dumps(dict_, indent=4)

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
initialPayload = developPayload(p_data=data, p_bRandom=args.p_mockup_data)
pub_future, _ = mqtt_connection.publish(args.topic, initialPayload, QoS.AT_MOST_ONCE)
pub_future.result()

print("Hit [ CTRL ] + [ C ] to exit!")

print('Published topic {}: {}\n'.format(args.topic, initialPayload))
time.sleep(INTERVAL)

try:
    while(True):
        data = callWeatherAPI(API_KEY, args.api_city)
        subsequentPayload = developPayload(p_data=data, p_bRandom=args.p_mockup_data)
        pub_future, _ = mqtt_connection.publish(args.topic, subsequentPayload, QoS.AT_MOST_ONCE)
        pub_future.result()
        print('Published topic {}: {}\n'.format(args.topic, subsequentPayload))
        time.sleep(INTERVAL)
    
except KeyboardInterrupt:
    print('Exiting')
    pass