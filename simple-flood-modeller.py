from google.cloud import datastore
from flask import Flask
import json
import subprocess
import math
import time
import os


project_id = os.getenv('GCLOUD_PROJECT')
dsagent = datastore.Client(project_id)
TIMESTAMP_STEP_SIZE = 10800
DEBUG = False

app = Flask(__name__)
JOB_FOLDER = "/home/yash95shah/cse546-proj2/"
FLOOD_FOLDER = "/home/yash95shah/cse546-proj2/"

# if DEBUG:
#     JOB_FOLDER = "../job_gateway/"
#     MY_FOLDER = "./"


@app.route('/proc')
def inject_into_datastore(data, name=None):
    key = dsagent.key('data', name)
    j_entity = datastore.Entity(key=key)
    for j_prop, j_val in data.items():
        j_entity[j_prop] = j_val
    dsagent.put(j_entity)
    return 'Job finished'


def query_fetch(kind):
    print("Entered into it")
    if kind is None:
        return
    query = client.query(kind=kind)
    first_key = client.key(kind, 'context')
    query.key_filter(first_key, '=')
    return (list(query.fetch())[0].items())


@app.route('/task/sc1')
def script1():
    return ("This is the cronscript 1")


@app.route('/task/sc2')
def script2():
    return ("This is the cronscript 2")
# def truncate(number, digits):
#     stepper = pow(10.0, digits)
#     return math.trunc(stepper * number) / stepper


@app.route('/test')
def test():
    return 'String Test'


@app.route('/')
def main():
    start_time = time.time()
    # trigger the job gateway to load the current job
    command = ['args // url to be used for fetching the job']
# This part needs work as cron jobs are to be fetched
    # log("Calling JobGateway...")
    # try:
    #     if DEBUG:
    #         # log("Skipping for debug!")
    #         pass
    #     else:
    #         subprocess.check_output(command, shell=False)
    # except subprocess.CalledProcessError as cpe:
    # log("JobGateway returned exit code 1 on 'fetch_job'")
    # with open("/home/cc/error_log.txt", "w") as errorlog:
    #     errorlog.write(json.dumps(str(cpe)))
    # return  # no job fetched
    # log("JobGateway fetched")

    # create an spatiotemporal index scaffolding for the current context
    with open(JOB_FOLDER + "context.json") as json_data:
        context = json.loads(json_data.read())
        inject_into_datastore(context, 'context')

    # job info, including sampling
    with open(JOB_FOLDER + "job.json") as json_data:
        job_details = json.loads(json_data.read())
        job_variables = job_details["variables"]
        inject_into_datastore(job_variables, 'job_variables')

    # load land/water legend
    with open(FLOOD_FOLDER + "is_water.json") as json_data:
        is_water = json.loads(json_data.read())
        inject_into_datastore(is_water, 'is_water')

    # # create hierarchical index for results

    # # temporal first
    # begin = context["temporal"]["begin"]
    # end = context["temporal"]["end"]
    # times = []
    # while begin <= end:
    #     times.append(begin)
    #     begin = begin + TIMESTAMP_STEP_SIZE

    # # then longitude
    # lons = []
    # west = truncate(context["spatial"]["left"], 1)
    # east = truncate(context["spatial"]["right"], 1)
    # while west <= east:
    #     lons.append(west)
    #     west = truncate(west + context["spatial"]["x_resolution"], 1)

    # # then latitude
    # lats = []
    # top = truncate(context["spatial"]["top"], 1)
    # bottom = truncate(context["spatial"]["bottom"], 1)
    # while top >= bottom:
#         lats.append(top)
#         top = truncate(top - context["spatial"]["y_resolution"], 1)

#     # set up zero water depths within the scaffold
#     water_depth = dict()
#     for timestamp in times:
#         water_depth[timestamp] = dict()
#         for lon in lons:
#             water_depth[timestamp][lon] = dict()
#             for lat in lats:
#                 water_depth[timestamp][lon][lat] = 0.0

#     # load data from job gateway folder (only from hurricane, which I expect to see)
#     with open(JOB_FOLDER + "input_data/hurricane/dsfr.json") as json_data:
#         dsfr_data = json.loads(json_data.read())

#     # for each time step, compute the water that arrived in that step
#     print("DSFR count: {0}".format(len(dsfr_data)))
#     with_rain = 0
#     for dsfr in dsfr_data:
#         lon, lat = dsfr["coordinate"]

#         lon = truncate(lon, 1)  # force coordinates into a format I can use
#         lat = truncate(lat, 1)

#         ts = dsfr["timestamp"]
#         rainfall = dsfr["observation"][0]  # rainfall is in the first key
#         lon_key = str(truncate(lon, 1))
#         lat_key = str(truncate(lat, 1))
#         if lon_key not in is_water:
#             continue  # TODO fix this, scrape more data
#         if lat_key not in is_water[lon_key]:
#             continue  # TODO fix this, scrape more data
#         if is_water[lon_key][lat_key]:
#             continue  # no rainfall on the ocean
#         try:
#             if rainfall != 0:
#                 with_rain = with_rain + 1
#                 #print(str(ts) + "\t" + str(lon) + "\t" + str(lat) + "\t" + str(rainfall))
#             water_depth[ts][truncate(lon, 1)][truncate(lat, 1)] = rainfall * job_variables["scaling_factor"]
#         except Exception as e:
#             log(e)

#     print("DSFRs with rain: {0}".format(with_rain))

#     # add each time step's depth to the sum of the previous time steps (water accumulates)
#     for i in range(1, len(times)):
#         timestamp = times[i]
#         last_timestamp = times[i - 1]
#         for lon in lons:
#             for lat in lats:
#                 lon_key = str(truncate(lon, 1))
#                 lat_key = str(truncate(lat, 1))
#                 if lon_key not in is_water:
#                     continue  # TODO fix this, scrape more data
#                 if lat_key not in is_water[lon_key]:
#                     continue  # TODO fix this, scrape more data
#                 if is_water[lon_key][lat_key]:
#                     continue  # no accumulation on ocean

#                 rain_this_step = water_depth[timestamp][lon][lat]
#                 #if rain_this_step != 0:
#                     #print(str(time) + "\t" + str(lon) + "\t" + str(lat) + "\t" + str(rain_this_step))  # *******
#                 last_depth = water_depth[last_timestamp][lon][lat]
#                 water_depth[timestamp][lon][lat] = \
#                     rain_this_step + last_depth

#     # package results for storage by the job gateway
#     record_list = []
#     for timestamp in times:
#         for lon in lons:
#             for lat in lats:
#                 record = dict()
#                 record["timestamp"] = timestamp
#                 record["coordinate"] = [lon, lat]
#                 depth = water_depth[timestamp][lon][lat]
#                 if depth == 0:
#                     continue  # exclude results with no water
#                 record["observation"] = [depth]
#                 record_list.append(record)
#     with open(JOB_FOLDER + "output_data/data.json", "w") as outfile:
#         outfile.write(json.dumps(record_list))

#     # TODO artificially introduce some delay for demo purposes; remove when desired
#     duration = time.time() - start_time
#     time_to_wait = 30 - duration
#     if time_to_wait > 0:
#         time.sleep(time_to_wait)

#     # trigger the job gateway to save the current job
#     command = ["/home/cc/job_gateway/venv/bin/python3",
#                "/home/cc/job_gateway/JobGateway.py",
#                "finish_job"]
#     try:
#         if DEBUG:
#             log("Skipping 'finish' for debug!")
#         else:
#             subprocess.check_output(command, shell=False, stderr=subprocess.STDOUT)
#     except subprocess.CalledProcessError as cpe:
#         log("JobGateway returned exit code 1 on 'finish_job'")
#         with open("/home/cc/error_log.txt", "w") as errorlog:
#             errorlog.write(json.dumps(str(cpe)))
#         return  # could not save results for some reason?

#     log("JobGateway is all done! Results have been saved.")


# def log(my_string):
#     try:
#         with open("water_log.txt", "a") as errorlog:
#             errorlog.write(json.dumps(str(my_string)) + "\n")
#     except FileNotFoundError:
#         print("No log file found.")


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8080, debug=True)
