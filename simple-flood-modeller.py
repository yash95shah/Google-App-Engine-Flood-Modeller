from google.cloud import datastore
import logging
import datetime
import json
import subprocess
import math
import time


dsagent = datastore.Client(project=gcp-demo-213218)
TIMESTAMP_STEP_SIZE = 10800
DEBUG = False

# JOB_FOLDER = "/home/cc/job_gateway/"
# MY_FOLDER = "/home/cc/flood_model/"

# if DEBUG:
#     JOB_FOLDER = "../job_gateway/"
#     MY_FOLDER = "./"

def truncate(number, digits) -> float:
    stepper = pow(10.0, digits)
    return math.trunc(stepper * number) / stepper

def main():
    start_time = time.time()

    # trigger the job gateway to load the current job
    command = ['args // url to be used for fetching the job']
# This part needs work as cron jobs are to be fetched
    log("Calling JobGateway...")
    try:
        if DEBUG:
            log("Skipping for debug!")
        else:
            subprocess.check_output(command, shell=False, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as cpe:
        log("JobGateway returned exit code 1 on 'fetch_job'")
        # with open("/home/cc/error_log.txt", "w") as errorlog:
        #     errorlog.write(json.dumps(str(cpe)))
        return  # no job fetched
    log("JobGateway fetched")

    # create an spatiotemporal index scaffolding for the current context
    with open(JOB_FOLDER + "context.json") as json_data:
        context = json.loads(json_data.read())

    # job info, including sampling
    with open(JOB_FOLDER + "job.json") as json_data:
        job_details = json.loads(json_data.read())
    job_variables = job_details["variables"]  # scaling_factor (used), evaporation_rate, transpiration_rate

    # load land/water legend
    with open(MY_FOLDER + "is_water.json") as json_data:
        is_water = json.loads(json_data.read())

    # create hierarchical index for results

    # temporal first
    begin = context["temporal"]["begin"]
    end = context["temporal"]["end"]
    times = []
    while begin <= end:
        times.append(begin)
        begin = begin + TIMESTAMP_STEP_SIZE

    # then longitude
    lons = []
    west = truncate(context["spatial"]["left"], 1)
    east = truncate(context["spatial"]["right"], 1)
    while west <= east:
        lons.append(west)
        west = truncate(west + context["spatial"]["x_resolution"], 1)

    # then latitude
    lats = []
    top = truncate(context["spatial"]["top"], 1)
    bottom = truncate(context["spatial"]["bottom"], 1)
    while top >= bottom:
        lats.append(top)
        top = truncate(top - context["spatial"]["y_resolution"], 1)

    # set up zero water depths within the scaffold
    water_depth = dict()
    for timestamp in times:
        water_depth[timestamp] = dict()
        for lon in lons:
            water_depth[timestamp][lon] = dict()
            for lat in lats:
                water_depth[timestamp][lon][lat] = 0.0

    # load data from job gateway folder (only from hurricane, which I expect to see)
    with open(JOB_FOLDER + "input_data/hurricane/dsfr.json") as json_data:
        dsfr_data = json.loads(json_data.read())

    # for each time step, compute the water that arrived in that step
    print("DSFR count: {0}".format(len(dsfr_data)))
    with_rain = 0
    for dsfr in dsfr_data:
        lon, lat = dsfr["coordinate"]

        lon = truncate(lon, 1)  # force coordinates into a format I can use
        lat = truncate(lat, 1)

        ts = dsfr["timestamp"]
        rainfall = dsfr["observation"][0]  # rainfall is in the first key
        lon_key = str(truncate(lon, 1))
        lat_key = str(truncate(lat, 1))
        if lon_key not in is_water:
            continue  # TODO fix this, scrape more data
        if lat_key not in is_water[lon_key]:
            continue  # TODO fix this, scrape more data
        if is_water[lon_key][lat_key]:
            continue  # no rainfall on the ocean
        try:
            if rainfall != 0:
                with_rain = with_rain + 1
                #print(str(ts) + "\t" + str(lon) + "\t" + str(lat) + "\t" + str(rainfall))  # *******
            water_depth[ts][truncate(lon, 1)][truncate(lat, 1)] = rainfall * job_variables["scaling_factor"]
        except Exception as e:
            log(e)

    print("DSFRs with rain: {0}".format(with_rain))

    # add each time step's depth to the sum of the previous time steps (water accumulates)
    for i in range(1, len(times)):
        timestamp = times[i]
        last_timestamp = times[i - 1]
        for lon in lons:
            for lat in lats:
                lon_key = str(truncate(lon, 1))
                lat_key = str(truncate(lat, 1))
                if lon_key not in is_water:
                    continue  # TODO fix this, scrape more data
                if lat_key not in is_water[lon_key]:
                    continue  # TODO fix this, scrape more data
                if is_water[lon_key][lat_key]:
                    continue  # no accumulation on ocean

                rain_this_step = water_depth[timestamp][lon][lat]
                #if rain_this_step != 0:
                    #print(str(time) + "\t" + str(lon) + "\t" + str(lat) + "\t" + str(rain_this_step))  # *******
                last_depth = water_depth[last_timestamp][lon][lat]
                water_depth[timestamp][lon][lat] = \
                    rain_this_step + last_depth

    # package results for storage by the job gateway
    record_list = []
    for timestamp in times:
        for lon in lons:
            for lat in lats:
                record = dict()
                record["timestamp"] = timestamp
                record["coordinate"] = [lon, lat]
                depth = water_depth[timestamp][lon][lat]
                if depth == 0:
                    continue  # exclude results with no water
                record["observation"] = [depth]
                record_list.append(record)
    with open(JOB_FOLDER + "output_data/data.json", "w") as outfile:
        outfile.write(json.dumps(record_list))

    # TODO artificially introduce some delay for demo purposes; remove when desired
    duration = time.time() - start_time
    time_to_wait = 30 - duration
    if time_to_wait > 0:
        time.sleep(time_to_wait)

    # trigger the job gateway to save the current job
    command = ["/home/cc/job_gateway/venv/bin/python3",
               "/home/cc/job_gateway/JobGateway.py",
               "finish_job"]
    try:
        if DEBUG:
            log("Skipping 'finish' for debug!")
        else:
            subprocess.check_output(command, shell=False, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as cpe:
        log("JobGateway returned exit code 1 on 'finish_job'")
        with open("/home/cc/error_log.txt", "w") as errorlog:
            errorlog.write(json.dumps(str(cpe)))
        return  # could not save results for some reason?

    log("JobGateway is all done! Results have been saved.")


def log(my_string):
    try:
        with open("water_log.txt", "a") as errorlog:
            errorlog.write(json.dumps(str(my_string)) + "\n")
    except FileNotFoundError:
        print("No log file found.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("Exception occurred during flood modelling:\n" + str(e))