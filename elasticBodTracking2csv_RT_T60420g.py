# Libraries
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q

import csv
from elasticsearch_dsl.aggs import Terms, Nested
from elasticsearch_dsl.search import Search
from elasticsearch_dsl import Q

import pandas as pd
from pandas.io.json import json_normalize
import json
import pickle
from datetime import datetime
import datetime as dtime
import numpy as np
import time

import tensorflow as tf

tf.test.is_gpu_available

# Additional useful libraries
from mpl_toolkits import mplot3d
import matplotlib.pyplot as plt
#  %matplotlib inline

#----------------------------------------------------------------------------------------------
def add_leading_zero(val1):
    # Add a leading zero to the string value of the number is the number is less than 10
    if val1 < 10:
        str1 = "0" + str(val1)
    else:
        str1 = str(val1)
    return str1




# Create a function to define the querie
def create_querie(current_time, deltaT):
    #----------------------------------------------------------------------------------------------
    # Define initial reference time string
    datum1=current_time

    month=datum1.month
    day=datum1.day
    hour=datum1.hour
    minute=datum1.minute
    second=datum1.second

    # HOURS --------------------------------------
    # Add five hours to convert to HST (Hawaii Standard Time)
    # Check if after 7PM, then increment the date by 1
    if hour+5 > 24:
        hour=hour+5-24
        day=day+1
    else:
        hour=hour+5

    # DAYS --------------------------------------
    days=add_leading_zero(day)

    # SECONDS --------------------------------------
    lower_sec=second-deltaT
    upper_sec=second
    lower_min=minute
    upper_min=minute
    lower_hr=hour
    upper_hr=hour

    if lower_sec < 0:
        lower_sec=lower_sec+60
        lower_min=lower_min-1
        if lower_min < 0:
            lower_min=lower_min+60
            lower_hr=lower_hr-1

    lower_seconds=add_leading_zero(lower_sec)
    upper_seconds=add_leading_zero(upper_sec)

    lower_minutes=add_leading_zero(lower_min)
    upper_minutes=add_leading_zero(upper_min)

    lower_hours=add_leading_zero(lower_hr)
    upper_hours=add_leading_zero(upper_hr)

    # MONTHS --------------------------------------
    months=add_leading_zero(month)    

    # Create a N-second time window
    lower_t=str(datum1.year) + "-" + months + "-" + days + "T" + lower_hours + ":" + lower_minutes + ":" + lower_seconds + ".000"
    upper_t=str(datum1.year) + "-" + months + "-" + days + "T" + upper_hours + ":" + upper_minutes + ":" + upper_seconds + ".000"

    # Define the body of the querie
    bodyJSON= {
            "query": {
                "range": {
                    "Sys_Date_Time": {
                        "gte": lower_t
                    }
                }
            }}

    return bodyJSON




def get_ES_data(index,bodyJSON,num_docs):
    #----------------------------------------------------------------------------------------------------------------------
    # Extract the data from ELK
    size=num_docs

    result = elastic_client.search(index=index, body=bodyJSON, size=size)
    elastic_docs = result['hits']['hits']

    # Create Pandas dataframe
    df2 = pd.DataFrame()
    for _size in range(size):
        example_df = pd.DataFrame(elastic_docs)['_source'][_size]

        example_df_clean = {}
        for key in example_df.keys():
            if type(example_df[key]) in [str, int, float]:
                #print("yay - - - - - - ", key)
                # Remove un-needed keys
                example_df_clean[key] = [example_df[key]]
            else:
                pass
                #print("---boo - - - ", key)

        del example_df_clean['message']
        df1 = pd.DataFrame.from_dict(example_df_clean)
        df2 = df2.append(df1, ignore_index=True)
        
    # Re-sort with the columns in order for use in Unity
    proper_list_of_columns =  ["Sys_Date_Time","DevTime","DeviceT_ms","|_k4b","Location","UnityT_ms","Body_ID","Body_Count","Joint_Count","SpaceScale_x","SpaceScale_y","SpaceScale_z","blsTracked","liTrackingID",
      "TrackingState_Pelvis","Pos_Pelvis_X_m","Pos_Pelvis_Y_m","Pos_Pelvis_Z_m","Yaw_Pelvis_X_deg","Pitch_Pelvis_Y_deg","Roll_Pelvis_Z_deg",
      "TrackingState_Spine_Naval","Pos_Spine_Naval_X_m","Pos_Spine_Naval_Y_m","Pos_Spine_Naval_Z_m","Yaw_Spine_Naval_X_deg","Pitch_Spine_Naval_Y_deg","Roll_Spine_Naval_Z_deg",
      "TrackingState_Spine_Chest","Pos_Spine_Chest_X_m","Pos_Spine_Chest_Y_m","Pos_Spine_Chest_Z_m","Yaw_Spine_Chest_X_deg","Pitch_Spine_Chest_Y_deg","Roll_Spine_Chest_Z_deg",
      "TrackingState_Neck","Pos_Neck_X_m","Pos_Neck_Y_m","Pos_Neck_Z_m","Yaw_Neck_X_deg","Pitch_Neck_Y_deg","Roll_Neck_Z_deg",
      "TrackingState_Clavicle_Left","Pos_Clavicle_Left_X_m","Pos_Clavicle_Left_Y_m","Pos_Clavicle_Left_Z_m","Yaw_Clavicle_Left_X_deg","Pitch_Clavicle_Left_Y_deg","Roll_Clavicle_Left_Z_deg",
      "TrackingState_Shoulder_Left","Pos_Shoulder_Left_X_m","Pos_Shoulder_Left_Y_m","Pos_Shoulder_Left_Z_m","Yaw_Shoulder_Left_X_deg","Pitch_Shoulder_Left_Y_deg","Roll_Shoulder_Left_Z_deg",
      "TrackingState_Elbow_Left","Pos_Elbow_Left_X_m","Pos_Elbow_Left_Y_m","Pos_Elbow_Left_Z_m","Yaw_Elbow_Left_X_deg","Pitch_Elbow_Left_Y_deg","Roll_Elbow_Left_Z_deg",
      "TrackingState_Wrist_Left","Pos_Wrist_Left_X_m","Pos_Wrist_Left_Y_m","Pos_Wrist_Left_Z_m","Yaw_Wrist_Left_X_deg","Pitch_Wrist_Left_Y_deg","Roll_Wrist_Left_Z_deg",
      "TrackingState_Hand_Left","Pos_Hand_Left_X_m","Pos_Hand_Left_Y_m","Pos_Hand_Left_Z_m","Yaw_Hand_Left_X_deg","Pitch_Hand_Left_Y_deg","Roll_Hand_Left_Z_deg",
      "TrackingState_HandTip_Left","Pos_HandTip_Left_X_m","Pos_HandTip_Left_Y_m","Pos_HandTip_Left_Z_m","Yaw_HandTip_Left_X_deg","Pitch_HandTip_Left_Y_deg","Roll_HandTip_Left_Z_deg",
      "TrackingState_Thumb_Left","Pos_Thumb_Left_X_m","Pos_Thumb_Left_Y_m","Pos_Thumb_Left_Z_m","Yaw_Thumb_Left_X_deg","Pitch_Thumb_Left_Y_deg","Roll_Thumb_Left_Z_deg",
      "TrackingState_Clavicle_Right","Pos_Clavicle_Right_X_m","Pos_Clavicle_Right_Y_m","Pos_Clavicle_Right_Z_m","Yaw_Clavicle_Right_X_deg","Pitch_Clavicle_Right_Y_deg","Roll_Clavicle_Right_Z_deg",
      "TrackingState_Shoulder_Right","Pos_Shoulder_Right_X_m","Pos_Shoulder_Right_Y_m","Pos_Shoulder_Right_Z_m","Yaw_Shoulder_Right_X_deg","Pitch_Shoulder_Right_Y_deg","Roll_Shoulder_Right_Z_deg",
      "TrackingState_Elbow_Right","Pos_Elbow_Right_X_m","Pos_Elbow_Right_Y_m","Pos_Elbow_Right_Z_m","Yaw_Elbow_Right_X_deg","Pitch_Elbow_Right_Y_deg","Roll_Elbow_Right_Z_deg",
      "TrackingState_Wrist_Right","Pos_Wrist_Right_X_m","Pos_Wrist_Right_Y_m","Pos_Wrist_Right_Z_m","Yaw_Wrist_Right_X_deg","Pitch_Wrist_Right_Y_deg","Roll_Wrist_Right_Z_deg",
      "TrackingState_Hand_Right","Pos_Hand_Right_X_m","Pos_Hand_Right_Y_m","Pos_Hand_Right_Z_m","Yaw_Hand_Right_X_deg","Pitch_Hand_Right_Y_deg","Roll_Hand_Right_Z_deg",
      "TrackingState_HandTip_Right","Pos_HandTip_Right_X_m","Pos_HandTip_Right_Y_m","Pos_HandTip_Right_Z_m","Yaw_HandTip_Right_X_deg","Pitch_HandTip_Right_Y_deg","Roll_HandTip_Right_Z_deg",
      "TrackingState_Thumb_Right","Pos_Thumb_Right_X_m","Pos_Thumb_Right_Y_m","Pos_Thumb_Right_Z_m","Yaw_Thumb_Right_X_deg","Pitch_Thumb_Right_Y_deg","Roll_Thumb_Right_Z_deg",
      "TrackingState_Hip_Left","Pos_Hip_Left_X_m","Pos_Hip_Left_Y_m","Pos_Hip_Left_Z_m","Yaw_Hip_Left_X_deg","Pitch_Hip_Left_Y_deg","Roll_Hip_Left_Z_deg",
      "TrackingState_Knee_Left","Pos_Knee_Left_X_m","Pos_Knee_Left_Y_m","Pos_Knee_Left_Z_m","Yaw_Knee_Left_X_deg","Pitch_Knee_Left_Y_deg","Roll_Knee_Left_Z_deg",
      "TrackingState_Ankle_Left","Pos_Ankle_Left_X_m","Pos_Ankle_Left_Y_m","Pos_Ankle_Left_Z_m","Yaw_Ankle_Left_X_deg","Pitch_Ankle_Left_Y_deg","Roll_Ankle_Left_Z_deg",
      "TrackingState_Foot_Left","Pos_Foot_Left_X_m","Pos_Foot_Left_Y_m","Pos_Foot_Left_Z_m","Yaw_Foot_Left_X_deg","Pitch_Foot_Left_Y_deg","Roll_Foot_Left_Z_deg",
      "TrackingState_Hip_Right","Pos_Hip_Right_X_m","Pos_Hip_Right_Y_m","Pos_Hip_Right_Z_m","Yaw_Hip_Right_X_deg","Pitch_Hip_Right_Y_deg","Roll_Hip_Right_Z_deg",
      "TrackingState_Knee_Right","Pos_Knee_Right_X_m","Pos_Knee_Right_Y_m","Pos_Knee_Right_Z_m","Yaw_Knee_Right_X_deg","Pitch_Knee_Right_Y_deg","Roll_Knee_Right_Z_deg",
      "TrackingState_Ankle_Right","Pos_Ankle_Right_X_m","Pos_Ankle_Right_Y_m","Pos_Ankle_Right_Z_m","Yaw_Ankle_Right_X_deg","Pitch_Ankle_Right_Y_deg","Roll_Ankle_Right_Z_deg",
      "TrackingState_Foot_Right","Pos_Foot_Right_X_m","Pos_Foot_Right_Y_m","Pos_Foot_Right_Z_m","Yaw_Foot_Right_X_deg","Pitch_Foot_Right_Y_deg","Roll_Foot_Right_Z_deg",
      "TrackingState_Head","Pos_Head_X_m","Pos_Head_Y_m","Pos_Head_Z_m","Yaw_Head_X_deg","Pitch_Head_Y_deg","Roll_Head_Z_deg",
      "TrackingState_Nose","Pos_Nose_X_m","Pos_Nose_Y_m","Pos_Nose_Z_m","Yaw_Nose_X_deg","Pitch_Nose_Y_deg","Roll_Nose_Z_deg",
      "TrackingState_Eye_Left","Pos_Eye_Left_X_m","Pos_Eye_Left_Y_m","Pos_Eye_Left_Z_m","Yaw_Eye_Left_X_deg","Pitch_Eye_Left_Y_deg","Roll_Eye_Left_Z_deg",
      "TrackingState_Ear_Left","Pos_Ear_Left_X_m","Pos_Ear_Left_Y_m","Pos_Ear_Left_Z_m","Yaw_Ear_Left_X_deg","Pitch_Ear_Left_Y_deg","Roll_Ear_Left_Z_deg",
      "TrackingState_Eye_Right","Pos_Eye_Right_X_m","Pos_Eye_Right_Y_m","Pos_Eye_Right_Z_m","Yaw_Eye_Right_X_deg","Pitch_Eye_Right_Y_deg","Roll_Eye_Right_Z_deg",
      "TrackingState_Ear_Right","Pos_Ear_Right_X_m","Pos_Ear_Right_Y_m","Pos_Ear_Right_Z_m","Yaw_Ear_Right_X_deg","Pitch_Ear_Right_Y_deg","Roll_Ear_Right_Z_deg"]

    df4xport=df2[proper_list_of_columns];

    # Sort by device time (ms)
    df5 = df4xport.sort_values(by ='DeviceT_ms' );

    return df5

# MAIN ----------------------------------------------------------------------------------------------------------------------------
# CONFIGURATION VALUES
index = 'INDEX NAME HERE'
deltaT=20      # How many seconds to go back in time from the current time
waitT=7        # How long to wait until another batch of documents is retrieved
num_docs=100   # Number of documents to extract at each cycle
maxCycles=25   # Number of cycles

# Save to local CSV file
out_csv = "CSV FILE NAME HERE.csv"

# (Future Use) Use camera frame rate to estimate the number of documents per batch ±Delta-T, so multiply by 2
#num_docs=deltaT*fRate
fRate=30      # fps


t = time.time()

# OBTAIN DATA FROM ELASTIC SEARCH AND APPEND TO CSV FILE
for i in range(1,maxCycles):

    # Obtain the current time and fetch the data from Elastic search in the last deltaT seconds
    current_time = dtime.datetime.now()
    print("Collecting data from: ", current_time, " to ", deltaT, " seconds ago")

    # Create the querie for ES
    bodyJSON=create_querie(current_time, deltaT)

    # Fetch the data
    dfTrackDatOut = get_ES_data(index,bodyJSON,num_docs)

    # Save to CSV
    chunksize=1
    if i == 1:
        dfTrackDatOut.to_csv(out_csv,
              index=False,
              header=True,
              mode='a',#append data to csv file
              chunksize=chunksize)#size of data to append for each loop
    else:
        dfTrackDatOut.to_csv(out_csv,
              index=False,
              header=False,
              mode='a',#append data to csv file
              chunksize=chunksize)#size of data to append for each loop
    
    time.sleep(waitT)  # Sleep for waitT second(s)
    print("Wait ", waitT, " seconds to get the next batch...")
    print("Time that took to complete this batch: ", t-time.time(), " sec; Cycle #: ", i)

# COMPLETE
print('Done')
print("Total timetook =", t-time.time(), " sec; Total Cycles #: ", i)
