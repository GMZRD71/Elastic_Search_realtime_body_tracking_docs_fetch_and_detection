# Libraries
import csv

import pandas as pd
from pandas.io.json import json_normalize
import json
import pickle
from datetime import datetime
import datetime as dtime
import numpy as np

from itertools import count

import time

# Additional useful libraries
from mpl_toolkits import mplot3d
import matplotlib.pyplot as plt

import matplotlib.animation as animation

def difpos2vel(s,dt):
    # Compute speed
    # USAGE: speed = difpos2vel(s,dt)
    # where:
    #         s - position data
    #        dt - sample interval
    #print('s= ',s)
    vel = np.zeros_like(s)
    # Internal values
    vel[1:-1] = (s[2:] - s[:-2])/(2*dt)
    # End points
    vel[0]  = (s[1]  - s[0]) /dt
    vel[-1] = (s[-1] - s[-2])/dt
    
    return vel
            

# CHECK MOTION
def ckMotion(Ts,x,y,z, threshold, segment):
    # CHECK THE LAST MOST RECENT READINGS, OBTAIN AVERAGE LATERAL SPEED
    # DETERMINE IF ABOVE THRESHOLD OF LATERAL SPEED
    most_recentD=25
    #print('Effective Delta-T= ',Ts)
    #print('Effective Frame Rate= ',1/Ts)
    dispLat=x[len(x)-most_recentD:len(x)]
    velLat=difpos2vel(dispLat,Ts)
    avgVel=abs(np.average(velLat))
    #print('Length of x: ',len(x))
    #print('Most recent N values: ', x[len(x)-most_recentD:len(x)])
    print('Absolute value for Average Lateral Speed: ',avgVel,' m/s')
    # Check threshold if the person moves more than THRESHOLD cm
    if avgVel >= threshold:
        textStr='Motion Detected'
        print(textStr)
        print(threshold)
    else:
        textStr='No Motion'
        print(textStr)
        print(threshold)       

    return textStr

in_csv = "bodyTrack_DatFromES96.csv"

# LAYOUT
# ---------------------------------------------------------------------
# Create figure for plotting

fig, axs = plt.subplots(2, sharex=True)

# Locate the window and size it
mngr = plt.get_current_fig_manager()
# Get the QTCore PyRect object
geom = mngr.window.geometry()
x,y,dx,dy = geom.getRect()
newX =1280
newY = 100
mngr.window.setGeometry(newX, newY, dx, dy)

# Threshold values
head_thresh=0.034  # m/s
pelv_thresh=0.034  # m/s
ymin=-1
ymax=2.5

# This function is called periodically from FuncAnimation
def animate(i):
        
    dfTrackDatIn = pd.read_csv(in_csv)
    
    # CONVERTED TIME TO ms AT THE SOURCE - DEVICE TIME (ms) (KINECTS)
    devTms = dfTrackDatIn['UnityT_ms'].to_numpy()
    devTms = devTms.astype(np.float)
    t = devTms/1000
    t = t - t[0]   # Zero the time vector
    t0=t[len(t)-51:len(t)-1]
    t1=t[len(t)-50:len(t)]
    Ts=np.average(t1-t0)

    # DATA FOR: pelvis, navel, and head (XYZ directions)
    # ---------------------------------------------------
    # Pelvis
    pelv_x = dfTrackDatIn['Pos_Pelvis_X_m'].to_numpy()
    pelv_x = pelv_x.astype(np.float)
    pelv_y = dfTrackDatIn['Pos_Pelvis_Y_m'].to_numpy()
    pelv_y = pelv_y.astype(np.float)
    pelv_z = dfTrackDatIn['Pos_Pelvis_Z_m'].to_numpy()
    pelv_z = pelv_z.astype(np.float)

    # Head
    head_x = dfTrackDatIn['Pos_Head_X_m'].to_numpy()
    head_x = head_x.astype(np.float)
    head_y = dfTrackDatIn['Pos_Head_Y_m'].to_numpy()
    head_y = head_y.astype(np.float)
    head_z = dfTrackDatIn['Pos_Head_Z_m'].to_numpy()
    head_z = head_z.astype(np.float)
    
    axs[0].clear()
    axs[1].clear()
    
    axs[0].plot(t, pelv_x, 'r', label='Side-Side Motion' )
    axs[0].plot(t, pelv_y, 'g', label='Front-Back Motion' )
    axs[0].plot(t, pelv_z, 'b', label='Up-Down Motion' )
    axs[0].set_ylim([ymin,ymax])    
    
    axs[1].plot(t, head_x, 'r', label='Lateral Head Motion' )
    axs[1].plot(t, head_y, 'g', label='Lateral Head Motion' )
    axs[1].plot(t, head_z, 'b', label='Lateral Head Motion' )
    axs[1].set_ylim([ymin,ymax])        
    
    axs[0].set_title('Head Motion Tracking Data over Time')
    axs[1].set_title('Pelvis Motion Tracking Data over Time')

    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)    
    
    headStr=ckMotion(Ts, head_x, head_y, head_z, head_thresh, "head")
    axs[0].text(0.5, 0.95, headStr, transform=axs[0].transAxes, fontsize=14,
         verticalalignment='top', bbox=props)
    
    pelvStr=ckMotion(Ts, pelv_x, pelv_y, pelv_z, pelv_thresh, "pelvis")
    axs[1].text(0.5, 0.95, pelvStr, transform=axs[1].transAxes, fontsize=14,
        verticalalignment='top', bbox=props)
    
    # Format plot
    axs[0].set(ylabel='meters')
    axs[1].set(ylabel='meters',xlabel='Real Time')
    
    axs[0].legend(loc='upper left')

# Set up plot to call animate() function periodically
ani = animation.FuncAnimation(fig, animate, interval=1000)
plt.show()