from kafka import KafkaConsumer
from tkinter import *
from tkinter import ttk
import tkintermapview
from multiprocessing import Process
import matplotlib.pyplot as plt
from matplotlib.backend_bases import key_press_handler
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg,
                                               NavigationToolbar2Tk)
import numpy as np
from matplotlib.figure import Figure
import numpy as np
import os
import json
import psycopg

def consumer():
    c = KafkaConsumer('SensorData')
    print(f'connected to consumer')
    for msg in c:
        ingest(msg)
#message ingest
#https://video.osgeo.org/w/9cZiX3fMCtpPwhZgRw3oqa
#https://www.psycopg.org/psycopg3/docs/api/module.html#psycopg.capabilities
def ingest(msg):
    global rt
    jsonstirng = msg
    rt = json.loads(jsonstring) #parse json to python dict
    lat = rt['latitude']
    lon = rt['longitude']
    tis = rt['timestamp']
    sas = rt['satellites']
    spm = rt['speed_meters_per_second']
    bat = rt['battery']
    dim = rt['distance_meters']
    elt = rt['elapsed_time_seconds']
    elv = rt['elevation']
    with psycopg.connect("dbname=gis user=gis pass=password") as conn:
        with conn.cursor() as cur:
            cur.execute
            ("""
             CREATE TABLE IF NOT EXISTS kafka 
             (
             id serial PRIMARY KEY,
             speed float,
             lat float,
             lot float,
             sats integer,
             dis_m float,
             lap_t float,
             elev float,
             )
            """)
            cur.executemany(
                "INSERT INTO kafka (speed, loc, sats, dis_m, lap_t, elev) VALUES (%s, %s, %s, %s, %s, %s)", (spm, lat, lot, sas, dim, elt, elv))
            conn.commit()
#consumer gui
#https://matplotlib.org/stable/gallery/user_interfaces/embedding_in_tk_sgskip.html
rt = {}

def interface():    
    root = Tk()
    frm = ttk.Frame(root, borderwidth=10)  
##workspace
    frm.grid(column=2, row=2)
##header

    tis = rt['timestamp']
    ts = StringVar(tis)
    timestamps = ttk.Combobox(frm, textvariable=ts).grid(column=0, row=0)
    ttk.Button(frm, text="Quit", command=root.destroy).grid(column=2, row=0)
##Q2 speed
    y = [4.8, 5.5, 3.5, 4.6, 6.5, 6.6, 2.6, 3.0]
    fig, ax = plt.subplots()
    ax.stairs(y, linewidth=2.5)
    ax.set(xlim=(0, 8), xticks=np.arange(1, 8),
    ylim=(0, 8), yticks=np.arange(1, 8))
    sot = FigureCanvasTkAgg(fig, frm)  # A tk.DrawingArea.
    sot.draw()
    sot.get_tk_widget().grid(column=0, row=1)
##Q1 distance
    np.random.seed(1)
    x = 4 + np.random.normal(0, 1.5, 200)
    fig1, ax = plt.subplots() 
    ax.ecdf(x)
    dot = FigureCanvasTkAgg(fig1, frm)  # A tk.DrawingArea.
    dot.draw()
    dot.get_tk_widget().grid(column=0, row=2)
#Q3 map
#https://github.com/TomSchimansky/TkinterMapView
    map = tkintermapview.TkinterMapView(frm,width=800, height=480).grid(column=2, row =1)
    root.mainloop()

if __name__ == '__main__':
    p = Process(target=consumer)
    p1 = Process(target=ingest, args=msg,)
    p.start()
    p1.start()
    interface()
    p.join()
    p1.join()

