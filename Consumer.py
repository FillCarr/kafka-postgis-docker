from kafka import KafkaConsumer
from tkinter import *
from tkinter import ttk
import tkintermapview
from multiprocessing import Process, Manager
import matplotlib.pyplot as plt
from matplotlib.backend_bases import key_press_handler
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg,
                                               NavigationToolbar2Tk)
import numpy as np
from matplotlib.figure import Figure
import os
import json
import psycopg

def consumer(rt):
    c = KafkaConsumer('SensorData')
    print(f'connected to consumer')
    for msg in c:
        ingest(msg, rt)
#message ingest
#https://video.osgeo.org/w/9cZiX3fMCtpPwhZgRw3oqa
#https://www.psycopg.org/psycopg3/docs/api/module.html#psycopg.capabilities
def ingest(msg, rt):
    data = json.loads(msg.value) #parse json to python dict
    rt.update(data)
    lat = data['latitude']
    lon = data['longitude']
    tis = data['timestamp']
    sas = data['satellites']
    spm = data['speed_meters_per_second']
    bat = data['battery']
    dim = data['distance_meters']
    elt = data['elapsed_time_seconds']
    elv = data['elevation']
    with psycopg.connect("dbname=gis user=gis password=password host=127.0.0.1 port=5433") as conn:
        with conn.cursor() as cur:
            cur.execute("""
             CREATE TABLE IF NOT EXISTS kafka
             (
             id serial PRIMARY KEY,
             speed float,
             lat float,
             lon float,
             sats integer,
             dis_m float,
             lap_t float,
             elev float
             )
            """)
            cur.executemany(
                "INSERT INTO kafka (speed, lat, lon, sats, dis_m, lap_t, elev) VALUES (%s, %s, %s, %s, %s, %s, %s)", [(spm, lat, lon, sas, dim, elt, elv)])
            conn.commit()
#consumer gui
#https://matplotlib.org/stable/gallery/user_interfaces/embedding_in_tk_sgskip.html

def interface(rt):
    root = Tk()
    frm = ttk.Frame(root, borderwidth=10)
##workspace
    frm.grid(column=2, row=2)
##header
# Q0 db logic
    # Shared state for the view marker placed by db_read
    current_view_marker = [None]

    # Single-form popup for create/edit — returns dict of field values or None on cancel
    def record_dialog(title, initial=None):
        dlg = Toplevel(root)
        dlg.title(title)
        dlg.resizable(False, False)
        dlg.grab_set()
        fields = ['speed', 'lat', 'lon', 'sats', 'dis_m', 'lap_t', 'elev']
        entries = {}
        for i, field in enumerate(fields):
            ttk.Label(dlg, text=field).grid(row=i, column=0, padx=8, pady=4, sticky='e')
            val = str(initial[field]) if initial and field in initial else ''
            var = StringVar(value=val)
            ent = ttk.Entry(dlg, textvariable=var, width=20)
            ent.grid(row=i, column=1, padx=8, pady=4)
            entries[field] = var
        result = {}
        def on_ok():
            try:
                result['speed'] = float(entries['speed'].get())
                result['lat']   = float(entries['lat'].get())
                result['lon']   = float(entries['lon'].get())
                result['sats']  = int(float(entries['sats'].get()))
                result['dis_m'] = float(entries['dis_m'].get())
                result['lap_t'] = float(entries['lap_t'].get())
                result['elev']  = float(entries['elev'].get())
                dlg.destroy()
            except ValueError:
                pass
        def on_cancel():
            dlg.destroy()
        btn_frm = ttk.Frame(dlg)
        btn_frm.grid(row=len(fields), column=0, columnspan=2, pady=8)
        ttk.Button(btn_frm, text="OK",     command=on_ok).pack(side='left', padx=4)
        ttk.Button(btn_frm, text="Cancel", command=on_cancel).pack(side='left', padx=4)
        root.wait_window(dlg)
        return result if result else None

    def db_create():
        data = record_dialog("Create Record")
        if data is None: return
        try:
            with psycopg.connect("dbname=gis user=gis password=password host=127.0.0.1 port=5433") as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO kafka (speed, lat, lon, sats, dis_m, lap_t, elev) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (data['speed'], data['lat'], data['lon'], data['sats'],
                                 data['dis_m'], data['lap_t'], data['elev']))
                    conn.commit()
            refresh_tree()
        except Exception as e:
            print(e)

    def db_read():
        refresh_tree()
        sel = tree.selection()
        if not sel: return
        # tree_cols: id(0), speed(1), lat(2), lon(3), sats(4), dis_m(5), lap_t(6), elev(7)
        vals = tree.item(sel[0])['values']
        lat, lon = float(vals[2]), float(vals[3])
        row_id, speed, lap_t, elev = vals[0], vals[1], vals[6], vals[7]
        # Toggle: if a marker is already shown, remove it and return
        if current_view_marker[0] is not None:
            current_view_marker[0].delete()
            current_view_marker[0] = None
            return
        marker = map_widget.set_marker(lat, lon,
            text=f"ID: {row_id} | Speed: {speed} m/s | Lap T: {lap_t}s | Elev: {elev}m")
        current_view_marker[0] = marker
        offset = 0.01
        map_widget.fit_bounding_box((lat + offset, lon - offset), (lat - offset, lon + offset))

    def db_update():
        sel = tree.selection()
        if not sel: return
        vals = tree.item(sel[0])['values']
        row_id = vals[0]
        # Pre-populate with current row values
        # tree_cols: id(0), speed(1), lat(2), lon(3), sats(4), dis_m(5), lap_t(6), elev(7)
        initial = {
            'speed': vals[1], 'lat': vals[2], 'lon': vals[3],
            'sats':  vals[4], 'dis_m': vals[5], 'lap_t': vals[6], 'elev': vals[7]
        }
        data = record_dialog("Edit Record", initial=initial)
        if data is None: return
        try:
            with psycopg.connect("dbname=gis user=gis password=password host=127.0.0.1 port=5433") as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE kafka SET speed=%s, lat=%s, lon=%s, sats=%s, dis_m=%s, lap_t=%s, elev=%s WHERE id=%s",
                        (data['speed'], data['lat'], data['lon'], data['sats'],
                         data['dis_m'], data['lap_t'], data['elev'], row_id))
                    conn.commit()
            refresh_tree()
        except Exception as e:
            print(e)

    def db_delete():
        sel = tree.selection()
        if not sel: return
        row_id = tree.item(sel[0])['values'][0]
        try:
            with psycopg.connect("dbname=gis user=gis password=password host=127.0.0.1 port=5433") as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM kafka WHERE id=%s", (row_id,))
                    conn.commit()
            tree.delete(sel[0])
        except Exception as e:
            print(e)

    ttk.Button(frm, text="Create", command=db_create).grid(column=0, row=0)
    ttk.Button(frm, text="Read",   command=db_read).grid(column=1, row=0)
    ttk.Button(frm, text="Update", command=db_update).grid(column=2, row=0)
    ttk.Button(frm, text="Delete", command=db_delete).grid(column=3, row=0)

    speed_data = []
    dist_data = []

    def poll():
        if rt.get('speed_meters_per_second') is not None:
            speed_data.append(float(rt['speed_meters_per_second']))
            ax_spd.cla()
            ax_spd.stairs(speed_data, linewidth=2.5)
            ax_spd.set_title("Speed over Samples")
            ax_spd.set_xlabel("Sample")
            ax_spd.set_ylabel("Speed (m/s)")
            sot.draw()
        if rt.get('distance_meters') is not None:
            dist_data.append(float(rt['distance_meters']))
            ax_dst.cla()
            if len(dist_data) > 1:
                ax_dst.ecdf(dist_data)
            ax_dst.set_title("Distance ECDF")
            ax_dst.set_xlabel("Distance (m)")
            ax_dst.set_ylabel("Cumulative Probability")
            dot.draw()
        if rt.get('latitude') is not None and rt.get('longitude') is not None:
            map_widget.set_position(float(rt['latitude']), float(rt['longitude']))
            map_widget.set_marker(float(rt['latitude']), float(rt['longitude']))
        refresh_tree()
        root.after(1000, poll)

##Q2 speed
    y = speed_data
    fig, ax_spd = plt.subplots()
    ax_spd.stairs(y, linewidth=2.5)
    ax_spd.set_title("Speed over Samples")
    ax_spd.set_xlabel("Sample")
    ax_spd.set_ylabel("Speed (m/s)")
    sot = FigureCanvasTkAgg(fig, frm)  # A tk.DrawingArea.
    sot.draw()
    sot.get_tk_widget().grid(column=0, row=1)
##Q1 distance
    x = dist_data
    fig1, ax_dst = plt.subplots()
    ax_dst.ecdf(x) if len(x) > 1 else None
    ax_dst.set_title("Distance ECDF")
    ax_dst.set_xlabel("Distance (m)")
    ax_dst.set_ylabel("Cumulative Probability")
    dot = FigureCanvasTkAgg(fig1, frm)  # A tk.DrawingArea.
    dot.draw()
    dot.get_tk_widget().grid(column=0, row=2)
#Q3 map
#https://github.com/TomSchimansky/TkinterMapView
    map_widget = tkintermapview.TkinterMapView(frm, width=800, height=480)
    map_widget.grid(column=2, row=1)
#Q4 table
    tree_frm = ttk.Frame(frm)
    tree_frm.grid(column=2, row=2)
    tree_cols = ('id', 'speed', 'lat', 'lon', 'sats', 'dis_m', 'lap_t', 'elev')
    tree = ttk.Treeview(tree_frm, columns=tree_cols, show='headings', height=10, selectmode='browse')
    for col in tree_cols:
        tree.heading(col, text=col)
        tree.column(col, width=100)
    verscrlbar = ttk.Scrollbar(tree_frm, orient='vertical', command=tree.yview)
    tree.configure(yscrollcommand=verscrlbar.set)
    tree.pack(side='left')
    verscrlbar.pack(side='right', fill='y')
# table polling
    def refresh_tree():
        try:
            with psycopg.connect("dbname=gis user=gis password=password host=127.0.0.1 port=5433") as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id, speed, lat, lon, sats, dis_m, lap_t, elev FROM kafka ORDER BY id DESC")
                    rows = cur.fetchall()
            existing = {tree.item(iid)['values'][0]: iid for iid in tree.get_children()}
            for row in rows:
                if row[0] not in existing:
                    tree.insert('', 'end', values=row)
        except Exception:
            pass

    poll()
    root.mainloop()

if __name__ == '__main__':
    with Manager() as manager:
        rt = manager.dict()
        p = Process(target=consumer, args=(rt,))
        p.start()
        interface(rt)
        p.join()
