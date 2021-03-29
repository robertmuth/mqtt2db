#!/usr/bin/python3
"""
mqtt2db - Simple MQTT to DB persister

Copyright (C) 2021  Robert Muth (robert@muth.org)

License GPL 3.0

"""
from typing import List, Dict, Optional, Any, Tuple

import argparse
import functools
import http.server
import logging
import paho.mqtt.client as mqtt
import platform
import queue
import re
import sqlite3
import threading
import time

PARSER = argparse.ArgumentParser(description="mqtt2db")
PARSER.add_argument("--mqtt_broker", default="192.168.1.1",
                    help="MQTT broker to subscribe to")
PARSER.add_argument("--mqtt_port", default=1883,
                    help="MQTT broker port")
PARSER.add_argument("--verbose", action="store_true", default=False,
                    help="Increase log level to INFO")
PARSER.add_argument("--debug", action="store_true", default=False,
                    help="Increase log level to DEBUG")

PARSER.add_argument("--host", default="",
                    help="hostname to use for debug webserver")
PARSER.add_argument("--port", default=7777,
                    help="port to use for debug webserver")
PARSER.add_argument("--sqlitedb", default="",
                    help="sqlite3 DB to persist messages to if non-empty")
PARSER.add_argument("--topic", action="append",
                    help="topic to subscribe to - can be repeated")


ARGS = PARSER.parse_args()
if ARGS.verbose:
    logging.basicConfig(level=logging.INFO)
if ARGS.debug:
    logging.basicConfig(level=logging.DEBUG)


MQTT_CLIENT: Optional["MqttClient"] = None


MSG_QUEUE = queue.Queue()


class MsgStats:

    def __init__(self):
        self.start_time = time.time()
        self.most_recent_msg_by_typic: Dict[str, str] = {}
        self.max_topic_len = 0
        self.num_msg_igonred = 0
        self.num_msg_processed = 0
        self.num_msg_flushed = 0
        self.num_msg_by_topic: Dict[str, int] = {}
        self.most_recent_timestamp_by_task: Dict[str, float] = {}

    def RegisterProcessed(self, timestamp, task, msg):
        self.most_recent_timestamp_by_task[task] = timestamp
        self.num_msg_processed += 1
        self.max_topic_len = max(self.max_topic_len, 1 + msg.topic.count("/"))
        self.most_recent_msg_by_typic[msg.topic] = msg.payload
        self.num_msg_by_topic[msg.topic] = self.num_msg_by_topic.get(
            msg.topic, 0) + 1

    def RegisterIgnored(self, timestamp, task):
        self.most_recent_timestamp_by_task[task] = timestamp
        self.num_msg_igonred += 1


MSG_STATS = MsgStats()

############################################################
# SQLITE
#
# Schema:
# soure TEXT: the data provider, e.g. collectd or esphome
# host TEXT: the  machine name (possibly qualified), e.g.  server.loc
# metric TEXT: the metric, e.g. thermal-cooling_device1/gauge
# timestamp DATETIME: the time component of the data point
# val STR/REAL: the value component of the data point
# valX REAL: additional values
############################################################

RE_FLOATS = re.compile("^[-+0-9.eE:]+$")


class MsgCache:
    """Avoid storing consecutive events without change """

    def __init__(self, max_age_sec=86400):
        self.max_age_sec = max_age_sec
        self.most_recent_event_value = {}
        self.most_recent_event_timestamp = {}
        self.most_recent_flush_timestamp = {}

    def _RecordFlush(self, timestamp, key, value):
        self.most_recent_event_value[key] = value
        self.most_recent_event_timestamp[key] = timestamp
        self.most_recent_flush_timestamp[key] = timestamp

    def ShouldFlush(self, timestamp, key, value):
        if key not in self.most_recent_flush_timestamp:
            self._RecordFlush(timestamp, key, value)
            return [(timestamp, value)]

        if value == self.most_recent_event_value[key]:
            if timestamp - self.most_recent_flush_timestamp[key] > self.max_age_sec:
                self._RecordFlush(timestamp, key, value)
                return [(timestamp, value)]
            self.most_recent_event_timestamp[key] = timestamp
            return []

        if self.most_recent_flush_timestamp[key] != self.most_recent_event_timestamp[key]:
            out = [(self.most_recent_event_timestamp[key], self.most_recent_event_value[key]),
                   (timestamp, value)]
            self._RecordFlush(timestamp, key, value)
            return out

        self._RecordFlush(timestamp, key, value)
        return [(timestamp, value)]


MSG_CACHE = MsgCache()


def MsgPersisterSqlite():
    global MSG_QUEUE, MSG_CACHE, MSG_STATS
    # create the DB if necessary
    logging.info(f"opening sqlite db [{ARGS.sqlitedb}]")
    con = sqlite3.connect(ARGS.sqlitedb)
    con.execute("PRAGMA foreign_keys = 1")
    cursor = con.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for t in tables:
        logging.info(f"fount table [{t}]")

    # this indirection is to save space
    if ("metric_enum",) not in tables:
        cursor.execute(
            """CREATE TABLE metric_enum (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE )""")
    if ("mqtt_floats",) not in tables:
        cursor.execute(
            """CREATE TABLE mqtt_floats (
                 source TEXT NOT_NULL,
                 host TEXT NOT NULL,
                 metric NOT NULL REFERENCES metric_enum(id),
                 timestamp INTEGER NOT NULL,
                 val REAL NOT NULL,
                 val2 REAL,
                 val3 REAL,
                 val4 REAL)""")
    if ("mqtt_str",) not in tables:
        cursor.execute(
            """CREATE TABLE mqtt_str (
                 source TEXT NOT NULL,
                 host TEXT NOT NULL,
                 metric NOT NULL REFERENCES metric_enum(id),
                 timestamp INTEGER NOT NULL,
                 val TEXT NOT NULL)""")

    def get_metrics_enum():
        cursor.execute("SELECT id, name FROM metric_enum;")
        return {n: i for i, n in cursor.fetchall()}

    metric_enum_map = get_metrics_enum()

    # this is to preserve some space
    #
    logging.info("MsgPersisterSqlite loop")
    most_recent_event = {}
    most_recent_flush_timestamp = {}
    while True:
        timestamp, msg = MSG_QUEUE.get()
        payload = msg.payload.decode("utf-8")
        source, host, metric = msg.topic.split("/", 2)
        if metric not in metric_enum_map:
            logging.info(f"adding metric to enum: [{metric}]")
            cursor.execute(f"INSERT INTO metric_enum(name) Values('{metric}')")
            metric_enum_map = get_metrics_enum()
        key = (source, host, metric_enum_map[metric])

        for timestamp, payload in MSG_CACHE.ShouldFlush(timestamp, key, payload):
            MSG_STATS.num_msg_flushed += 1
            row = list(key) + [timestamp]
            if RE_FLOATS.match(payload):
                logging.debug(f"processing floats {msg.topic}")
                row += [float(x) for x in payload.split(":")]
                row += [None] * (8 - len(row))
                cursor.executemany(
                    """INSERT INTO
                   mqtt_floats(source, host, metric, timestamp,
                               val, val2, val3, val4)
                   VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
                    [row])
            else:
                logging.debug(f"processing str {msg.topic} {repr(payload)}")
                row.append(payload)
                cursor.executemany(
                    """INSERT INTO
                   mqtt_str(source, host, metric, timestamp, val)
                   VALUES(?, ?, ?, ?, ?)""",
                    [row])
        con.commit()
        MSG_QUEUE.task_done()


if ARGS.sqlitedb:
    threading.Thread(target=MsgPersisterSqlite, daemon=False).start()


############################################################
# Misc Helpers
############################################################


def exception(function):
    """
    A decorator that makes sure that errors do not go unnoticed
    """
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as err:
            logging.error("in function [%s]: %s", function.__name__, err)
            raise err
    return wrapper


############################################################
# Status Page
############################################################
HTML_PROLOG = """ <!DOCTYPE html>
<html>
<head>
<style>
body {
  font-family: sans-serif;
}
table {
  border-collapse: collapse;
}
</style>
</head>
<body>
"""

HTML_EPILOG = """
</body>
</html>
"""


def RenderStatusPage():
    global MSG_STATS, ARGS
    html = ["<h3>Stats</h3>"
            "<pre>",
            f"up for: {int(time.time() - MSG_STATS.start_time)}s",
            f"processed: {MSG_STATS.num_msg_processed:7}",
            f"ingored:   {MSG_STATS.num_msg_igonred:7}",
            f"flushed:   {MSG_STATS.num_msg_flushed:7}",
            f"max topic length: {MSG_STATS.max_topic_len}",
            f"topics: {ARGS.topic}",
            "</pre>"
            ]
    html += ["<h3>Most recent message timestamp by task</h3>",
             "<table border=1>"]
    now = time.time()
    for task, timestamp in sorted(MSG_STATS.most_recent_timestamp_by_task.items()):
        when = int(now - timestamp)
        html.append(f"<tr><td>{task}</td><td>{when}s ago</td></tr>")
    html += ["</table>"]
    html += ["<h3>Most recent message by topic</h3>",
             "<table border=1>"]
    last = []
    for topic, payload in sorted(MSG_STATS.most_recent_msg_by_typic.items()):
        token = topic.split("/")
        token += ["&nbsp;"] * (MSG_STATS.max_topic_len - len(token))
        out = token[:]
        for i, t in enumerate(last):
            if out[i] == t:
                out[i] = "&nbsp;"
            else:
                break
        last = token
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        out.append(str(MSG_STATS.num_msg_by_topic[topic]))
        out.append(payload)
        row = [f"<td>{x}</td>" for x in out]
        html.append(f"<tr>{''.join(row)}</tr>")

    html += ["</table>"]

    return HTML_PROLOG + "\n".join(html) + HTML_EPILOG


class MqttClient:

    def __init__(self, name, host, port, on_message_handler):
        self.name = name
        self.client = mqtt.Client(name)
        self.client.will_set(
            f"{name}/{platform.node()}/sys/status", "0", retain=True)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_log = self.on_log
        self.client.connect(host, port, keepalive=60)
        self.on_message_handler = on_message_handler
        # note, this does not block
        self.client.loop_start()

    # paho API - problems will be silently ignored without this
    def on_log(client, userdata, level, buff):
        print("!!!!!!!!!!!!!!!!!")
        log.error("paho problem %s %s %s", userdata, level, buff)

    def EmitMessage(self, topic, message, retain=True):
        self.client.publish(topic, message, retain)

    def EmitStatusMessage(self):
        self.EmitMessage(
            f"{self.name}/{platform.node()}/sys/status", "1", retain=True)

    # in its infinite wisdom, paho silently drops errors in callbacks
    @exception
    def on_connect(self, client, userdata, rc, dummy):
        logging.info(
            "Connected with result code %s %s %s (if you see a lot of these you may have duplicate client names)",
            rc,
            userdata,
            dummy)
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        for topic in ARGS.topic:
            logging.info(f"subscribing to [{topic}]")
            self.client.subscribe(topic)
        self.EmitStatusMessage()

    # in its infinite wisdom, paho silently drops errors in callbacks
    @exception
    def on_message(self, client, userdata, msg):
        """allback for when a PUBLISH message is received from the server"""
        logging.debug(f"received: {msg.topic} {msg.payload} {userdata}")
        try:
            self.on_message_handler(msg)

        except Exception as err:
            logging.error("failure: %s", str(err))


FAV_ICON = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<svg
   xmlns:dc="http://purl.org/dc/elements/1.1/"
   xmlns:cc="http://creativecommons.org/ns#"
   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   xmlns:svg="http://www.w3.org/2000/svg"
   xmlns="http://www.w3.org/2000/svg"
   id="svg8"
   version="1.1"
   viewBox="0 0 264.58333 264.58334"
   height="1000"
   width="1000">
  <g
     transform="translate(0,-32.416641)"
     id="layer1">
    <text
       id="text817"
       y="147.32143"
       x="56.69643"
       style="font-style:normal;font-variant:normal;font-weight:bold;font-stretch:normal;font-size:141.1111145px;line-height:125%;font-family:monospace;-inkscape-font-specification:'monospace Bold';letter-spacing:0px;word-spacing:0px;fill:#000000;fill-opacity:1;stroke:none;stroke-width:0.26458332px;stroke-linecap:butt;stroke-linejoin:miter;stroke-opacity:1"
       xml:space="preserve"><tspan
         style="font-style:normal;font-variant:normal;font-weight:bold;font-stretch:normal;font-size:141.1111145px;font-family:monospace;-inkscape-font-specification:'monospace Bold';stroke-width:0.26458332px"
         y="147.32143"
         x="56.69643"
         id="tspan815">MQ</tspan></text>
    <text
       id="text821"
       y="282.63684"
       x="57.452377"
       style="font-style:normal;font-variant:normal;font-weight:bold;font-stretch:normal;font-size:141.1111145px;line-height:125%;font-family:monospace;-inkscape-font-specification:'monospace Bold';letter-spacing:0px;word-spacing:0px;fill:#000000;fill-opacity:1;stroke:none;stroke-width:0.26458332px;stroke-linecap:butt;stroke-linejoin:miter;stroke-opacity:1"
       xml:space="preserve"><tspan
         style="font-style:normal;font-variant:normal;font-weight:bold;font-stretch:normal;font-size:141.1111145px;font-family:monospace;-inkscape-font-specification:'monospace Bold';stroke-width:0.26458332px"
         y="282.63684"
         x="57.452377"
         id="tspan819">TT</tspan></text>
  </g>
</svg>
"""


class SimpleHTTPRequestHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        print("GET", self.path)
        self.send_response(200)
        if self.path == "/favicon.ico":
            self.send_header('Content-type', "image/svg+xml")
            self.end_headers()
            self.wfile.write(bytes(FAV_ICON, "utf-8"))
        else:
            self.end_headers()
            self.wfile.write(bytes(RenderStatusPage(), "utf-8"))


def ShouldIgnoreMaybeRewrite(msg: mqtt.MQTTMessage):
    """
    Contains some hacks for collectd and esphome generated messaeges
    """
    if msg.topic.startswith("esphome/"):
        if not msg.payload:
            return True
        if msg.topic.endswith("/debug"):
            return True
        if msg.payload == b"ON":
            msg.payload = b"1"
        if msg.payload == b"OFF":
            msg.payload = b"0"

    if msg.topic.startswith("collectd/"):
        # collects append a zero byte to the payload
        if msg.payload[-1] == 0:
            msg.payload = msg.payload[:-1]
        if msg.payload == b"nan" or msg.payload == b"nan:nan":
            return True
    return False


def GetTimestampMaybeRewrite(msg: mqtt.MQTTMessage) -> int:
    if msg.topic.startswith("collectd"):
        ts_str, msg.payload = msg.payload.split(b":", 1)
        return int(float(ts_str))
    else:
        return int(time.time())


def GetTask(msg: mqtt.MQTTMessage) -> str:
    pos = msg.topic.find("/")
    pos = msg.topic.find("/", pos + 1)
    return msg.topic[:pos]


def OnMessage(msg: mqtt.MQTTMessage):
    global MSG_STATS, MSG_QUEUE

    timestamp = GetTimestampMaybeRewrite(msg)
    task = GetTask(msg)

    # some cleanup
    if ShouldIgnoreMaybeRewrite(msg):
        MSG_STATS.RegisterIgnored(timestamp, task)
        return

    MSG_STATS.RegisterProcessed(timestamp, task, msg)
    MSG_QUEUE.put((timestamp, msg))


logging.info("starting mqtt handler")
MQTT_CLIENT = MqttClient("mqtt2db", ARGS.mqtt_broker,
                         ARGS.mqtt_port, OnMessage)


logging.info("starting web interfaces on port %d", ARGS.port)
WEB_SERVER = http.server.HTTPServer(
    (ARGS.host, ARGS.port), SimpleHTTPRequestHandler)
WEB_SERVER.serve_forever()
