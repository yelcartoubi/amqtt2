
import asyncio

import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import pytz


texts = []

token = "Cz8QK2TvIi35FNkkzWS1b-LU_9F7D-IW0lqujJ74tl82ddKjUVh7okoPwfB1ukgcLBrrioykciXvQ_5OwU4-JA=="
org = "org"
bucket = "BROKER2"
url = "http://localhost:8086"


def set_influxDB_point(measurement, client_id=None, ctype=None, connect=None, remain=None, topic=None, subscribe=None, subs_num=None, msg=None, qos=None, receiver_id=None, receiver_state=None, sender_id=None,sender_state=None, reach_out=None, user_name=None, password=None, clean_session=None, status=None, ban=None):

    # value = random.randint(1, 1234)
    # field = "field805"

    client = InfluxDBClient(url=url, token=token, org=org)

    write_api = client.write_api(
        write_options=SYNCHRONOUS)
    ## point0 = Point("table1").tag("Client", "publisher").field("arrival", False)  # .time(timestamp)
    # todo float
    # point = Point("publisher").tag("arrival", False).tag("tag2","sm").field(field, float(value)).field(field2, str(value2)).time(time)
    if measurement == "clients_data_sp":
        point = Point(measurement).tag("client_id", client_id).tag("type", ctype).tag("connect", connect).tag("remain", remain).field("field", 0)  # .time(timestamp)
        points = [point]
        write_api.write(bucket=bucket, org=org, record=points)
    elif measurement == "topics_data":
        point = Point(measurement).tag("topic", topic).tag("client_id", client_id).tag("qos", qos).tag("subs_num", subs_num).field("field", 0)  # .time(timestamp)
        points = [point]
        write_api.write(bucket=bucket, org=org, record=points)
    elif measurement == "sub_data":
        point = Point(measurement).tag("client_id", client_id).tag("type", ctype).tag("topic", topic).tag("subscribe",
                                                                                                          subscribe).field("field", 0)  # .time(timestamp)
        points = [point]
        write_api.write(bucket=bucket, org=org, record=points)
    elif measurement == "msg_data":
        if reach_out == "0":
            point = Point(measurement).tag("client_id", client_id).tag("type", ctype).tag("topic", topic).tag("msg",msg).tag("qos",qos).tag(
                "sender_id", sender_id).tag("receiver_id", "").tag("reach_out", reach_out).field("field", 0)  # .time(timestamp)
            points = [point]
            write_api.write(bucket=bucket, org=org, record=points)
        elif reach_out == "1":
            point = Point(measurement).tag("client_id", client_id).tag("type", ctype).tag("topic", topic).tag("msg",msg).tag("qos",qos).tag(
                "sender_id", sender_id).tag("receiver_id", receiver_id).tag("reach_out", reach_out).field("field", 0)  # .time(timestamp)
            points = [point]
            write_api.write(bucket=bucket, org=org, record=points)
    elif measurement == "retained_msg":
        point = Point(measurement).tag("sender_id", sender_id).tag("sender_state", sender_state).tag("qos", qos).tag("receiver_id", receiver_id).tag("receiver_state", receiver_state).field("field", 0)  # .time(timestamp)
        points = [point]
        write_api.write(bucket=bucket, org=org, record=points)
    elif measurement == "authen":
        point = Point(measurement).tag("client_id", client_id).tag("username", user_name).tag("password", password).tag("clean_session", clean_session).tag("qos", qos).tag("topic", topic).tag("msg", msg).tag("status", status).tag("ban", ban).field("field", 0)  # .time(timestamp)
        points = [point]
        write_api.write(bucket=bucket, org=org, record=points)
    elif measurement == "banned":
        point = Point(measurement).tag("client_id", client_id).tag("username", user_name).tag("password", password).tag("clean_session", clean_session).tag("qos", qos).tag("topic", topic).tag("msg", msg).tag("status", status).tag("ban", ban).field("field", 0)  # .time(timestamp)
        points = [point]
        write_api.write(bucket=bucket, org=org, record=points)
    client.close()


def get_influxDB_point(measurement, query):

    client = InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()
    # query = 'SELECT * FROM "clients_data"'  # (-10m) the last hour
    tables = query_api.query(query, org="org")
    list_m = []

    try:
        for table in tables:
            for record in table.records:
                msg = {}
                # start_time = record.get_start()
                # stop_time = record.get_stop()

                # time_value = record.get_time()
                # field_value = record.get_value()
                # measurement_name = record.get_measurement()

                # Point(measurement).tag("client_id", client_id).tag("type", ctype).field("topic", topic).field("subscribe",subscribe).field("subs_num", subs_num)  # .time(timestamp)
                if measurement == "clients_data_sp":
                    tag_client_id = record.values['client_id']
                    tag_type = record.values['type']

                    # field_topic = record.values['topic']
                    field_connect = record.values['connect']
                    field_remain = record.values['remain']
                    field_time = record.values['_time']

                    # print(f"arrival value : {tag}")

                    msg['client_id'] = tag_client_id
                    msg['type'] = tag_type
                    msg['connect'] = field_connect
                    msg['remain'] = field_remain
                    msg['_time'] = field_time
                elif measurement == "topics_data":
                    tag_client_id = record.values['topic']
                    tag_type = record.values['client_id']

                    # field_topic = record.values['topic']
                    field_connect = record.values['qos']
                    field_remain = record.values['subs_num']
                    field_time = record.values['_time']

                    # print(f"arrival value : {tag}")

                    msg['topic'] = tag_client_id
                    msg['client_id'] = tag_type
                    msg['qos'] = field_connect
                    msg['subs_num'] = field_remain
                    msg['_time'] = field_time
                elif measurement == "sub_data":
                    tag_client_id = record.values['client_id']
                    tag_type = record.values['type']

                    # field_topic = record.values['topic']
                    field_connect = record.values['topic']
                    field_remain = record.values['subscribe']
                    field_time = record.values['_time']

                    # print(f"arrival value : {tag}")

                    msg['client_id'] = tag_client_id
                    msg['type'] = tag_type
                    msg['topic'] = field_connect
                    msg['subscribe'] = field_remain
                    msg['_time'] = field_time
                elif measurement == "msg_data":


                    tag_client_id = record.values['client_id']
                    tag_type = record.values['type']

                    field_topic = record.values['topic']
                    field_msg = record.values['msg']
                    sender_id = record.values['sender_id']
                    reach_out = record.values['reach_out']
                    if reach_out == "1":
                        receiver_id = record.values['receiver_id']
                    elif reach_out == "0":
                        receiver_id = "just sending.."
                    qos = record.values['qos']


                    field_time = record.values['_time']

                    # print(f"arrival value : {tag}")

                    msg['client_id'] = tag_client_id
                    msg['type'] = tag_type

                    msg['topic'] = field_topic
                    msg['msg'] = field_msg
                    msg['sender_id'] = sender_id
                    msg['receiver_id'] = receiver_id
                    msg['qos'] = qos
                    msg['reach_out'] = reach_out

                    msg['_time'] = field_time
                elif measurement == "retained_msg":

                    sender_id = record.values['sender_id']
                    sender_state = record.values['sender_state']
                    qos = record.values['qos']
                    receiver_id = record.values['receiver_id']
                    receiver_state = record.values['receiver_state']
                    field_time = record.values['_time']

                    # print(f"arrival value : {tag}")

                    msg['sender_id'] = sender_id
                    msg['sender_state'] = sender_state
                    msg['qos'] = qos
                    msg['receiver_id'] = receiver_id
                    msg['receiver_state'] = receiver_state
                    msg['_time'] = field_time
                elif measurement == "authen":
                    client_id = record.values['client_id']
                    username = record.values['username']
                    password = record.values['password']
                    clean_session = record.values['clean_session']
                    qos = record.values['qos']
                    topic = record.values['topic']
                    message = record.values['msg']
                    status = record.values['status']
                    ban = record.values['ban']

                    field_time = record.values['_time']

                    # print(f"arrival value : {tag}")

                    msg['client_id'] = client_id
                    msg['username'] = username
                    msg['password'] = password
                    msg['clean_session'] = clean_session
                    msg['qos'] = qos
                    msg['topic'] = topic
                    msg['msg'] = message
                    msg['status'] = status
                    msg['ban'] = ban
                    msg['_time'] = field_time
                elif measurement == "banned":
                    client_id = record.values['client_id']
                    username = record.values['username']
                    password = record.values['password']
                    clean_session = record.values['clean_session']
                    qos = record.values['qos']
                    topic = record.values['topic']
                    message = record.values['msg']
                    status = record.values['status']
                    ban = record.values['ban']

                    field_time = record.values['_time']

                    # print(f"arrival value : {tag}")

                    msg['client_id'] = client_id
                    msg['username'] = username
                    msg['password'] = password
                    msg['clean_session'] = clean_session
                    msg['qos'] = qos
                    msg['topic'] = topic
                    msg['msg'] = message
                    msg['status'] = status
                    msg['ban'] = ban
                    msg['_time'] = field_time
                # msg.append(f"measurement_name: {measurement_name}\n\tstart_time: {start_time}\nstop_time: {stop_time}\ntime_value: {time_value}\ntag_client_id: {tag_client_id}\ntag_type: {tag_type}\nfield_topic: {field_topic}\nfield_subscribe: {field_subscribe}\nfield_subs_num: {field_subs_num}\n")
                ######################print("|||||||||||||||||influxDB content|||||||||||||||||||||||||||")
                #######################print(f"measurement_name: {measurement_name}\n\tstart_time: {start_time}\n\tstop_time: {stop_time}\n\ttime_value: {time_value}\n\ttag_client_id: {tag_client_id}\n\ttag_type: {tag_type}\n\tfield_connect: {field_connect}\n\tfield_remain: {field_remain}\n")
                # print(f"measurement_name: {measurement_name}\n\tstart_time: {start_time}\nstop_time: {stop_time}\ntime_value: {time_value}\ntag_client_id: {tag_client_id}\ntag_type: {tag_type}\nfield_topic: {field_topic}\nfield_subscribe: {field_subscribe}\nfield_subs_num: {field_subs_num}\n")
                # print("||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
                # print(stop_time)
                # print(time_value)
                # print(field_value)
                # print(measurement_name)
                # print(tag_values)
                # print(record.values)
                list_m.append(msg)

    except Exception as e:
        print(f"An error occurred: {e}")
    client.close()

    return list_m


def delete_influxDB_point_client_id(measurement, client_id):
    client = InfluxDBClient(url=url, token=token, org=org)

    query_api = client.query_api()
    delete_api = client.delete_api()

    query = f'''
            from(bucket: "{bucket}")
          |> range(start: -100000h)
          |> filter(fn: (r) => r["_measurement"] == "{measurement}")
           '''

    tables = query_api.query(query, org="org")


    try:
        for table in tables:
            for record in table.records:

                tag_client_id = record.values['client_id']
                if tag_client_id == client_id:

                    field_start = record.values['_start']
                    field_start = field_start.strftime('%Y-%m-%dT%H:%M:%SZ')

                    field_stop = record.values['_stop']
                    field_stop = field_stop.strftime('%Y-%m-%dT%H:%M:%SZ')

                    field_time = record.values['_time']
                    field_time = field_time.strftime('%Y-%m-%dT%H:%M:%SZ')

                    predicate = f'_measurement="{measurement}" AND client_id="{client_id}"'

                    delete_api.delete(field_start, field_stop, predicate, bucket=bucket, org=org)



    except Exception as e:
        print(f"An error occurred: {e}")
    client.close()

