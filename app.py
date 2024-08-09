import ast

from flask import Flask, render_template, request, redirect, url_for, session, jsonify, g
# from tasks import transcribe_audio
from celery_config import create_app, celery
from influxDB.influx_db import get_influxDB_point
import base64
import os
from influxDB.influx_db import set_influxDB_point, delete_influxDB_point_client_id
from tasks import fetch_clients_data
import tasks

import re
import json
from datetime import datetime
from dateutil import tz
# import datetime

bucket = "BROKER2"



app = create_app()

@app.route('/index.html')
def index():
    return render_template('index.html')

@app.route('/clients')
def clients():
    time_range = request.args.get('time_range', '24h')
    measurement = "clients_data_sp"
    query = f'''
        from(bucket: "{bucket}")
      |> range(start: -{time_range})
      |> filter(fn: (r) => r["_measurement"] == "{measurement}")
      |> keep(columns: ["_time", "_value", "_measurement", "client_id", "type", "connect", "remain"])
        '''
    # |> sort(columns: ["_time"], desc: true)
    clients_data_strings = get_influxDB_point(measurement, query)

    # task = celery.send_task('tasks.fetch_clients_data', args=[time_range])
    # clients_data_strings = task.get(timeout=10)

    # print(clients_data_strings)
    clients_data = []
    for client_ in clients_data_strings:
        client_id = client_["client_id"]
        # print(client_id)
        type = client_["type"]
        connect = client_["connect"]
        remain = client_["remain"]

        dt = client_["_time"]
        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')

        clients_data.append(
        {
            'Client_ID': client_id,
            'User_Name': '........',
            'Status': connect,
            'type': type,
            'Keepalive': '60',
            'Clean_Start': 'true',
            'remain': remain,
            'Connected_at': formatted_date
        })


    return render_template('clients.html', clients=clients_data, selected_time_range=time_range)


@app.route('/check_updates')
def check_updates():
    time_range = request.args.get('time_range', '24h')
    measurement = "clients_data_sp"
    query = f'''
        from(bucket: "{bucket}")
      |> range(start: -{time_range})
      |> filter(fn: (r) => r["_measurement"] == "{measurement}")
      |> keep(columns: ["_time", "_value", "_measurement", "client_id", "type", "connect", "remain"])
        '''
    clients_data_strings = get_influxDB_point(measurement, query)

    clients_data = []
    for client_ in clients_data_strings:
        client_id = client_["client_id"]
        type = client_["type"]
        connect = client_["connect"]
        remain = client_["remain"]

        dt = client_["_time"]
        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')

        clients_data.append(
        {
            'Client_ID': client_id,
            'User_Name': '........',
            'Status': connect,
            'type': type,
            'Keepalive': '60',
            'Clean_Start': 'true',
            'remain': remain,
            'Connected_at': formatted_date
        })

    return jsonify(clients_data)

@app.route('/Subscriptions')
def Subscriptions():
    time_range = request.args.get('time_range', '24h')
    measurement = "sub_data"
    query = f'''
            from(bucket: "{bucket}")
          |> range(start: -{time_range})
          |> filter(fn: (r) => r["_measurement"] == "{measurement}")
          '''
    # |> sort(columns: ["_time"], desc: true)
    clients_data_strings = get_influxDB_point(measurement, query)

    # print(clients_data_strings)
    clients_data = []
    for client_ in clients_data_strings:
        tag_client_id = client_['client_id']
        tag_type = client_['type']
        topic = client_['topic']
        sub = client_['subscribe']

        dt = client_["_time"]
        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')


        clients_data.append(
        {
            'Client_ID': tag_client_id,
            'type': tag_type,
            'Topic': topic,
            'sub': sub,
            'Created_at': formatted_date,
        }

        # Add more client data here
    )


    return render_template('subscriptions.html', clients=clients_data, selected_time_range=time_range)

@app.route('/topics')
def topics():
    time_range = request.args.get('time_range', '24h')
    measurement = "topics_data"
    query = f'''
                from(bucket: "{bucket}")
              |> range(start: -{time_range})
              |> filter(fn: (r) => r["_measurement"] == "{measurement}")
              '''
    # |> sort(columns: ["_time"], desc: true)
    clients_data_strings = get_influxDB_point(measurement, query)

    # print(clients_data_strings)
    clients_data = []
    for client_ in clients_data_strings:
        topic = client_['topic']
        client_id = client_['client_id']

        # field_topic = record.values['topic']
        qos = client_['qos']
        subs_num = client_['subs_num']

        dt = client_["_time"]
        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')

        clients_data.append(
        {
            'Topic': topic,
            'client_id': client_id,
            'qos': qos,
            "subs_num": subs_num,
            'created_at': formatted_date
        }

        # Add more client data here
        )

    return render_template('topics.html', clients=clients_data, selected_time_range=time_range)

@app.route('/Retained_msg')
def Retained_msg():

    time_range = request.args.get('time_range', '24h')
    measurement = "msg_data"
    query = f'''
            from(bucket: "{bucket}")
          |> range(start: -{time_range})
          |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            '''
    # |> sort(columns: ["_time"], desc: true)
    clients_data_strings = get_influxDB_point(measurement, query)

    # print(clients_data_strings)
    clients_data = []
    for client_ in clients_data_strings:
        client_id = client_["client_id"]
        ctype = client_["type"]
        topic = client_["topic"]
        msg = client_["msg"]
        sender_id = client_["sender_id"]
        receiver_id = client_["receiver_id"]
        qos = client_["qos"]
        reach_out = client_["reach_out"]

        dt = client_["_time"]
        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')

        clients_data.append(
        {
            'client_ID': client_id,
            'client_type': ctype,
            'topic': topic,
            'msg': msg,
            'sender_id': sender_id,
            'receiver_id': receiver_id,
            'qos': qos,
            'reach_out': reach_out,
            'created_at': formatted_date,
        }

        # Add more client data here
        )


    return render_template('retained_msg.html', clients=clients_data, selected_time_range=time_range)

@app.route('/check_msg_update')
def check_msg_update():

    time_range = request.args.get('time_range', '24h')
    measurement = "msg_data"
    query = f'''
            from(bucket: "{bucket}")
          |> range(start: -{time_range})
          |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            '''
    # |> sort(columns: ["_time"], desc: true)
    clients_data_strings = get_influxDB_point(measurement, query)

    # print(clients_data_strings)
    clients_data = []
    for client_ in clients_data_strings:
        client_id = client_["client_id"]
        ctype = client_["type"]
        topic = client_["topic"]
        msg = client_["msg"]
        sender_id = client_["sender_id"]
        receiver_id = client_["receiver_id"]
        qos = client_["qos"]
        reach_out = client_["reach_out"]

        dt = client_["_time"]
        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')

        clients_data.append(
        {
            'client_ID': client_id,
            'client_type': ctype,
            'topic': topic,
            'msg': msg,
            'sender_id': sender_id,
            'receiver_id': receiver_id,
            'qos': qos,
            'reach_out': reach_out,
            'created_at': formatted_date,
        }

        # Add more client data here
        )

    print(clients_data)
    return jsonify(clients_data)


@app.route('/Delayed_pub')
def Delayed_pub():
    time_range = request.args.get('time_range', '24h')
    measurement = "retained_msg"
    query = f'''
                from(bucket: "{bucket}")
              |> range(start: -{time_range})
              |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                '''
    # |> sort(columns: ["_time"], desc: true)
    clients_data_strings = get_influxDB_point(measurement, query)

    # print(clients_data_strings)
    clients_data = []
    for client_ in clients_data_strings:
        sender_id = client_['sender_id']
        sender_state = client_['sender_state']
        qos = client_['qos']
        receiver_id = client_['receiver_id']
        receiver_state = client_['receiver_state']

        dt = client_['_time']
        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')


        clients_data.append(
        {
            'sender_id': sender_id,
            'sender_state': sender_state,
            'qos': qos,
            'receiver_id': receiver_id,
            'receiver_state': receiver_state,
            'created_at': formatted_date,
        }

        # Add more client data here
        )


    return render_template('delayed_pub.html', clients=clients_data, selected_time_range=time_range)

clients_data_auth = []

@app.route('/authentication')
def authentication():
    time_range = request.args.get('time_range', '24h')
    measurement = "authen"
    query = f'''
                    from(bucket: "{bucket}")
                  |> range(start: -{time_range})
                  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                  |> sort(columns: ["_time"], desc: true)
                    '''
    clients_data_strings = get_influxDB_point(measurement, query)

    clients_data_auth.clear()  # Clear existing data
    for client_ in clients_data_strings:
        client_id = client_['client_id']
        username = client_['username']
        password = client_['password']
        clean_session = client_['clean_session']
        qos = client_['qos']
        topic = client_['topic']
        message = client_['msg']
        status = client_['status']
        ban = client_['ban']
        dt = client_['_time']
        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')

        session_status_key = f'client_{client_id}_status'
        session_ban_key = f'client_{client_id}_ban'

        session_status = session.get(session_status_key, status)
        session_ban = session.get(session_ban_key, ban)

        clients_data_auth.append(
        {
            'client_id': client_id,
            'username': username,
            'password': password,
            'clean_session': clean_session,
            'qos': qos,
            'topic': topic,
            'message': message,
            'status': session_status,
            'ban': session_ban,
            'formatted_date': formatted_date,
        }
        )

    return render_template('authentication.html', clients=clients_data_auth, selected_time_range=time_range, csp_nonce=g.csp_nonce)


clients_data_banC = [
    {
        'Value': "value_1",
        'Property': 'propertyA',
        'Reason': "normal",
        'Expire_At': '2024-06-12 12:00:00',
        'Action': ""
    },
    {
        'Value': "value_2",
        'Property': 'propertyB',
        'Reason': "critical",
        'Expire_At': '2024-06-13 13:00:00',
        'Action': ""
    }
]
@app.route('/banned_clients')
def banned_clients():
    time_range = request.args.get('time_range', '24h')
    measurement = "authen"
    query = f'''
                        from(bucket: "{bucket}")
                      |> range(start: -{time_range})
                      |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                      |> sort(columns: ["_time"], desc: true)
                        '''
    clients_data_strings = get_influxDB_point(measurement, query)

    clients_data_auth.clear()  # Clear existing data
    for client_ in clients_data_strings:
        ban = client_['ban']

        client_id = client_['client_id']
        username = client_['username']
        password = client_['password']
        clean_session = client_['clean_session']
        qos = client_['qos']
        topic = client_['topic']
        message = client_['msg']
        status = client_['status']
        dt = client_['_time']
        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')

        session_status_key = f'client_{client_id}_status'
        session_ban_key = f'client_{client_id}_ban'

        session_status = session.get(session_status_key, status)
        session_ban = session.get(session_ban_key, ban)
        if session_ban == "ban":
            set_influxDB_point(measurement="banned", client_id=f"{client_id}",
                               clean_session=f"{client_["clean_session"]}",
                               qos=f"{client_["qos"]}", topic=f"{client_["topic"]}", msg=f"{client_["msg"]}",
                               password=f"{client_["password"]}",
                               user_name=f"{client_["username"]}", status=f"{session_status}", ban=f"{session_ban}")
            clients_data_auth.append(
                {
                    'client_id': client_id,
                    'username': username,
                    'password': password,
                    'clean_session': clean_session,
                    'qos': qos,
                    'topic': topic,
                    'message': message,
                    'status': session_status,
                    'ban': session_ban,
                    'formatted_date': formatted_date,
                }
            )
    return render_template('banned_clients.html', clients=clients_data_auth, selected_time_range=time_range, csp_nonce=g.csp_nonce)


@app.route('/delete_client_point', methods=['POST'])
def delete_client_point():
    data = request.get_json()
    client_id = data['client_id']

    try:
        delete_influxDB_point_client_id("banned", client_id)
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/remove_client', methods=['POST'])
def remove_client():
    data = request.get_json()
    value = data['value']

    global clients_data_banC
    clients_data_banC = [client for client in clients_data_banC if client['Value'] != value]

    return jsonify({'status': 'success', 'value': value})


@app.before_request
def before_request():
    g.csp_nonce = base64.b64encode(os.urandom(16)).decode('utf-8')

@app.route('/toggle_client', methods=['POST'])
def toggle_client():
    data = request.get_json()
    client_id = data['client_id']
    status = data['status']
    ban = data['ban']

    session_status_key = f'client_{client_id}_status'
    session_ban_key = f'client_{client_id}_ban'

    session[session_status_key] = status
    session[session_ban_key] = ban

    for client in clients_data_auth:
        if client['client_id'] == client_id:
            client['status'] = status
            client['ban'] = ban
            delete_influxDB_point_client_id(measurement="authen", client_id=client_id)
            set_influxDB_point(measurement="authen", client_id=f"{client_id}", clean_session=f"{client["clean_session"]}",
                               qos=f"{client["qos"]}", topic=f"{client["topic"]}", msg=f"{client["message"]}",
                               password=f"{client["password"]}",
                               user_name=f"{client["username"]}", status=f"{status}", ban=f"{ban}")
            print(ban)
            break

    return jsonify({'status': 'success', 'client_id': client_id, 'status': status, 'ban': ban})

@app.route('/data_base')
def data_base():
    clients_data = [
        {
            'Logo': url_for('static', filename='img/logo_1.png'),
            'Name': 'influxDB',
            'Action': url_for('static', filename='img/disable.jpg')
        },
        {
            'Logo': url_for('static', filename='img/logo_2.png'),
            'Name': 'PostgreSQL',
            'Action': url_for('static', filename='img/enable.jpg')
        }
    ]

    return render_template('data_base.html', clients=clients_data)

@app.route('/toggle_database', methods=['POST'])
def toggle_database():
    data = request.get_json()
    name = data['name']
    action = data['action']

    # This part should normally modify the persistent state,
    # but here we modify the clients_data just for demonstration.
    # Ideally, you should fetch and update the data from a database.
    clients_data = [
        {
            'Logo': url_for('static', filename='img/logo_1.png'),
            'Name': 'influxDB',
            'Action': url_for('static', filename='img/disable.jpg')
        },
        {
            'Logo': url_for('static', filename='img/logo_2.png'),
            'Name': 'PostgreSQL',
            'Action': url_for('static', filename='img/enable.jpg')
        }
    ]

    for client in clients_data:
        if client['Name'] == name:
            client['Action'] = url_for('static', filename=f'img/{action}.jpg')
            break

    return jsonify({'status': 'success', 'name': name, 'action': action})

if __name__ == '__main__':
    app.run(debug=True)
