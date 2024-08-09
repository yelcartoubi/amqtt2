from celery_config import celery
from influxDB.influx_db import get_influxDB_point

bucket = "BROKER2"

@celery.task(name='tasks.fetch_clients_data')
def fetch_clients_data(time_range):
    measurement = "clients_data_sp"
    query = f'''
        from(bucket: "{bucket}")
      |> range(start: -{time_range})
      |> filter(fn: (r) => r["_measurement"] == "{measurement}")
      |> keep(columns: ["_time", "_value", "_measurement", "client_id", "type", "connect", "remain"])
    '''
    return get_influxDB_point(measurement, query)
