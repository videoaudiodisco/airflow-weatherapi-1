
from airflow import DAG
from datetime import timedelta, datetime, timezone
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
apikey = os.getenv('WEATHER_API_KEY')
awskey = os.getenv('AWS_KEY')
awssecret = os.getenv('AWS_SECRET')
awstoken = os.getenv('AWS_TOKEN')


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32

    return temp_in_fahrenheit

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]

    # original code; utcfromtimestamp is deprecated
    # time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    # sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    # sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    # Create UTC datetime from timestamp
    dt_utc = datetime.fromtimestamp(data['dt'], tz=timezone.utc)
    tz_offset = timezone(timedelta(seconds=data['timezone']))     # Apply the timezone offset
    time_of_record = dt_utc.astimezone(tz_offset)

    sunrise_utc = datetime.fromtimestamp(data['sys']['sunrise'], tz=timezone.utc)
    sunrise_time = sunrise_utc.astimezone(tz_offset)
    sunset_utc = datetime.fromtimestamp(data['sys']['sunset'], tz=timezone.utc)
    sunset_time = sunset_utc.astimezone(tz_offset)


    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    aws_credentials = {"key": f"{awskey}", "secret": f"{awssecret}", "token": f"{awstoken}"}

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_portland_' + dt_string
    # df_data.to_csv(f"{dt_string}.csv", index=False)
    df_data.to_csv(f"s3://weatherapi-airflow-gypark-bucket-yml/{dt_string}.csv", index=False, storage_options=aws_credentials)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}




with DAG('weather_dag',
        default_args=default_args,
        schedule = '@daily',
        catchup=False) as dag:
    
        # this is a sensor task that checks if the weather API is ready
        is_weather_api_ready = HttpSensor(
            task_id ='is_weather_api_ready',
            http_conn_id='weathermap_api',
            endpoint=f"/data/2.5/weather?q=Seoul&APPID={apikey}"
        )

        extract_weather_data = HttpOperator(
            task_id='extract_weather_data',
            http_conn_id='weathermap_api',
            endpoint=f"/data/2.5/weather?q=Seoul&APPID={apikey}",
            method='GET',
            # 응답 개체 후처리, 아무것도 안하면 기본적으로 r.text (응답 본문 문자열) 반환
            # 하지만 repsonse_filter 지정하면, 그 함수의 return 값이 xcom 으로 전달
            response_filter=lambda r:json.loads(r.text), # 전달될때 문자열 json을 python dict로 변환
            log_response=True,
        )

        transform_weather_data = PythonOperator(
            task_id='transform_weather_data',
            python_callable=transform_load_data
        )

        is_weather_api_ready >> extract_weather_data >> transform_weather_data