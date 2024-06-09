from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime

API_key = 'e840441f7859ab48c7deabf030f819ab' #API key
lat = 10.82302  #latitude of Ho Chi Minh city
lon = 106.62965  #longtitude of Ho Chi Minh city
url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}'
REGION = Dataset("D:\\Airflow\\region_file.txt")
WEATHER = Dataset("D:\\Airflow\\weather_file.txt")

def get_data(url):
    import requests
    r = requests.get(url)
    return r.json()

def write_region(response):
    search_time = datetime.now()
    name = response['name']
    country = response['sys']['country']
    coordinate = {'lon':response['coord']['lon'], 'lat':response['coord']['lat']}
    message = f'Time of search: {search_time}\nName: {name}, Country: {country}, Coordinate: {coordinate}\n'
    f = open('D:\\Airflow\\region_file.txt','a')
    f.write(message)
    f.close()

def write_weather(response):
    time = datetime.now()
    weather = response['weather'][0]['main']
    temp = response['main']['temp']
    temp_min = response['main']['temp_min']
    temp_max = response['main']['temp_max']
    wind_speed = response['wind']['speed']
    message1 = f'Time: {time}\nWeather: {weather}\nTemperature: {temp}K\nWind speed: {wind_speed}m/s\n'
    message2 = f'Minimum temperature: {temp_min}K\nMaximum temperature: {temp_max}K\n'
    f = open('D:\\Airflow\\weather_file.txt','a')
    f.write(message1 + message2)
    f.close()

default_arg = {
    'owner':'Thuan'
}
dag1 = DAG(
    dag_id = 'Write_weather_data',
    default_args = default_arg,
    start_date = datetime(2024,6,3),
    schedule_interval = None,
    catchup = True,
    render_template_as_native_obj = True   
)
task1 = PythonOperator(
    task_id = 'get_data',
    python_callable = get_data,
    op_kwargs = {'url':url},
    dag = dag1
)
task2 = PythonOperator(
    task_id = 'write_region_data',
    python_callable = write_region,
    op_kwargs = {'response':"{{ ti.xcom_pull(task_ids='get_data')}}"},
    dag = dag1,
    outlets = [REGION]
)
task3 = PythonOperator(
    task_id = 'write_weather_data',
    python_callable = write_weather,
    op_kwargs = {'response':"{{ti.xcom_pull(task_ids='get_data')}}"},
    dag = dag1,
    outlets = [WEATHER]
)
task1 >> [task2,task3]

def read_data():
    data = []
    for filename in ('region_file','weather_file'):
        with open(f"D:\\Airflow\\{filename}.txt") as f:
            contents = f.readlines()
            data.append(contents)
    return [cell for sublist in data for cell in sublist]

dag2 = DAG(
    dag_id = 'Read_weather_data',
    start_date = datetime(2024,6,6),
    schedule = [REGION, WEATHER],
    catchup = False
)
task4 = PythonOperator(
    task_id = 'read_data',
    python_callable = read_data,
    dag = dag2
)