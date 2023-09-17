import faust
import json
import snowflake.connector as sf

app = faust.App(
    'consume_and_store',
    broker='kafka://localhost:9092',
    value_serializer='raw',
)

user=""
password=""
account=""
conn=sf.connect(user=user,password=password,account=account,autocommit=False)

cursor = conn.cursor()

statement_1 = 'use warehouse COMPUTE_WH'
cursor.execute(statement_1)
statement2 = "alter warehouse COMPUTE_WH resume IF SUSPENDED"
cursor.execute(statement2)
statement3 = "use database CAR_DATABASE"
cursor.execute(statement3)
statement4 = "use role ACCOUNTADMIN"
cursor.execute(statement4)
statement5 = "use schema PUBLIC"
cursor.execute(statement5)

car_speed_topic = app.topic('car_speed')

@app.agent(car_speed_topic)
async def read_and_store(carspeed_stream_data):
    async for data in carspeed_stream_data:
        captured_event = json.loads(data)
        cursor.execute("""
        INSERT INTO car_speed_data (car_id, car_name,car_speed,capture_time) 
        VALUES(%s,%s,%s,%s)""", (captured_event["car_id"], captured_event["car_name"],captured_event["car_speed"],
                           captured_event["capture_time"]))
        conn.commit()

# Start the Faust App, which will block
app.main()

# close up the DB  connections on shutdown
cursor.close()
conn.close()