import paho.mqtt.client as mqtt
import psycopg2

def on_message(client, userdata, message):
    payload = message.payload.decode()
    print("----------------------------------------------------------------")
    print("Received message:", payload)

    data_fields = payload.split(', ')
    #print("Data fields:", data_fields)
    if len(data_fields) == 5:
        date = data_fields[0].split(': ')[1]
        time = data_fields[1].split(': ')[1]
        temperature = float(data_fields[2].split(': ')[1].replace('°C', ''))
        humidity = float(data_fields[3].split(': ')[1].replace('%', ''))
        relay_state = int(data_fields[4].split(': ')[1])

        conn = psycopg2.connect(
            database="rpi_data",
            user="postgres",
            host="16.171.195.147",
            port="5432"
        )

        cur = conn.cursor()
        
        try:
            cur.execute("INSERT INTO sensor_data (date, time, temperature, humidity, relay_state) VALUES (%s, %s, %s, %s, %s)",
                        (date, time, temperature, humidity, relay_state))
            conn.commit()
            print("Data stored successfully in PostgreSQL")
        except Exception as e:
            print("Error storing data in PostgreSQL:", e)
        finally:
            cur.close()
            conn.close()

    else:
        print("Invalid data format")

    

mqtt_broker_address = "test.mosquitto.org"
mqtt_port = 1883 
mqtt_topic = "sensor/data"

mqtt_client = mqtt.Client()

mqtt_client.on_message = on_message

mqtt_client.connect(mqtt_broker_address, mqtt_port)
mqtt_client.subscribe(mqtt_topic)

mqtt_client.loop_forever()

#username=pksm
#password=pksm
#database=dht_data
#table=sensor_data
