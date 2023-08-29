import time
import datetime
import board
import adafruit_dht
import RPi.GPIO as GPIO
import psutil
import psycopg2
import paho.mqtt.client as mqtt

for proc in psutil.process_iter():
    if proc.name() == 'libgpiod_pulsein' or proc.name() == 'libgpiod_pulsei':
        proc.kill()

relay_pin = 8

sensor = adafruit_dht.DHT11(board.D4)

GPIO.setmode(GPIO.BCM)
GPIO.setup(8,GPIO.OUT)
current_time = datetime.datetime.now()

mqtt_broker_address = "test.mosquitto.org"
mqtt_port = 1883
mqtt_topic = "sensor/data"

mqtt_client = mqtt.Client()

while True:
    try:
        temp = sensor.temperature
        humidity = sensor.humidity
        date = current_time.strftime('%Y-%m-%d')
        time_stamp = current_time.strftime('%H:%M:%S')
        relay_state = GPIO.input(relay_pin)
       
        mqtt_payload = "Date: {}, Time: {}, Temperature: {}°C, Humidity: {}%, Relay State: {}".format(date, time_stamp, temp, humidity, relay_state)
        result = mqtt_client.connect(mqtt_broker_address, mqtt_port)

        print("---------------------------------------------------------------------------------------")
        print("Date: {}    Time: {}    Temperature: {}*C    Humidity: {}%    Relay State: {}".format(date, time_stamp, temp, humidity, relay_state))
       
        if result == mqtt.MQTT_ERR_SUCCESS:
            print("Connected to MQTT broker")

            publish_result = mqtt_client.publish(mqtt_topic, mqtt_payload)

            if publish_result.rc == mqtt.MQTT_ERR_SUCCESS:
                print("Data has been published to MQTT topic: {}".format(mqtt_topic))
            else:
                print("Failed to publish data to MQTT topic: {} - {}".format(publish_result.rc, mqtt.error_string(publish_result.rc)))

        else:
            print("Failed to connect to MQTT broker: {} - {}".format(result, mqtt.error_string(result)))

        if humidity > 98 :
            GPIO.output(8, GPIO.HIGH)
        else:
            GPIO.output(8, GPIO.LOW)
    except RuntimeError as error:
        print(error.args[0])
        time.sleep(2.0)
        continue
    except Exception as error:
        sensor.exit()
        raise error
    time.sleep(2.0)