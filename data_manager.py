import json
import sqlite3
from queue import Queue
import threading

class DataManager:
    SENSOR_ACTIONS = {
        "dht": {
            "data_topic": "/RG_7557_GreenHouse/Sensors/DHT/Data",
            "control_topic": "/RG_7557_GreenHouse/Sensors/DHT/Control",
            "data_keys": [
                {
                    "data_key": "Temperature",
                    "threshold": 28.0,
                    "above_action": {
                        "device": "fan", 
                        "state": "ON", 
                        "topic": "/RG_7557_GreenHouse/Devices/Fan/Control"
                    },
                    "below_action": None
                },
                {
                    "data_key": "Humidity",
                    "threshold": 60.0,
                    "above_action": {
                        "device": "dehumidifier", 
                        "state": "ON", 
                        "topic": "/RG_7557_GreenHouse/Devices/Dehumidifier/Control"
                    },
                    "below_action": None
                }
            ]
        },
        "light": {
            "data_topic": "/RG_7557_GreenHouse/Sensors/Light/Data",
            "control_topic": "/RG_7557_GreenHouse/Sensors/Light/Control",
            "data_keys": [
                {
                    "data_key": "Light",
                    "threshold": 450,
                    "above_action": None,
                    "below_action": {
                        "device": "light", 
                        "state": "ON", 
                        "topic": "/RG_7557_GreenHouse/Devices/Light/Control"
                    }
                }
            ]
        },
        "moisture": {
            "data_topic": "/RG_7557_GreenHouse/Sensors/Moisture/Data",
            "control_topic": "/RG_7557_GreenHouse/Sensors/Moisture/Control",
            "data_keys": [
                {
                    "data_key": "Moisture",
                    "threshold": 30.0,
                    "above_action": None,
                    "below_action": {
                        "device": "irrigation_system", 
                        "state": "ON", 
                        "topic": "/RG_7557_GreenHouse/Devices/IrrigationSystem/Control"
                    }
                }
            ]
        },
        "device_control_topics": {
            "fan": "/RG_7557_GreenHouse/Devices/Fan/Control",
            "dehumidifier": "/RG_7557_GreenHouse/Devices/Dehumidifier/Control",
            "light": "/RG_7557_GreenHouse/Devices/Light/Control",
            "irrigation_system": "/RG_7557_GreenHouse/Devices/IrrigationSystem/Control",
        }
    }

    def __init__(self, db_path=":memory:", mqtt_client=None):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.create_tables()
        self.mqtt_client = mqtt_client

        self.control_topics = set()
        self.sensor_control_topics = set()
        for sensor_info in self.SENSOR_ACTIONS.values():
            self.sensor_control_topics.add(sensor_info.get('control_topic'))
            for data_key_info in sensor_info.get('data_keys', []):
                if data_key_info.get('above_action'):
                    self.control_topics.add(data_key_info['above_action']['topic'])
                if data_key_info.get('below_action'):
                    self.control_topics.add(data_key_info['below_action']['topic'])
        self.db_queue = Queue()
        self.db_thread = threading.Thread(target=self._db_thread_func)
        self.db_thread.start()

    

    def create_tables(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY,
                    topic TEXT,
                    payload TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def save_message(self, topic, payload):
        self.db_queue.put(('save_message', topic, payload))

    def get_messages(self):
        result_queue = Queue()
        self.db_queue.put(('get_messages', result_queue))
        return result_queue.get()

    def _db_thread_func(self):
        conn = sqlite3.connect(self.db_path)
        while True:
            task = self.db_queue.get()
            if task[0] == 'save_message':
                topic, payload = task[1], task[2]
                with conn:
                    conn.execute("""
                        INSERT INTO messages (topic, payload) VALUES (?, ?)
                    """, (topic, payload))
            elif task[0] == 'get_messages':
                result_queue = task[1]
                with conn:
                    cursor = conn.execute("SELECT * FROM messages")
                    result_queue.put(cursor.fetchall())

    def send_control_message(self, sensor_type, command):
        sensor_info = self.SENSOR_ACTIONS.get(sensor_type)

        if sensor_info is None:
            print(f"Error: No information defined for sensor type '{sensor_type}'")
            return
        topic = sensor_info.get("control_topic")
        if topic is None:
            print(f"Error: No control topic defined for sensor type '{sensor_type}'")
            return
        message = json.dumps({"sensor": sensor_type, "command": command})
        self.mqtt_client.publish_to(topic, message)  

    def process_sensor_data(self, topic, message_data):
        sensor_handled = False
        for sensor_name, sensor_info in self.SENSOR_ACTIONS.items():
            if sensor_name == 'device_control_topics':
                continue

            if sensor_info.get('data_topic') == topic:
                for data_key_info in sensor_info.get('data_keys', []):
                    sensor_value = message_data.get(data_key_info['data_key'])
                    if sensor_value is None:
                        continue

                    sensor_handled = True

                    action = None
                    if sensor_value > data_key_info["threshold"]:
                        action = data_key_info["above_action"]
                        print(f"Sensor value above threshold for {sensor_name}")
                    elif sensor_value < data_key_info["threshold"]:
                        action = data_key_info["below_action"]
                        print(f"Sensor value below threshold for {sensor_name}")

                    if action:
                        control_topic = action["topic"]
                        message = json.dumps({"device": action["device"], "state": action["state"]})
                        self.mqtt_client.publish_to(control_topic, message)

        if not sensor_handled:
            if topic not in self.sensor_control_topics:
                print("Unknown sensor topic: " + topic)