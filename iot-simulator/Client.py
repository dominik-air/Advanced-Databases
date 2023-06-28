import socket
import threading
import pandas as pd          
 
class SensorConnection(threading.Thread):
    def __init__(self, port, ip_address='localhost') -> None:
        threading.Thread.__init__(self)
        self.port = port
        self.ip_address = ip_address
        self.sensor_socket = socket.socket()
        self.buffer = []
    
    def run(self):
        try:
            self.sensor_socket.connect((self.ip_address, self.port))
            while True:
                self.buffer.append(self.sensor_socket.recv(1024).decode())
        except:
            self.sensor_socket.close()

def collect_data_from_sensors(sensors: list[SensorConnection]):
    for sensor in sensors:
        sensor.start()
    
    while True:
        for sensor in sensors:
            if len(sensor.buffer) > 0:
                print(sensor.buffer.pop(0))

if __name__ == '__main__':
    ports = pd.read_csv("iot_sensor_grid.csv").port.values
    sensors = [SensorConnection(port=port) for port in ports]
    collect_data_from_sensors(sensors)
