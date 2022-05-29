import socketserver
import threading
import struct
import time
from math import pi, sin, cos, sqrt, atan2
import requests

API_KEY = "6be451092bdc3b3ae40dec863dbc4e35"
Endpoint = "https://api.openweathermap.org/data/2.5/weather"


BAD_CODES_LIST = []
BAD_CODES_LIST.extend(range(200, 233))
BAD_CODES_LIST.extend(range(300, 322))
BAD_CODES_LIST.extend(range(500, 532))
BAD_CODES_LIST.append(616)
BAD_CODES = set(BAD_CODES_LIST)

TIME_LIMIT = 20 # in minuts
R_LIMIT = 1000 # in meters
R=6371000 # Средний радиус Земли
RAD = pi/180.0

# Задаем адрес сервера
SERVER_ADDRESS = ('localhost', 8080)

geo_point = struct.Struct("2f")
lock = threading.Lock()

RainPoints = []

def minindex(arr):
    im,mi=0,arr[0]
    for i,a in enumerate(arr[1:]):
        if a<mi:
            im,mi=i+1,a
    return im

def distances(lat1, lon1, lat2, lon2):
    # Вычисление расстояния по формуле Харвесина
    f1 = lat1 * RAD
    f2 = lat2 * RAD
    df = (lat2-lat1) * RAD
    dl = (lon2-lon1) * RAD
    a = sin(df/2) * sin(df/2) + cos(f1) * cos(f2) * sin(dl/2) * sin(dl/2)
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    d = R * c
    return d 


def RequestRain(lat, lon):
    payload = {'lat': f'{lat:.7f}', 'lon': f'{lon:.7f}', 'appid':API_KEY}
    r = requests.get(Endpoint, params=payload)
    # print(r.url)         
    if r.status_code != requests.codes.ok:
        print(r.status_code)
        return None
    else:
        r_json = r.json()
        weathers = r_json["weather"]
        # print(weathers)
        w_codes = [w["id"] for w in weathers]
        # print(w_codes)
        for code in w_codes:
            if code in BAD_CODES:
                return True
        return False


def CachedReq(lat, lon):
    if not RainPoints:
        lock.release()
        res = RequestRain(lat, lon)
        lock.acquire()
        if res is None:
            return 1
        else:
            RainPoints.append(RainPoint(lat, lon, res)) 
            return 2 if res else 1      
    else:
        dist = []
        near_points = []
        for rainpoint in RainPoints:
            r = rainpoint.limit_dist(lat, lon, R_LIMIT)
            if r is not None:
                dist.append(r)
                near_points.append(rainpoint)
        if not near_points:
            lock.release()
            res = RequestRain(lat, lon)
            lock.acquire()
            if res is None:
                return 1
            else:
                RainPoints.append(RainPoint(lat, lon, res)) 
                return 2 if res else 1
        else:
            nearest_point=near_points[minindex(dist)]
            if nearest_point.is_valid(TIME_LIMIT):
                return 2 if nearest_point.is_rain else 1
            else:
                lock.release()
                res = RequestRain(lat, lon)
                lock.acquire()
                if res is None:
                    return 1
                else:
                    nearest_point.update(lat, lon, res) 
                    return 2 if res else 1

    
class RainPoint:
    def __init__(self, lat, lon, is_rain) -> None:
        self.update(lat, lon, is_rain)
            
    
    def update(self, lat, lon, is_rain):
        self.lat = lat
        self.lon = lon
        self.is_rain = is_rain
        self.time = time.time()
        
    def is_valid(self, T) -> bool:
        cur_time = time.time()
        return cur_time - self.time < 60*T 
    
    def limit_dist(self, lat, lon, R_limit):
        R = distances(self.lat, self.lon, lat, lon)
        if R < R_limit:
            return R
        else: return None


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

class TCPHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        # self.request is the TCP socket connected to the client
        # print("Connect with {}:{} on thread {}".format(self.client_address[0], self.client_address[1], threading.current_thread().name))
        n_points_data = self.request.recv(2)
        n_points = struct.unpack("H", n_points_data)[0]
        data =self.request.recv(n_points*geo_point.size)
        # print(data)
        # print(len(data))
        geo_points = [geo_point.unpack(data[i*geo_point.size:(i+1)*geo_point.size]) for i in range(n_points)]
        # print(geo_points)
        weather_enc = b""
        for point in geo_points:
            lat3 = point[0]
            lon3 = point[1]
            # print(lat3, lon3)
            lock.acquire()
            weather_enc+=struct.pack("B", CachedReq(lat3, lon3))
            lock.release()
        # print(weather_enc)
        self.request.send(weather_enc)

    
if __name__ == "__main__":

    # Create the server
    server = ThreadedTCPServer(SERVER_ADDRESS, TCPHandler)
    with server:
        # Start the server -- that thread will then start one
        # more thread for each request
        server.serve_forever()
