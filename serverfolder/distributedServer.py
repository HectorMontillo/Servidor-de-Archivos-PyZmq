import zmq
import sys


# ip capacity proxiadd
ip = sys.argv[1].split(':')
capacity = sys.argv[2]
proxiAddress = sys.argv[3]


address = "{}:{}".format(ip[0],ip[1])

# Socket for clients
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:{}".format(ip[1]))


# Socket for proxi
socketProxi = context.socket(zmq.REQ)
socketProxi.connect("tcp://{}".format(proxiAddress))
socketProxi.send_multipart([b"addserver", capacity.encode('ascii'), address.encode('ascii')])
response = socketProxi.recv()
print(response.decode('ascii'))


while True:
    pass