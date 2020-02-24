import zmq
import sys


# ip port capacity proxiadd
ip = sys.argv[1]
port = sys.argv[2]
capacity = sys.argv[3]
proxiAddress = sys.argv[4]
address = "{}:{}".format(ip,port)

# Socket for clients
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:{}".format(port))


# Socket for proxi
socketProxi = context.socket(zmq.REQ)
socketProxi.connect("tcp://{}".format(proxiAddress))
socketProxi.send_multipart([b"addserver", capacity.encode('ascii'), address.encode('ascii')])
socketProxi.recv()


while True:
    pass