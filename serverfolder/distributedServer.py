import zmq
import sys
import hashlib 


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
socketProxi.close()
print(response.decode('ascii'))

def deco(data, format='ascii'):
	return data.decode(format)
def enco(data, format='ascii'):
	return data.encode(format)
def getHash(file):
	return hashlib.sha256(file).hexdigest()

def upload(file):
	name = getHash(file)
	newfile = open("files/"+name, "wb")
	newfile.write(file)
	newfile.close()
	socket.send(enco(name))

def download(filename):
	pass

def listing():
	pass

while True:
	request = socket.recv_multipart()
	requestAction = deco(request[0])
	if requestAction == 'upload':
		upload(request[1])
	elif requestAction == 'download':
		download(deco(request[1]))
	elif requestAction == 'list':
		listing()