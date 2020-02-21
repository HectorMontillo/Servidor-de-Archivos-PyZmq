import zmq

context = zmq.Context()

# socket
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5556")
print("Proxi has been initiated!! waiting for request!!")

# multipart structure
'''
addserver capacidad direccion
upload parts [hashes]
download filename
list
'''

# db structure
'''
{
    "ip:port": 10 #10 parts max capacity
}
'''
serverDB = dict()


def addServer(request):
    capacity = request[1].decode('ascii')
    address = request[2].decode('ascii')
    serverDB[address] = capacity
    print("addServer: {}, {}".format(address,capacity))

def upload(request):
    pass

def download(request):
    pass

def listing():
    pass

while True:
    request = socket.recv_multipart()
    requestAction = request[0].decode('ascii')
    if requestAction == 'addserver':
        addServer(request)
    elif requestAction == 'upload':
        upload(request)
    elif requestAction == 'download':
        download(request)
    elif requestAction == 'list':
        listing()


