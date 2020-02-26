import zmq
import json

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
# server db
serverDB = json.load(open("serverDB.json", "r"))
filesDB = json.load(open("filesDB.json", "r"))

'''
{
    "ip:port": 10 #10 parts max capacity
}
'''
#Save current information to json DB
def deco(data, format='ascii'):
    return data.decode(format)
def enco(data, format='ascii'):
    return data.encode(format)

def saveJSON(dbname,db):
	with open('{}.json'.format(dbname), 'w') as json_file:
		json.dump(db, json_file)

#Add a new server
def addServer(request):
    capacity = deco(request[1])
    address = deco(request[2])
    if not address in serverDB.keys():
        serverDB[address] = {"capacity": capacity, "parts":[]}
        print("addServer: {}, {}".format(address,capacity))
    else:
        print("addServer: {} is already registered")
    saveJSON('serverDB',serverDB)
    socket.send(b"conected")

def calc(val,len):
  return val-len*(val//len)
  
def balanceLoad(filename):
    listparts = filesDB[filename]
    servers = list(serverDB.keys())
    lenServers = len(servers)
    balance = dict()
    for i,part in enumerate(listparts):
       
        ser = servers[calc(i,lenServers)]
        if ser in balance.keys():
            balance[ser].append(part)
        else:
            balance[ser] = list()
            balance[ser].append(part)
        serverDB[ser]["parts"].append(part)
    saveJSON('serverDB',serverDB)   
    return balance


def upload(request):
    filename = deco(request[1])
    filesDB[filename] = list()
    #socket.send_multipart(list(map(enco,serverDB.keys())))
    for i, val in enumerate(request):
        if(i<2): continue
        filesDB[filename].append(deco(val))
    socket.send_json(balanceLoad(filename))
    saveJSON('filesDB', filesDB)

def download(request):
    pass

def listing():
    pass

while True:
    request = socket.recv_multipart()
    requestAction = deco(request[0])
    if requestAction == 'addserver':
        print(request)
        addServer(request)
    elif requestAction == 'upload':
        print(request)
        upload(request)
    elif requestAction == 'download':
        download(request)
    elif requestAction == 'list':
        listing()


