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
    listparts = filesDB[filename]['parts']
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
    filesDB[filename] = {
        "hash":deco(request[2]),
        "parts":list()
    }
    for i, val in enumerate(request):
        if(i<3): continue
        filesDB[filename]['parts'].append(deco(val))
    socket.send_json(balanceLoad(filename))
    saveJSON('filesDB', filesDB)

def download(request):
    name = deco(request[1])
    if name in filesDB.keys():
        parts = filesDB[name]['parts']
        hashC = filesDB[name]['hash']
        socket.send_json(find_servers(hashC,parts))
    else:
        socket.send_json({"error":"the file does not exist!"})
def find_servers(hashC,partes):
    download_servers = {
        "hash": hashC,
        "parts": partes,
        "server": []
    }
    for h in partes:
        for ser in serverDB.keys():
            if h in serverDB[ser]["parts"]:
                download_servers["server"].append(ser)
                break
    return download_servers

def listing():
    files = list(map(enco,list(filesDB.keys())))
    socket.send_multipart(files)

while True:
    request = socket.recv_multipart()
    requestAction = deco(request[0])
    if requestAction == 'addserver':
        addServer(request)
    elif requestAction == 'upload':
        upload(request)
    elif requestAction == 'download':
        download(request)
    elif requestAction == 'list':
        listing()


