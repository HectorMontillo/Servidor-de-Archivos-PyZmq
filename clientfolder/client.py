 
import zmq
import hashlib 
import sys
context = zmq.Context()

PS = 1024 * 1024

#  SocketProxi  to talk to server
socketProxi = context.socket(zmq.REQ)
socketProxi.connect("tcp://localhost:5556")

def deco(data, format='ascii'):
    return data.decode(format)
def enco(data, format='ascii'):
    return data.encode(format)

def compareHash(hash1, hash2):
    return True if hash1 == hash2 else False

def getHash(file):
    return hashlib.sha256(file).hexdigest()

def checkIntegrity(file, hash):
    filehash = hashlib.sha256(file).hexdigest()
    #print("Hash from file:\t\t"+filehash)
    #print("Hash from server:\t"+hash)
    print("Segment Succefully" if compareHash(filehash, hash) else "An error has occurred, retry the operation")

def upload(file):
    with open(file, 'rb') as f:
        parts = list()
        sha256 = hashlib.sha256()
        while True:
            segment = f.read(PS)
            if not segment:
                hashCompleto = sha256.hexdigest()
                socketProxi.send_multipart([b'upload', enco(file), enco(hashCompleto) ]+parts)
                balance = socketProxi.recv_json()
                distributed_upload(file, balance)
                break
            sha256.update(segment)
            parts.append(enco(getHash(segment)))

def send_request(address, req):
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://"+address)
    socket.send_multipart(req)
    message = socket.recv_multipart()
    socket.close()
    return message
    
def select_server(balance, segment_hash):
    for key in balance.keys():
        if segment_hash in balance[key]:
            return key
    return "error"

def distributed_upload(file, balance):
    with open(file, 'rb') as f:
        parts = 0
        while True:
            segment = f.read(PS)
            if not segment:
                print("Has been upload " + str(parts) + " parts")
                break
            server = select_server(balance, getHash(segment))
            if server=='error':
                print("An error has been occur!!")
            else:
                message = send_request(server,[b'upload',segment])
                print("Resutl for part " + str(parts))
                checkIntegrity(segment,deco(message[0])) 
                parts+=1
  
def download(file):
    socketProxi.send_multipart([b"download",file.encode('ascii')])
    response = socketProxi.recv_json()
    if "error" in response.keys():
        print(response["error"])
    else:
        distributed_download(file, response)

def distributed_download(file, response):
    newfile = open(file, "ab")
    sha256 = hashlib.sha256()
    for h,s in zip(response["parts"],response["server"]):
        res = send_request(s,[b"download", enco(h)])
        sha256.update(res[0])
        newfile.write(res[0])
    newfile.close()
    print("Hash from download:\t"+sha256.hexdigest())
    print("Hash from proxi:\t"+response["hash"])

def listing():
    socketProxi.send_multipart([b"list"])
    message = socketProxi.recv_multipart()
    print("Your files : ")
    for i in message:
        print(deco(i))  

if sys.argv[1] == 'upload':
    upload(sys.argv[2])
elif sys.argv[1] == 'list':
    listing()
elif sys.argv[1] == 'download':
    download(sys.argv[2])


