 
import zmq
import hashlib 
import sys
context = zmq.Context()

#  Socket to talk to server
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

def compareHash(hash1, hash2):
    return True if hash1 == hash2 else False

def checkIntegrity(file, hash):
    filehash = hashlib.sha256(file).hexdigest()
    print("Hash from file:\t\t"+filehash)
    print("Hash from server:\t"+hash)
    print("Succefully" if compareHash(filehash, hash) else "An error has occurred, retry the operation")

def upload(file):
    f = open(file, "rb").read()
    socket.send_multipart([b"upload",file.encode('ascii'), f])
    message = socket.recv()
    checkIntegrity(f,message.decode('ascii'))

def download(file):
    socket.send_multipart([b"download",file.encode('ascii')])
    message = socket.recv_multipart()
    checkIntegrity(message[1],message[0].decode('ascii'))
    newfile = open(file, "wb")
    newfile.write(message[1])
    newfile.close()

def listing():
    socket.send_multipart([b"list"])
    message = socket.recv()
    print("Your files : ")
    print(message.decode('ascii'))  

if sys.argv[1] == 'upload':
    upload(sys.argv[2])
elif sys.argv[1] == 'list':
    listing()
elif sys.argv[1] == 'download':
    download(sys.argv[2])


