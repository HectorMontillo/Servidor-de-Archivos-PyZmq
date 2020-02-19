import time
import zmq
import hashlib 
from math import sqrt
from os import listdir

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

actions = {"upload", "download", "list"}

while True:
    #  Wait for next request from client
    f = socket.recv_multipart()
    if f[0].decode('ascii') == "upload":
        newfile = open("files/"+f[1].decode('ascii'), "wb")
        newfile.write(f[2])
        newfile.close()
        socket.send_string(hashlib.sha256(f[2]).hexdigest())
    elif f[0].decode('ascii') == "list":
        filesfolder = listdir('files')
        stringfilesfoleder = ' '.join(filesfolder)
        socket.send(stringfilesfoleder.encode('ascii'))
    elif f[0].decode('ascii') == "download":
        f = open("files/"+f[1].decode('ascii'), "rb").read()
        hash = hashlib.sha256(f).hexdigest()
        socket.send_multipart([hash.encode(),f])

        
