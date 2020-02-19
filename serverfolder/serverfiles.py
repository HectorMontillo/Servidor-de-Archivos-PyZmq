import time
import zmq
import hashlib 
from math import sqrt
from os import listdir
import json

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

actions = {"upload", "download", "list"}

mockDB = json.load(open("mock.json", "r"))

def getHash(segment):
    return hashlib.sha256(segment).hexdigest()

def saveJSON():
    with open('mock.json', 'w') as json_file:
        json.dump(mockDB, json_file)

def upload(f):
    if f[2].decode('ascii') != 'end':
        hash = getHash(f[3])
        name = f[1].decode('ascii')
        newfile = open("files/"+hash, "wb")
        newfile.write(f[3])
        if name in mockDB.keys():
            mockDB[name].append(hash)
        else:
            mockDB[name] = [hash]
        saveJSON()
        socket.send_string(hash)
    else:
        socket.send_string("Succefully Upload")
        
        


while True:
    #  Wait for next request from client
    

    f = socket.recv_multipart()
    if f[0].decode('ascii') == "upload":
        '''
        newfile = open("files/"+f[1].decode('ascii'), "wb")
        newfile.write(f[2])
        newfile.close()
        socket.send_string(hashlib.sha256(f[2]).hexdigest())
        '''
        upload(f)
    elif f[0].decode('ascii') == "list":
        filesfolder = listdir('files')
        stringfilesfoleder = ' '.join(filesfolder)
        socket.send(stringfilesfoleder.encode('ascii'))
    elif f[0].decode('ascii') == "download":
        f = open("files/"+f[1].decode('ascii'), "rb").read()
        hash = hashlib.sha256(f).hexdigest()
        socket.send_multipart([hash.encode(),f])

        
