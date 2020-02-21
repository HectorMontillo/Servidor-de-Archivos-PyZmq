 
import zmq
import hashlib 
import sys
context = zmq.Context()

PS = 1024 * 1024

#  Socket to talk to server
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

def compareHash(hash1, hash2):
    return True if hash1 == hash2 else False

def checkIntegrity(file, hash):
    filehash = hashlib.sha256(file).hexdigest()
    print("Hash from file:\t\t"+filehash)
    print("Hash from server:\t"+hash)
    print("Segment Succefully" if compareHash(filehash, hash) else "An error has occurred, retry the operation")

def upload(file):
    with open(file, 'rb') as f:
        sha256 = hashlib.sha256()
        parts = 0
        while True:
            segment = f.read(PS)
            if not segment:
                socket.send_multipart([b'upload', file.encode('ascii'), b'end', sha256.hexdigest().encode('ascii')])
                message = socket.recv()
                print("Has been upload " + str(parts) + " parts")
                print(message.decode('ascii'))
                break
            sha256.update(segment)
            socket.send_multipart([b'upload', file.encode('ascii'), str(parts).encode('ascii'),segment])
            message = socket.recv()
            print("Resutl for part " + str(parts))
            checkIntegrity(segment,message.decode('ascii')) 
            parts+=1

def download(file):
    socket.send_multipart([b"download",file.encode('ascii')])
    sha256 = hashlib.sha256()
    while True:
        message = socket.recv_multipart()
        state = message[0].decode('ascii')
        print("Replay message state: " + state)
        
        if state == 'end':
            hashcompleteServer = message[1].decode('ascii')
            hashcomplete = sha256.hexdigest()
            print("Hash from server: "+ hashcompleteServer)
            print("Hash of file: "+ hashcomplete)
            print("Succefully download!!")
            break
        elif state == 'downloading':
            sha256.update(message[1])
            newfile = open(file, "ab")
            newfile.write(message[1])
            newfile.close()
        else:
            print(message[1].decode('ascii'))
            break
        
def download1(file):
    socket.send_multipart([b"download", b"request",file.encode('ascii')])
    sha256 = hashlib.sha256()
    message = socket.recv_multipart()

    if(message[0].decode('ascii') == 'replay'):
        parts = int(message[1].decode('ascii'))
        hashFromServer = message[2].decode('ascii')
        for i in range(parts):
            socket.send_multipart([b"download", b"index",file.encode('ascii'), str(i).encode('ascii')])
            downloading = socket.recv_multipart()
            sha256.update(downloading[1])
            newfile = open(file, "ab")
            newfile.write(downloading[1])
            newfile.close()
        hashFile = sha256.hexdigest()
        print("Hash from server : "+ hashFromServer)
        print("Hash downloaded file : "+hashFile)
            
    elif(message[0].decode('ascii') == 'error'):
        print(message[1].decode('ascii'))


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
    download1(sys.argv[2])


