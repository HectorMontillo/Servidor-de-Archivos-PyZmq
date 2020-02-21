import time
import zmq
import hashlib 
from math import sqrt
from os import listdir
import json
import re


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

def renameFile(fpart, name):
	#fpart f[2].decode('ascii')
	if name in mockDB.keys():
		inname = name
		for key in mockDB.keys():
			if key.startswith(inname):
				name = key
		if  fpart == '0':
			match = re.findall(r'\(\d+\)', name)
			if match and name[-1] == ')':
				number = int(re.search(r'\d+',match[-1]).group())
				name = name[:-len(match[-1])]+'('+str(number+1)+')'
			else:
				name = name + "(1)"
	return name

def upload(f):
	hash = getHash(f[3])
	name = f[1].decode('ascii')
			
	if f[2].decode('ascii') != 'end':
		name = renameFile(f[2].decode('ascii'),name)
		print(f[2].decode('ascii') +" "+ name +" "+ hash)
		newfile = open("files/"+hash, "wb")
		newfile.write(f[3])
		if name in mockDB.keys():
			mockDB[name]["parts"].append(hash)
		else:
			mockDB[name] = {"hash":"", "parts": [hash]}
		
		socket.send_string(hash)
	else:
		name = renameFile(f[2].decode('ascii'),name)
		print("Asigno hash completo y guardo " + f[3].decode('ascii') + " "+name)
		mockDB[name]["hash"] = f[3].decode('ascii')
		saveJSON()
		socket.send_string("Warning: The file was renamed like this:"+ name)

def listing():
	socket.send_string("\n".join(mockDB.keys()))

#has problems
def download(f):
	name = f[1].decode('ascii')
	print("Download request for file: "+name)
	if name in mockDB.keys():
		for filename in mockDB[name]["parts"]:
			print("send: "+ filename)
			filedata = open('files/'+filename, "rb")
			socket.send_multipart([b"downloading", filedata.read()])
			
			filedata.close()
		socket.send_multipart([b"end",mockDB[name]["hash"].encode('ascii')])
	else:
		socket.send_multipart([b"error", b"The file does not exist!!"])
		print("sent error: "+ name)

def download1(f):
	name = f[2].decode('ascii')
	if f[1].decode('ascii') == "request":
		if name in mockDB.keys():
			socket.send_multipart([b"replay",str(len(mockDB[name]["parts"])).encode('ascii'), mockDB[name]["hash"].encode('ascii') ])
		else:
			socket.send_multipart([b"error", b"The file does not exist!!"])
			
	elif f[1].decode('ascii') == "index":
		filename = mockDB[name]["parts"][int(f[3].decode('ascii'))]
		filedata = open('files/'+filename, "rb")
		socket.send_multipart([b"downloading", filedata.read()])
		filedata.close()
	else:
		socket.send_multipart([b"error", b"Invalid request syntax"])
		

		
while True:

	f = socket.recv_multipart()
	if f[0].decode('ascii') == "upload":
		upload(f)
	elif f[0].decode('ascii') == "list":
		listing()
	elif f[0].decode('ascii') == "download":
		download1(f)
		'''
		f = open("files/"+f[1].decode('ascii'), "rb").read()
		hash = hashlib.sha256(f).hexdigest()
		socket.send_multipart([hash.encode(),f])
		'''
		
