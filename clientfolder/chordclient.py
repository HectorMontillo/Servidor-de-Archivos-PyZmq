import zmq
import hashlib 
import sys

PS = 1024 * 1024

# Encode and Decode data
class Code:
	def __init__(self,f):
		self.format = f
	def deco(self, data):
		return data.decode(self.format)
	def enco(self, data):
		return data.encode(self.format)

# Get sha hash 
class Sha:
	def __init__(self, v):
		self.version = v
	def getHash(self, file):
		if (self.version == '1'):
			return hashlib.sha1(file).hexdigest()
		if (self.version == '256'):
			return hashlib.sha256(file).hexdigest()


class Chord_Client:
	def __init__(self):
		self.context = zmq.Context()
		#Utilidades
		self.coder = Code('ascii')
		self.naming = Sha('1')

	def run(self):
		try:
			command = sys.argv[1]
		except:
			print('You must use an action: upload, download or list')
		
		if (command == 'state'):
				try:
					address = sys.argv[2]
				except:
					print('You must indicate the server address')
				self.state(address)
		
	def send_request(self, req, address, send='multipart', recv='multipart'):
		socket = self.context.socket(zmq.REQ)
		socket.connect("tcp://"+address)

		if(send=='multipart'):
			socket.send_multipart(req)
		elif(send=='json'):
			socket.send_json(req)

		if(recv=='multipart'):
			res = socket.recv_multipart()
		elif(recv=='json'):
			res = socket.recv_json()

		socket.close()
		return res

	def state(self, address):
		request = [b'state']
		intial_address = address
		current_address = address
		while(True):
			response = self.send_request(request,current_address,recv='json')
			print("Chord State--------->")
			print("Name: "+response['name'])
			print("Address: "+response['address'])
			print("Lim: "+', '.join(list(map(str,response['lim']))))
			print("Successor: "+response['successor'])
			print("Predecessor: "+response['predecessor'])
			print("-------------------->")
			current_address = response['successor']
			if(intial_address == current_address):
				break

if __name__ == "__main__":
	client = Chord_Client()
	client.run()