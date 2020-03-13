import zmq
import sys
import hashlib 
import os
import random

LIMIT = (2**160)-1

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

#Cord Server
class Chord_Server:
	def __init__(self,address,connect):
		# Utilidades
		self.coder = Code('ascii')
		self.naming = Sha('1')
		# Atributos
		self.name = self.generate_name(40)
		self.address = address
		self.lim = [self.name+1,LIMIT,0,self.name] if connect=='genesis' else None
		#ZMQ
		#General Socket for listen Request
		self.context = zmq.Context()
		self.socket = self.context.socket(zmq.REP)
		self.socket.bind("tcp://*:{}".format(self.address.split(':')[1]))
		#Socket to send request to server successor and predecessor
		self.successor_server_socket = None
		self.successor_server_address = connect
		self.predecessor_server_socket = None
		self.predecessor_server_address = address

	def loop(self):
		while True:
			request = self.socket.recv_multipart()
			self.log(request)
			requestAction = self.coder.deco(request[0])
			if requestAction == 'join':
				self.join_server(int(self.coder.deco(request[1])),self.coder.deco(request[2]))
			elif requestAction == 'left successor':
				self.left_to_ring(self.deco_list(request))
			elif requestAction == 'left predecessor':
				self.left_predecessor(self.coder.deco(request[1]))
			elif requestAction == 'successor':
				self.successor_update(self.coder.deco(request[1]))
			elif requestAction == 'upload':
				pass
			elif requestAction == 'download':
				pass
			elif requestAction == 'list':
				pass
			elif requestAction == 'state':
				self.state()

	def run(self):
		if self.successor_server_address == 'genesis':
			print("Genesis server has been initialized!!")
			print("Name: {}, Address: {}".format(self.name, self.address))
			self.successor_server_address = self.address
		else:
			self.join_to_ring()
		self.loop()
	
	def down(self):
		if (self.successor_server_address == self.address):
			print("Ultimo servidor del anillo apagado!")
		else:
			print("Servidor Apagado")
			request = [b'left successor', self.coder.enco(self.predecessor_server_address)]+self.enco_list(self.lim[:-1])
			response = self.successor_send_request(request)
			self.log(response,'Response successor')

			request = [b'left predecessor', self.coder.enco(self.successor_server_address)]
			response = self.predecessor_send_request(request)
			self.log(response, 'Response predecessor')

	def generate_name(self,length):
		return int(self.naming.getHash(self.coder.enco(self.generate_random_string(40))),16)

	def generate_random_string(self,length):
		string = ""
		for i in range(length):
			char = random.randint(0,127)
			string+=chr(char)
		return string
	
	def successor_send_request(self,req):
		self.successor_server_socket = self.context.socket(zmq.REQ)
		self.successor_server_socket.connect("tcp://{}".format(self.successor_server_address))
		self.successor_server_socket.send_multipart(req)
		response = self.successor_server_socket.recv_multipart()
		self.successor_server_socket.close()
		return response

	def predecessor_send_request(self,req):
		self.predecessor_server_socket = self.context.socket(zmq.REQ)
		self.predecessor_server_socket.connect("tcp://{}".format(self.predecessor_server_address))
		self.predecessor_server_socket.send_multipart(req)
		response = self.predecessor_server_socket.recv_multipart()
		self.predecessor_server_socket.close()
		return response

	def left_to_ring(self, request):
		self.predecessor_server_address = request[1]
		newlim = request[2:]+self.lim[1:]
		self.lim = list(map(int,newlim))
		print("My lim: ")
		print(self.lim)
		self.socket.send_multipart([b'left success'])

	def left_predecessor(self,successor_server_address):
		self.successor_server_address = successor_server_address
		self.socket.send_multipart([b'left success'])
			
	def join_to_ring(self):
		request = [b"join", self.encode_name(), self.coder.enco(self.address)]
		response = self.successor_send_request(request)
		self.log(response,'Response Successor')
		if(self.coder.deco(response[0]) == 'success'):
			print("Server has been initialized!!")
			print("Name: {}, Address: {}".format(self.name, self.address))
			self.lim = list(map(self.deco_and_int, response[1:-1]))
			self.predecessor_server_address = self.coder.deco(response[-1])
			self.notify_successor()
		else:
			self.successor_server_address = self.coder.deco(response[1])
			self.join_to_ring()
		
	def join_server(self,server_num, server_address):
		if (server_num >= self.lim[0] and server_num <= self.lim[1]):
			res_lim = [self.coder.enco(str(self.lim[0])),self.coder.enco(str(server_num)), self.coder.enco(self.predecessor_server_address)]
			self.socket.send_multipart([b"success"]+res_lim)
			self.lim[0] = server_num+1
			self.predecessor_server_address = server_address
		else:
			try:
				if (server_num >= self.lim[2] and server_num <= self.lim[3]):
					res_lim = [self.coder.enco(str(self.lim[0])), self.coder.enco(str(self.lim[1])), self.coder.enco(str(self.lim[2])), self.coder.enco(str(server_num)), self.coder.enco(self.predecessor_server_address)]
					self.socket.send_multipart([b"success"]+res_lim)
					self.lim = [server_num+1, self.name]
					self.predecessor_server_address = server_address
				else:
					self.socket.send_multipart([b"rejoin", self.coder.enco(self.successor_server_address)])
			except IndexError:
				self.socket.send_multipart([b"rejoin", self.coder.enco(self.successor_server_address)])
				
		print("My lim: ")
		print(self.lim)
		
	def rejoin_server(self,address):
		self.successor_server_address = address
		self.join_to_ring()

	def notify_successor(self):
		request = [b"successor", self.coder.enco(self.address)]
		response = self.predecessor_send_request(request)
		self.log(response,'Response Predecessor')

	def successor_update(self, address):
		self.successor_server_address = address
		self.socket.send_multipart([b'success update'])
		
	def state(self):
		state = {
			'name': str(self.name),
			'address': self.address,
			'lim': self.lim,
			'successor': self.successor_server_address,
			'predecessor': self.predecessor_server_address
		}
		self.socket.send_json(state)

	def deco_and_int(self, data):
		return int(self.coder.deco(data))

	def enco_list(self, lista):
		return list(map(self.coder.enco,map(str, lista)))

	def deco_list(self, lista):
		return list(map(self.coder.deco,lista))

	def create_directory(self,name):
		pass
		'''
		try:
			os.mkdir(name)
			print("Directory " , name ,  " Created ") 
		except FileExistsError:
			print("Directory " , name ,  " already exists")
		'''
	def encode_name(self,i=0):
		return self.coder.enco(str(self.name+i))
	
	def log(self,req, att='General Socket'):
		print("{}: {}".format(self.coder.deco(req[0]),att))
		for i in req[1:]:
			print("--> "+self.coder.deco(i))

if __name__ == "__main__":
	server_address = sys.argv[1]
	try:
		successor_server_address = sys.argv[2]
	except:
		successor_server_address = 'genesis'
	server = Chord_Server(server_address,successor_server_address)
	try:
		server.run()
	except KeyboardInterrupt:
		server.down()
