import zmq
import sys
import hashlib 
import os
import random
from shutil import rmtree

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
			elif requestAction == 'transfer':
				self.transfer()
			elif requestAction == 'upload transfer file':
				self.upload(self.coder.deco(request[1]),request[2],excep=True)
			elif requestAction == 'transfer file':
				self.download(self.coder.deco(request[1]),delete=True)
			elif requestAction == 'upload':
				self.upload(self.coder.deco(request[1]),request[2])
			elif requestAction == 'upload chord file':
				self.upload(self.coder.deco(request[1]),request[2], chord_file=True)
			elif requestAction == 'download':
				self.download(self.coder.deco(request[1]))
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
			self.transfer_files_left()

			request = [b'left successor', self.coder.enco(self.predecessor_server_address)]+self.enco_list(self.lim[:-1])
			response = self.successor_send_request(request)
			self.log(response,'Response successor')

			request = [b'left predecessor', self.coder.enco(self.successor_server_address)]
			response = self.predecessor_send_request(request)
			self.log(response, 'Response predecessor')
			print("Servidor Apagado")

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
		newlim = request[2:]+self.lim[1:]
		self.lim = list(map(int,newlim))
		self.predecessor_server_address = request[1]
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
			self.transfer_files()
		else:
			self.successor_server_address = self.coder.deco(response[1])
			self.join_to_ring()
		
	def transfer_files_left(self):
		try:
			files = os.listdir('{}/'.format(self.address))
			res = self.upload_files(files)
			if not res:
				print('error: An error has occurred transfering files to left')
			else:
				rmtree('{}/'.format(self.address))
		except FileNotFoundError:
			print("There aren't files to tranfer")

	def upload_files(self, files):
		for name_file in files:
			with open("{}/{}".format(self.address,name_file), "rb") as f:
				data = f.read()
				request = [b'upload transfer file', self.coder.enco(name_file),data]
				response = self.successor_send_request(request)
				if not self.coder.deco(response[0]) == 'success upload':
					return False
				else:
					print("Trasfered: "+ name_file)
		return True
				
	def transfer_files(self, successor=True):
		request = [b'transfer']
		if successor:
			response = self.successor_send_request(request)
		else:
			print('Enviando request')
			response = self.predecessor_send_request(request)
			print('Enviada request')
			

		self.log(response,'Response successor')
		if self.coder.deco(response[0]) == 'files':
			res = self.download_files(self.deco_list(response[1:]))
			if res:
				print('success: Files transfered!')
			else:
				print('error: An error has occurred transfering de files!')

	def transfer(self):
		try:
			files = os.listdir('{}/'.format(self.address))
			res_files = self.check_files(self.enco_list(files))
			if len(res_files):
				self.socket.send_multipart([b'files']+res_files)
			else:
				self.socket.send_multipart([b'not files'])

		except FileNotFoundError:
			self.socket.send_multipart([b'not files'])
	
	def check_files(self, files):
		res_files = list()
		for f in files:
			if not self.check_segment(f):
				res_files.append(f)
		return res_files
	
	def download_files(self, files):
		self.create_directory()
		for f in files:
			request = [b'transfer file', self.coder.enco(f)]
			response = self.successor_send_request(request)
			if self.coder.deco(response[0]) == 'success download':
				with open("{}/{}".format(self.address,f), 'wb') as f:
					f.write(response[1])
			else:
				self.log(response,'Response successor')
				return False
		return True

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
		
	def check_segment(self,name_segment):
		number_segment = int(name_segment,16)
		if (number_segment >= self.lim[0] and number_segment <= self.lim[1]):
			return True
		else:
			try:
				if (number_segment >= self.lim[2] and number_segment <= self.lim[3]):
					return True
				else:
					return False
			except IndexError:
				return False

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

	def create_segment(self, name_segment, segment, chord_file=False):
		self.create_directory()
		pathfile = "{}/{}".format(self.address,name_segment)
		if os.path.exists(pathfile):
			if chord_file:
				return False
			else:
				return True
		else:
			with open(pathfile, "wb") as f:
				f.write(segment)
				return True

	def upload(self, name_segment, segment, excep=False, chord_file=False):
		if (self.check_segment(name_segment) or excep):
			res = self.create_segment(name_segment, segment, chord_file=chord_file)
			if res:
				self.socket.send_multipart([b'success upload'])
			else:
				self.socket.send_multipart([b'chord file exist'])
		else:
			self.socket.send_multipart([b'failure upload', self.coder.enco(self.successor_server_address)])

	def download(self, name_segment, delete = False):
		if (self.check_segment(name_segment) or delete):
			try:
				with open("{}/{}".format(self.address,name_segment), "rb") as f:
					segment = f.read()
					self.socket.send_multipart([b'success download',segment])

				if delete:
					os.remove("{}/{}".format(self.address,name_segment))
			except FileNotFoundError:
				self.socket.send_multipart([b'file not found error'])
		else:
			self.socket.send_multipart([b'failure download', self.coder.enco(self.successor_server_address)])

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

	def create_directory(self):
		try:
			os.mkdir(self.address)
			print("Directory " , self.address ,  " Created ") 
		except FileExistsError:
			#print("Using", self.address, "directory")
			pass

	def encode_name(self,i=0):
		return self.coder.enco(str(self.name+i))
	
	def log(self,req, att='General Socket'):
		print("{}: {}".format(self.coder.deco(req[0]),att))
		cont = 1
		for i in req[1:]:
			try:
				i = self.coder.deco(i)
			except:
				i = 'Binary data'

			if len(i) > 40:
				i = i.replace('\n',' ')[0:10]+'...'

			print(str(cont) + " --> "+i)
			cont += 1
			

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
