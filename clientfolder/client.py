import zmq
import hashlib 
import sys
import os

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
		self.hasher = Sha(256)

	def run(self):
		
		try:
			command = sys.argv[1]
		except:
			print('You must use an action: upload or download')

		if command == 'state':
			self.state(sys.argv[2])
		elif command == 'upload':
			self.upload(sys.argv[2], sys.argv[3])
		elif command == 'download':
			self.download(sys.argv[2], sys.argv[3])

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

	def upload_segment(self,name_file, segment, server_address, chord_file = False):
		name_segment = hashlib.sha1(segment).hexdigest()
		#print(name_segment, type(name_segment))
		if chord_file:
			request = [b'upload chord file',self.coder.enco(name_segment),segment]
		else:
			request = [b'upload',self.coder.enco(name_segment),segment]

		address = server_address
		while True:
			response = self.send_request(request,address)
			if self.coder.deco(response[0]) == 'success upload':
				print("Segment was upload: {} on server {}".format(name_segment,address))
				return True
			elif self.coder.deco(response[0]) == 'chord file exist':
				return False
			else:
				address = self.coder.deco(response[1])
				if address == server_address:
					return False

	def finish_chord_file(self, name_file, complete_hash):
		f = open(name_file+".chord", "r")
		lines = f.readlines()
		lines.insert(0,complete_hash+'\n')
		lines.insert(0,name_file+'\n')
		f.close()
		f = open(name_file+".chord", "w")
		f.writelines(lines)
		f.close()

	def upload_chord_file(self, name_file, server_address):
		with open(name_file, 'rb') as f:
			chord_data = f.read()
			hash_chord_file = hashlib.sha1(chord_data).hexdigest()
			res = self.upload_segment(hash_chord_file,chord_data,server_address, chord_file=True)
			if res:
				print('Magnet link is already!: '+ hash_chord_file)
				return hash_chord_file			
			else:
				print('The file already exist en chord, you can use this magnet link to donwload: '+ hash_chord_file)
				return ''

	def upload_file(self, name_file, server_address):
		with open(name_file, 'rb') as f:
			while True:
				segment = f.read(PS)
				if not segment:
					break
				res = self.upload_segment(name_file,segment, server_address)
				if not res:
					print("The upload was canceled")
					break
				
	def upload(self, name_file, server_address):
		self.clear_file(name_file+".chord")
		f = open(name_file, 'rb')
		cf = open(name_file+".chord", "a")
		sha1 = hashlib.sha1()
		while True:
			segment = f.read(PS)
			if not segment:
				cf.close()
				f.close()
				self.finish_chord_file(name_file,	sha1.hexdigest())
				res = self.upload_chord_file(name_file+".chord",server_address)
				if res:
					self.upload_file(name_file, server_address)
					print("The file was upload! The magnet link is: "+ res)
				break
			name_segment = hashlib.sha1(segment).hexdigest()
			cf.write(name_segment+"\n")
			sha1.update(segment)

	def download_segment(self,name_file,name_segment,server_address):
		request = [b'download',self.coder.enco(name_segment)]
		address = server_address
		while True:
			response = self.send_request(request,address)
			if self.coder.deco(response[0]) == 'success download':
				with open(name_file, 'ab') as f:
					f.write(response[1])
				return True
			elif self.coder.deco(response[0]) == 'file not found error':
				print("Segment not found!")
				return False
			else:
				address = self.coder.deco(response[1])
				if address == server_address:
					print("Hash out of limits!")
					return False

	def clear_file(self,name_file):
		f = open(name_file, 'w')
		f.write('')
		f.close()

	def download(self,chord_file, server_address):
		if '.chord' in chord_file:
			with open(chord_file, 'r') as f:
				lines = f.readlines()
				name_file = lines.pop(0)[:-1]
				complete_hash = lines.pop(0)[:-1]
				cont = 0
				self.clear_file(name_file)
				for name_segment in lines:
					name_segment = name_segment[:-1]
					print(cont, name_segment)
					cont += 1
					res = self.download_segment(name_file,name_segment,server_address)
					if not res:
						print('Donwload was canceled, an error has occurred')
						break
				self.check_integrity(complete_hash, name_file)
				print('Donwload finished!')
		else:
			if len(chord_file) == 40:
				print("Downloading with magnet link!")
				res = self.download_segment('temp.chord',chord_file,server_address)
				if not res:
					print("Magnet link invalid!")
				else:
					self.download('temp.chord',server_address)
					os.remove('temp.chord')
			else:
				print("Cord file or magnet link invalid")

	def check_integrity(self,complete_hash, name_file):
		with open(name_file, "rb") as f:
			sha1 = hashlib.sha1()
			while True:
				segment = f.read(PS)
				if not segment:
					if sha1.hexdigest() == complete_hash:
						print("Succefully download: \nHash registered:\t{}\nHash download:\t{}".format(complete_hash,sha1.hexdigest()))
					else:
						print("Failure download: File corrupted!")
					break
				sha1.update(segment)

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