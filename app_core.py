#!/usr/bin/python3

import sys
import zmq

from datetime import datetime, timedelta

from zmq.eventloop.ioloop import IOLoop, PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream
from zmq.auth.ioloop import IOLoopAuthenticator
from key_monkey import *

class DealerConnection(object):

	def __init__(self, app=None, keyname="client", remote_keyname="server", endpoint="tcp://127.0.0.1:5556", crypto=True):

		self.app = app
		self.keyname = keyname
		self.remote_keyname = remote_keyname
		self.endpoint = endpoint
		self.crypto = crypto

		self.ctx = zmq.Context()
		self.client = self.ctx.socket(zmq.DEALER)

		if self.crypto:
			self.keymonkey = KeyMonkey(keyname)
			self.client = self.keymonkey.setupClient(self.client, self.endpoint, remote_keyname)

		self.client.connect(self.endpoint)
		print("Connecting to", self.endpoint)
		self.client = ZMQStream(self.client)
		self.setup()

	def setup(self):
		pass

	def start(self):
		pass

class RouterListener(object):

	def __init__(self, app=None, keyname="server", bind_addr="tcp://127.0.0.1:5556", crypto=True, zap_auth=True):

		self.app = app
		self.keyname = keyname
		self.bind_addr = bind_addr
		self.crypto = crypto
		self.zap_auth = zap_auth

		self.ctx = zmq.Context()
		self.loop = IOLoop.instance()
		self.identities = {}

		self.server = self.ctx.socket(zmq.ROUTER)

		if self.crypto:
			self.keymonkey = KeyMonkey(self.keyname)
			self.server = self.keymonkey.setupServer(self.server, self.bind_addr)

		self.server.bind(self.bind_addr)
		print("%s listening for new client connections at %s" % ( self.keyname, self.bind_addr))
		self.server = ZMQStream(self.server)
		# Setup ZAP:
		if self.zap_auth:
			if not self.crypto:
				print("ZAP requires CurveZMQ (crypto) to be enabled. Exiting.")
				sys.exit(1)
			self.auth = IOLoopAuthenticator(self.ctx)
			print(self.auth)
			#self.auth.deny(None)
			print("ZAP enabled.\nAuthorizing clients in %s." % self.keymonkey.authorized_clients_dir)
			self.auth.configure_curve(domain='*', location=self.keymonkey.authorized_clients_dir)
		self.setup()

	def setup(self):
		#self.server.on_recv(self.on_recv)
		#self.periodic = PeriodicCallback(self.periodictask, 1000)
		pass

	def start(self):
		if self.zap_auth:
			self.auth.start()

def start_ioloop():
	loop = IOLoop.instance()
	try:
		loop.start()
	except KeyboardInterrupt:
		pass

def stop_ioloop():
	loop = IOLoop.instance()
	loop.stop()

# vim: ts=4 sw=4 noet
