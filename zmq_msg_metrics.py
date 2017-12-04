#!/usr/bin/python3

from zmq_msg_core import *
import json

class ControlMessage(MultiPartMessage):

	header = b"CTRL"

	def __init__(self, message):
		self.message = message

	@property
	def msg(self):
		return [ self.header, self.message.encode("utf-8") ]

	@classmethod
	def from_msg(cls, msg):
		"Construct a ControlMessage from a pyzmq message"
		if len(msg) != 2 or msg[0] != cls.header:
			#invalid
			return None
		return cls(msg[1].decode("utf-8"))

class MetricsMessage(MultiPartMessage):

	header = b"METR"

	def __init__(self, hostname, grid_dict):
		self.hostname = hostname
		self.grid_dict = grid_dict

	@property
	def msg(self):
		return [self.header, self.hostname.encode("utf-8"), json.dumps(self.grid_dict).encode("utf-8")]

	@classmethod
	def from_msg(cls, msg):
		"Construct a MetricsMessage from a pyzmq message"
		if len(msg) != 3 or msg[0] != cls.header:
			#invalid
			return None
		return cls(msg[1].decode("utf-8"), json.loads(msg[2].decode("utf-8")))

class ClientMetricsMessage(MultiPartMessage):

	header = b"CMET"

	def __init__(self, metrics_dict):
		self.metrics_dict = metrics_dict

	@property
	def msg(self):
		return [self.header, json.dumps(self.metrics_dict.encode("utf-8"))]

	@classmethod
	def from_msg(cls, msg):
		"Construct a ClientMetricsMessage from a pyzmq message"
		if len(msg) != 2 or msg[0] != cls.header:
			#invalid
			return None
		return cls(json.loads(msg[1].decode("utf-8")))

# vim: ts=4 sw=4 noet
