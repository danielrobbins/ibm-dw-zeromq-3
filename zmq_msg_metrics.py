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

	def log(self):
		logging.info("Sending ControlMessage: %s." % self.message)

	@classmethod
	def from_msg(cls, msg):
		"Construct a ControlMessage from a pyzmq message"
		if len(msg) != 2 or msg[0] != cls.header:
			#invalid
			return None
		return cls(msg[1].decode("utf-8"))


class MetricsMessage(MultiPartMessage):

	header = b"METR"

	def __init__(self, hostname, grid_dict, metrics_type="metrics"):
		self.hostname = hostname
		self.grid_dict = grid_dict
		self.metrics_type = metrics_type

	@property
	def msg(self):
		return [self.header, self.hostname.encode("utf-8"), json.dumps(self.grid_dict).encode("utf-8"),
			self.metrics_type.encode("utf-8") ]

	def log(self):
		logging.info("Sending MetricsMessage of type %s" % self.metrics_type)

	@classmethod
	def from_msg(cls, msg):

		"""Construct a MetricsMessage from a pyzmq message"""

		if len(msg) != 4 or msg[0] != cls.header:
			#invalid
			return None
		return cls(msg[1].decode("utf-8"), json.loads(msg[2].decode("utf-8")), msg[3].decode("utf-8"))

# vim: ts=4 sw=4 noet
