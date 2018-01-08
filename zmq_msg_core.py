#!/usr/bin/python3

from logging_settings import *

class MultiPartMessage(object):

	header = None

	@classmethod
	def recv(cls, socket):
		"Reads key-value message from socket, returns new instance."
		return cls.from_msg(socket.recv_multipart())

	@property
	def msg(self):
		return [ self.header ]

	def send(self, socket, identity=None):
		"Send message to socket"
		logging.info("Sending %s message" % self.header)
		msg = self.msg
		if identity:
			msg = [ identity ] + msg
		socket.send_multipart(msg)

# vim: ts=4 sw=4 noet
