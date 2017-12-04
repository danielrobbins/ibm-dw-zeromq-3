#! /usr/bin/python3

from app_core import *
from zmq_msg_metrics import ClientMetricsMessage, ControlMessage

class ClientDealerConnection(DealerConnection):

	def __init__(self, app, collector_host):
		DealerConnection.__init__(self,
		                          app=app,
		                          keyname="client",
		                          remote_keyname="collector",
		                          endpoint="tcp://%s:5556" % collector_host
		                          )

	def setup(self):
		self.client.on_recv(self.on_recv)

	def on_recv(self, msg):
		if msg[0] == ClientMetricsMessage.header:
			msg_obj = ClientMetricsMessage.from_msg(msg)
			self.app.update_metrics_data(msg_obj)

class AppClient(object):

	screen_interval_ms = 1000
	hello_interval_ms = 15000

	def __init__(self, collector_host):
		self.client_conn = ClientDealerConnection(self, collector_host)
		self.screen_periodic = PeriodicCallback(self.screen_periodictask, self.screen_interval_ms)
		self.hello_periodic = PeriodicCallback(self.hello_periodictask, self.hello_interval_ms)

	def update_metrics_data(self, client_metrics_msg):
		# TODO
		pass

	def screen_periodictask(self):
		pass
		# TODO: update graph

	def hello_periodictask(self):
		msg_obj = ControlMessage('hello')
		msg_obj.send(self.client_conn.client)

	def start(self):
		self.screen_periodic.start()
		self.hello_periodic.start()
		self.client_conn.start()
		start_ioloop()

if __name__ == "__main__":
	# Start client:
	if len(sys.argv) != 1:
		print("Please specify the client hostname or IP address as the first and only argument.")
		sys.exit(1)
	agent = AppClient(collector_host=sys.argv[1])
	agent.start()