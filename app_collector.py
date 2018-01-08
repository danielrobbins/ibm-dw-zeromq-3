#! /usr/bin/python3

from app_core import *
from zmq_msg_metrics import MetricsMessage, ControlMessage, ClientMetricsMessage
from datetime import datetime, timedelta
from logging_settings import *

# This file defines a collector, which is designed to accept connections from both agents (which are relaying back
# metrics data from monitored systems) and clients (who will be sent metrics data to be displayed in real-time by the
# system administrator, or possibly recorded to a database for later querying.)

# Because the collector allows connections from agents as well as clients, it has two ROUTER connections -- one for
# agents, and another for clients. These connections have different security policies. We accept metrics from any
# agent that connects to us, but we use ZAP to only allow authorized clients to connect, since we want to be able to
# connect to the collector as a client from anywhere on the Internet, and we consider our metrics data to be
# restricted and want to protect it from prying eyes.

# The agent ROUTER connection works as follows. We wait for one or more agents to connect. When an agent connects,
# we expect to receive an initial "hello" ControlMessage. Upon receipt of this message, we reply with a "model"
# ControlMessage, which tells the agent that we want not only metrics, but also infrequently-changing attributes of
# the remote host, such as configured RAM and swap, etc. We then expect to receive metrics (not including "model"
# attribute data) to arrive from the agent periodically. The frequency that we receive metrics something configured
# on the agent side. If we have not received any metrics from an agent for five minutes, then we consider the agent
# connection to be stale and we remove it from our list of identities. We also know that the agent expects to hear
# from us at least every 30 seconds. To be safe, we send a "hello" message to each agent every 15 seconds to let
# each know that we are still here, ready for more metrics. We will also be sure not to double-report a single agent,
# if it happens to disconnect and immediately reconnect. We detect a duplicate client connection via its unique
# identifier. #TODO

# When a client connects, we expect to immediately receive an initial "hello" ControlMessage. After receiving this
# message, we will immediately reply with a list of hosts and their infrequently-changing attributes -- our "model"
# data. After this initial exchange, we will forward metric data to the client(s) asynchronously, as we receive it
# from each agent. We will also expect to receive a "hello" ControlMessage from each client at least every 30 seconds;
# if we don't, we will consider the client connection to be dead and will drop the connection.

class CollectorMetricRouterListener(RouterListener):

	def __init__(self, app, listen_ip):
		RouterListener.__init__(self,
		                        app=app,
		                        keyname="collector",
		                        bind_addr="tcp://%s:5556" % listen_ip,
		                        zap_auth=False)
		self.identities = {}

	def setup(self):
		self.server.on_recv(self.on_recv)

	def on_recv(self, msg):
		conn_id = msg[0]
		self.identities[conn_id] = datetime.utcnow()
		if msg[1] == MetricsMessage.header:
			msg_obj = MetricsMessage.from_msg(msg[1:])
			self.app.record_metrics_from_id(conn_id, msg_obj.hostname, msg_obj.grid_dict)
		elif msg[1] == ControlMessage.header:
			msg_obj = ControlMessage.from_msg(msg[1:])
			if msg_obj.message == "hello":
				logging.info("Received 'hello' message from agent %s" % conn_id)
				resp_msg_obj = ControlMessage("model")
				resp_msg_obj.send(self.server, identity=conn_id)


class CollectorClientRouterListener(RouterListener):

	def __init__(self, app):
		RouterListener.__init__(self,
		                        app=app,
		                        keyname="collector",
		                        bind_addr="tcp://127.0.0.1:5557",
		                        zap_auth=True)
		self.identities = {}

	def setup(self):
		self.server.on_recv(self.on_recv)

	def on_recv(self, msg):
		client_conn_id = msg[0]
		self.identities[client_conn_id] = datetime.utcnow()
		if msg[1] == ControlMessage.header:
			msg_obj = ControlMessage.from_msg(msg[1:])
			if msg_obj.message == "hello":
				logging.info("Received 'hello' message from agent %s" % client_conn_id)
				if client_conn_id not in self.identities:
					resp_msg_obj = ControlMessage("model")
					resp_msg_obj.send(self.server, identity=client_conn_id)


	def remove_client(self, client_conn_id):
		del self.identities[client_conn_id]

class AppCollector(object):

	interval_ms = 1000
	stale_interval = timedelta(seconds=30)

	def __init__(self, listen_ip):

		self.listen_agents = CollectorMetricRouterListener(self, listen_ip)
		self.listen_clients = CollectorClientRouterListener(self)
		self.periodic = PeriodicCallback(self.periodictask, self.interval_ms)

		self.host_metrics = {}
		self.host_attributes = {}

	def send_msg(self):
		"""send metric data back to clients"""
		pass

	def record_metrics_from_id(self, conn_id, hostname, grid_dict):
		logging.info("Recording metrics from %s" % hostname, grid_dict)

	def periodictask(self):

		"""Send collected metrics periodically to all connected clients"""
		utc_now = datetime.utcnow()
		for client_conn_id, client_last_seen in self.listen_clients.identities.items():
			# Send metrics to active clients, and detect and clean up after stale clients
			# TODO: attributes
			msg_obj = ClientMetricsMessage(self.host_metrics)
			msg_obj.send(client_conn_id)
			if (utc_now - client_last_seen) > self.stale_interval:
				# zap client from our list of active clients
				self.listen_clients.remove_client(client_conn_id)


		to_remove = []

		# Periodically send "hello" message back to connected agents, or if we haven't seen them in a while, remove
		# them from our list of active agents.

		for conn_id, last_seen in self.listen_agents.identities.items():
			if (utc_now - last_seen) > self.stale_interval:
				logging.info("Adding %s to removal list" % conn_id)
				to_remove.append(conn_id)
			else:
				msg_obj = ControlMessage("hello")
				msg_obj.send(self.listen_agents.server, identity=conn_id)

		# remove from dictionary outside of loop iterating over dictionary:
		for conn_id in to_remove:
			del self.listen_agents.identities[conn_id]


		# TODO: perform other cleanups -- dump metrics

	def start(self):
		self.periodic.start()
		# call this last, because this runs until interrupted:
		start_ioloop()


if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Please specify the hostname or IP address to listen on as the first and only argument.")
		sys.exit(1)
	collector = AppCollector(listen_ip=sys.argv[1])
	collector.start()