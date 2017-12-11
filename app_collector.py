#! /usr/bin/python3

from app_core import *
from zmq_msg_metrics import MetricsMessage, ControlMessage, ClientMetricsMessage

class CollectorMetricRouterListener(RouterListener):

	def __init__(self, app):
		RouterListener.__init__(self,
		                        app=app,
		                        keyname="collector",
		                        bind_addr="tcp://127.0.0.1:5556",
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
				print("Received 'hello' message from agent %s" % conn_id)
				if conn_id not in self.identities:
					print("Sending 'ready' ControlMessage to agent %s" % conn_id)
					resp_msg_obj = ControlMessage("ready")
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
				print("Received 'hello' message from agent %s" % client_conn_id)
				if client_conn_id not in self.identities:
					print("Sending 'ready' ControlMessage to agent %s" % client_conn_id)
					resp_msg_obj = ControlMessage("ready")
					resp_msg_obj.send(self.server, identity=client_conn_id)


	def remove_client(self, client_conn_id):
		del self.identities[client_conn_id]

class AppCollector(object):

	interval_ms = 1000
	stale_interval = timedelta(seconds=30)

	def __init__(self):

		self.listen_agents = CollectorMetricRouterListener(self)
		self.listen_clients = CollectorClientRouterListener(self)
		self.periodic = PeriodicCallback(self.periodictask, self.interval_ms)

		self.host_metrics = {}
		self.host_attributes = {}

	def send_msg(self):
		"""send metric data back to clients"""
		pass

	def record_metrics_from_id(self, conn_id, hostname, grid_dict):
		print("Recording metrics from %s" % hostname, grid_dict)

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

		# Periodically send "ready" message back to connected agents, or if we haven't seen them in a while, remove
		# them from our list of active agents.

		for conn_id, last_seen in self.listen_agents.identities.items():
			if (utc_now - last_seen) > self.stale_interval:
				print("Adding %s to removal list" % conn_id)
				to_remove.append(conn_id)
			else:
				msg_obj = ControlMessage("ready")
				msg_obj.send(self.listen_agents.server, identity=conn_id)

		# remove from dictionary outside of loop iterating over dictionary:
		for conn_id in to_remove:
			del self.listen_agents.identities[conn_id]


		# TODO: perform other cleanups -- dump metrics

	def start(self):
		self.periodic.start()
		self.listen_agents.start()
		self.listen_clients.start()
		# call this last, because this runs until interrupted:
		start_ioloop()

if __name__ == "__main__":
	collector = AppCollector()
	collector.start()