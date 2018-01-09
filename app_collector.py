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

# AGENT CONNECTION - for monitoring a host

# The agent ROUTER connection works as follows. We wait for one or more agents to connect. When an agent connects, we
# expect to receive an initial "hello" ControlMessage. Upon receipt of this message, we reply with a "model"
# ControlMessage, which tells the agent to immediately send us infrequently-changing attributes of the remote host, such
# as configured RAM and swap, which we call 'model' data. We then expect to receive metrics (not including "model" data)
# to arrive from the agent periodically. The frequency that we receive metrics something configured on the agent side.
# If we have not received any messages from an agent for thirty seconds, then we consider the agent connection to be
# stale and we remove it from our list of identities. We also know that the agent expects to hear from us at least every
# 30 seconds. To be safe, we send a "ping" message to each agent every 15 seconds to let each know that we are still
# here, available for more metrics.

# If the collector restarts, it is possible that ZeroMQ will automatically reconnect a running agent without its
# explicit knowledge of the interrupted connection. In this scenario, it is possible that the collector will begin
# to receive metrics data for a host for which it has no model data. When this event occurs, the collector will send
# a "model" ControlMessage to the agent and expect an immediate reply of "model" data.

# CLIENT CONNECTION - for receiving forwarded metrics from all hosts

# When a client connects, we expect to immediately receive an initial "hello" ControlMessage. After receiving this
# message, we will immediately reply with a list of hosts and their infrequently-changing attributes -- our "model"
# data. After this initial exchange, we will forward metric data to the client(s) asynchronously, as we receive it from
# each agent. If we don't, we will consider the client connection to be dead and will remove the client from our list of
# connected clients. In turn, the collector will ensure that it sends a message of some form to each client at least
# every 30 seconds (we have configured it for 15 seconds, to be safe) We will send a "ping" ControlMessage so the client
# can know that its connection to the collector is active. These messages are send asynchonously and are not tied to one
# another in any kind of request/reply pattern.

class CollectorMetricRouterListener(RouterListener):

	def __init__(self, app, listen_ip):
		RouterListener.__init__(self,
		                        app=app,
		                        keyname="collector",
		                        bind_addr="tcp://%s:5556" % listen_ip,
		                        zap_auth=False)
		self.identities = {}
		self.hostname_to_conn_id = {}

	def setup(self):
		self.server.on_recv(self.on_recv)

	def on_recv(self, msg):
		conn_id = msg[0]

		# We record the datetime we last received a message from this agent:

		self.identities[conn_id] = datetime.utcnow()

		if msg[1] == MetricsMessage.header:

			# We have received some kind of metrics message:

			msg_obj = MetricsMessage.from_msg(msg[1:])

			# Here we deal with a scenario where a host disconnects, but then reconnects under a new connection ID.
			# When we detect the same host on a new connection, then we will automatically expire the old connection.

			if msg_obj.hostname in self.hostname_to_conn_id:
				if self.hostname_to_conn_id[msg_obj.hostname] != conn_id:
					# we are receiving data for this hostname from another connection -- maybe the previous connection
					logging.info("Receiving metrics data for %s on new connection" % msg_obj.hostname)
					old_conn_id = self.hostname_to_conn_id[msg_obj.hostname]
					if old_conn_id in self.identities:
						del self.identities[old_conn_id]
					self.hostname_to_conn_id[msg_obj.hostname] = conn_id
			else:
				# record a mapping of the agent's hostname to the connection ID.
				self.hostname_to_conn_id[msg_obj.hostname] = conn_id

			# If this is model data, we want to cache this information, since we send it to new agents when they
			# connect:

			if msg_obj.metrics_type == "model":
				self.app.record_model_data(msg_obj)
			else:
				# metrics data.
				if msg_obj.hostname not in self.app.model_data:
					# It looks like we restarted but this agent's ZeroMQ stack automatically reconnected to us. But
					# while we just received metrics data, we are missing the model data. So request it.
					logging.info("We are missing model data for %s; requesting it." % msg_obj.hostname)
					ControlMessage("model").send(self.server, identity=conn_id)

			logging.info("Received %s data from %s via identity %s." % (msg_obj.metrics_type, msg_obj.hostname, conn_id))

			# But all metrics data, either 'model' or 'metrics' flavors, gets forwarded to all connected clients as we
			# receive them. This way, connected clients will receive new 'model' messages for newly-connected agents,
			# as well as periodic metrics from the agents:

			self.app.relay_metrics_to_clients(msg_obj)

		elif msg[1] == ControlMessage.header:

			# We may also receive hello messages from agents when they initially connect to us.

			msg_obj = ControlMessage.from_msg(msg[1:])
			if msg_obj.message == "hello":
				logging.info("Received hello message from agent %s" % conn_id)
				ControlMessage("model").send(self.server, identity=conn_id)
			else:
				logging.info("Received %s message from agent %s" % msg_obj.message, conn_id)

	def remove_agent(self, agent_conn_id):
		del self.identities[agent_conn_id]

class CollectorClientRouterListener(RouterListener):

	def __init__(self, app):
		RouterListener.__init__(self,
		                        app=app,
		                        keyname="collector",
		                        bind_addr="tcp://127.0.0.1:5557",
		                        zap_auth=True)

		self.identities = {}
		self.identities_last_send = {}

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
					self.app.send_cached_model_data_to_client(client_conn_id)


	def remove_client(self, client_conn_id):
		del self.identities[client_conn_id]
		if client_conn_id in self.identities_last_send:
				del self.identities_last_send[client_conn_id]

class AppCollector(object):

	agent_interval_ms = 15000
	client_interval_ms = 5000
	stale_interval = timedelta(seconds=30)
	ping_interval = timedelta(seconds=20)
	ping = ControlMessage("ping")
	model_data = {}

	def __init__(self, listen_ip):

		self.listen_agents = CollectorMetricRouterListener(self, listen_ip)
		self.listen_clients = CollectorClientRouterListener(self)
		self.periodic_agent = PeriodicCallback(self.agent_periodictask, self.agent_interval_ms)
		self.periodic_client = PeriodicCallback(self.client_periodictask, self.client_interval_ms)

	def record_model_data(self, msg_obj):

		self.model_data[msg_obj.hostname] = msg_obj

	def send_cached_model_data_to_client(self, client_conn_id):

		"""
		When a new client connects, we want to immediately send it all the 'model' info we have for all the
		currently-connected agent systems. 'model' info is infreqently-changing information related to a host, such as
		the amount of RAM installed -- stuff we don't expect to change very often. We will relay 'metrics' info --
		rapidly changing data, like CPU usage -- in real-time as we get it. (but not in this method.)
		:param client_conn_id: The connection id of the client who needs the cached model data.
		:return: None
		"""

		for hostname, msg_obj in self.model_data.items():
			# relay our received agent message to a client:
			msg_obj.send(self.listen_clients.server, identity=client_conn_id)

			# update our record of when we last sent data to this client:
			self.listen_clients.identities_last_send[client_conn_id] = datetime.utcnow()

	def relay_metrics_to_clients(self, msg_obj):

		"""
		When new metric data is received from an agent -- this can be either 'model' data (from a new agent connection)
		or periodic 'metric' data -- we want to forward this metric information to each client.
		:param msg_obj: The message object we received from the agent.
		:return: None
		"""

		client_count = 0

		for client_conn_id, client_last_recv in self.listen_clients.identities.items():
			client_count += 1

			# relay the message to this client:
			msg_obj.send(client_conn_id)

			# update record of when we last sent data to this client:
			self.listen_clients.identities_last_send[client_conn_id] = datetime.utcnow()

		if client_count:
			logging.info("Relayed metrics to %s clients." % client_count)

	def client_periodictask(self):

		"""
		This periodic task performs two functions -- first, it will look for any clients that we haven't heard from
		in the last 30 seconds, and assume that they are stale connections, and remove them from our list of connected
		clients.

		The second thing we do here is to see if there are any clients to whom we have not sent any data recently,
		and send them a 'hello' ControlMessage. Clients will expect to hear from us at least once every 30 seconds.
		This scenario can occur when there are no connected agents and we have at least one connected client. We don't
		want our lack of metrics data to indicate to the client that its connection is stale.
		:return: None
		"""

		utc_now = datetime.utcnow()

		to_remove = []
		client_count = 0

		# Identify and remove any clients that appear to be dead. If we have not heard from them in 30 seconds, we
		# want to remove them from our list of active clients.

		for client_conn_id, client_last_recv in self.listen_clients.identities.items():
			# Have we not heard from a client in a while? Consider it stale:
			if (utc_now - client_last_recv) > self.stale_interval:
				to_remove.append(client_conn_id)
				client_count += 1
			else:
				# Have we not sent data to the client in a while? Send it a ping message:
				if client_conn_id not in self.listen_clients.identities_last_send or \
					(utc_now - self.listen_clients.identities_last_send[client_conn_id]) > self.ping_interval:
					self.ping.send(self.listen_clients.server, identity=client_conn_id)
					self.listen_clients.identities_last_send[client_conn_id] = utc_now

		for client_conn_id in to_remove:
			# we do this outside of the previous loop so we don't modify the dictionary while iterating over it:
			self.listen_clients.remove_client(client_conn_id)

		if client_count:
			logging.info("Removed %s stale clients." % client_count)

	def agent_periodictask(self):

		utc_now = datetime.utcnow()

		to_remove = []

		# Periodically send "ping" message back to connected agents, or if we haven't seen them in a while, remove
		# them from our list of active agents.

		for conn_id, last_seen in self.listen_agents.identities.items():
			if (utc_now - last_seen) > self.stale_interval:
				logging.info("Agent identity %s stale. Adding to removal list." % conn_id)
				to_remove.append(conn_id)
			else:
				self.ping.send(self.listen_agents.server, identity=conn_id)

		if len(to_remove):

			conn_id_to_hostname_map = dict((v, k) for k, v in self.listen_agents.hostname_to_conn_id.items())

			for conn_id in to_remove:
				if conn_id in conn_id_to_hostname_map:
					hostname = conn_id_to_hostname_map[conn_id]
					logging.info("Removing %s from list of active agents." % hostname)
					if hostname in self.model_data:
						del self.model_data[hostname]

				del self.listen_agents.identities[conn_id]

	def start(self):
		self.periodic_agent.start()
		self.periodic_client.start()
		# call this last, because this call will block the main thread until interrupted:
		start_ioloop()


if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Please specify the hostname or IP address to listen on as the first and only argument.")
		sys.exit(1)
	collector = AppCollector(listen_ip=sys.argv[1])
	collector.start()