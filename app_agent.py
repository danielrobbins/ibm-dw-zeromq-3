#! /usr/bin/python3

from app_core import *
from zmq_msg_metrics import MetricsMessage, ControlMessage
from metrics import Host, UptimeCollector, MeminfoCollector
from logging_settings import *
from datetime import datetime, timedelta

# This file defines an agent, which is designed to run on a monitored Linux system, and will report metrics to a
# remote collector.

# Upon initial connection to a remote collector, the agent will send a "hello" ControlMessage. When the remote collector
# receives this message, it will reply to the agent with a "model" ControlMessage. The "model" ControlMessage tells the
# agent to immediately send back infrequently-changing metrics, such as amount of RAM in the system -- we call this
# information "model data." Although we are using an asynchronous ROUTER/DEALER pattern, this initial exchange is
# implemented as effectively synchronous, meaning that the collector immediately responds to the agent's "hello" message
# with a "model" ControlMessage, and the agent immediately replies to the "model" ControlMessage with a message
# containing model data for this host. The agent will only respond to a maximum of one "model" request every 5 seconds
# from the collector.

# After this initial message exchange, the agent sends back system metrics every 5 seconds (this frequency is
# configurable.) These metrics are dynamic in nature so they will be reported periodically. This information is referred
# to as "metrics data".

# The agent will expect to receive a "ping" ControlMessage from the collector within 30 seconds of having received the
# initial "model" message, and will expect to continue to receive these "ping" messages at least every 30 seconds.

# If the agent does not receive a message from the collector at least every 30 seconds, then the agent will assume that
# the connection to the collector is stale, and will shut down its ioloop and attempt to reconnect. It is important to
# note that the ongoing metrics messages that are sent from agent to collector, as well as the periodic "ping"
# ControlMessages from collector back to agent are sent asynchronously -- the "ping" from the collector is not a
# direct response to the metrics message, and vice-versa.

# To use the agent, run it from the command-line and specify the hostname or IP address of the remote collector as a
# command-line argument. CurveZMQ is used for the transport, and the collector is configured to not have ZAP enabled,
# so the collector will not require the agent's public CurveZMQ key. However, our agent will require the collector's
# CurveZMQ key so that it can authenticate that it is connecting and reporting metrics back to the to the authentic
# (non-impersonated) collector. This key must be stored in ~/.curve/collector.key file.

class AgentDealerConnection(DealerConnection):

	"""The AgentDealerConnection defines a ZeroMQ DEALER connection to the collector. It also defines an on_recv()
	method so that it can know when it has received a 'model this hostname' message from the collector, and respond
	appropriately by sending a response back."""

	def __init__(self, app, collector_host):
		DealerConnection.__init__(self,
		                          app=app,
		                          keyname="agent",
		                          remote_keyname="collector",
		                          endpoint="tcp://%s:5556" % collector_host
		                          )

	def setup(self):
		self.client.on_recv(self.on_recv)

	def on_recv(self, msg):
		self.app.last_collector_msg_on = datetime.utcnow()
		if msg[0] == ControlMessage.header:
			msg_obj = ControlMessage.from_msg(msg)
			if msg_obj.message == "model":
				if self.app.received_model_request is None or (
						self.app.received_model_request is not None and datetime.now() -
						self.app.received_model_request > timedelta(seconds=5)
				):
					self.app.received_model_request = datetime.now()
					self.app.send_msg(metrics_type='model')
				if not self.app.periodic_metrics.is_running():
					self.app.periodic_metrics.start()
				return
			else:
				logging.info("Received %s message from collector." % msg_obj.message)
				return
		logging.warning("Received unknown message from collector.")

class AppAgent(object):

	"""The AppAgent is the main python class that wraps our agent application. It is configured to report metrics
	back to the collector every metrics_interval_ms seconds (15000 by default, configurable below.) The AppAgent defines
	a periodic task to make this happen. It also defines the helper send_msg() method which is used internally by
	AppAgent as well as by the AgentDealerConnection to reply to 'model this hostname' messages."""

	metrics_interval_ms = 1000
	stale_interval_ms = 10000
	stale_interval_timedelta = timedelta(seconds=stale_interval_ms//1000)

	def __init__(self):

		self.collector_host = None
		self.collector_conn = None
		self.periodic_stale = None
		self.periodic_metrics = None
		self.received_model_request = None

		# agent metrics initialization:

		self.localhost = Host()
		self.collectors = [UptimeCollector(), MeminfoCollector()]

		# These properties are used to track when we have last heard from the collector, and whether we should send
		# data back.

		self.last_collector_msg_on = None

	def setup_collector_connection(self, collector_host):

		self.collector_host = collector_host
		self.collector_conn = AgentDealerConnection(app=self, collector_host=self.collector_host)
		self.periodic_metrics = PeriodicCallback(self.periodictask_send_metrics, self.metrics_interval_ms)
		self.periodic_stale = PeriodicCallback(self.periodictask_stale_connection, self.stale_interval_ms)

	def periodictask_send_metrics(self):
		if self.received_model_request:
			self.send_msg()

	def send_msg(self, metrics_type='metrics'):

		"""collect metrics and send them back to the collector"""

		grid = {}
		for col in self.collectors:
			grid.update(col.get_samples(metrics_type=metrics_type))
		msg = MetricsMessage(self.localhost.hostname, grid, metrics_type=metrics_type)
		sys.stdout.write("M" if metrics_type == "model" else "m")
		sys.stdout.flush()
		msg.send(self.collector_conn.client)

	def periodictask_stale_connection(self):
		if self.last_collector_msg_on is None or datetime.utcnow() - self.last_collector_msg_on > self.stale_interval_timedelta:
			logging.warning("No response from collector, no longer sending metrics.")
			self.stop()

	def stop(self):
		logging.debug("Stopping IOLoop.")
		if self.periodic_metrics.is_running():
			self.periodic_metrics.stop()
		if self.periodic_stale.is_running():
			self.periodic_stale.stop()
		stop_ioloop()


	def run_forever(self, collector_host):

		self.setup_collector_connection(collector_host)

		# Our main connection loop...

		while True:

			# Attempt to connect and send "hello" to the collector...
			sys.stdout.write("h")
			sys.stdout.flush()
			ControlMessage("hello").send(self.collector_conn.client)

			# We want to now start a periodic task to check every 30 seconds whether we have heard from the collector.
			# Otherwise, we will consider our connection the the collector to be stale.

			if not self.periodic_stale.is_running():
				self.periodic_stale.start()

			# If the connection to the collector becomes stale, self.stop() will be called which will stop all active
			# periodic tasks and cause start_ioloop() to return.

			start_ioloop()

if __name__ == "__main__":
	# Start agent:
	if len(sys.argv) != 2:
		print("Please specify the collector hostname or IP address as the first and only argument.")
		sys.exit(1)
	agent = AppAgent()
	agent.run_forever(collector_host=sys.argv[1])
