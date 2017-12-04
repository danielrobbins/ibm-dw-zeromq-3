#! /usr/bin/python3

from app_core import *
from zmq_msg_metrics import MetricsMessage, ControlMessage
from metrics import Host, UptimeCollector, MeminfoCollector, CollectionGrid

# This file defines an agent, which is designed to run on a monitored Linux system, and will report metrics to a
# remote collector every 15 seconds. The metrics reported back are dynamic in nature so they must be reported often.

# In addition, the agent will listen for special 'model this hostname' messages from the collector. When such a message
# is received, the agent will reply immediately with current metrics, but will also include less-frequently changed
# information about the hostname, such as configured hardware, etc.

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
				print("Received model request from collector")
				self.app.send_msg(metric_type='model')
			elif msg_obj.message == "ready":
				print("Received ready message from collector")

class AppAgent(object):

	"""The AppAgent is the main python class that wraps our agent application. It is configured to report metrics
	back to the collector every interval_ms seconds (15000 by default, configurable below.) The AppAgent defines
	a periodic task to make this happen. It also defines the helper send_msg() method which is used internally by
	AppAgent as well as by the AgentDealerConnection to reply to 'model this hostname' messages."""

	interval_ms = 1000

	def __init__(self, collector_host):

		# agent ZeroMQ initialization:
		self.collector_host = collector_host
		self.collector_conn = AgentDealerConnection(app=self, collector_host=self.collector_host)
		self.periodic = PeriodicCallback(self.periodictask, self.interval_ms)

		# agent metrics initialization:
		self.localhost = Host()
		self.collectors = [UptimeCollector(), MeminfoCollector()]

		# These properties are used to track when we have last heard from the collector, and whether we should send
		# data back.

		self.last_collector_msg_on = None
		self.timeout = timedelta(seconds=10)
		self.sending = False

	def send_msg(self, metric_type='attributes'):
		"""collect metrics and send them back to the collector"""
		grid = CollectionGrid()
		for col in self.collectors:
			grid.add_samples(col.get_samples(self.localhost, metric_type=metric_type))
		msg = MetricsMessage(self.localhost.hostname, grid.get_grid())
		print("Sending metrics to collector")
		msg.send(self.collector_conn.client)

	def periodictask(self):

		"""This runs every interval_ms seconds, and we don't need any logic here -- just send the metrics back
		periodically."""
		if self.last_collector_msg_on is None or datetime.utcnow() - self.last_collector_msg_on > self.timeout:
			if self.sending == True:
				print("No response from collector, no longer sending metrics.")
				self.sending = False
			stop_ioloop()
			return

		if self.sending:
			self.send_msg()
		elif self.last_collector_msg_on is not None and datetime.utcnow() - self.last_collector_msg_on < self.timeout:
			if self.sending == False:
				print("Initiating sending of metrics to collector.")
				self.sending = True


	def start(self):
		self.periodic.start()
		self.collector_conn.start()
		while True:
			print("STARTUP")
			# TODO: tear down and rebuild connection for collector if we lose it.

			print("Sending hello message")
			msg = ControlMessage("hello")
			msg.send(self.collector_conn.client)
			# this runs for a long time:
			start_ioloop()

if __name__ == "__main__":
	# Start agent:
	if len(sys.argv) != 2:
		print("Please specify the collector hostname or IP address as the first and only argument.")
		sys.exit(1)
	agent = AppAgent(collector_host=sys.argv[1])
	agent.start()