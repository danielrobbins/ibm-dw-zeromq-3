#!/usr/bin/python3

from collections import defaultdict
from datetime import datetime
import socket

class Metric(object):
	# defines a kind of metric, its associated shortname.

	def __init__(self, key, desc, units, python_type):
		self.key = key
		self.desc = desc
		self.units = units
		self.python_type = python_type

class Sample(object):
	# a data sample, which points to the type of metric. contains hostname ID.

	def __init__(self, metric, host, timestamp, value):
		self.metric = metric
		self.host = host
		self.timestamp = timestamp
		self.value = value

class Host(object):
	# represents a single hostname for which we are collecting metrics

	def __init__(self, hostname=None):
		if hostname == None:
			if socket.gethostname().find('.') >= 0:
				self.hostname = socket.gethostname()
			else:
				self.hostname = socket.gethostbyaddr(socket.gethostname())[0]
		else:
			self.hostname = hostname

	def timestamp(self):
		return datetime.utcnow().timestamp()

	def get_file(self, filename):
		try:
			with open(filename, "r") as myf:
				return myf.read()
		except IOError:
			return None

class CollectionGrid(object):
	# processes samples and inserts them

	def __init__(self):
		self.metric_list = []
		self.host_samples = defaultdict(dict)

	def add_samples(self, samples):
		for sample in samples:
			if sample.metric.key not in self.metric_list:
				self.metric_list.append(sample.metric.key)
			self.host_samples[sample.host.hostname][sample.metric.key] = sample

	def get_grid(self):
		"""
		This function converts our dev-friendly dictionary to a network IO-friendly 'grid_dict' format optimized for sending over the wire.
	{
		"grid_dict" : {
			"metrics" : [ "io.read.B", "io.write.B", "net.send.B", "net.recv.B", "cpu.user.ms', "cpu.nice.ms", "cpu.system.ms" ],
			"nodes" : {
				"2000" : [ io.read.B.value, io.write.B.value, ... ] <--- appear in the same order as the 'metrics' list above
				...
			}
		}
		"""

		out = {
			"metrics" : self.metric_list,
			"hosts" : defaultdict(list)
		}

		for host, samples_dict in self.host_samples.items():
			out["hosts"][host] = []
			for metric_key in self.metric_list:
				if metric_key in samples_dict:
					out["hosts"][host].append((samples_dict[metric_key].value, samples_dict[metric_key].timestamp))
				else:
					out["hosts"][host].append(None)
		return out

class Collector(object):
	# collects a sample

	metric_defs = []

	def __init__(self):
		self.metrics = {}
		for m_type, m_def_components in self.metric_defs.items():
			for metric_def in m_def_components["keys"]:
				self.metrics[metric_def["key"]] = Metric(**metric_def)

	def get_metric(self, key):
		return self.metrics[key]

	# TODO: add modeling support, and 'attributes' for metrics that don't change frequently.

class UptimeCollector(Collector):

	metric_defs = {
		"metrics" : {
			"keys" : [
				{ "key" : "sys.uptime", "desc" : "System uptime, in seconds", "units" : "s", "python_type" : float }
			]
		}
	}

	def get_samples(self, host, metric_type='metrics'):
		if metric_type != 'metrics':
			return []
		# this method returns a list of some kind, possibly even an empty list if there were no samples.
		upt_data = host.get_file("/proc/uptime")
		timestamp = host.timestamp()
		if upt_data == None:
			return []
		try:
			return [Sample(self.get_metric("sys.uptime"), host, timestamp, float(upt_data.split()[0]))]
		except (IndexError, ValueError):
			return []

class NetworkCollector(Collector):

	# networkcollector models the network interfaces as components, and then associates metrics with each component.
	# need to define how this info gets put in a grid_dict.

	#"comp:netif" (network interfaces)

	# and we can 'griddify' this info to help us store it more efficiently:

	# { "component_key" : { "metric_name" : "metric_val", }

	pass

class MeminfoCollector(Collector):

	metric_defs = {
		"metrics" : {
			"keys" :
				[
					{"key": "mem.free", "desc": "Free memory", "units": "kB", "python_type": int},
					{"key": "mem.buffers", "desc": "Buffer memory", "units": "kB", "python_type": int},
					{"key": "mem.avail", "desc": "Available memory", "units": "kB", "python_type": int},
					{"key": "mem.cached", "desc": "Cached memory", "units": "kB", "python_type": int},
					{"key": "mem.dirty", "desc": "Dirty memory", "units": "kB", "python_type": int},
					{"key": "mem.writeback", "desc": "Writeback memory", "units": "kB", "python_type": int},
					{"key": "mem.swap.free", "desc": "Free swap memory", "units": "kB", "python_type": int}
				],
			"strings" : {
				"MemFree" : "mem.free",
				"MemAvailable" : "mem.avail",
				"Buffers" : "mem.buffers",
				"Cached" : "mem.cached",
				"Dirty" : "mem.dirty",
				"Writeback" : "mem.writeback",
				"SwapFree" : "mem.swap.free"
			}
		},
		"attributes" : {
			"keys":
				[
					{"key": "mem.total", "desc": "Total memory", "units": "kB", "python_type": int},
					{"key": "mem.swap.total", "desc": "Total swap memory", "units": "kB", "python_type": int},
				],
			"strings": {
				"MemTotal": "mem.total",
				"SwapTotal": "mem.swap.total",
			}
		}
	}

	def get_samples(self, host, metric_type="metrics"):

		mem_data = host.get_file("/proc/meminfo")
		timestamp = host.timestamp()
		metrics = []

		my_strings = self.metric_defs[metric_type]["strings"]

		if mem_data == None:
			pass
		for line in mem_data.split('\n'):
			try:
				line_split = line.split()
				meminfo_key = line_split[0][:-1]
				if len(line_split) and meminfo_key in my_strings.keys():
					metric_key = my_strings[meminfo_key]
					value = line_split[1]
					metrics.append(Sample(self.get_metric(metric_key), host, timestamp, int(value)))
			except (IndexError, ValueError):
				pass
		return metrics

if __name__ == "__main__":
	localhost = Host()
	collectors = [ UptimeCollector(), MeminfoCollector() ]
	grid = CollectionGrid()

	for col in collectors:
		grid.add_samples(col.get_samples(localhost, metric_type='attributes'))

	print(grid.get_grid())