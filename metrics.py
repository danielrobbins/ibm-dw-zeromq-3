#!/usr/bin/python3

from collections import defaultdict
from datetime import datetime
import socket


class Host(object):
	# represents a single hostname for which we are collecting metrics

	def __init__(self, hostname=None):
		if hostname is None:
			if socket.gethostname().find('.') >= 0:
				self.hostname = socket.gethostname()
			else:
				self.hostname = socket.gethostbyaddr(socket.gethostname())[0]
		else:
			self.hostname = hostname

	def get_file(self, filename):
		try:
			with open(filename, "r") as myf:
				return myf.read()
		except IOError:
			return None


class Collector(object):

	metric_defs = []
	host = Host()
	metrics = {}

	def get_metric(self, key):
		return self.metrics[key]


class UptimeCollector(Collector):

	metric_defs = {
		"metrics": {
			"sys.uptime": {"desc": "System uptime, in seconds", "units": "s", "python_type": float}
		}
	}

	def get_samples(self, metrics_type='metrics'):
		upt_data = self.host.get_file("/proc/uptime")
		try:
			yield "sys.uptime", float(upt_data.split()[0])
		except (IndexError, ValueError):
			return


class MeminfoCollector(Collector):

	metric_map = {
		"metrics": {
			"MemFree": "mem.free",
			"MemAvailable": "mem.avail",
			"Buffers": "mem.buffers",
			"Cached": "mem.cached",
			"Dirty": "mem.dirty",
			"Writeback": "mem.writeback",
			"SwapFree": "mem.swap.free"
		},
		"model": {
			"MemTotal": "mem.total",
			"SwapTotal": "mem.swap.total",
		}
	}

	metric_defs = {
		"metrics": {
			"mem.free": {"desc": "Free memory", "units": "kB", "python_type": int},
			"mem.buffers": {"desc": "Buffer memory", "units": "kB", "python_type": int},
			"mem.avail": {"desc": "Available memory", "units": "kB", "python_type": int},
			"mem.cached": {"desc": "Cached memory", "units": "kB", "python_type": int},
			"mem.dirty": {"desc": "Dirty memory", "units": "kB", "python_type": int},
			"mem.writeback": {"desc": "Writeback memory", "units": "kB", "python_type": int},
			"mem.swap.free": {"desc": "Free swap memory", "units": "kB", "python_type": int}
		},
		"model": {
			"mem.total": {"desc": "Total memory", "units": "kB", "python_type": int},
			"mem.swap.total": {"desc": "Total swap memory", "units": "kB", "python_type": int},
		}
	}

	def get_samples(self, metrics_type="metrics"):

		mem_data = self.host.get_file("/proc/meminfo")

		for line in mem_data.split('\n'):
			try:
				line_split = line.split()
				meminfo_key = line_split[0][:-1]
				if len(line_split) and meminfo_key in self.metric_map[metrics_type].keys():
					metric_key = self.metric_map[metrics_type][meminfo_key]
					value = line_split[1]
					yield metric_key, int(value)
			except (IndexError, ValueError):
				pass

