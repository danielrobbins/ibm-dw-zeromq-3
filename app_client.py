#! /usr/bin/python3

from app_core import *
from zmq_msg_metrics import MetricsMessage, ControlMessage
import curses
from datetime import datetime

class ClientDealerConnection(DealerConnection):

	def __init__(self, app, collector_host):
		DealerConnection.__init__(self, app=app, keyname="client", remote_keyname="collector", endpoint="tcp://%s:5557" % collector_host )

	def setup(self):
		self.client.on_recv(self.on_recv)

	def on_recv(self, msg):
		if msg[0] == MetricsMessage.header:
			msg_obj = MetricsMessage.from_msg(msg)
			self.app.update_metrics_data(msg_obj)

class AppClient(object):

	screen_interval_ms = 1000
	hello_interval_ms = 15000

	def __init__(self, collector_host, stdscr):
		self.client_conn = ClientDealerConnection(self, collector_host)
		self.screen_periodic = PeriodicCallback(self.screen_periodictask, self.screen_interval_ms)
		self.hello_periodic = PeriodicCallback(self.send_hello, self.hello_interval_ms)
		self.stdscr = stdscr

	def update_metrics_data(self, metrics_msg):
		curses_write(self.stdscr, 0, 1, metrics_msg.hostname)
		curses_write(self.stdscr, 0, 2, repr(metrics_msg.grid_dict))
		self.stdscr.refresh()

	def screen_periodictask(self):
		curses_write(self.stdscr, 0, 0, repr(datetime.now()))
		self.stdscr.refresh()

	def send_hello(self):
		msg_obj = ControlMessage('hello')
		msg_obj.send(self.client_conn.client)

	def start(self):
		self.stdscr.clear()
		curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
		curses_write(self.stdscr, 0, 0, repr(datetime.now()))
		self.stdscr.refresh()
		self.screen_periodic.start()
		self.hello_periodic.start()
		self.send_hello()
		start_ioloop()


def curses_write(stdscr, x, y, output):
	if y >= curses.LINES:
		return
	if x >= curses.COLS:
		return
	to_trunc = (x + len(output)) - curses.COLS
	if to_trunc > 0:
		output = output[:-to_trunc]
	stdscr.addstr(y, x, output)

def main(stdscr):
	agent = AppClient(sys.argv[1], stdscr)
	agent.start()

if __name__ == "__main__":
	# Start client:
	if len(sys.argv) != 2:
		print("Please specify the client hostname or IP address as the first and only argument.")
		sys.exit(1)
	curses.wrapper(main)

