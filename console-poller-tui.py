#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import sqlite3, contextlib
import os, sys, logging



class LogMessage(object):
	def __init__(self, fmt, a, k): self.fmt, self.a, self.k = fmt, a, k
	def __str__(self): return self.fmt.format(*self.a, **self.k) if self.a or self.k else self.fmt

class LogStyleAdapter(logging.LoggerAdapter):
	def __init__(self, logger, extra=None):
		super(LogStyleAdapter, self).__init__(logger, extra or {})
	def log(self, level, msg, *args, **kws):
		if not self.isEnabledFor(level): return
		log_kws = {} if 'exc_info' not in kws else dict(exc_info=kws.pop('exc_info'))
		msg, kws = self.process(msg, kws)
		self.logger._log(level, LogMessage(msg, args, kws), (), log_kws)

get_logger = lambda name: LogStyleAdapter(logging.getLogger(name))



class SQLiteBrowser:

	c = db = None

	def __init__(self, db_path):
		self.db_path = db_path

	def __enter__(self):
		self.db = sqlite3.connect(self.db_path, timeout=60)
		return self

	def __exit__(self, exc_t, exc_val, exc_tb):
		if self.c:
			self.c.endwin()
			self.c = None
		if self.db:
			self.db.close()
			self.db = None


	def run(self):
		import locale, curses
		locale.setlocale(locale.LC_ALL, '')
		self.c = curses
		self.c.wrapper(self._run)

	def _run(self, stdscr):
		c, self.c_stdscr = self.c, stdscr
		c.curs_set(0)
		c.use_default_colors()
		win = self.c_win_init()
		key_match = ( lambda *choices:
			key_name.lower() in choices or key in map(self.c_key, choices) )

		while True:
			self.c_win_draw(win)

			key = None
			while True:
				try: key = win.getch()
				except KeyboardInterrupt: key = self.c_key('q')
				except c.error: break
				try: key_name = c.keyname(key).decode()
				except ValueError: key_name = 'unknown' # e.g. "-1"
				break
			if key is None: continue
			log.debug('Keypress event: {} ({})', key, key_name)

			if key_match('resize'): pass
			elif key_match('q'): break


	def c_win_init(self):
		win = self.c_stdscr
		win.keypad(True)
		win.bkgdset(' ')
		return win

	def c_win_add(self, w, n, pos, line, hl=False):
		if n >= self.line_n: return
		line = line[:self.line_len - pos]
		try:
			w.addstr( n, pos, line,
				self.c.A_NORMAL if not hl else self.c.A_REVERSE )
		except Exception as err:
			log.error(
				'Failed to add line (n={} pos={} len={}) - {!r}: {} {}',
				n, pos, len(line), line, type(err), err )
			try: w.addstr(n, pos, '*ERROR*')
			except self.c.error: pass

	def c_key(self, k):
		if len(k) == 1: return ord(k)
		return getattr(self.c, 'key_{}'.format(k).upper(), object())

	def c_win_draw(self, w):
		w.erase()
		wh, ww = w.getmaxyx()
		self.line_len, self.line_n = ww, wh
		out = ft.partial(self.c_win_add, w)

		row, pos = 1, 1
		out(row, pos, 'hello world!', hl=True)


def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='ncurses-based tui for sqlite tables/columns/values.')
	parser.add_argument('db_path',
		help='Path to sqlite database to explore.')
	parser.add_argument('--debug', action='store_true', help='Verbose operation mode.')
	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	global log, print
	logging.basicConfig(
		level=logging.DEBUG if opts.debug else logging.WARNING,
		format='%(asctime)s :: %(levelname)s :: %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S' )
	log = get_logger('main')
	print = ft.partial(print, file=sys.stderr, flush=True) # stdout is used by curses

	if not os.access(opts.db_path, os.R_OK):
		parser.error(f'DB path does not exists or is inaccessible: {opts.db_path}')
	with SQLiteBrowser(opts.db_path) as app:
		log.debug('Entering curses ui loop...')
		app.run()
		log.debug('Finished')


if __name__ == '__main__': sys.exit(main())
