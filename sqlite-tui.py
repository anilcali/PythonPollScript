#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from collections import OrderedDict as odict
import sqlite3, contextlib
import os, sys, logging, string


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

class adict(dict):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		for k, v in self.items():
			assert not getattr(self, k, None)
			if '-' in k: self[k.replace('-', '_')] = self.pop(k)
		self.__dict__ = self

minmax = lambda v, a, b: max(a, min(b, v))

def to_str(v):
	if isinstance(v, bytes): return v.decode()
	return str(v)


class SQLiteBrowserError(Exception): pass

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


	@contextlib.contextmanager
	def cursor(self):
		with contextlib.closing(self.db.cursor()) as cursor: yield cursor

	def query( self, table, fields='*',
			raw_filter=None, col=None, v=None, chk='=', cursor=False ):
		params = list()
		if not isinstance(fields, str): fields = ', '.join(fields)
		if not raw_filter and col:
			raw_filter = 'WHERE {} {} ?'.format(col, chk)
			params.append(v)
		with self.cursor() as cur:
			q = 'SELECT {} FROM {} {}'.format(fields, table, raw_filter or '')
			log.debug('sql: {!r} {}', q, params)
			cur.execute(q, params)
			if cursor: yield cur
			else:
				for row in cur.fetchall(): yield row

	@contextlib.contextmanager
	def query_cursor(self, *args, **kws):
		query = self.query(*args, cursor=True, **kws)
		yield next(query)
		next(query)

	def pat_match(self, pat, val): return not pat or pat in val
	def pat_skip(self, pat, val): return pat and pat not in val


	def load_schema(self):
		assert self.db
		schema = odict()
		for table, in self.query('sqlite_master', fields='name', col='type', v='table'):
			with self.query_cursor(table, raw_filter='LIMIT 1') as cur:
				schema[table] = sorted(map(op.itemgetter(0), cur.description))
		if not schema: raise SQLiteBrowserError('Empty database')
		return schema

	def build_ui_info(self, schema):
		uii = adict()
		uii.sec_hrs = dict(table='Table', col='Column', val='Values')
		uii.sec_len = adict(
			table=max(map(len, schema.keys())),
			col=max(map(len, it.chain.from_iterable(schema.values()))), val=0 )
		for k, v in uii.sec_len.items(): uii.sec_len[k] = max(len(uii.sec_hrs[k]), v)
		uii.mark = '>>'
		uii.sep = adict(sec=3)
		uii.base = adict(row=1, pos=1)
		uii.data = adict(sec=['table', 'col', 'val'])
		uii.focus_update = None
		uii.focus = adict( sec=0, sec_name='',
			table=0, table_name='', col=0, col_name='', val=0, val_name='' )
		uii.seek = adict(table='', col='', val='', updated=True)
		uii.controls = {'esc': 'exit', 'arrow keys': 'move selection', 'type stuff': 'search'}
		self.update_ui_info(uii, schema)
		return uii

	def update_ui_info(self, uii=None, schema=None, w=None):
		if uii is None: uii = self.ui_info
		if schema is None: schema = self.schema

		def update_sec_focus(sec, check=True):
			if not len(uii.data[sec]):
				uii.focus[sec], uii.focus[f'{sec}_name'] = 0, ''
			else:
				if check:
					try: uii.focus[sec] = uii.data[sec].index(uii.focus[f'{sec}_name'])
					except ValueError: uii.focus[sec] = 0
				uii.focus[sec] = minmax(uii.focus[sec], 0, len(uii.data[sec])-1)
				uii.focus[f'{sec}_name'] = uii.data[sec][uii.focus[sec]]
		update_sec_focus('sec', False)

		update_sec = update_data = None
		if uii.seek.updated is not None:
			if uii.seek.updated in [True, 'table']: update_data = True
			elif uii.seek.updated == 'col': update_sec = 'table'
			elif uii.seek.updated == 'val': update_sec = 'col'
		if uii.focus_update:
			update_sec, n = uii.focus_update
			uii.focus[update_sec] += n
			update_sec_focus(update_sec, False)
		uii.seek.updated = uii.focus_update = None

		if update_data:
			uii.data.table = list(filter(
				ft.partial(self.pat_match, uii.seek.table), schema.keys() ))
			update_sec_focus('table')
		if update_sec == 'table': update_data = True

		if update_data:
			if uii.focus.table_name:
				uii.data.col = list(filter(
					ft.partial(self.pat_match, uii.seek.col),
					schema[uii.data.table[uii.focus.table]] ))
			else: uii.data.col = list()
			update_sec_focus('col')
		if update_sec == 'col': update_data = True

		if update_data:
			if uii.focus.col_name:
				query_filter = dict()
				if uii.seek.val:
					query_filter.update(
						chk='LIKE', col=uii.focus.col_name, v=f'%{uii.seek.val}%' )
				uii.data.val = list(to_str(row[0]) for row in self.query(
					uii.focus.table_name, uii.focus.col_name, **query_filter ))
				uii.data.val = list(filter(ft.partial(self.pat_match, uii.seek.val), uii.data.val))
			else: uii.data.val = list()
			uii.focus.val = 0
		update_sec_focus('val', False)


	def run(self):
		self.schema = self.load_schema()
		self.ui_info = self.build_ui_info(self.schema)
		import locale, curses
		locale.setlocale(locale.LC_ALL, '')
		self.c = curses
		self.c.wrapper(self._run)

	def _run(self, stdscr):
		c, self.c_stdscr = self.c, stdscr
		c.curs_set(0)
		c.use_default_colors()
		win, uii = self.c_win_init(), self.ui_info
		key_match = ( lambda *choices:
			key_name.lower() in choices or key in map(self.c_key, choices) )

		while True:
			self.c_win_draw(win)

			key = None
			try:
				while True:
					try: key = win.getch()
					except c.error: break
					try: key_name = c.keyname(key).decode()
					except ValueError: key_name = 'unknown' # e.g. "-1"
					break
			except KeyboardInterrupt: break
			if key is None: continue
			log.debug('Keypress event: {} ({})', key, key_name)

			if key_match('resize'): pass
			elif key_match('^['): break

			elif key_match('left'): uii.focus.sec -= 1
			elif key_match('right'): uii.focus.sec += 1
			elif key_match('up'): uii.focus_update = uii.focus.sec_name, -1
			elif key_match('down'): uii.focus_update = uii.focus.sec_name, 1

			elif key_match('backspace', '^?'):
				uii.seek[uii.focus.sec_name] = uii.seek[uii.focus.sec_name][:-1]
				uii.seek.updated = uii.focus.sec_name
			elif key_name in string.printable:
				uii.seek[uii.focus.sec_name] += key_name
				uii.seek.updated = uii.focus.sec_name

			self.update_ui_info()


	def c_win_init(self):
		win = self.c_stdscr
		win.keypad(True)
		win.bkgdset(' ')
		return win

	def c_win_add(self, w, *args, hl=False):
		if hl in [None, True, False]: hl = [self.c.A_NORMAL, self.c.A_REVERSE][bool(hl)]
		try: w.addstr(*args, hl)
		except self.c.error as err: log.debug('Failed to run addstr{}: {} {}', args, type(err), err)

	def c_key(self, k):
		if len(k) == 1: return ord(k)
		return getattr(self.c, 'key_{}'.format(k).upper(), object())

	def c_win_draw(self, w):
		uii = self.ui_info
		w.erase()
		uii.wh, uii.ww = w.getmaxyx()

		out = ft.partial(self.c_win_add, w)
		paste = lambda *a,**k: out(row, pos, *a, **k)

		row, pos = uii.base.row, uii.base.pos + uii.sep.sec
		for sec in 'table', 'col', 'val':
			hl = uii.focus.sec_name == sec
			paste(uii.sec_hrs[sec], hl=hl)
			if hl: out(row+1, pos-len(uii.mark), uii.mark)
			out(row+1, pos, uii.seek[sec])
			out(row+2, pos-1, '-' * (len(uii.sec_hrs[sec])+2))
			pos += uii.sec_len[sec] + uii.sep.sec

		row, pos = uii.base.row + 3, uii.base.pos + uii.sep.sec
		for n, table in enumerate(uii.data.table):
			hl = n == uii.focus.table
			if hl and uii.focus.sec_name == 'table':
				out(row+n, pos-len(uii.mark), uii.mark)
			out(row+n, pos, table, hl=hl)

		pos += uii.sec_len.table + uii.sep.sec
		for n, col in enumerate(uii.data.col):
			hl = n == uii.focus.col
			if hl and uii.focus.sec_name == 'col':
				out(row+n, pos-len(uii.mark), uii.mark)
			out(row+n, pos, col, hl=hl)

		pos += uii.sec_len.col + uii.sep.sec
		for n, val in enumerate(uii.data.val):
			hl = n == uii.focus.val
			if hl and uii.focus.sec_name == 'val':
				out(row+n, pos-len(uii.mark), uii.mark)
			out(row+n, pos, to_str(val), hl=hl)

		if uii.wh < 20: return
		out(uii.wh - 3, uii.base.pos, '')
		for key, desc in uii.controls.items():
			out(' ')
			out(key, hl=self.c.A_REVERSE)
			out(' - ')
			desc_max_len = uii.ww - w.getyx()[1] - 1
			out((desc + ' ')[:desc_max_len])
			if len(desc) >= desc_max_len: break


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
