#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from collections import namedtuple, Mapping, ChainMap, OrderedDict
import sqlite3, asyncio, signal, contextlib
import os, sys, logging, pathlib, time, re

import yaml # http://pyyaml.org/



### Misc helper / boilerplate classes and funcs

class LogMessage:
	def __init__(self, fmt, a, k): self.fmt, self.a, self.k = fmt, a, k
	def __str__(self): return self.fmt.format(*self.a, **self.k) if self.a or self.k else self.fmt

class LogStyleAdapter(logging.LoggerAdapter):
	def __init__(self, logger, extra=None):
		super(LogStyleAdapter, self).__init__(logger, extra or {})
	def log(self, level, msg, *args, **kws):
		if not self.isEnabledFor(level): return
		log_kws = {} if 'exc_info' not in kws else dict(exc_info=kws.pop('exc_info'))
		msg, kws = self.process(msg, kws)
		self.logger.log(level, LogMessage(msg, args, kws), **log_kws)

class LogPrefixAdapter(LogStyleAdapter):
	def __init__(self, logger, prefix=None, prefix_raw=False, extra=None):
		if isinstance(logger, str): logger = get_logger(logger)
		if isinstance(logger, logging.LoggerAdapter): logger = logger.logger
		super(LogPrefixAdapter, self).__init__(logger, extra or {})
		if not prefix: prefix = get_uid()
		if not prefix_raw: prefix = f'[{prefix}] '
		self.prefix = prefix
	def process(self, msg, kws):
		super(LogPrefixAdapter, self).process(msg, kws)
		return f'{self.prefix}{msg}', kws

get_logger = lambda name: LogStyleAdapter(logging.getLogger(name))


class ConfigError(Exception): pass

class Config(ChainMap):
	maps = None
	def __init__(self, *maps, **map0):
		if map0 or not maps: maps = [map0] + list(maps)
		super(Config, self).__init__(*maps)
	def __repr__(self):
		return ( f'<{self.__class__.__name__}'
			f' {id(self):x} {repr(self._asdict())}>' )
	def _asdict(self):
		items = dict()
		for k, v in self.items():
			if isinstance(v, self.__class__): v = v._asdict()
			items[k] = v
		return items
	def __getitem__(self, k):
		k_maps = list()
		for m in self.maps:
			if k in m:
				if isinstance(m[k], Mapping): k_maps.append(m[k])
				elif not (m[k] is None and k_maps): return m[k]
		if not k_maps: raise KeyError(k)
		return self.__class__(*k_maps)
	def __getattr__(self, k):
		try: return self[k]
		except KeyError: raise ConfigError(k)
	def __setattr__(self, k, v):
		for m in map(op.attrgetter('__dict__'), [self] + self.__class__.mro()):
			if k in m:
				self.__dict__[k] = v
				break
		else: self[k] = v
	def __delitem__(self, k):
		for m in self.maps:
			if k in m: del m[k]



### Main eventloop

StoreSpec = namedtuple('StoreSpec', 'table columns')
DataEntry = namedtuple('DataEntry', 'host ts cmd_id column_data')


class ConsolePollerDB:

	pool = None

	def __init__(self, loop, queue, conf, cmd_store):
		self.loop, self.conf, self.q, self.cmd_store = loop, conf, queue, cmd_store
		self.log = get_logger('poller.db')

	def convert_type(self, col_name, col_type, entry=None, value=None):
		t = re.findall(r'^\s*(\w+)', col_type)[0]
		if t in ['int', 'float', 'real', 'varchar', 'text', 'char']:
			if not value: value = None
			elif t == 'int': value = int(value)
			elif t in ['float', 'real']: col_type, value = 'real', int(value)
			else: value = str(value).encode()
			return col_type, value
		if t in ['host', 'time']:
			col_type = {'host': 'varchar', 'time': 'int'}[t]
			if not entry: value = None
			elif t == 'host': value = entry.host
			elif t == 'time': value = entry.ts
			return col_type, value
		raise ConfigError(f'Unknown db column type: {col_type} (column: {col_name})')

	def init(self):
		tables = dict()
		self.db = sqlite3.connect(self.conf.path, timeout=60)
		for store_spec in self.cmd_store.values():
			tables.setdefault(store_spec.table, OrderedDict()).update(store_spec.columns)
		for table, columns in tables.items():
			col_specs = list()
			for name, spec in columns.items():
				col_type, value = self.convert_type(name, spec)
				col_specs.append(f'{name} {col_type}')
			self.db.execute( 'CREATE TABLE IF NOT'
				' EXISTS {} (\n{}\n);'.format(table, ',\n'.join(col_specs)) )
		self.db.commit()

	def close(self):
		self.db.close()

	async def run(self):
		commit_delay = self.conf.get('commit_delay') or 0
		self.log.debug( 'Starting ConsolePollerDB'
			' task (commit_delay: {:.2f})...', commit_delay )
		while True:
			entries = [await self.q.get()]
			if commit_delay > 0: await asyncio.sleep(commit_delay)
			while True: # try to commit in as large batches as possible
				try: entries.append(self.q.get_nowait())
				except asyncio.QueueEmpty: break
			try:
				self.log.debug('Storing {} data entries', len(entries))
				for entry in entries:
					if entry is StopIteration: break
					entry_data, store_spec = OrderedDict(), self.cmd_store[entry.cmd_id]
					for (col_name, spec), value_raw in it.zip_longest(
							store_spec.columns.items(), entry.column_data ):
						col_type, value = self.convert_type(col_name, spec, entry, value_raw)
						if value is None:
							self.log.error( 'Missing value for column {!r} (type: {}, table: {}),'
								' discarding whole entry: {}', col_name, spec, store_spec.table, entry )
							break
						entry_data[col_name] = value
					else:
						self.db.execute(
							'INSERT INTO {} ({}) VALUES ({})'.format( store_spec.table,
								', '.join(entry_data.keys()), ', '.join(['?']*len(entry_data)) ),
							list(entry_data.values()) )
				else: continue
				break
			finally: self.db.commit()


class PollerError(Exception): pass

class ConsolePollerDaemon:

	@classmethod
	async def run_async(cls, *args, **kws):
		async with cls(*args, **kws) as self: return await self.run()

	def __init__(self, loop, conf):
		self.loop, self.conf = loop, conf
		self.log = get_logger('poller.daemon')

	async def __aenter__(self):
		self.success, self.exit_sig = False, None
		cmd_store = dict()
		for cmd_id, cmd_opts in enumerate(self.conf.commands):
			if 'store_line' not in cmd_opts: continue
			try:
				columns = OrderedDict()
				for col_spec in cmd_opts['store_line']['columns']:
					(col_name, col_spec), = col_spec.items()
					columns[col_name] = col_spec
				cmd_store[cmd_id] = StoreSpec(cmd_opts['store_line']['table'], columns)
			except KeyError:
				raise ConfigError(f'Invalid storage options for command: {cmd_opts}')
		self.db_queue = asyncio.Queue()
		self.db = ConsolePollerDB(self.loop, self.db_queue, self.conf.database, cmd_store)
		self.db.init()
		return self

	async def __aexit__(self, *err):
		self.db.close()

	async def run(self):
		tasks = [
			self.loop.create_task(self.db.run()),
			self.loop.create_task(self.run_daemon()) ]
		def sig_handler(sig):
			self.exit_sig = sig
			for task in tasks:
				if not task.done(): task.cancel()
		for sig in 'int', 'term':
			self.loop.add_signal_handler(
				getattr(signal, f'SIG{sig.upper()}'), ft.partial(sig_handler, sig) )
		for task in tasks:
			with contextlib.suppress(asyncio.CancelledError): await task
		return self.success

	async def run_daemon(self):
		pollers = dict()
		for host, host_opts in self.conf.hosts.items():
			opts = self.conf.options.copy()
			opts.update(host_opts)
			self.log.debug('Initializing poller for host: {}', host)
			poller = ConsolePoller.run_task(
				self.loop, self.db_queue, host, opts, self.conf.commands )
			pollers[host] = poller.task.cpd_poller = poller
		self.log.debug('Starting ConsolePollerDaemon loop...')
		try: await asyncio.gather(*(p.task for p in pollers.values()))
		finally:
			for p in pollers.values():
				if p.task.done(): continue
				p.task.cancel()
				with contextlib.suppress(asyncio.CancelledError): await p.task


class ConsolePoller:

	@classmethod
	def run_task(cls, loop, *args, **kws):
		self = cls(loop, *args, **kws)
		self.task = loop.create_task(self.run())
		return self

	def __init__(self, loop, queue, host, opts, cmds):
		self.loop, self.host, self.q, self.opts, self.cmds = loop, host, queue, opts, cmds
		self.log = get_logger('poller.host.{}'.format(host.replace('_', '__').replace('.', '_')))

	async def run(self):
		self.log.debug('Starting ConsolePoller task...')
		while True:
			try:
				await asyncio.sleep(self.opts.poll_interval)
			except asyncio.CancelledError:
				raise
			self.q.put_nowait(DataEntry(self.host, time.time(), 0, [1,2,3,4,5]))


	# async def __aenter__(self):
	# 	await self.run(wait=False)
	# 	return self

	# async def __aexit__(self, *err):
	# 	if self.finished and not self.finished.done():
	# 		self.finished.cancel()
	# 		with contextlib.suppress(asyncio.CancelledError): await self.finished

	# async def run(self, wait=True):
	# 	assert not self.proc
	# 	log.debug('[{!r}] running: {}', self.src, self.cmd_repr)
	# 	if self.progress_func: self.kws['stdout'] = subprocess.PIPE
	# 	self.proc = await asyncio.create_subprocess_exec(*self.cmd, **self.kws)
	# 	for k in 'stdin', 'stdout', 'stderr': setattr(self, k, getattr(self.proc, k))
	# 	self.finished = self.loop.create_task(self.wait())
	# 	if wait: await self.finished
	# 	return self

	# async def wait(self):
	# 	progress_task = None
	# 	if self.progress_func and self.proc.stdout:
	# 		progress_task = self.loop.create_task(self.print_progress())
	# 	try:
	# 		await self.proc.wait()
	# 		if progress_task: await progress_task
	# 		if self.proc.returncode != 0:
	# 			cmd_repr = '' if not self.cmd_repr else f': {self.cmd_repr}'
	# 			raise AudioConvError(( f'Command for src {self.src!r}'
	# 				f' exited with non-zero status ({self.proc.returncode}){cmd_repr}' ))
	# 	finally:
	# 		if progress_task and not progress_task.done():
	# 			progress_task.cancel()
	# 			with contextlib.suppress(asyncio.CancelledError): await progress_task



def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Tool to poll data via commands to remote ssh/telnet consoles.')

	parser.add_argument('-c', '--conf',
		action='append', metavar='path',
		help='Path to yaml configuration file(s).'
			' Can be specified multiple times, to load values from'
				' multiple files, with same-path values in latter ones override the former.'
			' Default is to try loading file with same'
				' name as the script, but with yaml extension, if it exists.'
			' See default config alongside this script for the general structure.'
			' Command-line options override values there.')

	parser.add_argument('-i', '--poll-interval',
		type=float, metavar='seconds',
		help='Default interval between running batches of commands on hosts.'
			' Overrides corresponding configuration file setting, including per-host settings.')

	parser.add_argument('--debug', action='store_true', help='Verbose operation mode.')
	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	logging.basicConfig( datefmt='%Y-%m-%d %H:%M:%S',
		format='%(asctime)s :: %(name)s %(levelname)s :: %(message)s',
		level=logging.DEBUG if opts.debug else logging.WARNING )
	log = get_logger('main')

	conf = [dict(options=dict(), hosts=dict(), commands=list())]
	conf_paths = list(filter(os.path.exists, [__file__.rsplit('.', 1)[0] + '.yaml']))
	conf_paths.extend(opts.conf or list())
	if conf_paths:
		for p in conf_paths:
			conf_dir = os.path.dirname(os.path.realpath(p))
			if conf_dir not in sys.path: sys.path.append(conf_dir)
			with open(p) as src: conf.append(yaml.safe_load(src))
	if len(conf) > 1: conf.append(dict()) # for local overrides
	conf = Config(*reversed(conf))

	if opts.poll_interval:
		conf.options.poll_interval = opts.poll_interval
		for host_opts in conf.hosts.values(): host_opts.poll_interval = opts.poll_interval

	if (conf.database.get('path') or 'auto') == 'auto':
		p = pathlib.Path(conf_paths[-1])
		conf.database.path = str(p.parent / (p.stem + '.sqlite'))

	log.debug('Starting main eventloop...')
	with contextlib.closing(asyncio.get_event_loop()) as loop:
		success = loop.run_until_complete(ConsolePollerDaemon.run_async(loop, conf))
		log.debug('Waiting for async generators to finish...')
		loop.run_until_complete(loop.shutdown_asyncgens())
	log.debug('Finished')
	return int(not success)

if __name__ == '__main__': sys.exit(main())
