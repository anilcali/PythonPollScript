#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from collections import namedtuple, Mapping, ChainMap, OrderedDict
import sqlite3, asyncio, socket, signal, contextlib
import os, sys, logging, pathlib, time, re, random

import yaml # http://pyyaml.org/



### Misc helper / boilerplate classes and funcs

class LogMessage:
	def __init__(self, fmt, a, k): self.fmt, self.a, self.k = fmt, a, k
	def __str__(self): return self.fmt.format(*self.a, **self.k) if self.a or self.k else self.fmt

class LogStyleAdapter(logging.LoggerAdapter):
	def __init__(self, logger, extra=None):
		super().__init__(logger, extra or {})
	def log(self, level, msg, *args, **kws):
		if not self.isEnabledFor(level): return
		log_kws = {} if 'exc_info' not in kws else dict(exc_info=kws.pop('exc_info'))
		msg, kws = self.process(msg, kws)
		self.logger.log(level, LogMessage(msg, args, kws), **log_kws)

get_logger = lambda name: LogStyleAdapter(logging.getLogger(name))


class ConfigError(Exception): pass

class Config(ChainMap):
	maps = None
	def __init__(self, *maps, **map0):
		if map0 or not maps: maps = [map0] + list(maps)
		super().__init__(*maps)
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


async def asyncio_wait_or_cancel(
		loop, task, timeout, default=..., cancel_suppress=None ):
	task = loop.create_task(task)
	try: return await asyncio.wait_for(task, timeout)
	except asyncio.TimeoutError as err:
		task.cancel()
		with contextlib.suppress(
			asyncio.CancelledError, *(cancel_suppress or list()) ): await task
		if default is ...: raise err
		else: return default



### Main eventloop

class PollerError(Exception): pass

StoreSpec = namedtuple('StoreSpec', 'table columns')
DataEntry = namedtuple('DataEntry', 'host ts cmd_id column_data')


class ConsolePollerDB:

	pool = None

	def __init__(self, loop, queue, conf, cmd_store):
		self.loop, self.conf, self.q, self.cmd_store = loop, conf, queue, cmd_store
		self.log = get_logger('poller.db')

	def convert_type(self, col_name, col_type, entry=None, value=None):
		if col_type in ['int', 'float', 'real', 'varchar', 'text', 'char']:
			if not value: value = None
			elif col_type == 'int': value = int(value)
			elif col_type in ['float', 'real']: col_type, value = 'real', int(value)
			else: value = str(value).encode()
			return col_type, value
		if col_type in ['host', 'time']:
			if not entry: value = None
			elif col_type == 'host': value = entry.host
			elif col_type == 'time': value = entry.ts
			return {'host': 'varchar', 'time': 'int'}[col_type], value
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
				' EXISTS {} (\n{}\n)'.format(table, ',\n'.join(col_specs)) )
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
				self.log.debug('Processing {} data entries', len(entries))
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
			assert cmd_opts['command']
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
		try: await asyncio.gather(*tasks)
		except asyncio.CancelledError: pass
		except Exception as err:
			self.log.exception('Fatal ConsolePollerDaemon error: {}', err)
		for task in tasks:
			if not task.done(): task.cancel()
			with contextlib.suppress(asyncio.CancelledError): await task
		return self.success

	async def run_daemon(self):
		pollers = dict()
		for host, host_conf in self.conf.hosts.items():
			conf = self.conf.options.copy()
			conf.update(host_conf)
			self.log.debug('Initializing poller for host: {}', host)
			poller = ConsolePoller.run_task(
				self.loop, self.db_queue, host, conf, self.conf.commands )
			pollers[host] = poller.task.cpd_poller = poller
		self.log.debug('Starting ConsolePollerDaemon loop...')
		try: await asyncio.gather(*(p.task for p in pollers.values()))
		finally:
			for p in pollers.values():
				if p.task.done(): continue
				p.task.cancel()
				with contextlib.suppress(asyncio.CancelledError): await p.task


class TelnetLineReader(asyncio.StreamReader):

	def telnet_decode(self, buff):
		buff_done = list()
		while True:
			if b'\xff' in buff:
				chunk, buff = buff.split(b'\xff', 1)
			else: chunk, buff = buff, b''
			buff_done.append(chunk)
			if not buff: break
			if buff[0] == b'\xff': skip = 0 # iac escape
			elif buff[:2] == b'\xff\xfa':
				skip = buff.find(b'\xff\xf0')
				if skip == -1: break
				skip += 2
			else: skip = 2
			buff = buff[skip:]
		return b''.join(buff_done).decode(), buff

	async def telnet_readline(self):
		buff = b''
		while True:
			buff += await self.readline()
			if buff == b'': break
			try: line, buff = self.telnet_decode(buff)
			except PollerError: continue
			return line

class TelnetWriter(asyncio.StreamWriter):

	def telnet_encode(self, buff):
		return buff.replace(b'\xff', b'\xff\xff')

	def telnet_writeline(self, line):
		line = self.telnet_encode(line.encode())
		if not line.endswith(b'\r\n'): line += b'\r\n'
		self.write(line)

class TelnetConsole:

	transport = r = w = None

	def __init__(self, loop): self.loop = loop

	async def __aenter__(self): return self
	async def __aexit__(self, *err):
		if self.transport: self.transport.abort()

	async def connect(self, conn_dst, timeout=None):
		self.r = TelnetLineReader(loop=self.loop)
		conn_proto = asyncio.StreamReaderProtocol(self.r)
		self.transport, _ = await asyncio_wait_or_cancel(
			self.loop, self.loop.create_connection(lambda: conn_proto, **conn_dst),
			timeout=timeout, cancel_suppress=[socket.error] )
		self.w = TelnetWriter(self.transport, conn_proto, self.r, loop=self.loop)

	async def disconnect(self):
		if self.w:
			await self.w.drain()
			self.w.close()
		if self.transport: self.transport.close()
		self.transport = self.r = self.w = None

	async def readline(self): return await self.r.telnet_readline()
	async def writeline(self, line, flush=False):
		self.w.telnet_writeline(line)
		if flush: await self.w.drain()

	async def match(self, **re_dict):
		for k, pat in list(re_dict.items()):
			if not pat: del re_dict[k]
			elif isinstance(pat, str): re_dict[k] = re.compile(pat)
		line_buff = list()
		while True:
			line = await self.readline()
			if not line or not re_dict: return None, line, line_buff
			for k, pat in re_dict.items():
				if pat.search(line): return k, line, line_buff
				else: line_buff.append(line)


class ConsolePoller:

	@classmethod
	def run_task(cls, loop, *args, **kws):
		self = cls(loop, *args, **kws)
		self.task = loop.create_task(self.run())
		return self

	def __init__(self, loop, queue, host, conf, cmds):
		self.loop, self.q, self.conf, self.cmds = loop, queue, conf, cmds
		self.host, self.host_cached, self.access_type = host, None, None
		self.log = get_logger('poller.host.{}'.format(host.replace('_', '__').replace('.', '_')))

	def host_resolve_cache(self):
		if not self.conf.cache.get('address'):
			self.host_cached = False
			return
		sock_proto = socket.IPPROTO_TCP
		try:
			addrinfo = socket.getaddrinfo( self.host,
				self.conf.telnet.port, proto=sock_proto, type=socket.SOCK_STREAM )
			if not addrinfo:
				self.log.debug('getaddrinfo - no address found for hostname {!r}', self.host)
				return
		except (socket.gaierror, socket.error) as err:
			self.log.debug('getaddrinfo - failed to'
				' resolve address for hostname {!r}: {}', self.host, err)
			return
		sock_af, _, _, _, sock_addr = addrinfo[0]
		self.host_cached = dict( host=sock_addr[0],
			port=sock_addr[1], family=sock_af, proto=sock_proto )

	async def run(self):
		self.log.debug('Starting ConsolePoller task...')
		ts_next_poll = self.loop.time()
		if (self.conf.get('poll_initial_jitter') or 0) > 0:
			ts_next_poll += random.random() * self.conf.poll_initial_jitter
		while True:
			delay = max(0, ts_next_poll - self.loop.time())
			if delay: await asyncio.sleep(delay)
			ts = self.loop.time()
			while ts_next_poll <= ts: ts_next_poll += self.conf.poll_interval

			for at in 'telnet', 'ssh':
				if self.access_type in [None, at]:
					data_entries = await getattr(self, f'run_poll_{at}')()
					if data_entries is not None:
						if self.conf.cache.access_type: self.access_type = at
						break
			else: continue
			for entry in data_entries or list(): self.q.put_nowait(entry)


	async def run_poll_telnet(self):
		if self.host_cached is None: self.host_resolve_cache()
		conn_dst = self.host_cached or dict(
			host=host, port=self.conf.telnet_port, proto=socket.IPPROTO_TCP )

		data = list()
		async with TelnetConsole(self.loop) as console:
			self.log.debug('[poll-status] connecting...')

			err_msg = None
			try: await console.connect(conn_dst, timeout=self.conf.timeout.connect)
			except asyncio.TimeoutError as err: err_msg = 'timed-out'
			except socket.error as err: err_msg = err
			if err_msg:
				return self.log.info( 'Telnet connection failed'
					' (host={}, port={}): {}', conn_dst['host'], conn_dst['port'], err_msg )

			user, password, shell = self.conf.user, self.conf.password, False
			if user and password:
				self.log.debug('[poll-status] auth...')
				err_msg = None
				try:
					await asyncio_wait_or_cancel( self.loop,
						self.run_poll_telnet_auth(console, user, password), self.conf.timeout.auth )
				except asyncio.TimeoutError: err_msg = 'timed-out'
				except PollerError as err: err_msg = f'failed (repeated {err} prompt)'
				if err_msg:
					return self.log.info( 'Telnet authentication {}'
						' (host={}, port={})', err_msg, conn_dst['host'], conn_dst['port'] )
				shell = True # is matched to confirm auth

			for cmd_id, cmd in enumerate(self.cmds):
				try:
					if not shell:
						m, line, line_buff = await asyncio_wait_or_cancel(
							self.loop, console.match(shell=self.conf.telnet.re_shell), self.conf.timeout.shell )
					else: shell = False
					self.log.debug('[poll-status] command #{}: {!r}', cmd_id, cmd['command'])
					await asyncio_wait_or_cancel( self.loop,
						console.writeline(cmd['command'], flush=True), self.conf.timeout.shell )
					if cmd.get('store_line'):
						entry = await asyncio_wait_or_cancel(
							self.loop, console.readline(), self.conf.timeout.data )
						self.log.debug('[poll-status] got data for #{}: {!r}', cmd_id, entry)
						data.append(DataEntry(self.host, time.time(), cmd_id, entry.split()))
				except asyncio.TimeoutError:
					return self.log.info( 'Telnet command timed-out'
						' (host={}, port={})', conn_dst['host'], conn_dst['port'] )

			await console.disconnect()

		return data

	async def run_poll_telnet_auth(self, console, user, password):
		while True:
			m, line, line_buff = await console.match(
				user=self.conf.telnet.re_login,
				password=self.conf.telnet.re_password,
				auth_done=self.conf.telnet.re_shell )
			if m == 'user':
				if user:
					await console.writeline(user)
					user = None
				else: raise PollerError('login')
			elif m == 'password':
				if password:
					await console.writeline(password)
					password = None
				else: raise PollerError('password')
			elif m == 'auth_done': break
			if not self.conf.telnet.re_shell and not (user or password): break


	async def run_poll_ssh(self):
		return


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

	parser.add_argument('-i', '--poll-interval', type=float, metavar='seconds',
		help='Default interval between running batches of commands on hosts.'
			' Overrides corresponding configuration file setting, including per-host settings.')
	parser.add_argument('-j', '--poll-initial-jitter', type=float, metavar='seconds',
		help='Override for options.poll_initial_jitter value, same as --poll-interval above.')

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

	conn_opts_cli = dict()
	if opts.poll_interval is not None:
		conn_opts_cli['poll_interval'] = opts.poll_interval
	if opts.poll_initial_jitter is not None:
		conn_opts_cli['poll_initial_jitter'] = opts.poll_initial_jitter
	for conn_opts in [conf.options] + list(conf.hosts.values()):
		conn_opts.update(conn_opts_cli)

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
