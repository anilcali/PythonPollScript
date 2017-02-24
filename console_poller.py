#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from collections import namedtuple, Mapping, ChainMap, OrderedDict
import sqlite3, asyncio, asyncio.subprocess, socket, signal, contextlib
import os, sys, logging, pathlib, time, re, random, tempfile
import readline, shutil # for querying ui

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

class LogPrefixAdapter(LogStyleAdapter):
	def __init__(self, logger, prefix=None, prefix_raw=False, extra=None):
		if isinstance(logger, str): logger = get_logger(logger)
		if isinstance(logger, logging.LoggerAdapter): logger = logger.logger
		super(LogPrefixAdapter, self).__init__(logger, extra or {})
		if not prefix: prefix = get_uid()
		if not prefix_raw: prefix = '[{}] '.format(prefix)
		self.prefix = prefix
	def process(self, msg, kws):
		super(LogPrefixAdapter, self).process(msg, kws)
		return ('{}{}'.format(self.prefix, msg), kws)

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

def log_err_or_exc(log, exc_info=None):
	log_debug = log.isEnabledFor(logging.DEBUG)
	func = log.exception if log_debug else log.error
	if log_debug and exc_info: func = ft.partial(func, exc_info=exc_info)
	return func

def repr_err(err, err_type=None, err_str=False):
	if not err_type: err_type = err.__class__
	if err_str: err = repr(str(err))
	return f'[{err_type.__name__}] {err}'



### Main poller eventloop components

class PollerError(Exception): pass

StoreSpec = namedtuple('StoreSpec', 'table columns')
DataEntry = namedtuple('DataEntry', 'host ts cmd_id column_data')


def get_db_schema_for_commands(conf_commands):
	cmd_store = dict()
	for cmd_id, cmd_opts in enumerate(conf_commands):
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
	return cmd_store


class ConsolePollerDB:

	pool = None

	def __init__(self, conf, cmd_store, loop=None, queue=None):
		self.loop, self.conf, self.q, self.cmd_store = loop, conf, queue, cmd_store
		self.log = get_logger('poller.db')

	def convert_type(self, col_name, col_type, entry=None, value=None):
		if col_type in ['int', 'float', 'real', 'varchar', 'text', 'char']:
			if not value: value = None
			elif col_type == 'int': value = int(value)
			elif col_type in ['float', 'real']: col_type, value = 'real', float(value)
			else: value = str(value).encode()
			return col_type, value
		if col_type in ['host', 'time']:
			if not entry: value = None
			elif col_type == 'host': value = entry.host
			elif col_type == 'time': value = entry.ts
			return {'host': 'varchar', 'time': 'int'}[col_type], value
		raise ConfigError(f'Unknown db column type: {col_type} (column: {col_name})')

	def __enter__(self):
		self.init()
		return self
	def __exit__(self, *err): self.close()

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

	def query(self, table, fields='*', raw_where=None, column=None, value=None, chk='='):
		params = list()
		if not isinstance(fields, str): fields = ', '.join(fields)
		if not raw_where and column:
			raw_where = '{} {} ?'.format(column, chk)
			params.append(value)
		if not re.search(r'^\s*where\b', raw_where.lower()): raw_where = f'WHERE {raw_where}'
		with contextlib.closing(self.db.cursor()) as cur:
			cur.execute('SELECT {} FROM {} {}'.format(fields, table, raw_where or ''), params)
			return cur.fetchall()

	async def run(self):
		commit_delay = self.conf.get('commit_delay') or 0
		self.log.debug( 'Starting ConsolePollerDB'
			' task (commit_delay: {:.2f}s)...', commit_delay )
		while True:
			entries = [await self.q.get()]
			if commit_delay > 0: await asyncio.sleep(commit_delay)
			while True: # try to commit in as large batches as possible
				try: entries.append(self.q.get_nowait())
				except asyncio.QueueEmpty: break
			try:
				self.log.debug('Processing {} data entries', len(entries))
				for entry in entries:
					if entry is StopIteration: return
					entry_data, store_spec = OrderedDict(), self.cmd_store[entry.cmd_id]
					for (col_name, spec), value_raw in it.zip_longest(
							store_spec.columns.items(), entry.column_data ):
						try: col_type, value = self.convert_type(col_name, spec, entry, value_raw)
						except (TypeError, ValueError) as err:
							log_err_or_exc(self.log)(
								'Unable to convert value for column {!r} (table={}),'
									' to db type ({}, {}) discarding whole entry: {}',
								col_name, store_spec.table, spec, repr_err(err, err_str=True), entry )
							break
						if value is None:
							self.log.error( 'Missing value for column {!r} (type={}, table={}),'
								' discarding whole entry: {}', col_name, spec, store_spec.table, entry )
							break
						entry_data[col_name] = value
					else:
						self.db.execute(
							'INSERT INTO {} ({}) VALUES ({})'.format( store_spec.table,
								', '.join(entry_data.keys()), ', '.join(['?']*len(entry_data)) ),
							list(entry_data.values()) )
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
		cmd_store = get_db_schema_for_commands(self.conf.commands)
		self.db_queue = asyncio.Queue()
		self.db = ConsolePollerDB( self.conf.database,
			cmd_store, loop=self.loop, queue=self.db_queue )
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
			conf.maps.insert(0, host_conf)
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

	def __init__(self, loop, log):
		self.loop, self.log = loop, log

	async def __aenter__(self): return self
	async def __aexit__(self, err_t, err, err_tb):
		if self.transport: self.transport.abort()
		if err is not None:
			err_msg = None
			if issubclass(err_t, socket.error): err_msg = repr_err(err, err_t)
			elif issubclass(err_t, asyncio.TimeoutError): err_msg = 'timed-out'
			if err_msg:
				log_err_or_exc(self.log, (err_t, err, err_tb))('Connection error: {}', err_msg)
				return True # won't be propagated

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


class SSHConsole:

	proc = askpass = None

	def __init__(self, loop, log):
		self.loop, self.log = loop, log

	async def __aenter__(self): return self
	async def __aexit__(self, *err):
		if self.askpass:
			with contextlib.suppress(OSError): os.unlink(self.askpass)
		if self.proc and self.proc.returncode is None:
			with contextlib.suppress(OSError): self.proc.kill()
			await self.proc.wait()

	async def start(self, cmd, password):
		fd, self.askpass = tempfile.mkstemp(suffix='.sh', prefix='.console-poller.ssh-askpass.')
		with os.fdopen(fd, 'w') as dst:
			dst.write('#!/bin/bash\necho \'{}\'\n'.format((password or '').replace("'", "'\\''")))
		os.chmod(self.askpass, 0o700)
		self.proc = await asyncio.create_subprocess_exec( *cmd,
			env=dict(DISPLAY=':1', SSH_ASKPASS=self.askpass),
			stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.DEVNULL, preexec_fn=os.setsid )

	async def stop(self, timeout=None):
		if not self.proc: return
		self.proc.stdin.close()
		if self.proc.returncode is None:
			ts_max = self.loop.time() + (timeout or 20)
			get_timeout = lambda: max(0.01, ts_max - self.loop.time())
			try:
				await asyncio_wait_or_cancel(
					self.loop, self.proc.wait(), min(2.0, get_timeout()) )
			except asyncio.TimeoutError:
				with contextlib.suppress(OSError): # no such pid
					self.proc.terminate()
					try:
						await asyncio_wait_or_cancel(
							self.loop, self.proc.wait(), get_timeout() )
					except asyncio.TimeoutError: self.proc.kill()
			if self.proc.returncode is None: await self.proc.wait() # must exit after kill
		exit_code, self.proc = self.proc.returncode, None
		return exit_code

	def writeline(self, line): self.proc.stdin.write(f'{line}\n'.encode())
	async def readline(self): return (await self.proc.stdout.readline()).decode()


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

	async def host_resolve_cache(self):
		if not self.conf.cache.get('address'):
			self.host_cached = False
			return
		sock_proto = socket.IPPROTO_TCP
		try:
			addrinfo = await self.loop.getaddrinfo( self.host,
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
		if self.host_cached is None: await self.host_resolve_cache()
		conn_dst = self.host_cached or dict(
			host=self.host, port=self.conf.telnet_port, proto=socket.IPPROTO_TCP )
		log = LogPrefixAdapter(self.log, 'telnet')

		data = None
		async with TelnetConsole(self.loop, log) as console:
			log.debug('Connecting to host={!r} port={!r}...', conn_dst['host'], conn_dst['port'])

			err_msg = None
			await console.connect(conn_dst, timeout=self.conf.timeout.connect)

			user, password, shell = self.conf.user, self.conf.password, False
			if user and password:
				log.debug('Auth...')
				err_msg = None
				try:
					await asyncio_wait_or_cancel( self.loop,
						self.run_poll_telnet_auth(console, user, password), self.conf.timeout.auth )
				except PollerError as err:
					return log.info('Authentication failed: {}', err)
				shell = True # is matched to confirm auth

			data = list()
			for cmd_id, cmd in enumerate(self.cmds):
				if not shell:
					m, line, line_buff = await asyncio_wait_or_cancel(
						self.loop, console.match(shell=self.conf.telnet.re_shell), self.conf.timeout.shell )
				else: shell = False
				log.debug('Sending command #{}: {!r}', cmd_id, cmd['command'])
				await asyncio_wait_or_cancel( self.loop,
					console.writeline(cmd['command'], flush=True), self.conf.timeout.shell )
				if cmd.get('store_line'):
					entry = await asyncio_wait_or_cancel(
						self.loop, console.readline(), self.conf.timeout.data )
					if not entry:
						raise socket.error(0, f'Connection closed in response to command #{cmd_id}')
					log.debug('Data for command #{}: {!r}', cmd_id, entry)
					data.append(DataEntry(self.host, time.time(), cmd_id, entry.split()))

			await console.disconnect()

		return data

	async def run_poll_telnet_auth(self, console, user, password):
		while True:
			m, line, line_buff = await console.match(
				user=self.conf.telnet.re_login,
				password=self.conf.telnet.re_password,
				auth_done=self.conf.telnet.re_shell )
			if not m: raise PollerError('connection closed')
			elif m == 'user':
				if user:
					await console.writeline(user)
					user = None
				else: raise PollerError('repeated login prompt')
			elif m == 'password':
				if password:
					await console.writeline(password)
					password = None
				else: raise PollerError('repeated password prompt')
			elif m == 'auth_done': break
			if not self.conf.telnet.re_shell and not (user or password): break


	async def run_poll_ssh(self):
		if self.host_cached is None: await self.host_resolve_cache()
		if self.host_cached:
			conn_dst = self.host_cached['host']
			if self.host_cached['family'] == socket.AF_INET6: conn_dst = f'[{conn_dst}]'
		else: conn_dst = self.host
		log = LogPrefixAdapter(self.log, 'ssh')

		data = None
		async with SSHConsole(self.loop, log) as console:
			cmd = self.conf.ssh.opts
			if isinstance(cmd, str): cmd = cmd.split()
			cmd = [self.conf.ssh.binary, f'{self.conf.user}@{conn_dst}', *cmd]
			log.debug('Starting ssh subprocess: {}', cmd)
			await console.start(cmd, self.conf.password)

			data = list()
			for cmd_id, cmd in enumerate(self.cmds):
				try:
					log.debug('Sending command #{}: {!r}', cmd_id, cmd['command'])
					console.writeline(cmd['command'])
					if cmd.get('store_line'):
						entry = await asyncio_wait_or_cancel(
							self.loop, console.readline(), self.conf.timeout.data )
						if not entry:
							return log.info('Connection closed in response to command #{}', cmd_id)
						log.debug('Data for command #{}: {!r}', cmd_id, entry)
						data.append(DataEntry(self.host, time.time(), cmd_id, entry.split()))
				except asyncio.TimeoutError:
					return log.info('Command #{} timed-out', cmd_id, conn_dst)

			log.debug('Stopping ssh subprocess...')
			exit_code = await console.stop(timeout=self.conf.timeout.kill)
			if exit_code != 0:
				log.error('Subprocess for {!r} exited with error code {}', conn_dst, exit_code)

		return data



### Interactive db-querying UI

class ReadlineQuery:

	prompt = '> '

	def log_debug_errors(func):
		@ft.wraps(func)
		def _wrapper(self, *args, **kws):
			try: return func(self, *args, **kws)
			except Exception as err:
				if not self.log.isEnabledFor(logging.DEBUG): raise
				self.log.exception('readline callback error: {}', err)
		return _wrapper

	def __init__(self):
		self.opts, self.log = list(), get_logger('readline')

	def __enter__(self):
		self.init()
		return self
	def __exit__(self, *err): pass

	def init(self):
		readline.set_completer_delims('')
		readline.set_completer(self.rl_complete)
		readline.parse_and_bind('tab: complete')

	@log_debug_errors
	def rl_complete(self, text, state):
		if state == 0:
			if not text: self.matches = self.opts[:]
			else: self.matches = list(s for s in self.opts if s and s.startswith(text))
		try: return self.matches[state]
		except IndexError: return None

	def input(self, query, options=None):
		self.opts.clear()
		if options: self.opts.extend(options)
		print(query)
		return input(self.prompt)


def db_interactive_query(conf, cmd_store):
	log = get_logger('query')
	log.debug('Initializing ConsolePollerDB...')
	with ConsolePollerDB(conf, cmd_store) as db, ReadlineQuery() as rlq:

		table_col_opts, table_col_check = list(), dict()
		for spec in sorted(cmd_store.values(), key=op.attrgetter('table')):
			for col_name, col_type in sorted(spec.columns.items()):
				option = f'{spec.table} {col_name}'
				table_col_opts.append(option)
				table_col_check[option.lower()] = col_type

		with contextlib.suppress(EOFError, KeyboardInterrupt):
			while True:
				while True:
					table_col = rlq.input('Enter TableName and ColumnName:', table_col_opts)
					table_col = ' '.join(table_col.strip().split(None, 1)).lower()
					if table_col in table_col_check: break
					if table_col: print(f'ERROR: Non-existent table/column spec: {table_col!r}\n')
				print()
				(table, col_name), col_type = table_col.split(None, 1), table_col_check[table_col]

				while True:
					while True:
						value = rlq.input( 'Enter column value'
							' (empty - return to table/column selection):' ).strip()
						if not value: break
						try:
							col_type, value = db.convert_type(col_name, col_type, None, value)
							if value is None: raise ValueError
						except:
							print(f'ERROR: Unable to convert specified value to type {col_type!r}: {value!r}\n')
							continue
						break
					print()
					if not value: break

					query = dict(column=col_name, value=value)
					if isinstance(value, bytes):
						query.update(value=f'%{value.decode()}%'.encode(), chk='LIKE')
					rows = db.query(table=table, **query)
					print(f'{len(rows)} matching result(s) found')
					for row in rows:
						print(' '.join((str(v) if not isinstance(v, bytes) else v.decode()) for v in row))
					print()



### Main non-interactive CLI and entry point

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

	parser.add_argument('-q', '--query',
		const=True, nargs='?', metavar='db-path',
		help='Interactively query data from the database, either specified'
			' in config file (see -c/--conf option) or as an argument to this option.')

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

	if (conf.database.get('path') or 'auto') == 'auto':
		p = pathlib.Path(conf_paths[-1])
		conf.database.path = str(p.parent / (p.stem + '.sqlite'))

	if opts.query:
		if opts.query is not True: conf.database.path = opts.query
		cmd_store = get_db_schema_for_commands(conf.commands)
		return db_interactive_query(conf.database, cmd_store)

	conn_opts_cli = dict()
	if opts.poll_interval is not None:
		conn_opts_cli['poll_interval'] = opts.poll_interval
	if opts.poll_initial_jitter is not None:
		conn_opts_cli['poll_initial_jitter'] = opts.poll_initial_jitter
	for conn_opts in [conf.options] + list(conf.hosts.values()):
		conn_opts.update(conn_opts_cli)

	log.debug('Starting main eventloop...')
	with contextlib.closing(asyncio.get_event_loop()) as loop:
		success = loop.run_until_complete(ConsolePollerDaemon.run_async(loop, conf))
		log.debug('Waiting for async generators to finish...')
		loop.run_until_complete(loop.shutdown_asyncgens())
	log.debug('Finished')
	return int(not success)

if __name__ == '__main__': sys.exit(main())
