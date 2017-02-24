================
 console-poller
================

Python3/asyncio-based tool to poll data via commands to remote ssh/telnet consoles.

Two main components are poller tool itself (console_poller.py), which can work
in two modes - poller daemon and interactive readline-based querying interface,
and console_poller_tui.py alternative ncurses-based tui querying interface.

.. contents::
  :backlinks: none



Installation
------------

It's a regular package for Python 3.6+, just not on PyPI.

Best way to install the tool is via pip_ (comes with python),
using **any of** the following commands::

  % pip install git+https://github.com/anilcali/PythonPollScript/
  % pip install https://github.com/anilcali/PythonPollScript/archive/master.tar.gz

(add --user option to install into $HOME for current user only)

After that, run::

  % console-poller -h
  % console-poller-tui -h

Both should work and show usage info, indicating that tool is installed
correctly and is ready to use.

When installing via pip_, example configuration file (console_poller.yaml from
this repo) should be installed to either /usr/share/doc/console-poller or its
~/.local/ equivalent (if installed via ``pip install --user``).

More info on python packaging can be found at `packaging.python.org`_.

Script requirements/dependencies (py modules get installed by pip automatically):

- Python 3.6+
- `PyYAML <http://pyyaml.org/>`_

.. _pip: http://pip-installer.org/
.. _packaging.python.org: https://packaging.python.org/installing/



Usage
-----

Small term-screencast demo video (1:30, ~1.2M):
`doc/console-poller-demo.2017-02-24.mp4
<https://raw.githubusercontent.com/anilcali/PythonPollScript/master/doc/console-poller-demo.2017-02-24.mp4>`_

When installed via pip, with config file in poller.yaml, can be started like this::

  % console-poller --debug -c poller.yaml
  2017-02-21 16:28:29 :: main DEBUG :: Starting main eventloop...
  2017-02-21 16:28:29 :: asyncio DEBUG :: Using selector: EpollSelector
  2017-02-21 16:28:29 :: poller.db DEBUG :: Starting ConsolePollerDB task (commit_delay: 0.50s)...
  2017-02-21 16:28:29 :: poller.daemon DEBUG :: Initializing poller for host: localhost
  2017-02-21 16:28:29 :: poller.daemon DEBUG :: Initializing poller for host: cane
  2017-02-21 16:28:29 :: poller.daemon DEBUG :: Starting ConsolePollerDaemon loop...
  2017-02-21 16:28:29 :: poller.host.localhost DEBUG :: Starting ConsolePoller task...
  2017-02-21 16:28:29 :: poller.host.cane DEBUG :: Starting ConsolePoller task...
  2017-02-21 16:28:29 :: poller.host.localhost DEBUG :: [telnet] Connecting...
  2017-02-21 16:28:29 :: poller.host.cane DEBUG :: [telnet] Connecting...
  2017-02-21 16:28:29 :: poller.host.localhost INFO :: [telnet] Connection failed (host=127.0.0.1, port=23): [Errno 111] Connect call failed ('127.0.0.1', 23)
  2017-02-21 16:28:29 :: poller.host.localhost DEBUG :: [ssh] Starting ssh subprocess: ['ssh', 'ssh-test@127.0.0.1', '-qT', '-oStrictHostKeyChecking=no', '-oUserKnownHostsFile=/dev/null']
  2017-02-21 16:28:29 :: poller.host.cane DEBUG :: [telnet] Auth...
  2017-02-21 16:28:29 :: poller.host.cane DEBUG :: [telnet] Sending command #0: 'echo val-1 val-2 val-3 444'
  2017-02-21 16:28:29 :: poller.host.localhost DEBUG :: [ssh] Sending command #0: 'echo val-1 val-2 val-3 444'
  2017-02-21 16:28:29 :: poller.host.cane DEBUG :: [telnet] Data for command #0: '1 2 3 4 5 6\n'
  2017-02-21 16:28:29 :: poller.host.cane DEBUG :: [telnet] Sending command #1: 'shutdown -r'
  2017-02-21 16:28:29 :: poller.host.localhost DEBUG :: [ssh] Data for command #0: 'val-1 val-2 val-3 444\n'
  2017-02-21 16:28:29 :: poller.host.localhost DEBUG :: [ssh] Sending command #1: 'shutdown -r'
  2017-02-21 16:28:29 :: poller.host.localhost DEBUG :: [ssh] Stopping ssh subprocess...
  2017-02-21 16:28:29 :: poller.host.localhost ERROR :: [ssh] Subprocess for '127.0.0.1' exited with error code 1
  2017-02-21 16:28:30 :: poller.db DEBUG :: Processing 2 data entries
  ^C
  2017-02-21 16:28:31 :: main DEBUG :: Waiting for async generators to finish...
  2017-02-21 16:28:31 :: main DEBUG :: Finished

More than one config file can be specified (e.g. ``-c base.yaml -c
production.yaml -c hosts1.yaml -c more-hosts.yaml``), with non-mapping values in
each shadowing/overriding values from previous ones.

Options like ``--debug -i2 -j0`` can be used to quickly test rapid-polling all
hosts, overriding intervals/jitter from the config.

To query collected data interactively via readline-based interface::

  % console-poller --debug -c poller.yaml -q
  2017-02-21 16:30:56 :: query DEBUG :: Initializing ConsolePollerDB...
  Enter TableName and ColumnName:
  > table1
  table1 column1   table1 column2   table1 column3   table1 column4   table1 hostname  table1 time
  > table1 column1

  Enter column value (empty - return to table/column selection):
  > val-1

  1 matching result(s) found
  val-1 val-2 val-3 444 localhost 1487676509.854319

  Enter column value (empty - return to table/column selection):
  > %

Additional console_poller_tui.py script can be used as an alternative
interactive interface to "table -> column -> values" sqlite data hierarchy,
without excessive typing that -q/--query option requires.

It's entirely separate from console_poller.py and yaml config and should work on
any sqlite db, e.g.: ``console-poller-tui poller.sqlite``

More info on various command-line options for either script can be displayed by
running each with -h/--help option.
