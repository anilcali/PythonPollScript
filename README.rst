================
 console-poller
================

Python3/asyncio-based tool to poll data via commands to remote ssh/telnet consoles.

.. contents::
  :backlinks: none


Requirements
------------

- Python 3.6+
- `PyYAML <http://pyyaml.org/>`_

Best way to install the latter is probably from a distro package, otherwise can
use e.g. ``pip install --user pyyaml`` (as the same uid that will be running the
daemon), or `virtualenv <https://virtualenv.pypa.io/>`_ and such.

See https://packaging.python.org/ for more info.


Usage
-----

Small term-screencast demo video (1:30, ~1.2M):
`doc/console-poller-demo.2017-02-24.mp4
<https://raw.githubusercontent.com/anilcali/PythonPollScript/master/doc/console-poller-demo.2017-02-24.mp4>`_

With both main files stored as console_poller.{py,yaml}, can be started like this::

  % ./console_poller.py --debug
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

And to then query the data::

  % ./console_poller.py --debug -q
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

There is also additional console_poller_tui.py script for simplier interactive
access to "table -> column -> values" sqlite data hierarchy, without excessive
typing that -q/--query option requires.

It's entirely separate from console_poller.py and yaml config and should work on
any sqlite db, e.g.: ``./console_poller_tui.py console_poller.sqlite``

More info on various startup options::

  % ./console_poller.py -h
  usage: console_poller.py [-h] [-c path] [-q [db-path]] [-i seconds]
                           [-j seconds] [--debug]

  Tool to poll data via commands to remote ssh/telnet consoles.

  optional arguments:
    -h, --help            show this help message and exit
    -c path, --conf path  Path to yaml configuration file(s). Can be specified
                          multiple times, to load values from multiple files,
                          with same-path values in latter ones override the
                          former. Default is to try loading file with same name
                          as the script, but with yaml extension, if it exists.
                          See default config alongside this script for the
                          general structure. Command-line options override
                          values there.
    -q [db-path], --query [db-path]
                          Interactively query data from the database, either
                          specified in config file (see -c/--conf option) or as
                          an argument to this option.
    -i seconds, --poll-interval seconds
                          Default interval between running batches of commands
                          on hosts. Overrides corresponding configuration file
                          setting, including per-host settings.
    -j seconds, --poll-initial-jitter seconds
                          Override for options.poll_initial_jitter value, same
                          as --poll-interval above.
    --debug               Verbose operation mode.

More than one config file can be specified (e.g. ``-c base.yaml -c
production.yaml -c hosts1.yaml -c more-hosts.yaml``), with non-mapping values
in each shadowing/overriding values from previous ones, going back to
default one (which is "console_poller.yaml" in the same path as script, if exists).

Options like ``--debug -i2 -j0`` can be used to quickly test rapid-polling all
hosts, overriding intervals/jitter from the config.
