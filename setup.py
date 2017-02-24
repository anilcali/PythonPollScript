from setuptools import setup, find_packages
import os, sys

# Error-handling here is to allow package to be built w/o README included
try:
	readme = open(os.path.join(
		os.path.dirname(__file__), 'README.rst' )).read()
except IOError: readme = ''

setup(

	name = 'console-poller',
	version = '17.2.24',
	author = 'Mike Kazantsev',
	author_email = 'mk.fraggod@gmail.com',
	# license = 'MIT',
	# keywords = [],

	url = 'https://github.com/anilcali/PythonPollScript',

	description = 'Python3/asyncio-based tool to poll'
		' data via commands to remote ssh/telnet consoles',
	long_description = readme,

	# classifiers = [],
	install_requires = ['PyYAML'],

	py_modules = ['console_poller', 'console_poller_tui'],
	data_files=[('share/doc/console-poller', ['console_poller.yaml'])],
	include_package_data = True,

	entry_points = dict(console_scripts=[
		'console-poller=console_poller:main',
		'console-poller-tui=console_poller_tui:main', ]) )
