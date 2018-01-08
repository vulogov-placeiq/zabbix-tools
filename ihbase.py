#!/usr/bin/env python
import sys
import argparse
import time
import random
import requests
import json
import base64
import urllib2
from urlparse import urlparse

class Client:
	headers = {
	    'Accept': 'application/json',
	    'Content-Type': 'application/json',
	}

	session = requests.session()

	def __init__(self, url):
		parsed = urlparse(url)
		self.url = '%s://%s' % (parsed.scheme, parsed.netloc)


	def version(self):
		r = self.session.get(self.url + '/version/cluster', headers=self.headers)
		return json.loads(r.text)

	def status(self):
		r = self.session.get(self.url + '/status/cluster', headers=self.headers)
		return json.loads(r.text)

	def info(self):
		r = self.session.get(self.url + '/version', headers=self.headers)
		return json.loads(r.text)

	def namespaces(self):
		r = self.session.get(self.url + '/namespaces', headers=self.headers)
		return json.loads(r.text)['Namespace']

	def namespace(self, ns):
		r = self.session.get(self.url + '/namespaces/%s' % ns, headers=self.headers)
		if r.status_code / 100 != 2:
			return
		return json.loads(r.text)

	def namespace_create(self, ns):
		r = self.session.post(self.url + '/namespaces/%s' % ns)
		return r.status_code / 100 == 2

	def namespace_tables(self, ns):
		r = self.session.get(self.url + '/namespaces/%s/tables' % ns, headers=self.headers)
		if r.status_code / 100 != 2:
			return []
		return [table['name'] for table in json.loads(r.text)['table']]

	def namespace_alter(self, ns):
		r = self.session.put(self.url + '/namespaces/%s' % ns, headers=self.headers)
		return r.status_code / 100 == 2

	def namespace_delete(self, ns):
		r = self.session.delete(self.url + '/namespaces/%s' % ns, headers=self.headers)
		return r.status_code / 100 == 2

	def tables(self):
		r = self.session.get(self.url + '/', headers={'Accept': 'application/json'})
		return [table['name'] for table in json.loads(r.text)['table']]

	def table_schema(self, name):
		r = self.session.get(self.url + '/%s/schema' % name, headers=self.headers)
		if r.status_code/100 == 2:
			return json.loads(r.text)

	def table_create(self, name, cf=None):
		if not cf or len(cf) == 0:
			raise Exception("Need at least one column family")

		data = {'name': name, 'ColumnSchema': cf}

		r = self.session.post(self.url + '/%s/schema' % name, headers=self.headers, json=data)
		return r.status_code/100 == 2

	def table_update(self, name, cf=None):
		if not cf or len(cf) == 0:
			raise Exception("Need at least one column family")

		data = {'name': name, 'ColumnSchema': cf}

		r = self.session.put(self.url + '/%s/schema' % name, headers=self.headers, json=data)
		return r.status_code/100 == 2

	def table_delete(self, name):
		r = self.session.delete(self.url + '/%s/schema' % name)
		return r.status_code/100 == 2

	def table_regions(self, name):
		r = self.session.get(self.url + '/%s/regions' % name, headers=self.headers)
		return json.loads(r.text)

	def merge_dicts(self, *dict_args):
	    result = {}
	    for dictionary in dict_args:
	        result.update(dictionary)
	    return result
	def delete(self, table, key, cf=None, ts=None):
		url = self.url + '/%s/%s' % (table, key)
		if cf:
			url += '/' + cf

		if ts:
			url += '/' + ts
		r = self.session.delete(url, headers=self.headers)
		if r.status_code/100 != 2:
			#no result
			return
		return r.text


	def scan(self, table, prefix=None, columns=None, batch_size=None, start_row=None, end_row=None, start_time=None, end_time=None, include_timestamp=None):
		data = {'batch': 1000}

		if prefix:
			encoded_prefix = base64.b64encode(prefix)
			data['startRow'] = encoded_prefix
			data['filter'] = '{"type": "PrefixFilter","value": "%s"}' % encoded_prefix

		if start_row:
			data['startRow'] = start_row

		if end_row:
			data['endRow'] = end_row

		if start_time:
			data['startTime'] = start_time

		if end_time:
			data['endTime'] = end_time

		if columns:
			data['column'] = [base64.b64encode(c) for c in columns]

		if batch_size:
			data['batch'] = batch_size

		r = self.session.put(self.url + '/%s/scanner/' % table, headers=self.headers, json=data)
		if r.status_code / 100 != 2:
			raise Exception("Error creating scanner %s" % r)

		url = r.headers['Location']

		last_row = None
		while True:
			#scroll
			r = self.session.get(url, headers=self.headers)

			#no more docs
			if r.status_code == 204:
				break

			doc = json.loads(r.text)
			total_row = len(doc['Row'])
			for i in range(total_row):
				row = doc['Row'][i]
				key, values = self.decode_row(row, include_timestamp=include_timestamp)
				full_row = {'key': key, 'values': values}

				#first row of new set
				if last_row != None:
					if last_row['key'] == full_row['key']:
						full_row['values'] = self.merge_dicts(full_row['values'], last_row['values'])
					else:
						#first row is not the continuation of the last set
						yield last_row['key'], last_row['values']

					last_row = None

				#is last row
				if i == total_row - 1:
					last_row = full_row
					break

				#yield full row
				yield full_row['key'], full_row['values']

		if last_row != None:
			yield last_row['key'], last_row['values']

		#print 'Delete'
		r = self.session.delete(url)

	def get(self, table, key, cf=None, ts=None, versions=None, include_timestamp=None):
		url = self.url + '/%s/%s' % (table, key)

		if cf:
			url += '/' + cf

		if ts:
			url += '/' + ts

		if versions:
			url += '?v=%s' % versions

		r = self.session.get(url, headers=self.headers)
		if r.status_code/100 != 2:
			#no result
			return

		row = json.loads(r.text)['Row'][0]
		return self.decode_row(row, include_timestamp=include_timestamp)

	def get_many(self, table, keys, include_timestamp=None):
		if len(keys) == 0:
			return

		url = self.url + '/%s/multiget?' % table
		for key in keys:
			url += 'row=%s&' % key


		r = self.session.get(url, headers=self.headers)
		if r.status_code/100 != 2:
			return

		doc = json.loads(r.text)
		for row in doc['Row']:
			yield self.decode_row(row, include_timestamp=include_timestamp)


	def put(self, table, values):
		if len(values) == 0:
			return

		rows = []
		for val in values:
			row = {'key': base64.b64encode(val['key']), 'Cell': []}
			for col, v in val['values'].iteritems():
				row['Cell'].append({'column': base64.b64encode(col), '$': base64.b64encode(v)})

			rows.append(row)

		data = {"Row": rows}

		r = self.session.put(self.url + '/%s/1' % table, headers=self.headers, json=data)
		return r.status_code/100 == 2

	def decode_row(self, row, include_timestamp=None):
		key = base64.b64decode(row['key'])
		values = {}
		for c in row['Cell']:
			col = base64.b64decode(c['column'])
			value = base64.b64decode(c['$'])
			values[col] = value

			if include_timestamp:
				values[col] = (value, c['timestamp'])

		return key, values

def get_from_stdin():
	buf = ""
	while True:
		buf += sys.stdin.read().strip()
		if not buf:
			break
	return buf
def get_from_url(url):
	req = urllib2.Request(url)
	response = urllib2.urlopen(req)
	value = response.read()


class iHBASE(Client):
	def _get(self):
		try:
			k, v = self.get(self.args.table, self.args.key)
		except:
			return
		if self.args.column in v.keys():
			print v[self.args.column]
	def _ls(self):
		for k,v in self.scan(self.args.table):
			if self.args.column in v.keys():
				print v[self.args.column]
	def _put(self):
		if self.args.get_value == 'args':
			val = self.args.value
		elif self.args.get_value == 'url':
			val = get_from_url(self.args.value)
		elif self.args.get_value == 'stdin':
			val = get_from_stdin()
		elif self.args.get_value == 'timestamp':
			val = str(int(time.time()))
		elif self.args.get_value == 'random':
			val = str(int(random.randint(1,1024)))
		else:
			val = self.args.value
		self.put(self.args.table, [{'key':self.args.key,
			'values':{self.args.column: val}}])
	def _delete(self):
		self.delete(self.args.table, self.args.key)
	def run(self):
		for cmd in self.args.cmd:
			cmd = cmd.lower()
			f = None
			try:
				f = getattr(self, "_%s"%cmd)
			except AttributeError:
				print "%s not found. Ignoring"%cmd
				continue
			try:
				res = apply(f, ())
			except KeyboardInterrupt:
				print "%s throws exception. Ignoring"%cmd
				continue

def main():
	parser = argparse.ArgumentParser(description='HBASE query and data admin tool.')
	parser.add_argument('-U', '--url',
		default="http://localhost:8070",
		help='URL of the HBASE REST API Endpoint')
	parser.add_argument('-T', '--table',
	 	required=True,
		help='Name of the table')
	parser.add_argument('-K', '--key',
	 	required=True,
		help='Table key')
	parser.add_argument('-C', '--column',
	 	required=True,
		help='Name of the column')
	parser.add_argument('--get-value',
		default="arg",
		help='Where we gonna get the value for the "put" command',
		choices=['arg', 'stdin', 'url', 'timestamp', 'random'])
	parser.add_argument('--value',
		help='Value for the "put" command')
	parser.add_argument('cmd',
	 	nargs="*",
		help='List of the commands')
	args = parser.parse_args()
	ihb = iHBASE(args.url)
	ihb.args = args
	ihb.run()


main()

# import time
# c = Client("http://gandalf-006.sec.placeiq.net:8070")
# print c.version()
# print c.status()
# print c.info()
# c.put('__replication_testing__', [{'key':'3','values':{'__rt__:stamp': str(time.time())}}])
# for i in c.scan("__replication_testing__"):
# 	print i
# c.put('__replication_testing__', [{'key':'2','values':{'__rt__:stamp': str(time.time())}}])
# print c.get('__replication_testing__', '2')
# print c.delete('__replication_testing__', '2')
# print c.get('__replication_testing__', '2')
