#!/usr/bin/env python
import socket
import struct
import sys
import json
import fnmatch

def str2packed(data):
    header_field =  struct.pack('<4sBQ', 'ZBXD', 1, len(data))
    return header_field + data

def packed2str(packed_data):
    try:
        header, version, length = struct.unpack('<4sBQ', packed_data[:13])
    except:
        return None
    (data, ) = struct.unpack('<%ds'%length, packed_data[13:13+length])
    return data

def zabbix_get(**args):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((args['host'], args['port']))
    s.sendall(str2packed(args['key']))

    data = ''
    while True:
        buff = s.recv(1024)
        if not buff:
            break
        data += buff

    response = packed2str(data)

    s.close()
    return response

def get(host, key, var, _match, _second_key, op):
    data =  json.loads(zabbix_get(host=host, port=10050, key=key))['data']
    out = []
    for d in data:
	if d.has_key(var) and fnmatch.fnmatch(d[var], _match):
	    out.append(float(zabbix_get(host=host, port=10050, key=_second_key.format(d[var]))))
    if op == 'sum':
	return '{0:.2f}'.format(sum(out))
    elif op == 'avg':
	return '{0:.2f}'.format(reduce(lambda x, y: x + y, out) / float(len(out)))
    elif op == 'count':
	return '{0:.2f}'.format(float(len(out)))
    elif op == 'max':
        return '{0:.2f}'.format(float(max(out)))
    elif op == 'min':
     	return '{0:.2f}'.format(float(min(out)))
    elif op == 'delta':
        return '{0:.2f}'.format(float(max(out)-min(out)))
    return 0.0


if __name__ == '__main__':
    print get('phoenix-091.nym1.placeiq.net', 'vfs.fs.discovery', '{#FSNAME}', '/data/*', 'vfs.fs.size[{0},free]', 'sum')
    print get('phoenix-091.nym1.placeiq.net', 'vfs.fs.discovery', '{#FSNAME}', '/data/*', 'vfs.fs.size[{0},pfree]', 'avg')
    print get('phoenix-091.nym1.placeiq.net', 'vfs.fs.discovery', '{#FSNAME}', '/data/*', 'vfs.fs.size[{0},pfree]', 'min')
    print get('phoenix-091.nym1.placeiq.net', 'vfs.fs.discovery', '{#FSNAME}', '/data/*', 'vfs.fs.size[{0},pfree]', 'max')
    print get('phoenix-091.nym1.placeiq.net', 'vfs.fs.discovery', '{#FSNAME}', '/data/*', 'vfs.fs.size[{0},pfree]', 'delta')
