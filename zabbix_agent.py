import socket
import struct
import sys

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

if __name__ == '__main__':
    print zabbix_get(host='phoenix-091.nym1.placeiq.net', port=10050, key='agent.version')
