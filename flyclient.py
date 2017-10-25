#! /user/bin/env python
# -*- coding=utf-8 -*-

'''
* author: panyuan 
* email: 376105482@qq.com
'''
import re
import sys
sys.path.append('../')
import traceback
import time
import socket
import msgpack
from io import BytesIO
from quant_backend.models import *


class FlyClient(object):
  
    def __init__(self, host="localhost", port=9999, timeout=30):
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection.settimeout(timeout) 
        self.connection.connect((host, port))

        self.buffer = BytesIO()
        self.read_length = 0
        self.write_length = 0

    def read_response(self, connection):
        try:
            data = ''
            while True:
                buffer = connection.recv(10000)
                self.write_length += len(buffer)
                if isinstance(buffer, bytes) and len(buffer) == 0:
                    raise socket.error("server closed connection")
                self.buffer.write(buffer)
                while self.read_length != self.write_length:
                    self.buffer.seek(self.read_length)
                    buffer = self.buffer.read()
                    if buffer.endswith('\\r\\n'):
                        data += buffer[:-4]
                        return data
                    else:
                        data += buffer
                        self.read_length += len(buffer)


        except Exception as e:
            print traceback.print_exc()


    def get(self, key):
        key = "G{}".format(key)
        return self.command(key)

    def delete(self, key):
        key = "D{}".format(key)
        return self.command(key)

    def save(self, key, data):
        key = "S{}\r\t".format(key)
        byte = bytearray()
        byte.extend(key)
        data = msgpack.packb(data)
        byte.extend(data)
        return self.command(byte)

    def format(self, col):
        for key, forkey in {'=':'?=?', '>':'?>?', '>=':'?>=?', '<':'?<?', '<=':'?<=?'}.iteritems():
            col = col.replace(key, forkey)
        return col
    
    def filter(self, table, columns, where=None, order=None):
        #table 表名
        #columns 字段名","隔开
        #where 多个条件","隔开 例如：pe<=1,pe>=0
        #order 1为从小到大，0为从大到小 例如：pe=1,pe=0
        key = "Fs{}|f{}".format(columns, table)
        if where:
        	key += "|w{}".format(self.format(where))
        if order:
        	key += "|o{}".format(order.replace("=", "?"))
        return self.command(key)
    
    def command(self, key):
        key += "\r\r\r\n"
        self.buffer.seek(0)
        self.buffer.truncate()
        self.read_length = 0
        self.write_length = 0
        self.connection.sendall(key)
        data = self.read_response(self.connection)
        return msgpack.unpackb(data)


if __name__ == '__main__':
    cache = FlyClient()
    '''
    df = pd.read_sql("select *  from valuation_201709 where date='2017-09-08'", engine)
    df = df.fillna(1) 
    del df['date']
    del df['create_time']
    del df['update_time']
    del df['ipo_date']
    #df = df.tail(1200)
    #print list(df['pe'])
    #df = df.head(32)
    plist =  df.to_dict(orient='list')
    '''
    now = time.time()
    #print plist
    
    #plist = {'pe':[1,2,3,3,4,5,3,1,8],'pb':[2,3,4,5,6,7,1,1,1]}
    
    #print "debug:", cache.save("valuation_2017-01-07", plist)
    
    #data = cache.get("valuation_2017-01-07")
    #for i in xrange(10000):
    print cache.filter("valuation_2017-01-07", "pb", "pb<100,pb>1", "pb=1")
    #print len(data['pe'])
    #print cache.save("test", [1,2,3,4])
    #"Fspe,pb|fvaluation_2017-01-02|wpe?>=?0,pb?<=?4|ope?0\n"
    '''
    for i in range(1000000):
        #cache.get("test")
        data = cache.get("valuation_2017-08-25")
        #data = cache.filter("valuation_2017-08-25", "pe", "pe>1", "pe=1")
    '''
    print time.time() - now
