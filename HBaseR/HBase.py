#!/usr/bin/env python

# A simple HBase wrapper
# -Andrew Melo

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from ThriftGlue.hbase import Hbase as HBaseThrift

import json
import httplib
import time

def query( server, url, method = None, data = None,extraHeaders = None ):
    if method == None:
        method = 'GET'
    
    headerList = { 'Accept' : 'application/json' }
    if extraHeaders:
        headerList.update( extraHeaders )

    conn = httplib.HTTPConnection(server)
    conn.request( method, url, data, headers= headerList )

    resp = conn.getresponse()
    output = resp.read()
    if resp.status != 200:
        if output:
            raise RuntimeError, "Error calling %s: %s" % (url, output )
        else:
            raise RuntimeError, "Error calling %s : data = %s" % (url, data)
         
    print "-->%s<--" % output
    if output:
        content = json.loads(output)
    else:
        content = ""
    return (content, resp)

class HBase:
    def __init__( self, server ):
        self.server = server

    def getVersion(self):
        return query( self.server, '/version' )[0]

    def listTables(self):
        retval = query( self.server, '/' )
        buffer = []
        for key in retval[0]['Table']:
            buffer.append( key['name'] )
        return buffer

    def createTable(self, tableName, schema):
        return query( self.server, '/%s/schema' % tableName, method = 'PUT', data = json.dumps(schema),
                    extraHeaders = {'Content-Type' : 'application/json'}) 

    def updateTable(self, tableName, schema):
        return query( self.server, '/%s/schema' % tableName, method = 'POST', data = json.dumps( schema ),
                    extraHeaders = {'Content-Type' : 'application/json'})

    def deleteTable( self, tableName ):
        return query( self.server, '/%s/schema' % tableName, method = 'DELETE' )[0]

    def putSingleCell( self, tableName, rowID, column, qualifier = None, data = None ):
        # I'm not sure that the qualifier is actually optional here, but the API
        # docs are unclear
        url = '/%s/%s/%s' % (tableName, rowID, column)
        if qualifier:
            url = url + ":" + qualifier
        return query( self.server, url, 'POST', data = data )

    def getSingleCell( self, tableName, rowID, column, qualifier = None ):
        # I'm not sure that the qualifier is actually optional here, but the API
        # docs are unclear
        url = '/%s/%s/%s' % (tableName, rowID, column)
        if qualifier:
            url = url + ":" + qualifier
        return query( self.server, url, 'GET' )


import unittest
class TestHBaseBasic( unittest.TestCase ):
    def setUp(self):
        self.hbase = HBase( 'localhost:8081' )

    def tearDown(self):
        if hasattr( self, 'tempTable' ):
            self.hbase.deleteTable( self.tempTable )

    def test_getVersion(self):
        retval = self.hbase.getVersion()
#{u'JVM': u'Sun Microsystems Inc. 1.6.0_20-19.0-b09', u'OS': u'Linux 2.6.18-274.el5 amd64', u'Jersey': u'1.4', u'REST': u'0.0.2', u'Server': u'jetty/6.1.26'}
        self.assertTrue( 'JVM' in retval )
        self.assertTrue( 'OS' in retval )
        self.assertTrue( 'Jersey' in retval )
        self.assertTrue( 'REST' in retval )
     
    def test_listDBs(self):
        self.tempTable = "listDB_%s" % int(time.time())
        self.hbase.createTable( self.tempTable, [{'name' : 'f1:'}, {'name' : 'f2:'}, {'name' : 'f3:'}] )
        print self.hbase.listTables
        print self.tempTable



if __name__ == '__main__':
    unittest.main()
