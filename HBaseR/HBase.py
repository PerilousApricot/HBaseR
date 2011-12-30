#!/usr/bin/env python

# A simple HBase wrapper
# -Andrew Melo

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from ThriftGlue.hbase import Hbase as HBaseThrift
from ThriftGlue.hbase.ttypes import *

import time

class HBaseException( Exception ):
    def __init__( self, innerException ):
        self.innerException = innerException
        self.message        = innerException.message

class HBase:
    def __init__( self, host, port ):
        transport = TBufferedTransport(TSocket(host, port))
        transport.open()
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        self.client = HBaseThrift.Client(protocol)
        self.client

    def listTables(self):
        return self.client.getTableNames()

    def createTable(self, tableName, schema):
        return self.client.createTable( tableName, schema )

    def getColumnDescriptors( self, tableName ):
        retval = []
        for oneColumn in self.client.getColumnDescriptors( tableName ):
            retval.append( ColumnDescriptor( oneColumn ) )
        return retval

    def deleteTable( self, tableName ):
        self.client.disableTable( tableName )
        return self.client.deleteTable( tableName )

    def putSingleCell( self, tableName, rowID, column,  data = None ):
        inputCell = Mutation( column = column,
                              value  = data )
        return self.client.mutateRow( tableName, rowID, [inputCell] )

    def getSingleCell( self, tableName, rowID, column ):
        return self.client.getRowWithColumns( tableName, rowID, [column] )[0].columns[column].value


import unittest
class TestHBaseBasic( unittest.TestCase ):
    def setUp(self):
        self.hbase = HBase( 'localhost', 9090 )

    def tearDown(self):
        if hasattr( self, 'tempTable' ):
            self.hbase.deleteTable( self.tempTable )

    def test_listDBs(self):
        self.tempTable = "listDB_%s" % int(time.time())
        dummyFamily = ColumnDescriptor( name = 'foo:' )
        self.hbase.createTable( self.tempTable, [dummyFamily] )
        self.assertTrue( self.tempTable in self.hbase.listTables() )
        self.assertEqual( self.hbase.getColumnDescriptors(self.tempTable), [dummyFamily] )
        self.assertRaises( TException, self.hbase.createTable, self.tempTable, [dummyFamily] ) 
        
        self.hbase.putSingleCell( self.tempTable, 'dummyRow', 'foo:col1', "TESTDATA" )
        self.assertEqual( self.hbase.getSingleCell( self.tempTable,
                                                    'dummyRow',
                                                    'foo:col1' ),
                          "TESTDATA" )



if __name__ == '__main__':
    unittest.main()
