#!/usr/bin/env python

# HBaseR - a tool to load R**T files into hbase (without needing R**T!)
#                          ^---I don't speak this name anymore

import os
import struct
from HBaseR.RByteRange import RByteRange
from HBaseR.RUnparsedBlob import RUnparsedBlob
from HBaseR.RKey import RKey, RKeyHeader, RKeyData
class RFile:
    # Implementation of TFile

    def __init__( self, source = None ):
        self.keys   = []
        self.padding= []
        self.layout = RByteRange( owner = self )
        if source:
            self.source = source

    def setByteRange( self, startKey, endKey ):
        self.layout.setByteRange( startKey, endKey )

    def setHeader( self, header ):
        self.header = header
        self.layout.addChildRange( header.getLayout() )

    def getHeader( self ):
        return self.header

    def addKey( self, key ):
        self.keys.append( key )
        self.layout.addChildRange( key.getLayout() )

    def addChildRange( self, range ):
        self.layout.addChildRange( range )

    def getLayout( self ):
        return self.layout
    
    def addPadding( self, padding ):
        self.padding.append( padding )
        self.layout.addChildRange( padding.getLayout() )
    
    def setFileName( self, fileName ):
        self.layout.setFileName( fileName )
        
class RFileHeader:
    def __init__(self, data, source = None ):
        self.data = data
        self.layout = RByteRange( owner = self )
        if source:
            self.source = source

    def setByteRange( self, startKey, endKey ):
        self.layout.setByteRange( startKey, endKey )

    def getLayout( self ):
        return self.layout

if __name__ == '__main__':
    import os.path    
    myfile = RFile()
    myfile.open( os.path.join( os.path.dirname( __file__ ), "..", "test_file.root") )
    myfile.loadChildren()
    #myfile.dumpTree()
    #for line in myfile.getByteMapping():
    #    print line
