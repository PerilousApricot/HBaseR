#!/usr/bin/env python

# Implementation of the R**T TKey class described at
# http://root.cern.ch/root/html/TKey.html
from HBaseR.RByteRange import RByteRange


class RKeyHeader( RByteRange ):
    def __init__( self, data, startByte = None, endByte = None ):
        if ( (startByte == None) != (endByte == None ) ):
            raise RuntimeError, "Must either set or not set startByte and endByte"
        
        if ( startByte != None ) and ( endByte != None ):
            self.setByteRange( startByte = startByte,
                               endByte   = endByte )

        self.data = data

class RKey( RByteRange ):
    def setHeader( self, header ):
        #TODO sanity checking
        self.addChildRange( header )
        self.header      = header
        header.parentKey = self

    def setData( self, data ):
        self.addChildRange( data )
        self.data        = data
        data.parentKey   = self
    pass
class RKeyData( RByteRange ):
    pass
