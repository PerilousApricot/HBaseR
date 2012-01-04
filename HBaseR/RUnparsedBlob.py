#!/usr/bin/env python

from HBaseR.RByteRange import RByteRange

class RUnparsedBlob():
    def __init__( self, startKey, endKey, source = None ):
        self.layout = RByteRange( self, startKey, endKey )
        if source:
            self.source = source

    def getRawBytes( self ):
        return self.source.getRawBytes( self )

    def getLayout( self ):
        return self.layout

    def loadFromByteRange( self ):
        # get fh
        fh = self.getFileHandleFromByteRange()

        self
