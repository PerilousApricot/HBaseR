#!/usr/bin/env python

from HBaseR.RByteRange import RByteRange

class RUnparsedBlob(RByteRange):
    def loadFromByteRange( self ):
        # get fh
        fh = self.getFileHandleFromByteRange()

        self
