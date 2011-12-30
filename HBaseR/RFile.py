#!/usr/bin/env python

# HBaseR - a tool to load R**T files into hbase (without needing R**T!)
#                          ^---I don't speak this name anymore

import os
import struct
from HBaseR.RByteRange import RByteRange
from HBaseR.RUnparsedBlob import RUnparsedBlob
from HBaseR.RKey import RKey, RKeyHeader, RKeyData
class RFile( RByteRange ):
    # Handles loading a file. Provides simple iterators to walk through the
    # object

    # some constants
    SMALL_FILE = 1
    BIG_FILE   = 2

    def __init__( self ):
        self.keys = []

    def open( self, fileName ):
        # opens a file, loads the header
        fh = open( fileName, 'r' )
        initialRootHeader = ">4si"
        ( rootKey, fVersion ) = self.unpackFromFD( fh, initialRootHeader )
        if ( rootKey != "root" ):
            raise RuntimeError, "The file's header doesn't match 'root'. Corrupt File?"

        # Description from http://root.cern.ch/root/html/TFile.html

        # The first data record starts at byte fBEGIN (currently set to kBEGIN).
        # Bytes 1->kBEGIN contain the file description, when fVersion >= 1000000
        # it is a large file (> 2 GB) and the offsets will be 8 bytes long and
        # fUnits will be set to 8:
        #1->4            "root"      = Root file identifier
        #5->8            fVersion    = File format version
        #9->12           fBEGIN      = Pointer to first data record
        #13->16 [13->20] fEND        = Pointer to first free word at the EOF
        #17->20 [21->28] fSeekFree   = Pointer to FREE data record
        #21->24 [29->32] fNbytesFree = Number of bytes in FREE data record
        #25->28 [33->36] nfree       = Number of free data records
        #29->32 [37->40] fNbytesName = Number of bytes in TNamed at creation time
        #33->33 [41->41] fUnits      = Number of bytes for file pointers
        #34->37 [42->45] fCompress   = Compression level and algorithm
        #38->41 [46->53] fSeekInfo   = Pointer to TStreamerInfo record
        #42->45 [54->57] fNbytesInfo = Number of bytes in TStreamerInfo record
        #46->63 [58->75] fUUID       = Universal Unique ID

        # note: R**T always uses big-endian
        # need to pull out the first bit of the header to get fVersion, which determines    
        # if it is a "big" or "small" file (why don't they just make all files big?)
        if fVersion < 1000000:
            # < 2GB struct format:
            rootHeader = ">11i"
            self.largeSwitch = self.SMALL_FILE
        else:
            # > 2GB struct format:
            rootHeader = ">11Q"
            self.largeSwitch = self.BIG_FILE

        ( fBegin, fEnd, fSeekFree, 
          fNbytesFree, nfree, fNbytesName,
          fUnits, fcompress, fSeekInfo, 
          fNbytesInfo, fUUID ) = self.unpackFromFD( fh, rootHeader )
        headerInfo = { 'rootKey'    : rootKey, 
                       'fVersion'   : fVersion, 
                       'fBegin'     : fBegin,
                       'fEnd'       : fEnd,
                       'fSeekFree'  : fSeekFree,
                       'fNbytesFree': fNbytesFree,
                       'nfree'      : nfree,
                       'fNbytesName': fNbytesName,
                       'fUnits'     : fUnits,
                       'fcompress'  : fcompress,
                       'fSeekInfo'  : fSeekInfo,
                       'fNbytesInfo': fNbytesInfo,
                       'fUUID'      : fUUID }

        RByteRange.__init__( self, 0, fEnd, fh )
        self.header = RFileHeader( data = headerInfo )
        self.header.setByteRange( 0, fh.tell() )
        self.addChildRange( self.header )
        self.padding = RUnparsedBlob( fh.tell() + 1, fEnd )
        self.addChildRange( self.padding )
    def loadChildren( self ):
        # get the file handle
        fh = self.getFileHandleFromByteRange()
        # seek to fBegin to start reading keys
        position = self.header.data['fBegin']
        fh.seek( position )
        initialKeyHeader  = ">ihiihh"
        smallKeyHeader  = ">iib"
        bigKeyHeader  = ">QQb"

        while ( position < self.header.data['fEnd'] ):
            # Another thing R**T does that sucks. Instead of having all the 
            # fixed length bits of the header together, it intersperses fixed
            # length fields describing what the next variable length field is
            # So, you can't just slurp it all at once, you have to do multiple reads
            # to parse things out. Genius. Oh, and the headers are different
            # sizes depending on the position of the object in the file.

            # This code sucks because I have to follow that behavior.
            ( keyBytes, version, objLen, datime, keyLen, cycle ) = \
                self.unpackFromFD( fh, initialKeyHeader )

            # from http://root.cern.ch/root/html/src/TFile.cxx.html#1343
            if version > 1000:
                ( seekKey, seekPdir, classNameLen ) = \
                    self.unpackFromFD( fh, bigKeyHeader )
            else:
                ( seekKey, seekPdir, classNameLen ) = \
                    self.unpackFromFD( fh, smallKeyHeader )
            
            if seekKey != position:
                    raise RuntimeError, "Sanity check failed, seekKey != current position"
            className      = fh.read( classNameLen )

            ( objNameLen, ) = self.unpackFromFD( fh, ">b" )
            objName        = fh.read( objNameLen )

            ( objTitleLen, )= self.unpackFromFD( fh, ">b" )
            objTitle       = fh.read( objTitleLen )
#            print "%s - %s -  %s " % (className, objName, objTitle )
            
            # Do some weird overrides http://root.cern.ch/root/html/src/TFile.cxx.html#1354
            if (position == self.header.data['fSeekFree']) : className = "FreeSegments"
            if (position == self.header.data['fSeekInfo']) : className = "StreamerInfo"
#            if (position == seekKeys)  : className = "KeysList"

            # Store information about the key
            keyInfo = { "seekKey"  : seekKey,
                        "className": className,
                        "objName"  : objName,
                        "objTitle" : objTitle,
                        "keyBytes" : keyBytes,
                        "cycle"    : cycle }
            keyContainer = RKey( position, position + keyBytes -1 )
            self.padding.addChildRange( keyContainer )

            keyHeader    = RKeyHeader( data = keyInfo )
            keyData      = RKeyData( fh.tell() + 1, position + keyBytes -1 )

            keyHeader.setByteRange( position, fh.tell() )

            keyContainer.setHeader( keyHeader )
            keyContainer.setData( keyData )
            self.keys.append( keyInfo )

            # seek forward to the next key
            position += keyBytes
            fh.seek( position )

        # Now that we've loaded all the children, fill in any gaps with placeholder byteranges
        for blankSpace in myfile.getEmptyRanges():
            self.padding.addChildRange( blankSpace )

    def unpackFromFD( self, handle, pattern ):
        # helper function to read structs out of a file
        sizeNeeded = struct.calcsize( pattern )
        buffer     = handle.read( sizeNeeded )
        return  struct.unpack( pattern, buffer )

class RFileHeader( RByteRange ):
    def __init__(self, data ):
        self.data = data

if __name__ == '__main__':
    import os.path    
    myfile = RFile()
    myfile.open( os.path.join( os.path.dirname( __file__ ), "..", "test_file.root") )
    myfile.loadChildren()
    #myfile.dumpTree()
    #for line in myfile.getByteMapping():
    #    print line
