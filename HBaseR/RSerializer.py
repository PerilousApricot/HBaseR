#!/usr/bin/env python
import os.path, struct, StringIO
from HBaseR.RFile import RFile, RFileHeader
from HBaseR.RUnparsedBlob import RUnparsedBlob
class RSerializer:

    SMALL_FILE = 1
    BIG_FILE   = 2

    def serializeToString( self, input ):
        streamer = StringIO.StringIO()
        self.serializeToFile( input, streamer )
        output = streamer.getvalue()
        streamer.close()
        return output
    
    def serializeToFile( self, input, streamer ):
        # do some sanity checking
        if input.__class__ != RFile:
            raise RuntimeError, "This serializer can only start with RFile objects"

        # Get the list of byteranges with their accompanying objects to fill them
        children     = input.getLayout().getByteMapping()
        children.sort( key = lambda x: x[0] )

        position = 0
        for child in children:
            if position != child[0]:
                raise RuntimeError, "There was a gap when deserializing (%s != %s)" % (position, child[0])
            bytesWritten = self.serializeOneObject( child[2].owner(), streamer )

            print "starting at %s ending at %s, wrote %s bytes" % ( child[0], child[1], bytesWritten )
            if position + bytesWritten - 1 != child[1]:
                raise RuntimeError, \
                    "An unexpected number of bytes were written (%s != %s)" % \
                        ( position + bytesWritten - 1, child[1] )
            position += bytesWritten

    def serializeOneObject( self, input, streamer ):
        # handle serializing individual objects to file
        # TODO: make this into a factory
        positionBefore = streamer.tell()
        if input.__class__  == RFileHeader:
            streamer.write( self.serializeRFileHeader( input ))
        elif input.__class__ == RUnparsedBlob:
            streamer.write( input.getRawBytes() )
        else:
            raise RuntimeError, "Attempted to serialize unsupported object type: %s" % input.__class__
        positionAfter = streamer.tell()
        print "pos before %s after %s " % (positionBefore, positionAfter)
        return positionAfter - positionBefore

    def serializeRFileHeader( self, input ):
        if input.data['fVersion'] < 1000000:
            # < 2GB struct format:
            rootHeader = ">4si6ib3i18s"
        else:
            # > 2GB struct format:
            rootHeader = ">4si6Qb3Q18s"
        print "struct length is %s "% struct.calcsize( rootHeader )
        return struct.pack( rootHeader,
                                input.data['rootKey'],
                                input.data['fVersion'],
                                input.data['fBegin'],
                                input.data['fEnd'],
                                input.data['fSeekFree'],
                                input.data['fNbytesFree'],
                                input.data['nfree'],
                                input.data['fNbytesName'],
                                input.data['fUnits'],
                                input.data['fcompress'],
                                input.data['fSeekInfo'],
                                input.data['fNbytesInfo'],
                                input.data['fUUID'] )

    def getRawBytes( self, input ):
        fh = input.getLayout().getFileHandleFromByteRange()
 
    def deserialize( self, fileName ):
        # opens a file, loads the header
        fh = open( fileName, 'rb' )
        
        output = RFile( source = self )
        header = self.getHeader( fh )
        output.setByteRange( 0, header.data['fEnd'] )
        output.setFileName( fileName )
        output.setHeader( header )

        padding = RUnparsedBlob( fh.tell(), header.data['fEnd'], source = self )
        output.addPadding( padding )
        fh.close()
        return output

    def getHeader( self, fh ):
        initialRootHeader = ">4si"
        ( rootKey, fVersion ) = self.unpackFromFD( fh, initialRootHeader )
        if ( rootKey != "root" ):
            raise RuntimeError, "The file's header doesn't match 'root'. Corrupt File?"
        headerSize = struct.calcsize( initialRootHeader )
        print "intial header is %s" % headerSize
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
            print "SMALL"
            rootHeader = ">6ib3i18s"
            self.largeSwitch = self.SMALL_FILE
        else:
            # > 2GB struct format:
            rootHeader = ">6Qb3Q18s"
            self.largeSwitch = self.BIG_FILE

        ( fBegin, fEnd, fSeekFree, 
          fNbytesFree, nfree, fNbytesName,
          fUnits, fcompress, fSeekInfo, 
          fNbytesInfo, fUUID ) = self.unpackFromFD( fh, rootHeader )
        print "rest of header is %s" % struct.calcsize( rootHeader )

        headerSize += struct.calcsize( rootHeader )
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
        
        header = RFileHeader( data = headerInfo, source = self )
        print "Header size: %s but tell is at %s" % ( headerSize, fh.tell() )
        header.setByteRange( 0, fh.tell() - 1 )
        return header

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

