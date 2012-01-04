#!/usr/bin/env python

# byte ranges are (possibly nested) ranges of bytes describing an object
# in R**T's on-disk format.

# ex:
# |TFile                               |
# |THeader |TKey                       |
# |THeader |TKeyHeader|THistogram      |

import weakref

class RByteRange:

    def __init__( self, owner, startByte = None, endByte = None, fh = None ):
        print owner
        if startByte > endByte:
            raise RuntimeError, "startByte > endByte (%s > %s)" % (startByte, endByte )

        if ( startByte == None ) != ( endByte == None ):
            raise RuntimeError, "Need to set both start and endbyte simultaneously"

        if ( startByte != None ):
            self.startByte = startByte
            self.endByte   = endByte
        # TODO: need to make this into an interval tree
        self.children  = []
        self.parent    = None
        self.owner     = weakref.ref( owner )

        if fh:
            self.fh = fh
    
    def getOwner( self ):
        if self.owner() != None:
            return self.owner()
        else:
            raise RuntimeError, "Owner of the RByteRange has been reaped"

    def setByteRange( self, startByte, endByte ):
        if hasattr( self, 'startByte' ) or hasattr( self, 'endByte' ):
            raise RuntimeError, "Can't change an existing byterange. Get it right the first time"
        if hasattr( self, 'parent' ) and self.parent:
            raise RuntimeError, "Can't change the byterange when I've already been added as a child"
        if hasattr( self, 'children') and self.children:
            raise RuntimeError, "Can't change the byterange when I already have children"
        if startByte > endByte:
            raise RuntimeError, "startByte > endByte. This is no good"

        self.startByte = startByte
        self.endByte   = endByte
        self.children  = []
        self.parent    = None

    def addChildRange( self, childRange ):
        if ( childRange.startByte < self.startByte ):
            raise RuntimeError, "Child begins before the parent"
        if ( childRange.endByte > self.endByte ):
            raise RuntimeError, "Child ends after the parent"
        for sibling in self.children:
            if ( ( sibling.startByte < childRange.endByte ) and ( childRange.endByte < sibling.endByte )
               or( sibling.startByte < childRange.startByte ) and ( childRange.endByte < sibling.startByte ) ):
               raise RuntimeError, "Child is within the range of another child"
        self.children.append( childRange )
        childRange.parent = self

    def getFileHandleFromByteRange( self ):
        if hasattr( self, 'fh' ):
            return self.fh
        else:
            if hasattr( self, 'parent' ) and self.parent:
                return self.parent.getFileHandleFromByteRange()
            else:
                if hasattr( self, 'fileName' ):
                    return open( self.fileName, 'rb' )
                else:
                    raise RuntimeError, "Couldn't find a filehandle"

    def setFileName( self, fileName ):
        self.fileName = fileName

    def getEmptyRanges( self ):
        # returns the empty ranges in a given byterange.
        # can be used to make sure every byte is accounted for
        currPosition = self.startByte - 1
        children     = self.getByteMapping()
        children.sort( key = lambda x: x[0] )
        retval = []
        for child in children:
            if child[0] <= currPosition:
                # Shouldn't happen
                raise RuntimeError, "This child exceeds the bounds of its parent/sibling"
            if child[0] == currPosition + 1:
                currPosition = child[1]
                continue
            else:
                # have to do this here to fix a circular import
                from HBaseR.RUnparsedBlob import RUnparsedBlob
                retval.append( RUnparsedBlob( currPosition +1, child[0] - 1 ) )
                currPosition = child[1]
        if currPosition + 1 > self.endByte:
            raise RuntimeError, "Child exceeds the (supposed) end of the file"

        if currPosition + 1 != self.endByte:
            retval.append( RUnparsedBlob( currPosition +1, self.endByte ) )

        return retval
    
    def getByteMapping( self ):
        # returns a list of byteranges and the objects responsible for them
        if self.children:
            # if we have children, they are responsible for this byte range
            retval = []
            for child in self.children:
                retval.extend( child.getByteMapping() )
            return retval
        else:
            return [[ self.startByte, self.endByte, self ]]
   
    def dumpTree( self, indent = 0 ):
        # debug method
        indentString = "  " * indent
        indent = indent + 2
        print "%s%s [%s->%s]" % (indentString, self.__class__, self.startByte, self.endByte )
        for child in self.children:
            child.dumpTree( indent ) 
