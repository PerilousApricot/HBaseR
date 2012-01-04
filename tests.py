#!/usr/bin/env python

import sys
import os.path
import StringIO
sys.path.insert( 0, os.path.dirname( __file__ ) )

from HBaseR.RSerializer import RSerializer

testFilename = os.path.join( os.path.dirname( __file__ ), "test_file.root") 
fh = open( testFilename, 'r' )
fileAsString = fh.read()
fh.close()

serializer = RSerializer()
rfile = serializer.deserialize( os.path.join( testFilename ) )

rfile.layout.dumpTree()
byteRepresentation = serializer.serializeToString( rfile )

if fileAsString != byteRepresentation:
    raise RuntimeError, "failed to round-trip file"


print rfile
