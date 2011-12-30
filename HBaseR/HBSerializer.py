#!/usr/bin/env python

# top level serialization class
from HBaseR.SerializerBase import SerializerBase
import HBaseR.Serializers.HBase

class HBSerializer(SerializerBase):
    defaultProcessVersion = 'HBaseR.Serializers.HBase.Latest'
