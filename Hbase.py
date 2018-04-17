from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase.ttypes import *
from hbase import Hbase
from hdfs import *
import binascii


class HbaseWrite():
    def __init__(self):
        self.tableName = 'database'
        self.transport = TSocket.TSocket('student62', 9090)
        self.transport = TTransport.TBufferedTransport(self.transport)
        self.transport.open()
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)

    def createTable(self):
        col1 = ColumnDescriptor(name="data:", maxVersions=1)
        self.client.createTable(self.tableName, [col1])

    def write(self, PicPath, PicName):
        row = PicName.split('.')[0]
        _data = PicName.split('.')[1]
        PicData = open('%s/%s' % (PicPath, PicName), 'rb').read()
        PicData = binascii.hexlify(PicData)
        self.client.mutateRow(self.tableName, row, [Mutation(column='data:%s' % _data, value=PicData)])

    def write_feature(self, PicFeature, PicName):
        row = PicName.split('.')[0]
        self.client.mutateRow(self.tableName, row, [Mutation(column='data:feature', value=PicFeature)])

    def read(self, tableName, PicName):
        row = PicName.split('.')[0]
        data_type = PicName.split('.')[1]
        get_data = self.client.get(tableName, row, 'data:%s' % data_type)[0]
        if get_data:
            return get_data.value
        else:
            return 'Error'

    def read_feature(self, tableName, PicName):
        row = PicName.split('.')[0]
        get_data = self.client.get(tableName, row, 'data:feature')[0]
        if get_data:
            return get_data.value
        else:
            return 'Error'

