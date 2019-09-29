import data_pb2
import data_pb2_grpc
import gpss_pb2
import gpss_pb2_grpc
import grpc
import google.protobuf
import google.protobuf.text_format as tf
from google.protobuf.timestamp_pb2 import Timestamp
import pandas as pd
from dateutil.parser import parse
from datetime import datetime

#generated-members = Timestamp.*

mSession = gpss_pb2.Session()
gpMasterHost = "127.0.0.1"
gpMasterPort = 5000   # port of gpss which is different from the port of gpdb
gpRoleName = "gpadmin"
gpPasswd = "changeme"
dbname = "gpadmin"
gpdbPort = 5432  # port of gpdb

csvHaveTitle = False
csvPath = 'table32_100.txt'
deleminator = '\t'


def ConnectToGPSS():
    #connect to gpdb
    #return channel, mSession, stub
    channel = grpc.insecure_channel(gpMasterHost + ':' + str(gpMasterPort))
    stub = gpss_pb2_grpc.GpssStub(channel)
    mConnectReq = gpss_pb2.ConnectRequest(
        Host=gpMasterHost,
        Port=gpdbPort,
        Username=gpRoleName,
        Password=gpPasswd,
        DB=dbname,
        UseSSL=True
    )
    mSession = stub.Connect(mConnectReq)
    if mSession is not None:
        print("Connect to gpdb successfully")
        print("Responce from gpss: ", mSession)
        return channel, mSession, stub
    else:
        print("error when connect to gpss")


def ListSchema(mSession, stub):
    #list schemas
    #return the first schema of the grpc response

    mListSchemaReq = gpss_pb2.ListSchemaRequest(Session=mSession)
    schemaInfo = gpss_pb2.Schemas()
    schemaInfo = stub.ListSchema(mListSchemaReq)
    theSchema = schemaInfo.Schemas[0].Name
    #ListTable(mSession, stub, theSchema)
    return theSchema


def ListTable(mSession, stub, mSchema):
    # list tables in the shchema
    print("list table from schema", mSchema, ">>>>>>")
    print
    mListTableReq = gpss_pb2.ListTableRequest(Session=mSession, Schema=mSchema)
    tableInfo = gpss_pb2.Tables()
    tableInfo = stub.ListTable(mListTableReq)
    print(tableInfo)


def WritData(mSession, stub, schema):
    #insert data to the given schema
    #return none

    #start an insert service
    insOpt = gpss_pb2.InsertOption(
        InsertColumns=["a", "b"],  # colum list to be inserted
        TruncateTable=False,  # truncate the table before inserting or not
        ErrorLimitCount=5,            #
        ErrorLimitPercentage=5
    )
    openReq = gpss_pb2.OpenRequest(Session=mSession,
                                   SchemaName=schema,
                                   TableName="test",
                                   PreSQL="",
                                   PostSQL="",
                                   Timeout=10,       # seconds
                                   Encoding="UTF_8",
                                   StagingSchema="",
                                   InsertOption=insOpt)
    stub.Open(openReq)

    #perpare dummy data to write
    #table test (a text, b integer)
    #need to specify all the columns in the table
    myRowData = []
    for i in range(5):
        valA = data_pb2.DBValue(StringValue=str('aa'+str(i)))
        valB = data_pb2.DBValue(Int64Value=i)
        myRow = data_pb2.Row(Columns=[valA, valB])
        #print("original data: ", myRow)
        myRowinBytes = myRow.SerializeToString()
        myRowData.append(gpss_pb2.RowData(Data=myRowinBytes))

    writeReq = gpss_pb2.WriteRequest(Session=mSession, Rows=myRowData)
    stub.Write(writeReq)

    #close the write service
    closeReq = gpss_pb2.CloseRequest(session=mSession,
                                     MaxErrorRows=5)
    state = stub.Close(closeReq)
    print("write response from server: ", state)

    #write into gpdb from a csv file

    #start an insert service
    insOpt = gpss_pb2.InsertOption(
        InsertColumns=["a", "b"],  # colum list to be inserted
        TruncateTable=False,  # truncate the table before inserting or not
        ErrorLimitCount=5,            #
        ErrorLimitPercentage=5
    )
    openReq = gpss_pb2.OpenRequest(Session=mSession,
                                   SchemaName=schema,
                                   TableName="test",
                                   PreSQL="",
                                   PostSQL="",
                                   Timeout=10,       # seconds
                                   Encoding="UTF_8",
                                   StagingSchema="",
                                   InsertOption=insOpt)
    stub.Open(openReq)

    #read data from csv file
    data = pd.read_csv(csvPath)
    myRowData = []
    for index, item in data.iterrows():
        if csvHaveTitle == True and index == 0:  # skip the first line if has title
            continue
        valA = data_pb2.DBValue(StringValue=item[0])
        valB = data_pb2.DBValue(Int64Value=int(item[1]))
        myRow = data_pb2.Row(Columns=[valA, valB])
        #print("original data: ", myRow)
        myRowinBytes = myRow.SerializeToString()
        myRowData.append(gpss_pb2.RowData(Data=myRowinBytes))

    writeReq = gpss_pb2.WriteRequest(Session=mSession, Rows=myRowData)
    stub.Write(writeReq)

    #close the write service
    closeReq = gpss_pb2.CloseRequest(session=mSession,
                                     MaxErrorRows=5)
    state = stub.Close(closeReq)
    print("write response from server: ", state)


def writeFromCsv(mSession, stub, schema):
    columns = []
    for i in range(32):
        columns.append("col"+str(i+1))
    insOpt = gpss_pb2.InsertOption(
        InsertColumns=columns,  # colum list to be inserted
        TruncateTable=False,  # truncate the table before inserting or not
        ErrorLimitCount=10,            #
        ErrorLimitPercentage=10
    )
    openReq = gpss_pb2.OpenRequest(Session=mSession,
                                   SchemaName=schema,
                                   TableName="table32",
                                   PreSQL="",
                                   PostSQL="",
                                   Timeout=10,       # seconds
                                   Encoding="UTF_8",
                                   StagingSchema="",
                                   InsertOption=insOpt)
    stub.Open(openReq)

    #read data from csv file
    start_time = datetime.now()
    data = pd.read_csv(csvPath, sep=deleminator)
    myRowData = []
    for index, item in data.iterrows():
        if csvHaveTitle == True and index == 0:  # skip the first line if has title
            continue
        colData = []
        for i in range(18):
            colData.append(data_pb2.DBValue(Float64Value=float(item[i])))
        for i in range(18, 22):
            d = parse(item[i])
            d2 = Timestamp()
            d2.FromDatetime(d)
            colData.append(data_pb2.DBValue(TimeStampValue=d2))
        for i in range(22, 32):
            s = str(item[i])
            colData.append(data_pb2.DBValue(StringValue=s.encode()))
        myRow = data_pb2.Row(Columns=colData)
        #print("original data: ", myRow)
        myRowinBytes = myRow.SerializeToString()
        myRowData.append(gpss_pb2.RowData(Data=myRowinBytes))

    writeReq = gpss_pb2.WriteRequest(Session=mSession, Rows=myRowData)
    stub.Write(writeReq)
    print('time consumed in writing:', float(
        (datetime.now() - start_time).seconds), 'seconds')

    #close the write service
    closeReq = gpss_pb2.CloseRequest(session=mSession,
                                     MaxErrorRows=5)
    state = stub.Close(closeReq)
    print("write response from server: ", state)


if __name__ == '__main__':
    channel, mSession, stub = ConnectToGPSS()
    theSchema = ListSchema(mSession, stub)
    #WritData(mSession, stub, theSchema)
    writeFromCsv(mSession, stub, theSchema)

    #close channel
    stub.Disconnect(mSession)
    channel.close()
    print("done!")
