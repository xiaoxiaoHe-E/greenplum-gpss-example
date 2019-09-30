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


mSession = gpss_pb2.Session()
gpMasterHost = "127.0.0.1"
gpMasterPort = 5000       # port of gpss which is different from the port of gpdb
gpRoleName = "gpadmin"
gpPasswd = "changeme"
dbname = "gpadmin"
gpdbPort = 5432  # port of gpdb
MyTableName = 'table32'  # insert table

csvHaveTitle = False
csvPath = 'table32_100.txt'
deleminator = '\t'

WriteEvery = 3000  # grpc has a max message length restrition (4M)


def ConnectToGPSS():
    #connect to gpdb
    #return channel, mSession, stub
    options = [('grpc.max_receive_message_length', 100 * 1024 * 1024),
               ('grpc.max_send_message_length', 100 * 1024 * 1024)]
    channel = grpc.insecure_channel(
        gpMasterHost + ':' + str(gpMasterPort),  options=options)
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


def is_valid_date(strdate):
    try:
        parse(strdate)
        return True
    except:
        return False


def writeFromCsv(mSession, stub, schema):
    avePre = 0  # average preparing time
    aveWrite = 0  # average writing time
    t = 0

    columns = []
    for i in range(32):
        columns.append("col"+str(i+1))
    insOpt = gpss_pb2.InsertOption(
        InsertColumns=columns,  # colum list to be inserted
        TruncateTable=False,  # truncate the table before inserting or not
        ErrorLimitCount=10000,            #
        ErrorLimitPercentage=80
    )
    openReq = gpss_pb2.OpenRequest(Session=mSession,
                                   SchemaName=schema,
                                   TableName=MyTableName,
                                   PreSQL="",
                                   PostSQL="",
                                   Timeout=10,       # seconds
                                   Encoding="UTF_8",
                                   StagingSchema="",
                                   InsertOption=insOpt)
    stub.Open(openReq)

    #read data from csv file
    st = datetime.now()
    data = pd.read_csv(csvPath, sep=deleminator)
    print("time consumed in reading: ",
          (datetime.now() - st).microseconds, "microseconds")
    myRowData = []
    st = datetime.now()
    for index, item in data.iterrows():
        if csvHaveTitle == True and index == 0:  # skip the first line if has title
            continue
        colData = []
        for i in item:

            '''
            # take all data as string
            s = str(i)
            colData.append(data_pb2.DBValue( StringValue= s.encode() ))
            '''

            # data is composed of number / timestamp / string
            # if i is a number,  pandas recoginze data type(number/string) automaticly
            if type(i) == float or type(i) == int:
                colData.append(data_pb2.DBValue(Float64Value=float(i)))
            elif is_valid_date(i):  # else if i is a timestamp
                d = parse(i)
                d2 = Timestamp()
                d2.FromDatetime(d)
                colData.append(data_pb2.DBValue(TimeStampValue=d2))
            else:  # else take i as a string
                s = str(i)
                colData.append(data_pb2.DBValue(StringValue=s.encode()))

        myRow = data_pb2.Row(Columns=colData)
        myRowinBytes = myRow.SerializeToString()
        myRowData.append(gpss_pb2.RowData(Data=myRowinBytes))

        if index % WriteEvery == 0:  # grpc max message length 4M
            print("\ntime consumed in preparing data:",
                  (datetime.now() - st).microseconds, 'microseconds')
            avePre += (datetime.now() - st).microseconds
            start_time = datetime.now()  # time on
            writeReq = gpss_pb2.WriteRequest(Session=mSession, Rows=myRowData)
            stub.Write(writeReq)
            print('time consumed in writing ', WriteEvery, ' lines:',
                  (datetime.now() - start_time).microseconds, 'microseconds')
            aveWrite += (datetime.now() - start_time).microseconds
            t += 1
            myRowData = []
            st = datetime.now()

    if myRowData:  # write residual lines
        start_time = datetime.now()  # time on
        writeReq = gpss_pb2.WriteRequest(Session=mSession, Rows=myRowData)
        stub.Write(writeReq)
        print('time consumed in writing residual lines:', float(
            (datetime.now() - start_time).microseconds), 'microseconds')

    #close the write service
    closeReq = gpss_pb2.CloseRequest(session=mSession,
                                     MaxErrorRows=5)
    state = stub.Close(closeReq)
    print("\nwrite response from server: ", state)

    print("average prearing time:", avePre/t, "microseconds")
    print("average writing time:", aveWrite/t, "microseconds")


if __name__ == '__main__':
    channel, mSession, stub = ConnectToGPSS()
    theSchema = ListSchema(mSession, stub)
    #WritData(mSession, stub, theSchema)
    writeFromCsv(mSession, stub, theSchema)

    #close channel
    stub.Disconnect(mSession)
    channel.close()
    print("done!")
