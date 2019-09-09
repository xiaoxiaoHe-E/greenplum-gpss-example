import data_pb2
import data_pb2_grpc
import gpss_pb2
import gpss_pb2_grpc
import grpc
import google.protobuf 
import google.protobuf.text_format as tf

mSession = gpss_pb2.Session()
gpMasterHost = "localhost"
gpMasterPort = 8080         #port of gpss which is different from the port of gpdb
gpRoleName = "gpadmin"
gpPasswd = "changeme"
dbname = "testdb"

def ConnectToGPSS():
    #connect to gpdb
    #return channel, mSession, stub
    channel = grpc.insecure_channel( gpMasterHost + ':' + str(gpMasterPort) )
    stub = gpss_pb2_grpc.GpssStub(channel)
    mConnectReq = gpss_pb2.ConnectRequest(
        Host = gpMasterHost,
        Port = 15432,             #port of gpdb which is different from port of gpss
        Username = gpRoleName,  
        Password = gpPasswd,
        DB = dbname,
        UseSSL = True
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
    print(mListSchemaReq)
    schemaInfo = gpss_pb2.Schemas()
    schemaInfo = stub.ListSchema(mListSchemaReq)
    print(schemaInfo)
    theSchema = schemaInfo.Schemas[0].Name
    return theSchema


def WritData(mSession, stub, schema):
    #insert data to the given schema 
    #return none

    #start an insert service 
    insOpt = gpss_pb2.InsertOption (
        InsertColumns = ["a", "b"],     #colum list to be inserted
        TruncateTable = False,          #truncate the table before inserting or not
        ErrorLimitCount = 5,            #
        ErrorLimitPercentage = 5
    )
    openReq = gpss_pb2.OpenRequest(Session= mSession,
                                   SchemaName=schema,
                                   TableName="test",
                                   PreSQL="",
                                   PostSQL="",
                                   Timeout=10,     # seconds
                                   Encoding="UTF_8",
                                   StagingSchema= "",
                                   Option=insOpt)
    stub.Open(openReq)

    #perpare dummy data to write
    myRowData = []
    for i in range(5):
        valB = data_pb2.DBValue(StringValue = str("aa"+str(i)))
        valA = data_pb2.DBValue(Int64Value = 1)
        myRow = data_pb2.Row(Columns = [valA, valB])
        print("original", myRow)
        myRowinBytes = tf.MessageToBytes(myRow )
        myRowData.append(gpss_pb2.RowData(Data=myRowinBytes))

    writeReq = gpss_pb2.WriteRequest(Session=mSession, Rows= myRowData)
    stub.Write(writeReq)

    #close the write service
    closeReq = gpss_pb2.CloseRequest(session = mSession, 
                                     CloseRequest = 5)
    state = stub.Close(closeReq)
    print("response from server: ", state)


if __name__ == '__main__':
    channel, mSession, stub = ConnectToGPSS()
    theSchema = ListSchema(mSession, stub)
    WritData(mSession, stub, theSchema)

    #close channel
    stub.Disconnect(mSession)
    channel.close()
    print("done!")
