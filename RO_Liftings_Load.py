from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_LIFTINGS_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_LIFTINGS"

commit=20000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"liftings*.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"liftings*.csv",1,",",commit)[1]            
if len(datalist[0])==0:
    sys.exit("No Input Data")

#  Connect to Teradata using global variable parameters
print("Connecting to Database")
con=TSQL.connect(host=GV.TDLogVar["SERVER"],user=GV.TDLogVar["USER"],password=GV.TDLogVar["PASSWORD"],logmech=GV.TDLogVar["LOGMECH"])    

# Clear old and error tables and create new table
with con.cursor() as cur:
    GV.ClearStageEnvironment(cur,Tablename_Stg)
    
    print("Creating Stage Tables")
    try:
        sStageCreate = "CREATE MULTISET TABLE " + Tablename_Stg + ", \
            NO BEFORE JOURNAL,NO AFTER JOURNAL,CHECKSUM = DEFAULT,DEFAULT MERGEBLOCKRATIO \
                (Ref_Location VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
                BOL_Number VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
                BOL_Line_Number VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
                BOL_Version VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
                BOL_Product VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Material_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
                BOL_Last_Modified_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
                BOL_Last_Modified_Time VARCHAR(8) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Station VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Order_ID VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
                PO_Number VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Contract_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Start_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Start_Time VARCHAR(8) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Stop_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Stop_Time VARCHAR(8) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Customer VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Carrier VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Destination VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Driver VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Container_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Transport_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Gross_Volume VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Net_Volume VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Volume_UOM VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC)\
            PRIMARY INDEX (Ref_Location,BOL_Number,BOL_Line_Number,BOL_Version)"
        cur.execute(sStageCreate)
        print("       Stage Tables Created")
    except:
        print("       Stage Tables Creation Failed")
        print(sStageCreate)
        sys.exit("Stage Create Failure")

    #  Load stage table
    print()
    print("Loading Stage Table With " + str(rowcount) + " Records")
    print("Database Commits Occur Every " + str(commit) + " records")
    try:
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,\
        #    ?,?,?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,\
            ?,?,?,?,?,?,?,?,?,?,?,?)"
        counter=1
        for sublist in datalist:            
            cur.execute(sInsert,sublist)
            print("       Commit " + str(counter) + " Complete")
            counter+=1
        print("Stage Load Complete")
        print("       " + str(rowcount) + " records loaded")
    except:
        print("       Load Failed")
        print(sInsert)
        sys.exit("Load Failure")
        
    #  Setting empty cells to null
    NullColumnList=["Material_ID","BOL_Last_Modified_Date","BOL_Last_Modified_Time"\
                   ,"Station","Order_ID","PO_Number","Contract_ID","Start_Date","Start_Time"\
                   ,"Stop_Date","Stop_Time","Customer","Carrier","Destination","Driver"\
                   ,"Container_ID","Transport_ID","Gross_Volume","Net_Volume","Volume_UOM"]
    if GV.StageNullUpdate(cur,Tablename_Stg,NullColumnList)==0:
        sys.exit("Column Null Failure")
    else:
        print("       Null Update completed")

    #  Merge Stage to Core
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print("Merging Stage to Core")
    try:
        sMerge="MERGE INTO " + Tablename_Core + " CORE USING " + Tablename_Stg + " STG \
            ON CORE.Ref_Location=STG.Ref_Location \
                AND CORE.BOL_Number=STG.BOL_Number \
                AND CORE.BOL_Line_Number=STG.BOL_Line_Number \
                AND CORE.BOL_Version=STG.BOL_Version \
                AND CORE.BOL_Product=STG.BOL_Product \
            WHEN MATCHED THEN UPDATE SET \
                Material_ID=STG.Material_ID,\
                BOL_Last_Modified_Date=STG.BOL_Last_Modified_Date,\
                BOL_Last_Modified_Time=STG.BOL_Last_Modified_Time,\
                Station=STG.Station,\
                Order_ID=STG.Order_ID,\
                PO_Number=STG.PO_Number,\
                Contract_ID=STG.Contract_ID,\
                Start_Date=STG.Start_Date,\
                Start_Time=STG.Start_Time,\
                Stop_Date=STG.Stop_Date,\
                Stop_Time=STG.Stop_Time,\
                Customer=STG.Customer,\
                Carrier=STG.Carrier,\
                Destination=STG.Destination,\
                Driver=STG.Driver,\
                Container_ID=STG.Container_ID,\
                Transport_ID=STG.Transport_ID,\
                Gross_Volume=STG.Gross_Volume,\
                Net_Volume=STG.Net_Volume,\
                Volume_UOM=STG.Volume_UOM,\
                Last_Update_Dttm='" + current_timestamp + "' \
            WHEN NOT MATCHED THEN INSERT VALUES \
                (STG.Ref_Location,STG.BOL_Number,STG.BOL_Line_Number,STG.BOL_Version,STG.BOL_Product,\
                STG.Material_ID,STG.BOL_Last_Modified_Date,STG.BOL_Last_Modified_Time,STG.Station,\
                STG.Order_ID,STG.PO_Number,STG.Contract_ID,STG.Start_Date,STG.Start_Time,STG.Stop_Date,\
                STG.Stop_Time,STG.Customer,STG.Carrier,STG.Destination,STG.Driver,STG.Container_ID,\
                STG.Transport_ID,STG.Gross_Volume,STG.Net_Volume,STG.Volume_UOM\
                ,'" + current_timestamp + "','" + current_timestamp + "')"
        cur.execute(sMerge)
        print("       Merge completed")
    except:
        print("       Merge failed")
        print(sMerge)
        sys.exit("Merge Failure")
    
    #  Drop stage tables
    print()
    print("Dropping Stage Tables")
    try:
        cur.execute("DROP TABLE " + Tablename_Stg)
        print("       " + Tablename_Stg + " dropped")
    except:
        print("       " + Tablename_Stg + " does not exist")
con.close()

print()
print("Load Complete")
print("Execution took " + str(int(time.time()-starttime)) + " seconds") 