from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_PRICE_OPISFEED_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_PRICE"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"] + "/pricing/opis"
datalist=GV.ReadInputData(input,"opisfeedstocksdatafiles.csv",0,",",commit)[0]            
rowcount=GV.ReadInputData(input,"opisfeedstocksdatafiles.csv",0,",",commit)[1]            
if len(datalist)==0:
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
            (\
            Price_Col1 VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Description VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Price_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Price_Low VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_High VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Avg VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Country VARCHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_UOM VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_DeliveryType VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Freq VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC\
            ) \
            PRIMARY INDEX (price_description,price_date)"
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
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?)"
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
    NullColumnList=["Price_Avg","Price_High","Price_Low","Price_Country","Price_UOM",\
                   "Price_DeliveryType","Price_Freq"]
    if GV.StageNullUpdate(cur,Tablename_Stg,NullColumnList)==0:
        sys.exit("Column Null Failure")
    else:
        print("       Null Update completed")

    #  Merge Stage to Core
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print("Merging Stage to Core")
    try:
        sMerge="MERGE INTO " + Tablename_Core + " CORE USING (\
            SELECT Price_Source,Price_Type,Price_Date,Price_Time,\
            Price_Code,Price_Freq,Price_Location,Price_Description,\
            NULL AS Price_Open,Price_Low,Price_High,Price_Avg,NULL AS Price_Close,\
            NULL AS Price_Index,NULL AS Price_Volume,Price_UOM,Price_Country,\
            Price_DeliveryType,Price_Currency \
            FROM " + Tablename_Stg + "_VW) STG \
        ON CORE.Price_Source=STG.Price_Source \
            AND CORE.Price_Type=STG.Price_Type \
            AND CORE.Price_Date=STG.Price_Date \
            AND CORE.Price_Time=STG.Price_Time \
            AND CORE.Price_Code=STG.Price_Code \
            AND CORE.Price_Freq=STG.Price_Freq \
        WHEN MATCHED THEN UPDATE SET \
            Price_Low=STG.Price_Low,Price_High=STG.Price_High,Price_Avg=STG.Price_Avg,\
            Price_Close=STG.Price_Close,Price_UOM=STG.Price_UOM,\
            Last_Update_DTTM = '" + current_timestamp + "' \
        WHEN NOT MATCHED THEN INSERT \
            VALUES(STG.Price_Source,STG.Price_Type,STG.Price_Date,STG.Price_Time,\
            STG.Price_Code,STG.Price_Freq,STG.Price_Location,STG.Price_Description,\
            STG.Price_Open,STG.Price_Low,STG.Price_High,STG.Price_Avg,STG.Price_Close,\
            STG.Price_Index,STG.Price_Volume,STG.Price_UOM,STG.Price_Country,\
            STG.Price_DeliveryType,STG.Price_Currency,'"\
            + current_timestamp + "','" + current_timestamp + "')"
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