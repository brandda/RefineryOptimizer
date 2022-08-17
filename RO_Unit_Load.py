from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_UNIT_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_UNIT"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"unit.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"unit.csv",1,",",commit)[1]            
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
            (Ref_Location VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Unit_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Unit_Time VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Unit_Name VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Unit_RateType VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Unit_RateUOM VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Unit_RateValue VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC)\
            PRIMARY INDEX (Ref_Location,Unit_Date,Unit_Time,Unit_Name,Unit_RateType)"
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
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?)"
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
    NullColumnList=["Unit_RateUOM","Unit_RateValue"]
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
                AND CORE.Unit_Date=STG.Unit_Date \
                AND CORE.Unit_Time=STG.Unit_Time \
                AND CORE.Unit_Name=STG.Unit_Name \
                AND CORE.Unit_RateType=STG.Unit_RateType \
            WHEN MATCHED THEN UPDATE SET \
                Unit_RateUOM=STG.Unit_RateUOM,Unit_RateValue=STG.Unit_RateValue,\
                Last_Update_Dttm='" + current_timestamp + "' \
            WHEN NOT MATCHED THEN INSERT VALUES \
                (STG.Ref_Location,STG.Unit_Date,STG.Unit_Time,STG.Unit_Name,\
                STG.Unit_RateType,STG.Unit_RateUOM,STG.Unit_RateValue\
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