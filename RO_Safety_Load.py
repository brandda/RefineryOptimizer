from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_SAFETY_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_SAFETY"

commit=400000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"safety.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"safety.csv",1,",",commit)[1]            
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
            Safety_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Safety_Time VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Safety_ActPlan VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Safety_Metric VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Safety_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC)\
            PRIMARY INDEX (Ref_Location,Safety_Date,Safety_Time,Safety_ActPlan,Safety_Metric)"
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
        sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?)"
        #sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?)"
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
    NullColumnList=["Safety_Value"]
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
                AND CORE.Safety_Date=STG.Safety_Date \
                AND CORE.Safety_Time=STG.Safety_Time \
                AND CORE.Safety_ActPlan=STG.Safety_ActPlan \
                AND CORE.Safety_Metric=STG.Safety_Metric \
            WHEN MATCHED THEN UPDATE SET \
                Safety_Value=STG.Safety_Value,Last_Update_Dttm='" + current_timestamp + "' \
            WHEN NOT MATCHED THEN INSERT VALUES \
                (STG.Ref_Location,STG.Safety_Date,STG.Safety_Time,STG.Safety_ActPlan,\
                STG.Safety_Metric,STG.Safety_Value\
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