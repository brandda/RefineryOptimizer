from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_MOVEMENTS_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_MOVEMENTS"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"movements.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"movements.csv",1,",",commit)[1]            
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
            Movement_Type VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Movement_Start_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Movement_Start_Time VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Movement_Source_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Movement_Dest_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Movement_End_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Movement_End_Time VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Material_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Volume VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Weight VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Ticket_Number VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Comments VARCHAR(500) CHARACTER SET LATIN NOT CASESPECIFIC) \
            PRIMARY INDEX (Ref_Location,Movement_Type,Movement_Start_Date,Movement_Start_Time,\
                Movement_Source_ID,Movement_Dest_ID)"
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
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
    NullColumnList=["Material_ID","Volume","Weight","Ticket_Number","Comments"]
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
                AND CORE.Movement_Type=STG.Movement_Type \
                AND CORE.Movement_Start_Date=STG.Movement_Start_Date \
                AND CORE.Movement_Start_Time=STG.Movement_Start_Time \
                AND CORE.Movement_Source_ID=STG.Movement_Source_ID \
                AND CORE.Movement_Dest_ID=STG.Movement_Dest_ID \
                AND CORE.Movement_End_Date=STG.Movement_End_Date \
                AND CORE.Movement_End_Time=STG.Movement_End_Time \
            WHEN MATCHED THEN UPDATE SET \
                Material_ID=STG.Material_ID,Volume=STG.Volume,Weight=STG.Weight,\
                Ticket_Number=STG.Ticket_Number,Comments=STG.Comments,\
                Last_Update_Dttm='" + current_timestamp + "' \
            WHEN NOT MATCHED THEN INSERT VALUES \
                (STG.Ref_Location,STG.Movement_Type,STG.Movement_Start_Date,STG.Movement_Start_Time,\
                STG.Movement_Source_ID,STG.Movement_Dest_ID,STG.Movement_End_Date,STG.Movement_End_Time,\
                STG.Material_ID,STG.Volume,STG.Weight,STG.Ticket_Number,STG.Comments\
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