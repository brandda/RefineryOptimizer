from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_BLEND_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_BLEND"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"Blend.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"Blend.csv",1,",",commit)[1]            
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
            Ref_Location VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Blend_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Blend_Spec VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Blend_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Blend_Time VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Material_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Container_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC\
            )\
            PRIMARY INDEX (Ref_Location,Blend_ID)"
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
    NullColumnList=["Blend_Spec","Blend_Date","Blend_Time","Material_ID","Container_ID"]
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
                AND CORE.Blend_ID=STG.Blend_ID \
            WHEN MATCHED THEN UPDATE SET \
                Blend_Spec=STG.Blend_Spec,Blend_Date=STG.Blend_Date,Blend_Time=STG.Blend_Time,\
                Material_ID=STG.Material_ID,Container_ID=STG.Container_ID,\
                Last_Update_Dttm='" + current_timestamp + "' \
            WHEN NOT MATCHED THEN INSERT VALUES \
                (STG.Ref_Location,STG.Blend_ID,STG.Blend_Spec,STG.Blend_Date,STG.Blend_Time,\
                STG.Material_ID,STG.Container_ID\
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