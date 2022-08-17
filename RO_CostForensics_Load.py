from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_COSTFORENSICS_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_COSTFORENSICS"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"CostForensics.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"CostForensics.csv",1,",",commit)[1]            
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
        sStageCreate="CREATE MULTISET TABLE " + Tablename_Stg + ", \
            NO BEFORE JOURNAL,NO AFTER JOURNAL,CHECKSUM = DEFAULT,DEFAULT MERGEBLOCKRATIO \
            (Ref_Location VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            CF_Type VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            CF_Unit VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            CF_ActPlan VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            CF_Category1 VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            CF_Category2 VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            CF_Category3 VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            CF_Date VARCHAR(10),\
            CF_Cost VARCHAR(20)) \
        PRIMARY INDEX (Ref_Location,CF_Type,CF_Unit,CF_ActPlan,CF_Category1,CF_Category2,CF_Category3,CF_Date)"
        cur.execute(sStageCreate)        
        print("       Stage Tables Created")
    except:
        print("       Stage Tables Creation Failed")
        prinrt(sStageCreate)
        sys.exit("Stage Create Failure")

    #  Load stage table
    print()
    print("Loading Stage Table With " + str(rowcount) + " Records")
    print("Database Commits Occur Every " + str(commit) + " records")
    try:
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?)"
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
    NullColumnList=["CF_Cost"]
    if GV.StageNullUpdate(cur,Tablename_Stg,NullColumnList)==0:
        sys.exit("Column Null Failure")
    else:
        print("       Null Update completed")

    #  Merge Stage to Core
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print("Truncating and Reloading Core")
    try:
        sTrunc="DELETE FROM " + Tablename_Core
        cur.execute(sTrunc)
        sReload="INSERT INTO " + Tablename_Core + " SELECT Ref_Location,CF_Type,CF_Unit,\
            CF_ActPlan,CF_Category1,CF_Category2,CF_Category3,CF_Date,CF_Cost,\
        '" + current_timestamp + "','" + current_timestamp + "' FROM " + Tablename_Stg
        cur.execute(sReload)
        print("       Trunc/Reload completed")
    except:
        print("       Trunc/Reload failed")
        print(sReload)
        sys.exit("Trunc/Reload Failure")
   
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