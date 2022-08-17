from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_PRICE_PLATTS_DCD_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_PRICE_PLATTS_DCD"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"] + "pricing/platts"
datalist=GV.ReadInputData(input,"*.csv",2,",",commit)[0]            
rowcount=GV.ReadInputData(input,"*.csv",2,",",commit)[1]        
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
            Price_MDC VARCHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Price_Code VARCHAR(8) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Price_Bates VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Precision VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Freq VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Currency VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_UOM VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Conv VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_ConvArith VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_ToUOM VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_EarliestDate VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_LatestDate VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Act VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Description VARCHAR(200) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Location VARCHAR(200) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Price_Group VARCHAR(200) CHARACTER SET LATIN NOT CASESPECIFIC\
            ) \
            PRIMARY INDEX (price_MDC,price_code)"
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
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
    NullColumnList=["Price_Bates","price_Precision","Price_Freq","Price_Currency","Price_UOM",\
                   "Price_Conv","Price_ConvArith","Price_ToUOM","Price_EarliestDate","Price_LatestDate",\
                   "Price_Act","Price_Description","Price_Location","Price_Group"]
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
        sReload="INSERT INTO " + Tablename_Core + " SELECT Price_MDC,Price_Code,\
            Price_Bates,Price_Precision,Price_Freq,Price_Currency,Price_UOM,Price_Conv,\
            Price_ConvArith,Price_ToUOM,Price_EarliestDate,Price_LatestDate,Price_Act,\
            Price_Description,Price_Location,Price_Group FROM " + Tablename_Stg
        cur.execute(sReload)
        print("       Trunc/Reload completed")
    except:
        print("       Trunc/Reload failed")
        print(sReload)
    
    #  Drop stage tables
    print()
    print("Dropping Stage Tables")
    try:
        cur.execute("DROP TABLE " + Tablename_Stg)
        print("       " + Tablename_Stg + " dropped")
    except:
        print("       " + Tablename_Stg + " does not exist")
con.close()
    
#  Archive load files
#print("Archiving Load Files")
#input=GV.TDLogVar["DATADIR"]+"/Tags"
#for file in FileList:
#    os.rename(input+"/"+file,input+"/Archive/"+file)

print()
print("Load Complete")
print("Execution took " + str(int(time.time()-starttime)) + " seconds") 