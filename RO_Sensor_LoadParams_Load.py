from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_SENSOR_LOADPARAMS_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_SENSOR_LOADPARAMS"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"sensor_loadparams.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"sensor_loadparams.csv",1,",",commit)[1]            
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
                (Extract_Group VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Ref_Location VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag1_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag2_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag3_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag4_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag5_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag6_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag7_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag8_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag9_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag10_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag11_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag12_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag13_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag14_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag15_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag16_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag17_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag18_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag19_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag20_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag21_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag22_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag23_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag24_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag25_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag26_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag27_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag28_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag29_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag30_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag31_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag32_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag33_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag34_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag35_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag36_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag37_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag38_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag39_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag40_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag41_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag42_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag43_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag44_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag45_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag46_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag47_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag48_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag49_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
                Tag50_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC)\
            PRIMARY INDEX (Ref_Location ,Extract_Group )"
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
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
        #?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
    NullColumnList=["Tag1_ID","Tag2_ID","Tag3_ID","Tag4_ID","Tag5_ID","Tag6_ID","Tag7_ID","Tag8_ID","Tag9_ID","Tag10_ID"\
                   ,"Tag11_ID","Tag12_ID","Tag13_ID","Tag14_ID","Tag15_ID","Tag16_ID","Tag17_ID","Tag18_ID","Tag19_ID","Tag20_ID"\
                   ,"Tag21_ID","Tag22_ID","Tag23_ID","Tag24_ID","Tag25_ID","Tag26_ID","Tag27_ID","Tag28_ID","Tag29_ID","Tag30_ID"\
                   ,"Tag31_ID","Tag32_ID","Tag33_ID","Tag34_ID","Tag35_ID","Tag36_ID","Tag37_ID","Tag38_ID","Tag39_ID","Tag40_ID"\
                   ,"Tag41_ID","Tag42_ID","Tag43_ID","Tag44_ID","Tag45_ID","Tag46_ID","Tag47_ID","Tag48_ID","Tag49_ID","Tag50_ID"]
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
        sReload="INSERT INTO " + Tablename_Core + " SELECT Extract_Group,Ref_Location,\
        Tag1_ID,Tag2_ID,Tag3_ID,Tag4_ID,Tag5_ID,Tag6_ID,Tag7_ID,Tag8_ID,Tag9_ID,Tag10_ID,\
        Tag11_ID,Tag12_ID,Tag13_ID,Tag14_ID,Tag15_ID,Tag16_ID,Tag17_ID,Tag18_ID,Tag19_ID,Tag20_ID,\
        Tag21_ID,Tag22_ID,Tag23_ID,Tag24_ID,Tag25_ID,Tag26_ID,Tag27_ID,Tag28_ID,Tag29_ID,Tag30_ID,\
        Tag31_ID,Tag32_ID,Tag33_ID,Tag34_ID,Tag35_ID,Tag36_ID,Tag37_ID,Tag38_ID,Tag39_ID,Tag40_ID,\
        Tag41_ID,Tag42_ID,Tag43_ID,Tag44_ID,Tag45_ID,Tag46_ID,Tag47_ID,Tag48_ID,Tag49_ID,Tag50_ID \
        FROM " + Tablename_Stg
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