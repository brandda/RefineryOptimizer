from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import os
import sys
import csv
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_SENSOR_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_SENSOR"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]+"Tags"
datalist=GV.ReadInputData(input,"*.csv",4,",",commit)[0]            
rowcount=GV.ReadInputData(input,"*.csv",4,",",commit)[1]        
if len(datalist)==0:
    sys.exit("No Input Data")

#  Connect to Teradata using global variable parameters
print("Connecting to Database")
con=TSQL.connect(host=GV.TDLogVar["SERVER"],user=GV.TDLogVar["USER"],password=GV.TDLogVar["PASSWORD"],logmech=GV.TDLogVar["LOGMECH"])    

# Clear old stage and error tables and create new stage table
with con.cursor() as cur:
    GV.ClearStageEnvironment(cur,Tablename_Stg)

    print("Creating Stage Tables")
    try:
        sStageCreate="CREATE MULTISET TABLE " + Tablename_Stg + ", \
        NO BEFORE JOURNAL,NO AFTER JOURNAL,CHECKSUM = DEFAULT,DEFAULT MERGEBLOCKRATIO \
            (Row_Num INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MAXVALUE 999999999),\
            Trans_Timestamp VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Ref_Location VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Tag1_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag1_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag2_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag2_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag3_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag3_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag4_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag4_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag5_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag5_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag6_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag6_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag7_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag7_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag8_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag8_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag9_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag9_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag10_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag10_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag11_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag11_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag12_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag12_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag13_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag13_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag14_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag14_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag15_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag15_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag16_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag16_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag17_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag17_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag18_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag18_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag19_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag19_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag20_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag20_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag21_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag21_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag22_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag22_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag23_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag23_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag24_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag24_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag25_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag25_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag26_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag26_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag27_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag27_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag28_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag28_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag29_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag29_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag30_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag30_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag31_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag31_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag32_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag32_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag33_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag33_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag34_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag34_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag35_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag35_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag36_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag36_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag37_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag37_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag38_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag38_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag39_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag39_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag40_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag40_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag41_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag41_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag42_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag42_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag43_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag43_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag44_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag44_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag45_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag45_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag46_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag46_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag47_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag47_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag48_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag48_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag49_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag49_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag50_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Tag50_Value VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC) \
        PRIMARY INDEX(row_num)"
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
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + \
        #" (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
        #?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + \
        " (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        
        counter=1
        for sublist in datalist:            
            cur.execute(sInsert,sublist)
            print("       Commit " + str(counter) + " Complete")
            counter+=1
        print("Stage Load Complete")
        print("       " + str(rowcount) + " records loaded")
    except:
        sys.exit("Stage Load Failed")
    
    #  Call stored procedures to drop AJI, transform data from stage to core and recreate AJI
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print("Dropping Core AJI")
    try:
        cur.execute("CALL TRNG_RefineryOptimizer.DB250092_SENSOR_AJI_IDX_DROP()")
        print("       AJI Dropped")
    except:
        print("       AJI Drop Failed")
    print("Executing Stage to Core Transform")
    try:
        cur.execute("CALL TRNG_RefineryOptimizer.DB250092_MERGE_SENSOR_STG_TO_SENSOR()")
        print("       Transform Successful")
    except:
        print("       Transform Failed")
    print("Creating Core AJI")
    try:
        cur.execute("CALL TRNG_RefineryOptimizer.DB250092_SENSOR_AJI_IDX_CREATE()")
        print("       AJI Created")
    except:
        print("       AJI Create Failed")
    print("Dropping Stage Table")
    try:
        cur.execute("DROP TABLE " + Tablename_Stg)
        print("       " + Tablename_Stg + " dropped")
    except:
        print("       " + Tablename_Stg + " does not exist")
con.close()

#  Archive load files
print()
print("Archiving Load Files")
print("       "+str(GV.ArchiveFiles(input,"*.csv"))+" Load Files Archived")

print()
print("Load Complete")
print("Execution took " + str(int(time.time()-starttime)) + " seconds")    
