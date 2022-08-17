from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_INVENTORY_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_INVENTORY"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"Inventory.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"Inventory.csv",1,",",commit)[1]            
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
            Inv_Timestamp VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Tank VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Material VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Gross_Qty VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            BSW VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Water VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Net_Qty VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Units VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Gauge VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Temp VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Temp_UOM CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC\
            )\
            PRIMARY INDEX (Ref_Location,Inv_Timestamp,Tank)"
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
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?)"
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
    NullColumnList=["Material","Gross_Qty","BSW","Water","Net_Qty","Units","Temp","Temp_UOM"]
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
            SELECT Inv_Stg.Ref_Location,Container_Id,\
                CAST(SUBSTRING(inv_timestamp from 1 for 10) AS DATE) as Inv_Date,\
                CAST(SUBSTRING(inv_timestamp from 12 for 8) AS TIME(0)) as Inv_Time,\
                Material_Id,Inv_Stg.Gross_Qty,Inv_Stg.Net_Qty,Inv_Stg.Temp AS Temperature,\
                Inv_Stg.Temp_UOM as Temperature_UOM,Inv_Stg.Gauge,NULL as Gauge_Ft,\
                NULL as Gauge_In,Inv_Stg.BSW as BSW,NULL as BSW_Ft,NULL as BSW_In,\
                Inv_Stg.Water as Water,Inv_Stg.Units FROM " + Tablename_Stg + " Inv_Stg \
            INNER JOIN " + GV.TDLogVar["DATABASE"] + ".DB250092_RO_Container Cont \
                ON Inv_Stg.Ref_Location = Cont.Ref_Location \
                AND Inv_Stg.Tank = Cont.Container_Desc \
            INNER JOIN " + GV.TDLogVar["DATABASE"] + ".DB250092_RO_Inventory_Dcd Inv_Dcd \
                ON Inv_Stg.Ref_Location = Inv_Dcd.Ref_Location \
                AND Inv_Stg.Material = Inv_Dcd.Material\
            ) STG \
        ON CORE.Ref_Location = STG.Ref_Location \
            AND CORE.Inventory_Date = STG.Inv_Date \
            AND CORE.Inventory_Time = STG.Inv_Time \
            AND CORE.Container_ID = STG.Container_ID \
        WHEN MATCHED THEN UPDATE SET \
            Material_ID=STG.Material_ID,Gross_Qty=STG.Gross_Qty,Net_Qty=STG.Net_Qty,\
            Temperature=STG.Temperature,Temperature_UOM=STG.Temperature_UOM,\
            Gauge=STG.Gauge,Gauge_Ft=STG.Gauge_Ft,Gauge_In=STG.Gauge_In,\
            BSW=STG.BSW,BSW_Ft=STG.BSW_Ft,BSW_In=STG.BSW_In,Water=STG.Water,Units=STG.Units,\
            Last_Update_Dttm='" + current_timestamp + "' \
        WHEN NOT MATCHED THEN INSERT VALUES\
        (STG.Ref_Location,STG.Container_ID,STG.Inv_Date,STG.INV_Time,STG.Material_ID,\
        STG.Gross_Qty,STG.Net_Qty,STG.Temperature,STG.Temperature_UOM,STG.Gauge,\
        STG.Gauge_Ft,STG.Gauge_In,STG.BSW,STG.BSW_Ft,STG.BSW_In,STG.Water,STG.Units\
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