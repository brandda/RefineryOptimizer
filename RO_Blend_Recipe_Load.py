from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_BLEND_RECIPE_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_BLEND_RECIPE"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"Blend_Recipe.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"Blend_Recipe.csv",1,",",commit)[1]            
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
            Blend_Rep VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Blend_PlanAct VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            LCC_42_49 VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            WREF_44_9 VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            HREF_48_48 VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            LREF_45_23 VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            LOALK_41_31 VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            HBAT_85_20 VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            TOL_86_21  VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            OLE_70_22  VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            BUT_46_78  VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            TRANS_87_108  VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC) \
            PRIMARY INDEX (Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct)"
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
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
    NullColumnList=["LCC_42_49","WREF_44_9","HREF_48_48","LREF_45_23","LOALK_41_31","HBAT_85_20"\
                    ,"TOL_86_21","OLE_70_22","BUT_46_78","TRANS_87_108"]
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
        sReload="INSERT INTO " + Tablename_Core + " \
            SELECT Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct,42 as Material_Id,49 as Container_Id,\
                LCC_42_49 as Blend_Component_Volume,current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct,44 as Material_Id,9 as Container_Id,\
                WREF_44_9 as Blend_Component_Volume,current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct,48 as Material_Id,48 as Container_Id,\
                HREF_48_48 as Blend_Component_Volume,current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct,45 as Material_Id,23 as Container_Id,\
                LREF_45_23 as Blend_Component_Volume,current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct,41 as Material_Id,31 as Container_Id,\
                LOALK_41_31 as Blend_Component_Volume,current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct,85 as Material_Id,20 as Container_Id,\
                HBAT_85_20 as Blend_Component_Volume,current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct,86 as Material_Id,21 as Container_Id,\
                TOL_86_21 as Blend_Component_Volume,current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct,70 as Material_Id,22 as Container_Id,\
                OLE_70_22 as Blend_Component_Volume,current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct,46 as Material_Id,78 as Container_Id,\
                BUT_46_78 as Blend_Component_Volume,current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,Blend_Rep,Blend_PlanAct,87 as Material_Id,108 as Container_Id,\
                TRANS_87_108 as Blend_Component_Volume,current_timestamp(0),current_timestamp(0) \
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