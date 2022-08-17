from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_BLEND_PRED_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_BLEND_PRED"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"Blend_Pred.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"Blend_Pred.csv",1,",",commit)[1]            
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
            RON_76 VARCHAR(20),\
            MON_67 VARCHAR(20),\
            TVLF_94 VARCHAR(20),\
            DI_32 VARCHAR(20),\
            BENZ_20 VARCHAR(20),\
            SULF_85 VARCHAR(20),\
            AROM_19  VARCHAR(20),\
            OLEF_95  VARCHAR(20),\
            E200_34  VARCHAR(20),\
            E300_35  VARCHAR(20),\
            D8610_1  VARCHAR(20),\
            D8650_2  VARCHAR(20),\
            D8690_3  VARCHAR(20),\
            API_62  VARCHAR(20),\
            RVP_80 VARCHAR(20),\
            RVP_MinSpec VARCHAR(20),\
            RM2_78 VARCHAR(20),\
            RM2_MinSpec VARCHAR(20))\
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
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
    NullColumnList=["RON_76","MON_67","TVLF_94","DI_32","BENZ_20","SULF_85","AROM_19","OLEF_95"\
                   ,"E200_34","E300_35","D8610_1","D8650_2","D8690_3","API_62","RVP_80"\
                   ,"RVP_MinSpec","RM2_78","RM2_MinSpec"]
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
            SELECT Ref_Location,Blend_ID,76 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,RON_76 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,67 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,MON_67 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,94 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,TVLF_94 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,32 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,DI_32 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,20 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,BENZ_20 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,85 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,SULF_85 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,19 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,AROM_19 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,95 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,OLEF_95 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL\
            SELECT Ref_Location,Blend_ID,34 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,E200_34 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
                FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,35 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,E300_35 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,1 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,D8610_1 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,2 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,D8650_2 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,3 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,D8690_3 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,62 as Component_ID,CAST(NULL AS VARCHAR(20)) AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,\
                API_62 as Predicted_Value,current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,80 as Component_ID,RVP_MinSpec AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,RVP_80 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Blend_ID,78 as Component_ID,RM2_MinSpec AS Spec_Min,\
                CAST(NULL AS VARCHAR(20)) AS Spec_Max,RM2_78 as Predicted_Value,\
                current_timestamp(0),current_timestamp(0) \
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