from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_BLEND_LAB_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_LAB"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"Blend_Lab.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"Blend_Lab.csv",1,",",commit)[1]            
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
            Sample_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Test_Rep VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Sample_Point VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
            Material_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Container_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Sample_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Sample_Time VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Sample_By VARCHAR(45) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Receipt_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Receipt_Time VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Receipt_By VARCHAR(45) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Login_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Login_Time VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Login_By VARCHAR(45) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Complete_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Complete_Time VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Complete_By VARCHAR(45) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Authorize_Date VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Authorize_Time VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Authorize_By VARCHAR(45) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Status VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,\
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
            RM2_78 VARCHAR(20))\
            PRIMARY INDEX (Ref_Location,Sample_ID,Test_Rep,Sample_Point)"
        cur.execute(sStageCreate)
        print("       Stage Tables Created")
    except:
        print("       Stage Tables Creation Failed")
        print(sStageCreate)
        sys.exit("Stage Create Failure")

    #  Fastload stage table
    print()
    print("Loading Stage Table With " + str(rowcount) + " Records")
    print("Database Commits Occur Every " + str(commit) + " records")
    try:
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,\
        #    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,\
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
    NullColumnList=["Material_ID","Container_ID","Sample_Date","Sample_Time","Sample_By"\
                   ,"Receipt_Date","Receipt_Time","Receipt_By","Login_Date","Login_Time","Login_By"\
                   ,"Complete_Date","Complete_Time","Complete_By","Authorize_Date","Authorize_Time","Authorize_By"\
                   ,"Status","RON_76","MON_67","TVLF_94","DI_32","BENZ_20","SULF_85","AROM_19","OLEF_95"\
                   ,"E200_34","E300_35","D8610_1","D8650_2","D8690_3","API_62","RVP_80","RM2_78"]
    if GV.StageNullUpdate(cur,Tablename_Stg,NullColumnList)==0:
        sys.exit("Column Null Failure")
    else:
        print("       Null Update completed")

    #  Merge Stage to Core
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print("Truncating and Reloading Core")
    try:
        sTrunc="DELETE FROM " + Tablename_Core + " WHERE Container_ID Between 97 and 101"
        cur.execute(sTrunc)
        sReload="INSERT INTO " + Tablename_Core + " \
            SELECT Ref_Location,Sample_ID,76 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                RON_76 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,67 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                MON_67 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,94 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                TVLF_94 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,32 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                DI_32 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,20 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                BENZ_20 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,85 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                SULF_85 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
                FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,19 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                AROM_19 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,95 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                OLEF_95 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,34 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                E200_34 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,35 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                E300_35 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
                FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,1 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                D8610_1 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,2 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                D8650_2 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,3 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                D8690_3 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,62 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                API_62 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,80 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                RVP_80 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
                current_timestamp(0),current_timestamp(0) \
            FROM " + Tablename_Stg + " \
            UNION ALL \
            SELECT Ref_Location,Sample_ID,78 as Component_ID,\
                Test_Rep,Sample_Point,Material_ID,Container_ID,\
                Sample_Date,Sample_Time,Sample_By,Receipt_Date,Receipt_Time,Receipt_By,\
                Login_Date,Login_Time,Login_By,Complete_Date,Complete_Time,Complete_By,\
                Authorize_Date,Authorize_Time,Authorize_By,Status,\
                RM2_78 as Result_Value,NULL as Result_MinSpec,NULL as Result_MaxSpec,\
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