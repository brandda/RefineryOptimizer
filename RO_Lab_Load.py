from importlib import reload
import RO_GlobalVar as GV
import teradatasql as TSQL
import sys
import time
import datetime

starttime=time.time()
GV=reload(GV)  # Reload picks up global variable changes after initial load
FileList=[]
Tablename_Stg=GV.TDLogVar["DATABASE"] + ".DB250092_RO_LAB_STG"
Tablename_Core=GV.TDLogVar["DATABASE"] + ".DB250092_RO_LAB"

commit=10000
print("Reading Input Data")
input=GV.TDLogVar["DATADIR"]
datalist=GV.ReadInputData(input,"Lab.csv",1,",",commit)[0]            
rowcount=GV.ReadInputData(input,"Lab.csv",1,",",commit)[1]            
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
            Component_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,\
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
            Result_Value VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Result_MinSpec VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,\
            Result_MaxSpec VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC\
            )\
            PRIMARY INDEX (Ref_Location,Sample_ID,Component_ID,Test_Rep,Sample_Point)"
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
        #sInsert="{fn teradata_try_fastload}INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,\
        #    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        sInsert="INSERT INTO " + Tablename_Stg + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        counter=1
        for sublist in datalist:            
            cur.execute(sInsert,sublist)
            print("       Commit " + str(counter) + " Complete")
            counter+=1
        print("Stage Load Complete")
        print("       " + str(rowcount) + " records loaded")
    except:
        print("       Load Failed")
        #print(sInsert)
        print(i)
        sys.exit("Load Failure")
        
    #  Setting empty cells to null
    NullColumnList=["Material_ID","Container_ID","Sample_Date","Sample_Time","Sample_By"\
                   ,"Receipt_Date","Receipt_Time","Receipt_By","Login_Date","Login_Time","Login_By"\
                   ,"Complete_Date","Complete_Time","Complete_By","Authorize_Date","Authorize_Time"\
                   ,"Authorize_By","Status","Result_Value","Result_MinSpec","Result_MaxSpec"]
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
                AND CORE.Sample_ID=STG.Sample_ID \
                AND CORE.Component_ID=STG.Component_ID \
                AND CORE.Test_Rep=STG.Test_Rep \
                AND CORE.Sample_Point=STG.Sample_Point \
            WHEN MATCHED THEN UPDATE SET \
                Material_ID=STG.Material_ID,Container_ID=STG.Container_ID,Sample_Date=STG.Sample_Date,\
                Sample_Time=STG.Sample_Time,Sample_By=STG.Sample_By,Receipt_Date=STG.Receipt_Date,\
                Receipt_Time=STG.Receipt_Time,Receipt_By=STG.Receipt_By,Login_Date=STG.Login_Date,\
                Login_Time=STG.Login_Time,Login_By=STG.Login_By,Complete_Date=STG.Complete_Date,\
                Complete_Time=STG.Complete_Time,Complete_By=STG.Complete_By,Authorize_Date=STG.Authorize_Date,\
                Authorize_Time=STG.Authorize_Time,Authorize_By=STG.Authorize_By,Status=STG.Status,\
                Result_Value=STG.Result_Value,Result_MinSpec=STG.Result_MinSpec,Result_MaxSpec=STG.Result_MaxSpec,\
                Last_Update_Dttm='" + current_timestamp + "' \
            WHEN NOT MATCHED THEN INSERT VALUES \
                (STG.Ref_Location,STG.Sample_ID,STG.Component_ID,STG.Test_Rep,STG.Sample_Point,STG.Material_ID,\
                STG.Container_ID,STG.Sample_Date,STG.Sample_Time,STG.Sample_By,STG.Receipt_Date,STG.Receipt_Time,\
                STG.Receipt_By,STG.Login_Date,STG.Login_Time,STG.Login_By,STG.Complete_Date,STG.Complete_Time,\
                STG.Complete_By,STG.Authorize_Date,STG.Authorize_Time,STG.Authorize_By,STG.Status,STG.Result_Value,\
                STG.Result_MinSpec,STG.Result_MaxSpec\
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