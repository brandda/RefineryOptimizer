import os
import csv
import fnmatch

TDLogVar = {"SERVER":"tdprd.td.teradata.com","DATABASE":"TRNG_RefineryOptimizer",
	"LOGMECH":"LDAP","USER":"DB250092","PASSWORD": "*********",
	"DATADIR":"c:/teradata/current/general/refineryoptimizer/data/"}


#################################################################
def ReadInputData(InputDir,InputFileMask,SkipRows,Delimiter,Commit):
    #  Read files via filter in target subdirectory
    FileList=[]
    dir = os.listdir(InputDir)
    for file in dir:
        if fnmatch.fnmatch(file,InputFileMask):
            FileList.append(file)
    datalist=[]
    totalrowcount=0
    filecount=0
    for file in FileList:
        filecount+=1
        filerowcount=0
        input=InputDir+"/"+file
        with open(input,'r') as read_obj:
            csv_reader=csv.reader(read_obj,delimiter=Delimiter)
            for line in csv_reader:
                totalrowcount+=1
                filerowcount+=1
                if filerowcount > SkipRows:
                    datalist.append(line)
   
    # Break large lists into list of lists for partial commit to database
    finallist=[]
    interlist=[]
    counter=1
    for row in datalist:
        interlist.append(row)
        counter+=1
        if counter%Commit==0:
            finallist.append(interlist)
            interlist=[]
    finallist.append(interlist)

    totalrowcount-=filecount*SkipRows

    return finallist,totalrowcount
#################################################################


#################################################################
def ClearStageEnvironment(cur,Tablename_Stg):
    print("Clearing Environment")
    try:
        cur.execute("DROP TABLE " + Tablename_Stg + "_ET")
        print("       " + Tablename_Stg + "_ET dropped")
    except:
        print("       " + Tablename_Stg + "_ET does not exist")
    try:
        cur.execute("DROP TABLE " + Tablename_Stg + "_UV")
        print("       " + Tablename_Stg + "_UV dropped")
    except:
        print("       " + Tablename_Stg + "_UV does not exist")
    try:
        cur.execute("DROP TABLE " + Tablename_Stg + "_LT")
        print("       " + Tablename_Stg + "_LT dropped")
    except:
        print("       " + Tablename_Stg + "_LT does not exist")
    try:
        cur.execute("DROP TABLE " + Tablename_Stg + "_LOG")
        print("       " + Tablename_Stg + "_LOG dropped")
    except:
        print("       " + Tablename_Stg + "_LOG does not exist")
    try:
        cur.execute("DROP TABLE " + Tablename_Stg)
        print("       " + Tablename_Stg + " dropped")
    except:
        print("       " + Tablename_Stg + " does not exist")
#################################################################


#################################################################
def StageNullUpdate(cur,Tablename_Stg,NullColumnList):
    print()
    print("Beginning Null Update")
    for column in NullColumnList:
        try:
            #sNullUpdate="UPDATE " + Tablename_Stg + " SET " + column + "=NULL WHERE " + column + "='' AND " + column + " <> 0"
            #sNullUpdate="UPDATE " + Tablename_Stg + " SET " + column + "=NULL WHERE " + column + "=''"
            sNullUpdate="UPDATE " + Tablename_Stg + " SET " + column + "=NULL WHERE " + column + "='' AND " + column + " <> '0'"
            cur.execute(sNullUpdate)
            output=1
        except:
            output=0
            print("       Null Update failed")
            print(sNullUpdate)
            break
    return output
#################################################################


#################################################################
def ArchiveFiles(InputDir,InputFileMask):
    #  Archive files
    filecount=0
    dir = os.listdir(InputDir)
    for file in dir:
        if fnmatch.fnmatch(file,InputFileMask):
            try:
                os.remove(InputDir+"/Archive/"+file)
            except:
                pass
            os.rename(InputDir+"/"+file,InputDir+"/Archive/"+file)
            filecount+=1
    return filecount
#################################################################




