import pandas as pd
import os,sys,datetime
from itertools import repeat
import shutil
import sqlite3
from DBConnector import Save_User,readControlDataLake,Update,connect_db,consultar_bd
#import configparser
from configparser import ConfigParser




# This function will create an ID to identify tables
def UniqueID (TableName):
    mybytes = TableName.encode('utf-8')
    ID = int.from_bytes(mybytes, 'little')
    return ID
    

#We must check if the functions have appropriate format and they are correct   
def Check_Parameters (Path,DestinationPath,DestinationPathToRename): 
    if (len(Path)>0 and os.path.isdir(Path)== True): 
            if(len(DestinationPath)>0):
                print(os.path.isdir(DestinationPath))
                return print("Everything Ok")
            else: 
                return print("Error: Variable empty")
    else: 
            return print("Error: Variable empty or it does not exist the origin path")   
    
    return 1

def checkcredentials ():
    parser = ConfigParser()
    parser.read('Variables.ini')
    #Access 
    user = parser.get('Access_User', 'user')
    password = parser.get('Access_User', 'password')
    #Save_User(user,password)
    check = checkuser(user)

    if user == check[1] and password == check[2]:
        print ("Log in correctly")
    else: 
        print ("Incorrect Password")
        
        

##//***************** Database  \\******************

 #We create a function that will be use for register table operations
def Update (FileID,File,UploadDate,ModificationDate,Origin,Destiny,OperationType): 
    FileIDstr = str(FileID)
    db = connect_db()    
    #print(TableID,Table_Name,UploadDate,ModificationDate,Origin,Destiny,OperationType)
    cursor = db.cursor()
    cursor.execute("INSERT INTO ControlDataLake(FileID,File,UploadDate,ModificationDate,Origin,Destiny,OperationType) VALUES(?,?,?,?,?,?,?)",
                   (FileIDstr,File,UploadDate,ModificationDate,Origin,Destiny,OperationType))
    db.commit()
    

#This Function will insert MetaData information to a database in order to control which type files and data goes to Datalake
def InsertMetadata(DataGovernTable):

    db = connect_db()    
    cursor = db.cursor()

    for i in range(len(DataGovernTable)):
        
        cursor.execute("INSERT INTO MetaData(FileID,TableName,Comment,CreationDate,ModificationDate,FileSize,FileType,StructuredData,Load,DateLoad) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (str(DataGovernTable.iloc[i][0]),DataGovernTable.iloc[i][1],DataGovernTable.iloc[i][2],str(DataGovernTable.iloc[i][3]),str(DataGovernTable.iloc[i][4]),
         DataGovernTable.iloc[i][5],DataGovernTable.iloc[i][6],DataGovernTable.iloc[i][7],DataGovernTable.iloc[i][8],DataGovernTable.iloc[i][9]))
        
    db.commit()
    
    return 0


#This Function will insert data table information in case the file would be a csv or excel or any kind of table
def  InsertTablesData(TableFields):
    db = connect_db()    
    cursor = db.cursor()
    for i in range(len(TableFields)):
        cursor.execute("INSERT INTO TableControl(FileID,ColumnNames,DataTypes) VALUES(?,?,?)",(str(TableFields.iloc[i][0]),str(TableFields.iloc[i][1]),str(TableFields.iloc[i][2])))
    db.commit()

    
    return 0
    
#// *********************  Extracting Data and Metadata  **************\\
    
    
def ExtractMetadata (Path): #Function to extract the metadata in order to include in the governed table
    metadata = dict()
    arr = os.listdir(Path)
    #Extracting all the metadata from files
    for Files in arr:
        FilePath = Path + '/' + Files
        metadata.update({Files:os.stat(FilePath)})
    return metadata

def ExtractFilenames (Path): #The function will extract all the names from the path origin 
    arr = os.listdir(Path)
    FileNames = []
    for Files in arr:
        FilePath = Files
        FileNames.append(FilePath)
    return FileNames


#//******************   Create Governed Table (Metadata)  *****************//


def GovernedTable (Path): #This function will create a table to index and filter fields and information about data files
    FileNames = ExtractFilenames(Path)
    metadata = ExtractMetadata(Path)
    DataGovern = pd.DataFrame ()
    TableFields = pd.DataFrame ()
    
    
    for names in FileNames:  #It will explore diferents files which are load
        listacolumnas = dict()
        
        Extension = names[len(names)-3 : len(names)] #Getting the extension and the file type
        if Extension == 'csv': #file is a csv
            df= pd.read_csv(Path + '/' + names)        
            columns = list(df.columns)
            types   = list(df.dtypes)
            IDTables  =list (repeat(UniqueID(names),len(types)))
            TableFieldsnew = pd.DataFrame ({'TableId':IDTables,'Column Names':columns,'Data Type':types }) 
            TableFields = pd.concat([TableFields, TableFieldsnew])

        
        if Extension == 'csv' or Extension == 'xlsx':   
            StructureData = 'Yes'
        else: 
            StructureData = 'No'
        #Metadata is save in a dataframe and it will be load to a table into the database 
        DataGovernDict = {'TableId':UniqueID(names),
                'Table Name': names,
                'Comment':'',  
                'Creation Date': datetime.datetime.fromtimestamp(metadata[names][9]),
                'Modification Date': datetime.datetime.fromtimestamp(metadata[names][8]),
                'File Size': metadata[names][6], 
                'File Type': Extension,
                'Structured Data':'Yes',
                'Load': 'No',
                'DateLoad': 'Never'}
        # Data Governed table which inform about the metadata
        DataGovernnew =  pd.DataFrame ([DataGovernDict]) # index=[0]) 
        DataGovern  = pd.concat([DataGovern, DataGovernnew], ignore_index=True)
        
    #Reset the index    
    DataGovern.reset_index(drop= True, inplace = True) 
    InsertMetadata(DataGovern)
    TableFields.reset_index(drop= True, inplace = True) 
    InsertTablesData(TableFields)
    display(TableFields) 
    display (DataGovern)
    
    
    #//***************** Ingestion to DataLake  ********************//
    
    
    
def copycsvfile (TableName):
    try:
        shutil.copy(Path + '/' + TableName , DestinationPath + '/' + TableName)
        print("File copied successfully.")
    except shutil.SameFileError:
         print("Source and destination represents the same file.")
    # If there is any permission issue
    except PermissionError:
        print("Permission denied.") 
    # For other errors
    except:
        print("Error occurred while copying file.")
    Update (UniqueID(TableName),str(TableName),str(datetime.datetime.now()),str(datetime.datetime.now()),Path + '/' + TableName,DestinationPath + '/' + TableName,Operations_Types[1] )
    return print("Save it in the data lake")

def create_directory():
    print("Creating directory")
    directory = DestinationPath
    try:
        os.mkdir(directory) 
    except OSError as error: 
        print(error)  
    print("The " + directory + " has been created ")
    print(os.listdir(DestinationPath))
        
def delete_directory():
    print("Deleting directory")
    directory = DestinationPath
    try:
        os.remove(DestinationPath)
    except OSError as e:
        print("Error: %s : %s" % (directory, e.strerror))
    print("The " + directory + " has been removed ")
    print(os.listdir(DestinationPath))
    
def delete_file(File):
    print("Deleting folder")
    directory = FolderPath
    try:
        os.remove(FolderPath)
    except OSError as e:
        print("Error: %s : %s" % (directory, e.strerror))
    Update (str(UniqueID (File)),File,str(datetime.datetime.now()),str(datetime.datetime.now()),Path + '/' + File,DestinationPath + '/' + File,Operations_Types[2] )
    print("The " + directory + " has been removed ")
    print(os.listdir(DestinationPath))
        
def rename_directory():
    print("Renaming directory")
    directory = DestinationPath
    try:
        os.rename(DestinationPath,DestinationPathToRename)
    except Exception as e:
        print(e)
    print(os.listdir(DestinationPath))
    print("The " + directory + " has changed the name ")    
    

def list_directory_contents():
    try:
        print ("Directory of the data lake is " + DestinationPath )
        print ("There are  these files in the data lake: ") 
        print(os.listdir(DestinationPath))
    except Exception as e:
        print(e)
        
        
        
# //************* Orchestrator/Pipeline  *********************//


#Main 

parser = ConfigParser()
parser.read('Variables.ini')
#Access 
user = parser.get('Access_User', 'user')
password = parser.get('Access_User', 'password')
consultar_bd(user)



#Variables
#First we define variables in order to extract the files from origins 
Path = parser.get('Variables_config', 'OriginPath')
global DestinationPath
DestinationPath = parser.get('Variables_config', 'DestinationPath')
DestinationPathToRename = parser.get('Variables_config', 'DestinationPathToRename')
global FolderPath
FolderPath = parser.get('Variables_config', 'FolderPath')
File =  parser.get('Variables_config', 'File')
File 
global Operations_Types
Operations_Types = {
    1: "File Saved",
    2: "File Deleted",
    3: "File Modificated"
}

Check_Parameters (Path,DestinationPath,DestinationPathToRename)

print(parser.get('Directory_Options', 'list_directory_contents'))        
if parser.get('Directory_Options', 'Create_directory') == "1":
    create_directory()
if parser.get('Directory_Options', 'Delete_directory') == "1":
    delete_directory()
if parser.get('Directory_Options', 'Delete_file') == "1":
    delete_file(File)
if parser.get('Directory_Options', 'Rename_directory') == "1":
    rename_directory()
if parser.get('Directory_Options', 'list_directory_contents') == "1":
    list_directory_contents()
    
GovernedTable(Path)
if parser.get('Variables_config', 'FilesLoad') == "ALL":
    FileNames = ExtractFilenames(Path)
    for Files in FileNames: #Loading all files in the origin
        copycsvfile(Files)
    else: 
        parser.get('Variables_config', 'FilesLoad')
        copycsvfile(Files)
