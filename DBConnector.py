import sqlite3
import sqlalchemy

def connect_db():
    db = sqlite3.connect("DataLakeDB.db")
    cursor = db.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS Users (id INTEGER(30) PRIMARY KEY, User varchar(250) NOT NULL UNIQUE, Password varchar(250) NOT NULL)")
    cursor.execute("CREATE TABLE IF NOT EXISTS ControlDataLake (FileID varchar(50) NOT NULL,File varchar(50) ,"
                   "UploadDate varchar(50)  NOT NULL, ModificationDate varchar(50),Origin varchar(250),Destiny varchar(250),OperationType varchar (15) )")
    cursor.execute("CREATE TABLE IF NOT EXISTS TableControl (FileID varchar(50) NOT NULL,ColumnNames varchar(50),DataTypes varchar(10))")
    cursor.execute("CREATE TABLE IF NOT EXISTS MetaData (FileID varchar(50),TableName varchar(250),Comment varchar(250), "
                    "CreationDate varchar(50),ModificationDate varchar(50),FileSize varchar(50) ,FileType varchar(10),"
                    "StructuredData varchar(250),Load varchar(50),DateLoad varchar(50)) ")
    db.commit()
    return db


def Save_User(user,password):
    
     db = connect_db()    
    #  user = User.user
    #  contra = User.pass

     cursor = db.cursor()
     cursor.execute("INSERT INTO Users(User,Password) VALUES(?, ?)",(user, password))
     
     db.commit()
     return print("Todo ok")

 #We create a function that will be use for register table operations
def Update (TableID,Table_Name,UploadDate,ModificationDate,Origin,Destiny,OperationType): 
    TableIDstr = str(TableID)
    db = connect_db()    
    print(TableID,Table_Name,UploadDate,ModificationDate,Origin,Destiny)
    cursor = db.cursor()
    cursor.execute("INSERT INTO ControlDataLake(FileID,File,UploadDate,ModificationDate,Origin,Destiny,OperationType) VALUES(?,?,?,?,?,?)",(TableIDstr,Table_Name,UploadDate,ModificationDate,Origin,Destiny,OperationType))
    db.commit()

def checkuser(user):
    db = connect_db() 
    cursor = db.cursor()
    cursor.execute("SELECT * FROM Users where User = ? ",[user])
    respuesta_consulta = cursor.fetchall()
    db.commit()
    if len(respuesta_consulta) >0:
        lista = respuesta_consulta[0]
    else:
        lista=""
    #contra = User.contraseña

    return lista


     
