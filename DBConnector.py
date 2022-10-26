import sqlite3

def connect_db():
    db = sqlite3.connect("DataLakeDB.db")
    cursor = db.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS Users (id INTEGER(30) PRIMARY KEY, User varchar(250) NOT NULL UNIQUE, Contraseña varchar(250) NOT NULL)")
    cursor.execute("CREATE TABLE IF NOT EXISTS ControlDataLake (TableID varchar(50) NOT NULL,TableName varchar(250) ,UploadDate varchar(250)  NOT NULL, ModificationDate varchar(250),Origin varchar(250),Destiny varchar(250) )")
    #

    #,Origin varchar(250),Destiny varchar(250)
    db.commit()
    return db


def Save_User(User):
    
     db = connect_db()    
     user = User.usuario
     contra = User.contraseña

     cursor = db.cursor()
     cursor.execute("INSERT INTO ControlDataLake(TableID,UploadDate) VALUES(?, ?)",(user, contra))
     
     db.commit()
     return print("Todo ok")

 #We create a function that will be use for register table operations
def Update (TableID,Table_Name,UploadDate,ModificationDate,Origin,Destiny): 
    TableIDstr = str(TableID)
    db = connect_db()    
    print(TableID,Table_Name,UploadDate,ModificationDate,Origin,Destiny)
    cursor = db.cursor()
    cursor.execute("INSERT INTO ControlDataLake(TableID,TableName,UploadDate,ModificationDate,Origin,Destiny) VALUES(?,?,?,?,?,?)",(TableIDstr,Table_Name,UploadDate,ModificationDate,Origin,Destiny))
    db.commit()

 #We create a function that will be use to get info from Data Governede table
def readControlDataLake (TableID):
    db = sqlite3.connect("Usuarios.db")
    cursor = db.cursor()
    cursor.execute("SELECT * FROM ControlDataLake where TableID = ? ",[TableID])
    respuesta_consulta = cursor.fetchall()
    if len(respuesta_consulta) >0:
        print(respuesta_consulta)
        lista = respuesta_consulta[0]
    else:
        lista=""
    return lista
 

def consultar_bd(usuario):
    db = sqlite3.connect("Usuarios.db")
    cursor = db.cursor()
    cursor.execute("SELECT * FROM Usuarios where usuario = ? ",[usuario])
    respuesta_consulta = cursor.fetchall()
    if len(respuesta_consulta) >0:
        print(respuesta_consulta)
        lista = respuesta_consulta[0]
    else:
        lista=""
    #contra = User.contraseña
    return lista


     
