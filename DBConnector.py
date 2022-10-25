import sqlite3

def conectar():
    db = sqlite3.connect("DataLakeDB.db")
    cursor = db.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS Users (id INTEGER PRIMARY KEY, User varchar(250) NOT NULL UNIQUE, Contraseña varchar(250) NOT NULL)")
    cursor.execute("CREATE TABLE IF NOT EXISTS ControlDataLake (ID INTEGER(5) NOT NULL UNIQUE,Table varchar(250) ,TableID	INTEGER(5) NOT NULL UNIQUE,UploadDate	varchar(250)  NOT NULL,ModificationDate	varchar(250))")

    #UploadDate #Table #Size #Modification Date
    #cursor.execute("INSERT INTO books VALUE(1, 'Harry Potter', 'J. K. Rowling', '9.3')")
    db.commit()
    return db


def guardar_bd(User):
    
     db = conectar()    
     user = User.usuario
     contra = User.contraseña

     cursor = db.cursor()
     cursor.execute("INSERT INTO Usuarios(usuario,Contraseña) VALUES(?, ?)",(user, contra))
     
     db.commit()
     return print("Todo ok")
 
 
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

conectar ()
