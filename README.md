 # Local Data Lake #



### Getting Started ###
 
 Get set up locally in two steps:

#### Change variables according to your local configuration and run cell by cell the file DataLakeProcessor.ipynb ####

### Variables ### 

#### Replace the values with your options in order to make it work. ####
In order make it a bit customizable there is a config file where it will help to make diferet operations in the datalake
**FLASK_ENV:** The environment in which to run your application; either *development* or *production*. <br />
**OriginPath:** The source path where the files are before go,if there are more than one source path it can be integrated in a future with a list of paths  <br />
**DestinationPath:** The path of the local DataLake where the files will be saved <br />
**DestinationPathToRename** The new path and name for the DataLake <br />
**FileDel:** The file to be deleted (We need to set to 1 the variable Delete_file) <br />
**FolderPath:** The path to the the file we want to delete <br />
**FilesLoad**  if we want to load all the files from the source we must set this option to ALL, if we want to select only one file we can put the name <br />

#### This variables can be use to modify the datalake ubication ####
**Create_directory:** Set to 1 in order to create the DataLake (We need to fill DestinationPath )<br />
**Delete_directory:** Set to 1 in order to delete the DataLake (We need to fill DestinationPath ) <br />
**Rename_directory:** Set to 1 in order to rename the DataLake (We need to fill DestinationPathToRename ) <br />
**list_directory_contents:** Set to 1 in order to list the files in DataLake (We need to fill DestinationPath ) <br />
**Delete_file:**  Set to 1 in order to delete the files in DataLake (We need to fill FileDel and  FolderPath) <br />

#### Security Variables ####
**user:** You need to input a correct user to run the program <br />
**password:** You need to input a correct pass to run the program <br />

## Tutorial ##

In order to make work the script we need to install and import these list of libraries:
pandas <br />
os <br />
datetime <br />
itertools <br />
repeat <br />
shutil <br />
sqlite3 <br />
configparser <br />


## Data Lake Flowchart ##

This Data lake will be divided in several phases: 


![DataLakeFlowChart](https://github.com/hdelas/Loka-Exercise/tree/main/Images/DataLakeFlowChart.png "DataLakeFlowChart")


First we will check credentials. In the future, users and roles could be created as well as security by file.

During the second phase, we should be able to perform different operations over the datalake or simply leave it as is and schedule a time to run the script and load the files automatically. This option could be implemented with Apache Airflow, but in this case it does not make sense as we will need a server running.

Another option could be to make a .batch file forcing windows to launch the script doing daily reloads for instance.
To track the loads and files we are moving, several tables will be created each time the script is run: Metada Table: in order to know when a file was created, its type, and features.

Table Information: When the file contains semistructured or structured data, we should store the field information in order to be able to search the file and load it into a data warehouse in the future.
After analyzing the type of data we have if the load variable is activated the program will copy the files selected into the data lake. A governance table will be generated in order to control the operations that we do in the data lake.


## Data Base Flowchart ##

As far as we want to control what happens in the data lake we will need different tables to know the information about files and the data we are storing into the data lake.

We will use these tables:

![DataBaseDiagram](https://github.com/hdelas/Loka-Exercise/tree/main/Images/DataBaseDiagram.png "DataBaseDiagram")

The Users table is used to grant access to the different users of the data lake. We could add a role field and join it to the controlDatalake in order to restrict the access of some users to sensible data.

For each file, we will have information such as the modification date or the size that could be useful in the future to manage the data in a data warehouse. For tables, we should store the field ID, so that it could be localized quickly and used in a ETL.

Finally, it is critical to register every operation we do in the data lake so the information could be lost or deleted by error, this table could be used to create a backup process every time a movement is done.

## Analyzing aspects of this data lake ##

These implementations of a data lake classify the information on the basis of two types of files: csvs and excels.

With regards to ingestion techniques, in this case we need to control what data is storing, so if we have tables, we should add a way to locate it easily, so the data lake follows these principles: unified view of data, simplified data management, and traceability.

In order to follow the data lifecycle, we have different stages, such as capturing, storing, and deleting, as well as governing and organizing it for future use.

A further development of access control could be made by encrypting passwords, creating roles, and assigning security to lines and files.

A process is monitored and managed to ensure data quality. Verifications are conducted and parts that cannot be loaded or deleted are notified.

In the case of data enrichment, orchestrator can be programmed to identify changes in the source data so that updated data can be loaded.

## Imporvements for the future ##

Create backups every time a modification is done.
Create security by role and by file.
Improve the Ingestion techniques in order to be able to process streaming data or large files with spark or Hdfs Hadoop.
