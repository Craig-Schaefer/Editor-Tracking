import arceditor
import time
import datetime
from datetime import datetime, timedelta
from datetime import date
import arcpy, re, os, errno
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
#import pandas as pd
#import numpy as np
#import subprocess
#import shutil
#from shutil import copyfile
arcpy.SetLogHistory(False)
arcpy.env.overwriteOutput = True



# Global variables:

lookbackdays = 14

#lookbackdaysSTR = raw_input('Enter Number of Look-back Days: ' + str())
#lookbackdays = int(lookbackdaysSTR)

rootpath = r"C:"
topfolder = r"\ET"   # additional sub-folders can be added here if necessary
filegdb = r"\ET.gdb"
ET_gdb = rootpath + topfolder + filegdb
ET_X_DAYStem_csv = r"\\main\___redactedFolderPath___\ET_X_DAYStem.csv"   # .csv file used as template to make a table
ET_X_DaysEdits = ET_X_DAYStem_csv
d = datetime.today() - timedelta(days=lookbackdays)
dconv = str(d)
lookback = dconv[:19]
# f = datetime.today() - datetime(2020, 3, 23)
# fconv = str(f)
# flookback = fconv[:19]
crrntdate = time.strftime("%Y" + "-" + "%m" + "-" + "%d" + " " + "%H" + ":" + "%M" + ":" + "%S")
crrntdatetrunc = crrntdate[:10]
ET_90_DaysEdits = "ET_" + str(lookbackdays) + "_DaysEdits"
Delete_succeeded = "false"
# staticlookback = datetime(2020, 3, 23)
# hardlookback = str(staticlookback)

# print (staticlookback)
# print flookback
# print dconv
# print crrntdate
# print crrntdatetrunc
print lookback


if arcpy.Exists(rootpath + topfolder):
	print "The local path " + rootpath + topfolder + " already exists"
else:
	# Process: Create Database Connection
	# Process: Create File GDB
	arcpy.CreateFolder_management(rootpath, topfolder)
	print "Created local path " + rootpath + topfolder 
	
if arcpy.Exists(rootpath + topfolder + filegdb):
	print "Connection to " + rootpath + topfolder + filegdb + ".gdb already exists"
else:
	# Process: Create Database Connection
	# Process: Create File GDB
	arcpy.CreateFileGDB_management(rootpath + topfolder, filegdb, "CURRENT")
	print "Created connection to " + rootpath + topfolder + filegdb


# Process: Delete
print "Deleting " + ET_90_DaysEdits + " table."
arcpy.Delete_management(ET_90_DaysEdits, "Table")

print "Creating empty " + ET_90_DaysEdits + " table."
# Process: Create Table
arcpy.CreateTable_management(ET_gdb, ET_90_DaysEdits , ET_X_DAYStem_csv, "")




# ---  Cycle through databases, aggregating LASTEDITOR information...  ---

# Database Connection details
# The name you choose to give the SDE connection goes here
databaseConnectionName = "TUVW_Water_Infr"
# The root path to which you want the data sent goes here
SavePath = rootpath + topfolder
# The unchanging information about the database connection goes here
targetDatabase = "VVVVVVV"   # target database removed - was something like "TUVW_WaterInfrastructure"
targetInstance = "ZZZZZZZ"   # instance removed - was something like "g112345dbs05"
targetUsername = "YYYYYYY"   # username removed
targetPassword = "XXXXXXX"   # password removed
targetVersion = "UUUUUUU"   # target version removed - was something like "INFR_WATER.INFR_Water_Quality"
Database_Connection_Folder_Path = "Database Connections"
Database_Connections = Database_Connection_Folder_Path
# The information about the temporary file geodatabase goes here
tempFileGDBName = "AllDefault"
# additional folder-path \\ elements added to aid in path creation
OutputPath = SavePath + "\\"
discipline = "Water"

# Test for databaseConnectionName and Create Connection if necessary	
if arcpy.Exists('Database Connections\\' + databaseConnectionName + '.sde'):
	print "Connection to " + databaseConnectionName + ".sde already exists"
else:
    	
	# Process: Create Database Connection
	arcpy.CreateDatabaseConnection_management(Database_Connection_Folder_Path, databaseConnectionName, "SQL_SERVER", targetInstance, "DATABASE_AUTH", targetUsername, targetPassword, "SAVE_USERNAME", targetDatabase, "", "TRANSACTIONAL", targetVersion, "")
	print "Created connection to " + databaseConnectionName + ".sde"
	
# Test for file geodatabase and Create file geodatabase if necessary	
# Local variables:
AllDefault_gdb = SavePath

if arcpy.Exists(OutputPath + tempFileGDBName + '.gdb'):
	print "Connection to " + OutputPath + tempFileGDBName + ".gdb already exists"
else:
	# Process: Create Database Connection
	# Process: Create File GDB
	arcpy.CreateFileGDB_management(SavePath, tempFileGDBName, "CURRENT")
	print "Created connection to " + OutputPath + tempFileGDBName + ".gdb"

# Convert feature classes within feature datasets to shapefiles
arcpy.env.workspace = "Database Connections\\" + databaseConnectionName + ".sde"

print "Database is " + databaseConnectionName
# Print all the feature datasets 
datasets = arcpy.ListDatasets("", "")

print "Datasets are:"
for dataset in datasets:
    print(dataset)	
	 
# Rename and export all the feature classes within each dataset
for dataset in datasets:
	arcpy.env.workspace = "Database Connections\\" + databaseConnectionName + ".sde\\" + dataset
	
	print "Now processing feature classes from " + dataset
		
	# get a list of feature classes in arcpy.env.workspace
	listFC = arcpy.ListFeatureClasses()
	# reformats the array/list of feature classes and datasets
	for fc in listFC:
		print fc
		
		# Local variables:
		fZc = fc
		func = re.sub('[.]', '', fZc)
		fundc = re.sub('[_]', '', func)
		fbrlc = re.sub('\[', '', fundc)
		fbrrc = re.sub('\]', '', fbrlc)
		f_c = fbrrc
		TUVW_featureClassVariable = fc
		emexditem = str(f_c)
		featureClassLayer = emexditem + "_Layer"
		featureClassLayer2 = emexditem
		featureClassLayer3 = featureClassLayer2
		featureClassLayer4 = featureClassLayer3
		featureClassLayer5 = featureClassLayer4
		Stats_featureClass = rootpath + topfolder + filegdb + "\\Stats_" + emexditem 
		Stats_featureClass__2_ = Stats_featureClass
		Stats_featureClass__3_ = Stats_featureClass__2_
		ET_X_DaysEdits__2_ = Stats_featureClass__3_
		ET_X_DaysEdits = rootpath + topfolder + filegdb + "\\ET_" + str(lookbackdays) + "_DaysEdits"

		# Local variables:
		#TUVW_featureClassVariable = "TUVW_Database.SDE.featureclassname"
		#featureClassLayer = "SDE.featureclass_Layer"
		print "Trying " + fc + " as " + emexditem

		try:
			# Process: Make Feature Layer
			arcpy.MakeFeatureLayer_management(TUVW_featureClassVariable, featureClassLayer, "LASTUPDATE > '" + str(lookback) + "'", "", "OBJECTID OBJECTID VISIBLE NONE;OWNEDBY OWNEDBY VISIBLE NONE;MAINTBY MAINTBY VISIBLE NONE;BASINID BASINID VISIBLE NONE;PROJ_NAME PROJ_NAME VISIBLE NONE;PROJ_UPDATE PROJ_UPDATE VISIBLE NONE;ATLAS_PAGE ATLAS_PAGE VISIBLE NONE;STATUS STATUS VISIBLE NONE;INSTALLDATE INSTALLDATE VISIBLE NONE;LASTUPDATE LASTUPDATE VISIBLE NONE;LOCDESC LOCDESC VISIBLE NONE;ACTIVEFLAG ACTIVEFLAG VISIBLE NONE;RECDRAW RECDRAW VISIBLE NONE;UNIQUE_ID UNIQUE_ID VISIBLE NONE;COMMENTS COMMENTS VISIBLE NONE;DRAWING_NUMBER DRAWING_NUMBER VISIBLE NONE;DWG_UPDATE DWG_UPDATE VISIBLE NONE;FACILITYID FACILITYID VISIBLE NONE;LUCITYAUTO_ID LUCITYAUTO_ID VISIBLE NONE;STRUCTURE_ID STRUCTURE_ID VISIBLE NONE;MATERIAL MATERIAL VISIBLE NONE;DIAMETER DIAMETER VISIBLE NONE;DISCHRGTYP DISCHRGTYP VISIBLE NONE;FLOWELEV FLOWELEV VISIBLE NONE;FLOW FLOW VISIBLE NONE;TOPELEV TOPELEV VISIBLE NONE;INVERTELEV INVERTELEV VISIBLE NONE;DISSIPATOR DISSIPATOR VISIBLE NONE;SHAPE SHAPE VISIBLE NONE;Enabled Enabled VISIBLE NONE;GlobalID GlobalID VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE")

			# Process: Summary Statistics
			arcpy.Statistics_analysis(featureClassLayer, Stats_featureClass, "LASTEDITOR COUNT", "LASTEDITOR")

			# Process: Make Table View
			arcpy.MakeTableView_management(Stats_featureClass, featureClassLayer2, "", "", "OBJECTID OBJECTID VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE;FREQUENCY FREQUENCY VISIBLE NONE;COUNT_LASTEDITOR COUNT_LASTEDITOR VISIBLE NONE")

			# Process: Add Field
			arcpy.AddField_management(featureClassLayer3, "Discipline", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

			# Process: Calculate Field
			arcpy.CalculateField_management(featureClassLayer4, "Discipline", "\"" + discipline + "\"", "PYTHON_9.3", "")

			# Process: Append
			arcpy.Append_management("" + featureClassLayer5 + "", ET_X_DaysEdits, "NO_TEST", "OBJECTID_1 \"OBJECTID\" true true false 4 Long 0 0 ,First,#;Rowid_ \"Rowid_\" true true false 255 Text 0 0 ,First,#;OBJECTID_12 \"OBJECTID_1\" true true false 255 Text 0 0 ,First,#;LASTEDITOR \"LASTEDITOR\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",LASTEDITOR,-1,-1;FREQUENCY \"FREQUENCY\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",FREQUENCY,-1,-1;COUNT_LASTEDITOR \"COUNT_LASTEDITOR\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",COUNT_LASTEDITOR,-1,-1;DISCIPLINE \"DISCIPLINE\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",Discipline,-1,-1", "")


		except:
			print "Couldn't process " + TUVW_featureClassVariable
		print "Next feature class..."
	print "Next dataset........."
	
	
	
	

# Database Connection details
# The name you choose to give the SDE connection goes here
databaseConnectionName = "TUVW_RawWater_Infr"
# The root path to which you want the data sent goes here
SavePath = rootpath + topfolder
# The unchanging information about the database connection goes here
targetDatabase = "VVVVVVV"   # target database removed - was something like "TUVW_RawWaterInfrastructure"
targetInstance = "ZZZZZZZ"   # instance removed - was something like "g112345dbs05"
targetUsername = "YYYYYYY"   # username removed
targetPassword = "XXXXXXX"   # password removed
targetVersion = "UUUUUUU"   # target version removed - was something like "INFR_RAWWATER.INFR_RAWWater_Quality"
Database_Connection_Folder_Path = "Database Connections"
Database_Connections = Database_Connection_Folder_Path
# The information about the temporary file geodatabase goes here
tempFileGDBName = "AllDefault"
# additional folder-path \\ elements added to aid in path creation
OutputPath = SavePath + "\\"
discipline = "RawWater"


# Test for databaseConnectionName and Create Connection if necessary	
if arcpy.Exists('Database Connections\\' + databaseConnectionName + '.sde'):
	print "Connection to " + databaseConnectionName + ".sde already exists"
else:
    	
	# Process: Create Database Connection
	arcpy.CreateDatabaseConnection_management(Database_Connection_Folder_Path, databaseConnectionName, "SQL_SERVER", targetInstance, "DATABASE_AUTH", targetUsername, targetPassword, "SAVE_USERNAME", targetDatabase, "", "TRANSACTIONAL", targetVersion, "")
	print "Created connection to " + databaseConnectionName + ".sde"

# Local variables:
AllDefault_gdb = SavePath

if arcpy.Exists(OutputPath + tempFileGDBName + '.gdb'):
	print "Connection to " + OutputPath + tempFileGDBName + ".gdb already exists"
else:
	# Process: Create Database Connection
	# Process: Create File GDB
	arcpy.CreateFileGDB_management(SavePath, tempFileGDBName, "CURRENT")
	print "Created connection to " + OutputPath + tempFileGDBName + ".gdb"

# Convert feature classes within feature datasets to shapefiles
arcpy.env.workspace = "Database Connections\\" + databaseConnectionName + ".sde"

# Print all the feature datasets 
datasets = arcpy.ListDatasets("", "")

for dataset in datasets:
    print(dataset)	
	 
# Rename and export all the feature classes within each dataset
for dataset in datasets:
	arcpy.env.workspace = "Database Connections\\" + databaseConnectionName + ".sde\\" + dataset
		
	# get a list of feature classes in arcpy.env.workspace
	listFC = arcpy.ListFeatureClasses()
	# reformats the array/list of feature classes and datasets
	for fc in listFC:
		print fc
		
		# Local variables:
		fZc = fc
		func = re.sub('[.]', '', fZc)
		fundc = re.sub('[_]', '', func)
		fbrlc = re.sub('\[', '', fundc)
		fbrrc = re.sub('\]', '', fbrlc)
		f_c = fbrrc
		TUVW_featureClassVariable = fc
		emexditem = str(f_c)
		featureClassLayer = emexditem + "_Layer"
		featureClassLayer2 = emexditem
		featureClassLayer3 = featureClassLayer2
		featureClassLayer4 = featureClassLayer3
		featureClassLayer5 = featureClassLayer4
		Stats_featureClass = rootpath + topfolder + filegdb + "\\Stats_" + emexditem 
		Stats_featureClass__2_ = Stats_featureClass
		Stats_featureClass__3_ = Stats_featureClass__2_
		ET_X_DaysEdits__2_ = Stats_featureClass__3_
		ET_X_DaysEdits = rootpath + topfolder + filegdb + "\\ET_" + str(lookbackdays) + "_DaysEdits"

		# Local variables:
		#TUVW_featureClassVariable = "TUVW_Database.SDE.featureclassname"
		#featureClassLayer = "SDE.featureclass_Layer"
		print "Trying " + fc + " as " + emexditem

		try:
			# Process: Make Feature Layer
			arcpy.MakeFeatureLayer_management(TUVW_featureClassVariable, featureClassLayer, "LASTUPDATE > '" + str(lookback) + "'", "", "OBJECTID OBJECTID VISIBLE NONE;OWNEDBY OWNEDBY VISIBLE NONE;MAINTBY MAINTBY VISIBLE NONE;BASINID BASINID VISIBLE NONE;PROJ_NAME PROJ_NAME VISIBLE NONE;PROJ_UPDATE PROJ_UPDATE VISIBLE NONE;ATLAS_PAGE ATLAS_PAGE VISIBLE NONE;STATUS STATUS VISIBLE NONE;INSTALLDATE INSTALLDATE VISIBLE NONE;LASTUPDATE LASTUPDATE VISIBLE NONE;LOCDESC LOCDESC VISIBLE NONE;ACTIVEFLAG ACTIVEFLAG VISIBLE NONE;RECDRAW RECDRAW VISIBLE NONE;UNIQUE_ID UNIQUE_ID VISIBLE NONE;COMMENTS COMMENTS VISIBLE NONE;DRAWING_NUMBER DRAWING_NUMBER VISIBLE NONE;DWG_UPDATE DWG_UPDATE VISIBLE NONE;FACILITYID FACILITYID VISIBLE NONE;LUCITYAUTO_ID LUCITYAUTO_ID VISIBLE NONE;STRUCTURE_ID STRUCTURE_ID VISIBLE NONE;MATERIAL MATERIAL VISIBLE NONE;DIAMETER DIAMETER VISIBLE NONE;DISCHRGTYP DISCHRGTYP VISIBLE NONE;FLOWELEV FLOWELEV VISIBLE NONE;FLOW FLOW VISIBLE NONE;TOPELEV TOPELEV VISIBLE NONE;INVERTELEV INVERTELEV VISIBLE NONE;DISSIPATOR DISSIPATOR VISIBLE NONE;SHAPE SHAPE VISIBLE NONE;Enabled Enabled VISIBLE NONE;GlobalID GlobalID VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE")

			# Process: Summary Statistics
			arcpy.Statistics_analysis(featureClassLayer, Stats_featureClass, "LASTEDITOR COUNT", "LASTEDITOR")

			# Process: Make Table View
			arcpy.MakeTableView_management(Stats_featureClass, featureClassLayer2, "", "", "OBJECTID OBJECTID VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE;FREQUENCY FREQUENCY VISIBLE NONE;COUNT_LASTEDITOR COUNT_LASTEDITOR VISIBLE NONE")

			# Process: Add Field
			arcpy.AddField_management(featureClassLayer3, "Discipline", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

			# Process: Calculate Field
			arcpy.CalculateField_management(featureClassLayer4, "Discipline", "\"" + discipline + "\"", "PYTHON_9.3", "")

			# Process: Append
			arcpy.Append_management("" + featureClassLayer5 + "", ET_X_DaysEdits, "NO_TEST", "OBJECTID_1 \"OBJECTID\" true true false 4 Long 0 0 ,First,#;Rowid_ \"Rowid_\" true true false 255 Text 0 0 ,First,#;OBJECTID_12 \"OBJECTID_1\" true true false 255 Text 0 0 ,First,#;LASTEDITOR \"LASTEDITOR\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",LASTEDITOR,-1,-1;FREQUENCY \"FREQUENCY\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",FREQUENCY,-1,-1;COUNT_LASTEDITOR \"COUNT_LASTEDITOR\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",COUNT_LASTEDITOR,-1,-1;DISCIPLINE \"DISCIPLINE\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",Discipline,-1,-1", "")


		except:
			print "Couldn't process " + TUVW_featureClassVariable
		print "Next feature class..."
	print "Next dataset........."
	
	
	
	

# Database Connection details
# The name you choose to give the SDE connection goes here
databaseConnectionName = "TUVW_Storm_Infr"
# The root path to which you want the data sent goes here
SavePath = rootpath + topfolder
# The unchanging information about the database connection goes here
targetDatabase = "VVVVVVV"   # target database removed - was something like "TUVW_WaterInfrastructure"
targetInstance = "ZZZZZZZ"   # instance removed - was something like "g112345dbs05"
targetUsername = "YYYYYYY"   # username removed
targetPassword = "XXXXXXX"   # password removed
targetVersion = "UUUUUUU"   # target version removed - was something like "INFR_STORM.INFR_Water_Quality"
Database_Connection_Folder_Path = "Database Connections"
Database_Connections = Database_Connection_Folder_Path
# The information about the temporary file geodatabase goes here
tempFileGDBName = "AllDefault"
# additional folder-path \\ elements added to aid in path creation
OutputPath = SavePath + "\\"
discipline = "Storm"


# Test for databaseConnectionName and Create Connection if necessary	
if arcpy.Exists('Database Connections\\' + databaseConnectionName + '.sde'):
	print "Connection to " + databaseConnectionName + ".sde already exists"
else:
    	
	# Process: Create Database Connection
	arcpy.CreateDatabaseConnection_management(Database_Connection_Folder_Path, databaseConnectionName, "SQL_SERVER", targetInstance, "DATABASE_AUTH", targetUsername, targetPassword, "SAVE_USERNAME", targetDatabase, "", "TRANSACTIONAL", targetVersion, "")
	print "Created connection to " + databaseConnectionName + ".sde"

	
# Test for file geodatabase and Create file geodatabase if necessary	
# Local variables:
AllDefault_gdb = SavePath

if arcpy.Exists(OutputPath + tempFileGDBName + '.gdb'):
	print "Connection to " + OutputPath + tempFileGDBName + ".gdb already exists"
else:
	# Process: Create Database Connection
	# Process: Create File GDB
	arcpy.CreateFileGDB_management(SavePath, tempFileGDBName, "CURRENT")
	print "Created connection to " + OutputPath + tempFileGDBName + ".gdb"
		
# Convert feature classes within feature datasets to shapefiles
arcpy.env.workspace = "Database Connections\\" + databaseConnectionName + ".sde"

# Print all the feature datasets 
datasets = arcpy.ListDatasets("", "")

for dataset in datasets:
    print(dataset)	
	 
# Rename and export all the feature classes within each dataset
for dataset in datasets:
	arcpy.env.workspace = "Database Connections\\" + databaseConnectionName + ".sde\\" + dataset
		
	# get a list of feature classes in arcpy.env.workspace
	listFC = arcpy.ListFeatureClasses()
	# reformats the array/list of feature classes and datasets
	for fc in listFC:
		print fc
		
		# Local variables:
		fZc = fc
		func = re.sub('[.]', '', fZc)
		fundc = re.sub('[_]', '', func)
		fbrlc = re.sub('\[', '', fundc)
		fbrrc = re.sub('\]', '', fbrlc)
		f_c = fbrrc
		TUVW_featureClassVariable = fc
		emexditem = str(f_c)
		featureClassLayer = emexditem + "_Layer"
		featureClassLayer2 = emexditem
		featureClassLayer3 = featureClassLayer2
		featureClassLayer4 = featureClassLayer3
		featureClassLayer5 = featureClassLayer4
		Stats_featureClass = rootpath + topfolder + filegdb + "\\Stats_" + emexditem 
		Stats_featureClass__2_ = Stats_featureClass
		Stats_featureClass__3_ = Stats_featureClass__2_
		ET_X_DaysEdits__2_ = Stats_featureClass__3_
		ET_X_DaysEdits = rootpath + topfolder + filegdb + "\\ET_" + str(lookbackdays) + "_DaysEdits"

		# Local variables:
		#TUVW_featureClassVariable = "TUVW_Database.SDE.featureclassname"
		#featureClassLayer = "SDE.featureclass_Layer"
		print "Trying " + fc + " as " + emexditem

		try:
			# Process: Make Feature Layer
			arcpy.MakeFeatureLayer_management(TUVW_featureClassVariable, featureClassLayer, "LASTUPDATE > '" + str(lookback) + "'", "", "OBJECTID OBJECTID VISIBLE NONE;OWNEDBY OWNEDBY VISIBLE NONE;MAINTBY MAINTBY VISIBLE NONE;BASINID BASINID VISIBLE NONE;PROJ_NAME PROJ_NAME VISIBLE NONE;PROJ_UPDATE PROJ_UPDATE VISIBLE NONE;ATLAS_PAGE ATLAS_PAGE VISIBLE NONE;STATUS STATUS VISIBLE NONE;INSTALLDATE INSTALLDATE VISIBLE NONE;LASTUPDATE LASTUPDATE VISIBLE NONE;LOCDESC LOCDESC VISIBLE NONE;ACTIVEFLAG ACTIVEFLAG VISIBLE NONE;RECDRAW RECDRAW VISIBLE NONE;UNIQUE_ID UNIQUE_ID VISIBLE NONE;COMMENTS COMMENTS VISIBLE NONE;DRAWING_NUMBER DRAWING_NUMBER VISIBLE NONE;DWG_UPDATE DWG_UPDATE VISIBLE NONE;FACILITYID FACILITYID VISIBLE NONE;LUCITYAUTO_ID LUCITYAUTO_ID VISIBLE NONE;STRUCTURE_ID STRUCTURE_ID VISIBLE NONE;MATERIAL MATERIAL VISIBLE NONE;DIAMETER DIAMETER VISIBLE NONE;DISCHRGTYP DISCHRGTYP VISIBLE NONE;FLOWELEV FLOWELEV VISIBLE NONE;FLOW FLOW VISIBLE NONE;TOPELEV TOPELEV VISIBLE NONE;INVERTELEV INVERTELEV VISIBLE NONE;DISSIPATOR DISSIPATOR VISIBLE NONE;SHAPE SHAPE VISIBLE NONE;Enabled Enabled VISIBLE NONE;GlobalID GlobalID VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE")

			# Process: Summary Statistics
			arcpy.Statistics_analysis(featureClassLayer, Stats_featureClass, "LASTEDITOR COUNT", "LASTEDITOR")

			# Process: Make Table View
			arcpy.MakeTableView_management(Stats_featureClass, featureClassLayer2, "", "", "OBJECTID OBJECTID VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE;FREQUENCY FREQUENCY VISIBLE NONE;COUNT_LASTEDITOR COUNT_LASTEDITOR VISIBLE NONE")

			# Process: Add Field
			arcpy.AddField_management(featureClassLayer3, "Discipline", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

			# Process: Calculate Field
			arcpy.CalculateField_management(featureClassLayer4, "Discipline", "\"" + discipline + "\"", "PYTHON_9.3", "")

			# Process: Append
			arcpy.Append_management("" + featureClassLayer5 + "", ET_X_DaysEdits, "NO_TEST", "OBJECTID_1 \"OBJECTID\" true true false 4 Long 0 0 ,First,#;Rowid_ \"Rowid_\" true true false 255 Text 0 0 ,First,#;OBJECTID_12 \"OBJECTID_1\" true true false 255 Text 0 0 ,First,#;LASTEDITOR \"LASTEDITOR\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",LASTEDITOR,-1,-1;FREQUENCY \"FREQUENCY\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",FREQUENCY,-1,-1;COUNT_LASTEDITOR \"COUNT_LASTEDITOR\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",COUNT_LASTEDITOR,-1,-1;DISCIPLINE \"DISCIPLINE\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",Discipline,-1,-1", "")


		except:
			print "Couldn't process " + TUVW_featureClassVariable
		print "Next feature class..."
	print "Next dataset........."





# Database Connection details
# The name you choose to give the SDE connection goes here
databaseConnectionName = "TUVW_Sewer_Infr"
# The root path to which you want the data sent goes here
SavePath = rootpath + topfolder
# The unchanging information about the database connection goes here
targetDatabase = "VVVVVVV"   # target database removed - was something like "TUVW_SEWERInfrastructure"
targetInstance = "ZZZZZZZ"   # instance removed - was something like "g112345dbs05"
targetUsername = "YYYYYYY"   # username removed
targetPassword = "XXXXXXX"   # password removed
targetVersion = "UUUUUUU"   # target version removed - was something like "INFR_SEWER.INFR_SEWER_Quality"
Database_Connection_Folder_Path = "Database Connections"
Database_Connections = Database_Connection_Folder_Path
# The information about the temporary file geodatabase goes here
tempFileGDBName = "AllDefault"
# additional folder-path \\ elements added to aid in path creation
OutputPath = SavePath + "\\"
discipline = "Sewer"


# Test for databaseConnectionName and Create Connection if necessary	
if arcpy.Exists('Database Connections\\' + databaseConnectionName + '.sde'):
	print "Connection to " + databaseConnectionName + ".sde already exists"
else:
    	
	# Process: Create Database Connection
	arcpy.CreateDatabaseConnection_management(Database_Connection_Folder_Path, databaseConnectionName, "SQL_SERVER", targetInstance, "DATABASE_AUTH", targetUsername, targetPassword, "SAVE_USERNAME", targetDatabase, "", "TRANSACTIONAL", targetVersion, "")
	print "Created connection to " + databaseConnectionName + ".sde"

	
# Test for file geodatabase and Create file geodatabase if necessary	
# Local variables:
AllDefault_gdb = SavePath

if arcpy.Exists(OutputPath + tempFileGDBName + '.gdb'):
	print "Connection to " + OutputPath + tempFileGDBName + ".gdb already exists"
else:
	# Process: Create Database Connection
	# Process: Create File GDB
	arcpy.CreateFileGDB_management(SavePath, tempFileGDBName, "CURRENT")
	print "Created connection to " + OutputPath + tempFileGDBName + ".gdb"

# Convert feature classes within feature datasets to shapefiles
arcpy.env.workspace = "Database Connections\\" + databaseConnectionName + ".sde"

# Print all the feature datasets 
datasets = arcpy.ListDatasets("", "")

for dataset in datasets:
    print(dataset)	
	 
# Rename and export all the feature classes within each dataset
for dataset in datasets:
	arcpy.env.workspace = "Database Connections\\" + databaseConnectionName + ".sde\\" + dataset
		
	# get a list of feature classes in arcpy.env.workspace
	listFC = arcpy.ListFeatureClasses()
	# reformats the array/list of feature classes and datasets
	for fc in listFC:
		
		# Local variables:
		fZc = fc
		func = re.sub('[.]', '', fZc)
		fundc = re.sub('[_]', '', func)
		fbrlc = re.sub('\[', '', fundc)
		fbrrc = re.sub('\]', '', fbrlc)
		f_c = fbrrc
		TUVW_featureClassVariable = fc
		emexditem = str(f_c)
		featureClassLayer = emexditem + "_Layer"
		featureClassLayer2 = emexditem
		featureClassLayer3 = featureClassLayer2
		featureClassLayer4 = featureClassLayer3
		featureClassLayer5 = featureClassLayer4
		Stats_featureClass = rootpath + topfolder + filegdb + "\\Stats_" + emexditem 
		Stats_featureClass__2_ = Stats_featureClass
		Stats_featureClass__3_ = Stats_featureClass__2_
		ET_X_DaysEdits__2_ = Stats_featureClass__3_
		ET_X_DaysEdits = rootpath + topfolder + filegdb + "\\ET_" + str(lookbackdays) + "_DaysEdits"

		# Local variables:
		#TUVW_featureClassVariable = "TUVW_Database.SDE.featureclassname"
		#featureClassLayer = "SDE.featureclass_Layer"
		print "Trying " + fc + " as " + emexditem

		try:
			# Process: Make Feature Layer
			arcpy.MakeFeatureLayer_management(TUVW_featureClassVariable, featureClassLayer, "LASTUPDATE > '" + str(lookback) + "'", "", "OBJECTID OBJECTID VISIBLE NONE;OWNEDBY OWNEDBY VISIBLE NONE;MAINTBY MAINTBY VISIBLE NONE;BASINID BASINID VISIBLE NONE;PROJ_NAME PROJ_NAME VISIBLE NONE;PROJ_UPDATE PROJ_UPDATE VISIBLE NONE;ATLAS_PAGE ATLAS_PAGE VISIBLE NONE;STATUS STATUS VISIBLE NONE;INSTALLDATE INSTALLDATE VISIBLE NONE;LASTUPDATE LASTUPDATE VISIBLE NONE;LOCDESC LOCDESC VISIBLE NONE;ACTIVEFLAG ACTIVEFLAG VISIBLE NONE;RECDRAW RECDRAW VISIBLE NONE;UNIQUE_ID UNIQUE_ID VISIBLE NONE;COMMENTS COMMENTS VISIBLE NONE;DRAWING_NUMBER DRAWING_NUMBER VISIBLE NONE;DWG_UPDATE DWG_UPDATE VISIBLE NONE;FACILITYID FACILITYID VISIBLE NONE;LUCITYAUTO_ID LUCITYAUTO_ID VISIBLE NONE;STRUCTURE_ID STRUCTURE_ID VISIBLE NONE;MATERIAL MATERIAL VISIBLE NONE;DIAMETER DIAMETER VISIBLE NONE;DISCHRGTYP DISCHRGTYP VISIBLE NONE;FLOWELEV FLOWELEV VISIBLE NONE;FLOW FLOW VISIBLE NONE;TOPELEV TOPELEV VISIBLE NONE;INVERTELEV INVERTELEV VISIBLE NONE;DISSIPATOR DISSIPATOR VISIBLE NONE;SHAPE SHAPE VISIBLE NONE;Enabled Enabled VISIBLE NONE;GlobalID GlobalID VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE")

			# Process: Summary Statistics
			arcpy.Statistics_analysis(featureClassLayer, Stats_featureClass, "LASTEDITOR COUNT", "LASTEDITOR")

			# Process: Make Table View
			arcpy.MakeTableView_management(Stats_featureClass, featureClassLayer2, "", "", "OBJECTID OBJECTID VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE;FREQUENCY FREQUENCY VISIBLE NONE;COUNT_LASTEDITOR COUNT_LASTEDITOR VISIBLE NONE")

			# Process: Add Field
			arcpy.AddField_management(featureClassLayer3, "Discipline", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

			# Process: Calculate Field
			arcpy.CalculateField_management(featureClassLayer4, "Discipline", "\"" + discipline + "\"", "PYTHON_9.3", "")

			# Process: Append
			arcpy.Append_management("" + featureClassLayer5 + "", ET_X_DaysEdits, "NO_TEST", "OBJECTID_1 \"OBJECTID\" true true false 4 Long 0 0 ,First,#;Rowid_ \"Rowid_\" true true false 255 Text 0 0 ,First,#;OBJECTID_12 \"OBJECTID_1\" true true false 255 Text 0 0 ,First,#;LASTEDITOR \"LASTEDITOR\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",LASTEDITOR,-1,-1;FREQUENCY \"FREQUENCY\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",FREQUENCY,-1,-1;COUNT_LASTEDITOR \"COUNT_LASTEDITOR\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",COUNT_LASTEDITOR,-1,-1;DISCIPLINE \"DISCIPLINE\" true true false 255 Text 0 0 ,First,#," + featureClassLayer5 + ",Discipline,-1,-1", "")


		except:
			print "Couldn't process " + TUVW_featureClassVariable
		print "Next feature class..."
	print "Next dataset........."
	
	
	
	
# ---  Repeat additional database connections as necessary  ---


	
# Local variables:
EditsSince20200323 = rootpath + topfolder + filegdb + "\\" + ET_90_DaysEdits
EditsSince20200323_View = ET_90_DaysEdits + "_View"
EditsSince20200323_View__2_ = EditsSince20200323_View
EditsSince20200323_View__3_ = EditsSince20200323_View__2_
table_Edits_Since_20200323 = ET_90_DaysEdits
Edits_Since_20200323 = rootpath + topfolder + filegdb + "\\" + table_Edits_Since_20200323 + "_"

# Process: Make Table View
arcpy.MakeTableView_management(EditsSince20200323, EditsSince20200323_View, "", "", "OBJECTID OBJECTID VISIBLE NONE;OBJECTID_1 OBJECTID_1 VISIBLE NONE;Rowid_ Rowid_ VISIBLE NONE;OBJECTID_12 OBJECTID_12 VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE;FREQUENCY FREQUENCY VISIBLE NONE;COUNT_LASTEDITOR COUNT_LASTEDITOR VISIBLE NONE;DISCIPLINE DISCIPLINE VISIBLE NONE")

# Process: Add Field
arcpy.AddField_management(EditsSince20200323_View, "Edits", "SHORT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

# Process: Calculate Field
arcpy.CalculateField_management(EditsSince20200323_View__2_, "Edits", "!FREQUENCY!", "PYTHON_9.3", "")

# Process: Summary Statistics
arcpy.Statistics_analysis(EditsSince20200323_View__3_, Edits_Since_20200323, "Edits SUM", "LASTEDITOR;DISCIPLINE")
	
# Local variables:
ET_7_DaysEdits = Edits_Since_20200323
ET_7_DaysEdits_2_ = ET_7_DaysEdits
ET_7_DaysEdits_View = ET_90_DaysEdits + "_View"
ET_7_DaysEdits_View_2_ = ET_7_DaysEdits_View
ET_7_DaysEdits_xls = rootpath + topfolder + "\\" + table_Edits_Since_20200323 + "_" + crrntdatetrunc + ".xls"
ET_7_DaysEdits_xls_2_ = rootpath + topfolder + "\\" + table_Edits_Since_20200323 + ".xls"

# Create Excel file with unique filename (by date) for report logging
# Process: Make Table View
arcpy.MakeTableView_management(ET_7_DaysEdits, ET_7_DaysEdits_View, "", "", "OBJECTID OBJECTID VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE;DISCIPLINE DISCIPLINE VISIBLE NONE;FREQUENCY FREQUENCY VISIBLE NONE;SUM_Edits SUM_Edits VISIBLE NONE")

# Process: Table To Excel
arcpy.TableToExcel_conversion(ET_7_DaysEdits_View, ET_7_DaysEdits_xls, "NAME", "CODE")

# open the Excel file, once made
os.startfile(ET_7_DaysEdits_xls)

# Create Excel file with unchanging filename for automating reporting
# Process: Make Table View
arcpy.MakeTableView_management(ET_7_DaysEdits_2_, ET_7_DaysEdits_View_2_, "", "", "OBJECTID OBJECTID VISIBLE NONE;LASTEDITOR LASTEDITOR VISIBLE NONE;DISCIPLINE DISCIPLINE VISIBLE NONE;FREQUENCY FREQUENCY VISIBLE NONE;SUM_Edits SUM_Edits VISIBLE NONE")

# Process: Table To Excel
arcpy.TableToExcel_conversion(ET_7_DaysEdits_View_2_, ET_7_DaysEdits_xls_2_, "NAME", "CODE")

# open the Excel file, once made
os.startfile(ET_7_DaysEdits_xls_2_)

#  ---  Email a copy of the non-unique-filename Excel file
# the gmail account below must have Access For Less Secure Apps enabled

 
fromaddr = "gmail_account_name@gmail.com"
toaddr = "target_recipient@emailserver.com"
nameofattach = ET_7_DaysEdits_xls_2_ + ".xls"
pathandname = rootpath + topfolder + "\\" + nameofattach

#now = datetime.datetime.now()
 
msg = MIMEMultipart()
 
msg['From'] = fromaddr
msg['To'] = toaddr
#Subject of the email
msg['Subject'] = "14 Day Editor Tracking for " + crrntdatetrunc
 
#Body of the email
body = r"14 Day Editor Tracking report for " + crrntdatetrunc + " is attached.  Copies of these files can also be found here:  \\\main\___differentRedactedFolderPath___\EditorTracking"

msg.attach(MIMEText(body, 'plain'))
 
#Must have the filename with extension
filename = nameofattach
#Must have the file path AND filename with extension
attachment = open(pathandname, "rb")
 
part = MIMEBase('application', 'octet-stream')
part.set_payload((attachment).read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
 
msg.attach(part)
 
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
#Supply correct password
server.login(fromaddr, "__redacted_password__")
text = msg.as_string()
server.sendmail(fromaddr, toaddr, text)
server.quit()

print "Done."