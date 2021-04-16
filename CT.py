import pydicom
import mysql.connector

cnx = mysql.connector.connect(user="root", password="root",
                              host="127.0.0.1", database="BME")
fpath = "sample/006/CT.1.2.840.113619.2.55.3.51045121.848.1507151727.835.17-no-phi.dcm"
ds = pydicom.dcmread(fpath, force=True)

print(ds)
DB_Name = "BME"
tables = {}
tables["Patient"] = ("CREATE TABLE IF NOT EXISTS Patient (patient_id VARCHAR(500), PatientName varchar("
                     "255), "
                     "BirthDate Date , Gender varchar(255) ,EthnicGroup varchar(255))")

tables["Study"] = ("CREATE TABLE IF NOT EXISTS Study (StudyID VARCHAR(255) PRIMARY KEY, StudyDate Date,"
                   "StudyDescription varchar(50), TotalSeries int ,patient_id VARCHAR(255))")

tables["Series"] = ("CREATE TABLE IF NOT EXISTS Series (series_id VARCHAR(255) PRIMARY KEY, series_number varchar("
                    "45),StudyID VARCHAR(255))")

tables["CT_Image"] = ("CREATE TABLE IF NOT EXISTS CT_Image (pixel_spacing VARCHAR(255),position "
                      "VARCHAR(255) , orientation varchar(50) , "
                      "number_of_exposure int, reference_UID int, pixel_data VARCHAR(255), Series_series_id VARCHAR("
                      "255))")

# for table_name in tables.keys():
#     cursor = cnx.cursor()
#     cursor.execute(tables[table_name])

# patient table ================================================================================================
patient_id = str(ds[0x0010, 0x0020].value)
PatientName = str(ds[0x0010, 0x0010].value)
BirthDate = ds[0x0010, 0x0030].value
Gender = ds[0x0010, 0x0030].value

if len(BirthDate) == 0:
    BirthDate = '2021-04-14'
if len(Gender) == 0:
    Gender = "NULL"

val = (patient_id, PatientName, BirthDate, Gender)
patient_query = "insert Ignore into Patient(Patient_id,PatientName,BirthDate,Gender) VALUES (%s,%s,%s,%s)"

cursor = cnx.cursor()
cursor.execute(patient_query, val)
cnx.commit()
#
nocheck = "SET FOREIGN_KEY_CHECKS = 0;"
cursor = cnx.cursor()
cursor.execute(nocheck)
cnx.commit()

StudyID = str(ds[0x0020, 0x0010].value)
StudyDate = ds[0x0008, 0x0020].value
StudyDescription = ds[0x0008, 0x1030].value
patient_id = ds[0x0010, 0x0020].value
study_val = (str(StudyID), StudyDate, StudyDescription, str(patient_id))
study_query = "insert Ignore into Study(StudyID, StudyDate, StudyDescription, Patient_patient_id) VALUES (%s,%s,%s,%s)"
cursor = cnx.cursor()
cursor.execute(study_query, study_val)
cnx.commit()

#series table, series number is a string
series_id = str(ds[0x0020, 0x0011].value)
StudyID = str(ds[0x0020, 0x0010].value)
series_val = (series_id , StudyID)
series_query = "insert Ignore into Series (series_id, Study_StudyID) VALUES (%s, %s)"
cursor = cnx.cursor()
cursor.execute(series_query, series_val)
cnx.commit()

# CT_Image table
# ct_image_id = str(1)
pixel_data = str(ds[0x0028, 0x0103].value)
position = ds[0x0020, 0x0032].value
orientation = ds[0x0020, 0x0037].value
number_of_exposure = str(ds[0x0018, 0x1152].value)
reference = ds[0x0020, 0x1040].value
pixel_spacing = ds[0x0028, 0x0030].value
Series_series_id = str(ds[0x0020, 0x0011].value)

str_position = ""
str_orientation = ""
str_spacing = ""
for each in position:
    str_position += str(each)+","
for i in orientation:
    str_orientation += str(i)+","
for j in pixel_spacing:
    str_spacing += str(j)+","

ct_val = (pixel_data, str_position, str_orientation, number_of_exposure, reference, str_spacing, Series_series_id)
ctimage_query = "insert into CT_Image(pixel_data, position, orientation, number_of_exposure," \
                "reference," \
                "pixel_spacing,Series_series_id) VALUES (%s,%s,%s,%s,%s,%s,%s)"
cursor = cnx.cursor()
cursor.execute(ctimage_query, ct_val)
cnx.commit()
