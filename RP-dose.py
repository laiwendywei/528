from pydicom import dcmread
import mysql.connector
cnx = mysql.connector.connect(user='root', password='',
        host='127.0.0.1', database='BME')

# specify your image path
image_path = '006/RP.1.2.246.352.71.5.992070825148.795520.20171010163306-no-phi.dcm'
ds = dcmread(image_path)
#print(ds)
# For  table
# common attributes
patient_id = ds[0x0010,0x0020].value
series_id = ds[0x0020,0x0011].value
study_id = ds[0x0020,0x0010].value
patient_id = str(patient_id)
series_id = str(series_id)
if(study_id == ''):
    study_id = '-1'
#print(patient_id)
#print(series_id)
temp = ds[0x3006, 0x0010].value
#dose_sequence = ds[0x300c,0x0042].value
print(temp)