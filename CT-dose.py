from pydicom import dcmread
import mysql.connector
cnx = mysql.connector.connect(user='root', password='',
        host='127.0.0.1', database='BME')

# specify your image path
image_path = '006/CT.1.2.840.113619.2.55.3.51045121.848.1507151727.835.173-no-phi.dcm'
ds = dcmread(image_path)
print(ds)
# For RT_Contour table
patient_id = ds[0x0010,0x0020].value
series_id = ds[0x0020,0x0011].value
study_id = ds[0x0020,0x0010].value
patient_id = str(patient_id)
series_id = str(series_id)
if(study_id == ''):
    study_id = 'NULL'

cnx.close()
