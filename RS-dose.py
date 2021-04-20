from pydicom import dcmread
import mysql.connector
cnx = mysql.connector.connect(user='root', password='',
        host='127.0.0.1', database='BME')

# specify your image path
image_path = '006/RS.1.2.246.352.71.4.992070825148.228408.20171011093506-no-phi.dcm'
ds = dcmread(image_path)
#print(ds)
# For RT_Contour table
patient_id = ds[0x0010,0x0020].value
series_id = ds[0x0020,0x0011].value
study_id = ds[0x0020,0x0010].value
patient_id = str(patient_id)
series_id = str(series_id)
if(study_id == ''):
    study_id = 'NULL'


item = ds[0x3006,0x0010].value
temp = item[0][0x3006,0x0012].value
rt_referenced_sequence = temp[0][0x3006,0x0014].value

temp2 = rt_referenced_sequence[0]
contour_sequence = temp2[0x3006,0x0016].value
item2 = ds[0x3006,0x0039].value
temp3 = item2[0][0x3006,0x0040].value
#print(temp3[0])
list1 = []
for i in temp3:
    temp4 = i[0x3006,0x0016].value
    for x in temp4:
        sop_class_uid = x[0x0008, 0x1150].value
        sop_instance_uid = x[0x0008, 0x1155].value
        contour_geometric_type = i[0x3006,0x0042].value
        num_of_con_points = i[0x3006, 0x0046].value
        contour_data = ','.join(str(e) for e in i[0x3006,0x0050].value)
        list1.append(contour_geometric_type)
    query = "INSERT INTO RT_Contour (ReferencedSOPClassUID,ReferencedSOPInstanceUID,patient_id, series_id, study_id, " \
            "ContourGeometricType, NumberOfContourPoints, ContourData) " \
            "VALUES('" + \
            str(sop_class_uid) + "','" + str(sop_instance_uid) + "','" + str(patient_id) + "','" + str(series_id) + \
            "','" + str(study_id) + "','" + str(contour_geometric_type) + "'," + str(num_of_con_points)  + ",'"\
            + contour_data +"');"
    print(query)

    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()

#print(temp3[0])
#print(num_of_con_points)
cnx.close()
