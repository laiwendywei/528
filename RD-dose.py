from pydicom import dcmread
import mysql.connector
cnx = mysql.connector.connect(user='root', password='',
        host='127.0.0.1', database='BME')

# specify your image path
image_path = '006/RD.1.2.246.352.71.7.992070825148.1626941.20171011085759-no-phi.dcm'
ds = dcmread(image_path)
#print(ds)
# For RT_DVH table
dose_units = ds[0x3004,0x0002].value
dose_type = ds[0x3004,0x0004].value
dvh_dose_scaling = ds.data_element("DVH Dose Scaling")
dvh_sequence = ds[0x3004, 0x0050].value
# for every dvh in dvh_sequence
query_sequence = []
for i in dvh_sequence:
    query = ''
    dvh_sample = i
    dvh_dose_scaling = dvh_sample[0x3004, 0x0052].value
    dvh_minimum_dose = dvh_sample[0x3004, 0x0070].value
    dvh_maximum_dose = dvh_sample[0x3004, 0x0072].value
    dvh_mean_dose = dvh_sample[0x3004, 0x0074].value
    dvh_referenced_roi_ds = dvh_sample[0x3004, 0x0060].value # sequence
    dvh_referenced_roi = dvh_referenced_roi_ds[0][0x3004,0x0062].value + "," + \
                         str(dvh_referenced_roi_ds[0][0x3006,0x0084].value)
    # DVH ROI Contribution Type, Referenced ROI Number
    dvh_type = dvh_sample[0x3004, 0x0001].value
    dvh_volume_units = dvh_sample[0x3004, 0x0054].value
    query = str(dvh_dose_scaling) +"," + str(dvh_maximum_dose) + "," + str(dvh_mean_dose) + "," \
          + str(dvh_minimum_dose) + ",'"+ dvh_referenced_roi + "','"+ dvh_type + "','"+ str(dvh_volume_units) + "'"
    print(query)
    query_sequence.append(query)

print(query_sequence)

dose_units = ds[0x3004,0x0002].value
dose_type = ds[0x3004,0x0004].value
dvh_data = ','.join(str(e) for e in dvh_sample[0x3004,0x0058].value)
patient_id = ds[0x0010,0x0010].value
series_id = ds[0x0020,0x0011].value
study_id = ds[0x0020,0x0010].value
patient_id = str(patient_id)
series_id = str(series_id)
if(study_id == ''):
    study_id = 'NULL'

id = 0
for x in query_sequence:
    sql = "INSERT INTO RT_DVH (RT_DVH_id,DVHDoseScaling,DVHMaximumDose, DVHMeanDose, " \
          "DVHMinimumDose, DVHReferencedROI, DVHType, DVHVolumeUnits, DoseType, DoseUnits, DVHData, " \
          "fk_patient_id_id, fk_series_id_id, fk_study_id) VALUES (" + str(id) + "," + x + \
          ",'" + str(dose_type) + "','" + str(dose_units) + "','" + str(dvh_data) +"','" + \
          patient_id + "','" + series_id + "','" + study_id + "');"
    print(sql)
    cursor = cnx.cursor()
    cursor.execute(sql)
    cnx.commit()
    id = id + 1

cnx.close()