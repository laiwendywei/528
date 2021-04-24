import mysql.connector
from pydicom import dcmread

# dicom_file = input("DICOM File: ")
# dc = dcmread(str(dicom_file))
# username = input("User Name: ")
# database = input("Which database: ")
# cnx = mysql.connector.connect(user=str(username), host='127.0.0.1',
#
#                              database=str(database), auth_plugin='mysql_native_password')

cnx = mysql.connector.connect(user='root', host='127.0.0.1', database='BME')

tables = {}

'''''Empty Tables'''''

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
tables['StructSet'] = (
    "CREATE TABLE IF NOT EXISTS StructSet (StructNo varchar(100) PRIMARY KEY, NumberOfROIObservations int ,NumberOfReferFrame int , NumberOFStructSetROI int ,"
    " Series_series_id varchar(255))")

tables['ROIObservation'] = (
    "CREATE TABLE IF NOT EXISTS ROIObservation (StructSet_StructNo varchar(100), Observation_Number varchar(10) ,Referenced_ROI_Number varchar(10) , ROI_Observation_Label varchar(255))")

tables['ReferFrame'] = (
    "CREATE TABLE IF NOT EXISTS ReferFrame (StructSet_StructNo varchar(100), FrameOfReferenceUID varchar(255))"
)

tables['ReferStudy'] = (
    "CREATE TABLE IF NOT EXISTS ReferStudy (ReferFrame_FrameOfReferenceUID varchar(255), Referenced_SOP_Class_UID varchar(255),Referenced_SOP_Instance_UID varchar(255))"
)

tables['ReferSeries'] = (
    "CREATE TABLE IF NOT EXISTS ReferSeries (ReferStudy_Referenced_SOP_Instance_UID varchar(255), Series_Instance_UID  varchar(255))"
)

tables['ContourImage'] = (
    "CREATE TABLE IF NOT EXISTS ContourImage (ReferSeries_Series_Instance_UID  varchar(255), Referenced_SOP_Class_UID varchar(255), Referenced_SOP_Instance_UID varchar(255))"
)

for table_name in tables.keys():
    cursor = cnx.cursor()
    cursor.execute(tables[table_name])


'''''CT Image'''''

def ct_image(dc):
    # patient table ================================================================================================
    patient_id = str(dc[0x0010, 0x0020].value)
    PatientName = str(dc[0x0010, 0x0010].value)
    BirthDate = dc[0x0010, 0x0030].value
    Gender = dc[0x0010, 0x0030].value

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

    StudyID = dc[0x0020, 0x000d].value #str(dc[0x0020, 0x0010].value)  # update from study id to study instance uid
    StudyDate = dc[0x0008, 0x0020].value
    StudyDescription = dc[0x0008, 0x1030].value
    patient_id = dc[0x0010, 0x0020].value
    study_val = (str(StudyID), StudyDate, StudyDescription, str(patient_id))
    study_query = "insert Ignore into Study(StudyID, StudyDate, StudyDescription, Patient_patient_id) VALUES (%s,%s,%s,%s)"
    cursor = cnx.cursor()
    cursor.execute(study_query, study_val)
    cnx.commit()

    #series table, series number is a string
    series_id = str(dc[0x0020, 0x000e].value) # update from series number to series instance uid
    StudyID = str(dc[0x0020, 0x000d].value) # update from study id to study instance uid
    series_val = (series_id , StudyID)
    series_query = "insert Ignore into Series (series_id, Study_StudyID) VALUES (%s, %s)"
    cursor = cnx.cursor()
    cursor.execute(series_query, series_val)
    cnx.commit()

    # CT_Image table
    # ct_image_id = str(1)
    pixel_data = str(dc[0x0028, 0x0103].value)
    position = dc[0x0020, 0x0032].value
    orientation = dc[0x0020, 0x0037].value
    number_of_exposure = str(dc[0x0018, 0x1152].value)
    reference = dc[0x0020, 0x1040].value
    pixel_spacing = dc[0x0028, 0x0030].value
    Series_series_id = str(dc[0x0020, 0x000e].value) # update from series number to series instance uid

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


def struct_set(dc):
    nocheck = "SET FOREIGN_KEY_CHECKS = 0;"
    cursor = cnx.cursor()
    cursor.execute(nocheck)
    cnx.commit()


    '''Patient Table'''
    patient_id = str(dc[0x0010, 0x0020].value)
    PatientName = str(dc[0x0010, 0x0010].value)
    BirthDate = dc[0x0010, 0x0030].value
    Gender = dc[0x0010, 0x0030].value

    if len(BirthDate) == 0:
        BirthDate = '2021-04-14'
    if len(Gender) == 0:
        Gender = "NULL"

    val = (patient_id, PatientName, BirthDate, Gender)
    patient_query = "insert Ignore into Patient(Patient_id,PatientName,BirthDate,Gender) VALUES (%s,%s,%s,%s)"

    cursor = cnx.cursor()
    cursor.execute(patient_query, val)
    cnx.commit()


    '''Study Table'''
    StudyID = dc[0x0020, 0x000d].value  # str(dc[0x0020, 0x0010].value)  # update from study id to study instance uid
    StudyDate = dc[0x0008, 0x0020].value
    StudyDescription = dc[0x0008, 0x1030].value
    patient_id = dc[0x0010, 0x0020].value
    study_val = (str(StudyID), StudyDate, StudyDescription, str(patient_id))
    study_query = "insert Ignore into Study(StudyID, StudyDate, StudyDescription, Patient_patient_id) VALUES (%s,%s,%s,%s)"
    cursor = cnx.cursor()
    cursor.execute(study_query, study_val)
    cnx.commit()

    '''Series Table'''
    SeriesInstanceUID_value = dc[0x0020, 0x000e].value
    StudyInstanceUID = dc[0x0020, 0x000d].value
    val_st = ( "'"+SeriesInstanceUID_value+"'",StudyInstanceUID)
    st_query = "insert Ignore into Series (series_id, Study_StudyID) values (%s, %s)"%val_st
    cursor = cnx.cursor()
    cursor.execute(st_query)

    cnx.commit()


    '''StructSet Table'''
    StructSetLabel = dc[0x3006, 0x0002].value
    NumberOfROIObservations = len(dc[0x3006, 0x0080].value)
    NumberOfReferFrame = len(dc[0x3006, 0x0010].value)
    NumberOfStructSetROI = len(dc[0x3006, 0x0020].value)
    SeriesInstanceUID_value = dc[0x0020, 0x000e].value
    val = ("'" + StructSetLabel + "'", NumberOfROIObservations, NumberOfReferFrame, NumberOfStructSetROI,
           "'" + SeriesInstanceUID_value + "'")
    ss_query = "insert Ignore into StructSet (StructNo, NumberOfROIObservations ,NumberOfReferFrame , NumberOFStructSetROI ,Series_series_id) VALUES (%s,%s,%s,%s,%s)" % val
    cursor = cnx.cursor()
    cursor.execute(ss_query)

    cnx.commit()

    '''ROIObservation table'''
    ROI_Observations = dc[0x3006, 0x0080].value
    for roi_ob in ROI_Observations:
        val_ob = ("'" + StructSetLabel + "'", "'" + str(roi_ob[0x3006, 0x0082].value) + "'",
                  "'" + str(roi_ob[0x3006, 0x0084].value) + "'", "'" + str(roi_ob[0x3006, 0x0085].value) + "'")
        roiob_query = "insert Ignore into ROIObservation (StructSet_StructNo, Observation_Number ,Referenced_ROI_Number, ROI_Observation_Label) VALUES (%s,%s,%s,%s)" % val_ob
        cursor = cnx.cursor()
        cursor.execute(roiob_query)
        cnx.commit()

    '''ReferFrame table'''
    ReferFrame = dc[0x3006, 0x0010].value
    for rf in ReferFrame:
        val_rf = ("'" + StructSetLabel + "'", "'" + rf[0x0020, 0x0052].value + "'")
        rf_query = "insert Ignore into ReferFrame (StructSet_StructNo,FrameOfReferenceUID ) VALUES (%s,%s)" % val_rf
        cursor = cnx.cursor()
        cursor.execute(rf_query)
        cnx.commit()
        '''ReferStudy table'''
        ReferStudy = ReferFrame[0][0x3006, 0x0012].value
        for rs in ReferStudy:
            FrameOfReferenceUID = rf[0x0020, 0x0052].value
            Referenced_SOP_Class_UID = rs[0x0008, 0x1150].value
            Referenced_SOP_Instance_UID = rs[0x0008, 0x1155].value
            val_rs = ("'" + FrameOfReferenceUID + "'", "'" + Referenced_SOP_Class_UID + "'",
                      "'" + Referenced_SOP_Instance_UID + "'")
            rs_query = "insert Ignore into ReferStudy (ReferFrame_FrameOfReferenceUID, Referenced_SOP_Class_UID,Referenced_SOP_Instance_UID) values (%s,%s,%s)" % val_rs
            cursor = cnx.cursor()
            cursor.execute(rs_query)
            cnx.commit()
            '''ReferSeries'''
            ReferSeries = rs[0x3006, 0x0014].value
            for rse in ReferSeries:
                Series_Instance_UID = rse[0x0020, 0x000e].value
                val_rse = ("'" + Referenced_SOP_Instance_UID + "'", "'" + Series_Instance_UID + "'")
                rse_query = "insert Ignore into ReferSeries (ReferStudy_Referenced_SOP_Instance_UID, Series_Instance_UID) values (%s,%s)" % val_rse
                cursor = cnx.cursor()
                cursor.execute(rse_query)
                cnx.commit()
                '''ContourImage'''
                ContourImage = rse[0x3006, 0x0016].value
                for ci in ContourImage:
                    Referenced_SOP_Class_UID_ci = ci[0x0008, 0x1150].value
                    Referenced_SOP_Instance_UID_ci = ci[0x0008, 0x1155].value
                    val_ci = ("'" + Series_Instance_UID + "'", "'" + Referenced_SOP_Class_UID_ci + "'",
                              "'" + Referenced_SOP_Instance_UID_ci + "'")
                    ci_query = "insert Ignore into ContourImage (ReferSeries_Series_Instance_UID, Referenced_SOP_Class_UID,Referenced_SOP_Instance_UID) values (%s,%s,%s)" % val_ci
                    cursor = cnx.cursor()
                    cursor.execute(ci_query)
                    cnx.commit()

    '''RT Contour'''
    item = dc[0x3006, 0x0010].value
    temp = item[0][0x3006, 0x0012].value
    rt_referenced_sequence = temp[0][0x3006, 0x0014].value
    series_id = dc[0x0020, 0x000e].value # dc[0x0020, 0x0011].value   updated from series number to series instance UID
    study_id = dc[0x0020, 0x000d].value #dc[0x0020, 0x0010].value   updated from study id to study instance UID

    temp2 = rt_referenced_sequence[0]
    contour_sequence = temp2[0x3006, 0x0016].value
    item2 = dc[0x3006, 0x0039].value
    temp3 = item2[0][0x3006, 0x0040].value
    # print(temp3[0])
    list1 = []
    for i in temp3:
        temp4 = i[0x3006, 0x0016].value
        for x in temp4:
            sop_class_uid = x[0x0008, 0x1150].value
            sop_instance_uid = x[0x0008, 0x1155].value
            contour_geometric_type = i[0x3006, 0x0042].value
            num_of_con_points = i[0x3006, 0x0046].value
            contour_data = ','.join(str(e) for e in i[0x3006, 0x0050].value)
            list1.append(contour_geometric_type)
        query = "INSERT INTO RT_Contour (ReferencedSOPClassUID,ReferencedSOPInstanceUID,patient_id, series_id, study_id, " \
                "ContourGeometricType, NumberOfContourPoints, ContourData) " \
                "VALUES('" + \
                str(sop_class_uid) + "','" + str(sop_instance_uid) + "','" + str(patient_id) + "','" + str(series_id) + \
                "','" + str(study_id) + "','" + str(contour_geometric_type) + "'," + str(num_of_con_points) + ",'" \
                + contour_data + "');"

        cursor = cnx.cursor()
        cursor.execute(query)
        cnx.commit()


'''''Dose'''''
def RD_Dose(dc):
    dose_units = dc[0x3004, 0x0002].value
    dose_type = dc[0x3004, 0x0004].value
    dvh_dose_scaling = dc.data_element("DVH Dose Scaling")
    dvh_sequence = dc[0x3004, 0x0050].value
    # for every dvh in dvh_sequence
    query_sequence = []
    for i in dvh_sequence:
        query = ''
        dvh_sample = i
        dvh_dose_scaling = dvh_sample[0x3004, 0x0052].value
        dvh_minimum_dose = dvh_sample[0x3004, 0x0070].value
        dvh_maximum_dose = dvh_sample[0x3004, 0x0072].value
        dvh_mean_dose = dvh_sample[0x3004, 0x0074].value
        dvh_referenced_roi_ds = dvh_sample[0x3004, 0x0060].value  # sequence
        dvh_referenced_roi = dvh_referenced_roi_ds[0][0x3004, 0x0062].value + "," + \
                             str(dvh_referenced_roi_ds[0][0x3006, 0x0084].value)
        # DVH ROI Contribution Type, Referenced ROI Number
        dvh_type = dvh_sample[0x3004, 0x0001].value
        dvh_volume_units = dvh_sample[0x3004, 0x0054].value
        query = str(dvh_dose_scaling) + "," + str(dvh_maximum_dose) + "," + str(dvh_mean_dose) + "," \
                + str(dvh_minimum_dose) + ",'" + dvh_referenced_roi + "','" + dvh_type + "','" + str(
            dvh_volume_units) + "'"

        query_sequence.append(query)

    dose_units = dc[0x3004, 0x0002].value
    dose_type = dc[0x3004, 0x0004].value
    dvh_data = ','.join(str(e) for e in dvh_sample[0x3004, 0x0058].value)
    patient_id = dc[0x0010, 0x0010].value
    series_id = dc[0x0020, 0x000e].value #dc[0x0020, 0x0011].value #updated from series number to series instance uid
    study_id = dc[0x0020, 0x000d].value # dc[0x0020, 0x0010].value  updated from study ID to study instance UID
    patient_id = str(patient_id)
    series_id = str(series_id)
    if (study_id == ''):
        study_id = 'NULL'

    id = 0
    for x in query_sequence:
        sql = "INSERT IGNORE INTO RT_DVH (RT_DVH_id,DVHDoseScaling,DVHMaximumDose, DVHMeanDose, " \
              "DVHMinimumDose, DVHReferencedROI, DVHType, DVHVolumeUnits, DoseType, DoseUnits, DVHData, " \
              "fk_patient_id_id, fk_series_id_id, fk_study_id) VALUES (" + str(id) + "," + x + \
              ",'" + str(dose_type) + "','" + str(dose_units) + "','" + str(dvh_data) + "','" + \
              patient_id + "','" + series_id + "','" + study_id + "');"

        cursor = cnx.cursor()
        cursor.execute(sql)
        cnx.commit()
        id = id + 1

'''''Treatment Plan'''''
def RT(dc):
    '''Patient Table'''
    patient_id = str(dc[0x0010, 0x0020].value)
    PatientName = str(dc[0x0010, 0x0010].value)
    BirthDate = dc[0x0010, 0x0030].value
    Gender = dc[0x0010, 0x0030].value

    if len(BirthDate) == 0:
        BirthDate = '2021-04-14'
    if len(Gender) == 0:
        Gender = "NULL"

    val = (patient_id, PatientName, BirthDate, Gender)
    patient_query = "insert Ignore into Patient(Patient_id,PatientName,BirthDate,Gender) VALUES (%s,%s,%s,%s)"

    cursor = cnx.cursor()
    cursor.execute(patient_query, val)
    cnx.commit()

    '''Study Table'''
    StudyID = dc[0x0020, 0x000d].value  # str(dc[0x0020, 0x0010].value)  # update from study id to study instance uid
    StudyDate = dc[0x0008, 0x0020].value
    StudyDescription = dc[0x0008, 0x1030].value
    patient_id = dc[0x0010, 0x0020].value
    study_val = (str(StudyID), StudyDate, StudyDescription, str(patient_id))
    study_query = "insert Ignore into Study(StudyID, StudyDate, StudyDescription, Patient_patient_id) VALUES (%s,%s,%s,%s)"
    cursor = cnx.cursor()
    cursor.execute(study_query, study_val)
    cnx.commit()

    '''Series Table'''
    SeriesInstanceUID_value = dc[0x0020, 0x000e].value
    StudyInstanceUID = dc[0x0020, 0x000d].value
    val_st = ("'" + SeriesInstanceUID_value + "'", StudyInstanceUID)
    st_query = "insert Ignore into Series (series_id, Study_StudyID) values (%s, %s)" % val_st
    cursor = cnx.cursor()
    cursor.execute(st_query)

    cnx.commit()

if __name__ == '__main__':
    ct_image(dc)
    RT(dc)
    RD_Dose(dc)
    struct_set(dc)
