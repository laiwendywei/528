import mysql.connector
from pydicom import dcmread

# dicom_file = input("DICOM File: ")
# dc = dcmread(str(dicom_file))
# username = input("User Name: ")
# database = input("Which database: ")
# cnx = mysql.connector.connect(user=str(username), host='127.0.0.1',
#                               database=str(database), auth_plugin='mysql_native_password')

cnx = mysql.connector.connect(user='root', host='127.0.0.1',
                              database='BME', auth_plugin='mysql_native_password')

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

import glob

for dc_file in glob.glob('006/*.dcm'):
    # print (dc_file)
    dc = dcmread(str(dc_file))
    # print (dc.file_meta[0x0002, 0x0002].value)
    if dc.file_meta[0x0002, 0x0002].value == '1.2.840.10008.5.1.4.1.1.481.3':
        struct_set(dc)
        print ('inserted structure set dicom')
    if dc.file_meta[0x0002, 0x0002].value == '1.2.840.10008.5.1.4.1.1.2':
        ct_image(dc)
        print('inserted ct dicom')
    else:
        print ('Cannot find a parser.')