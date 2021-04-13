import mysql.connector
from pydicom import dcmread

# Import File
dc = dcmread('RS.1.2.246.352.71.4.992070825148.228408.20171011093506-no-phi.dcm')


cnx = mysql.connector.connect(user='root', password='2K?!__aK!cy(',
                              host='127.0.0.1',
                              database='BME',
                              auth_plugin='mysql_native_password')

TABLES = {}

'''create structure_set_series'''
# TABLES['Structure_Set_Series_test'] = (
#     "CREATE TABLE IF NOT EXISTS Structure_Set_Series_test ( StructNo vatchar(100) PRIMARY KEY, Series_id varchar(100), PRIMARY KEY (StructNo))")


TABLES['StructSet'] = (
    "CREATE TABLE IF NOT EXISTS StructSet (StructNo varchar(100) PRIMARY KEY, NumberOfROIObservations int ,NumberOfReferFrame int , NumberOFStructSetROI int ,"
    " Series_series_id varchar(255))")

TABLES['ROIObservation'] = (
    "CREATE TABLE IF NOT EXISTS ROIObservation (StructSet_StructNo varchar(100), Observation_Number varchar(10) ,Referenced_ROI_Number varchar(10) , ROI_Observation_Label varchar(255))")

TABLES['ReferFrame'] = (
    "CREATE TABLE IF NOT EXISTS ReferFrame (StructSet_StructNo varchar(100), FrameOfReferenceUID varchar(255))"
)

TABLES['ReferStudy'] = (
    "CREATE TABLE IF NOT EXISTS ReferStudy (ReferFrame_FrameOfReferenceUID varchar(255), Referenced_SOP_Class_UID varchar(255),Referenced_SOP_Instance_UID varchar(255))"
)

TABLES['ReferSeries'] = (
    "CREATE TABLE IF NOT EXISTS ReferSeries (ReferStudy_Referenced_SOP_Instance_UID varchar(255), Series_Instance_UID  varchar(255))"
)

TABLES['ContourImage'] = (
    "CREATE TABLE IF NOT EXISTS ContourImage (ReferSeries_Series_Instance_UID  varchar(255), Referenced_SOP_Class_UID varchar(255), Referenced_SOP_Instance_UID varchar(255))"
)



for k in TABLES.keys():
    cursor = cnx.cursor()
    cursor.execute(TABLES[k])


'''''''Start when Media Storage SOP Class UID = RT Structure Set Storage'''''''

if dc.file_meta[0x0002, 0x0002].value == '1.2.840.10008.5.1.4.1.1.481.3':

    nocheck = "SET FOREIGN_KEY_CHECKS = 0;"
    cursor = cnx.cursor()
    cursor.execute(nocheck)
    cnx.commit()


    # '''Series Table'''
    # SeriesInstanceUID_value = dc[0x0020, 0x000e].value
    # Series_Number = dc[0x0020, 0x0011].value
    # val_st = ( "'"+SeriesInstanceUID_value+"'",Series_Number)
    # st_query = "insert into Series (series_id, StudyInstanceUID) values (%s, %s)"%val_st
    # cursor = cnx.cursor()
    # cursor.execute(st_query)
    #
    # cnx.commit()


    '''StructSet Table'''
    StructSetLabel = dc[0x3006, 0x0002].value
    NumberOfROIObservations = len(dc[0x3006, 0x0080].value)
    NumberOfReferFrame = len(dc[0x3006, 0x0010].value)
    NumberOfStructSetROI = len(dc[0x3006, 0x0020].value)
    SeriesInstanceUID_value = dc[0x0020, 0x000e].value
    val = ( "'"+StructSetLabel+"'",NumberOfROIObservations, NumberOfReferFrame,NumberOfStructSetROI,  "'" + SeriesInstanceUID_value +"'")
    ss_query = "insert into StructSet (StructNo, NumberOfROIObservations ,NumberOfReferFrame , NumberOFStructSetROI ,Series_series_id) VALUES (%s,%s,%s,%s,%s)"%val
    cursor = cnx.cursor()
    cursor.execute(ss_query)

    cnx.commit()


    '''ROIObservation table'''
    ROI_Observations = dc[0x3006, 0x0080].value
    for roi_ob in ROI_Observations:
        val_ob = ("'" + StructSetLabel+"'", "'" + str(roi_ob[0x3006, 0x0082].value)+"'", "'" + str(roi_ob[0x3006, 0x0084].value)+"'", "'" + str(roi_ob[0x3006, 0x0085].value)+"'")
        roiob_query = "insert into ROIObservation (StructSet_StructNo, Observation_Number ,Referenced_ROI_Number, ROI_Observation_Label) VALUES (%s,%s,%s,%s)"%val_ob
        cursor = cnx.cursor()
        cursor.execute(roiob_query)
        cnx.commit()

    '''ReferFrame table'''
    ReferFrame = dc[0x3006, 0x0010].value
    for rf in ReferFrame:
        val_rf = ("'" + StructSetLabel+"'", "'" + rf[0x0020, 0x0052].value+"'")
        rf_query = "insert into ReferFrame (StructSet_StructNo,FrameOfReferenceUID ) VALUES (%s,%s)"%val_rf
        cursor = cnx.cursor()
        cursor.execute(rf_query)
        cnx.commit()
        '''ReferStudy table'''
        ReferStudy = ReferFrame[0][0x3006, 0x0012].value
        for rs in ReferStudy:
            FrameOfReferenceUID = rf[0x0020, 0x0052].value
            Referenced_SOP_Class_UID = rs[0x0008, 0x1150].value
            Referenced_SOP_Instance_UID = rs[0x0008, 0x1155].value
            val_rs = ("'" + FrameOfReferenceUID+"'", "'" + Referenced_SOP_Class_UID+"'", "'" + Referenced_SOP_Instance_UID+"'")
            rs_query = "insert into ReferStudy (ReferFrame_FrameOfReferenceUID, Referenced_SOP_Class_UID,Referenced_SOP_Instance_UID) values (%s,%s,%s)"%val_rs
            cursor = cnx.cursor()
            cursor.execute(rs_query)
            cnx.commit()
            '''ReferSeries'''
            ReferSeries = rs[0x3006, 0x0014].value
            for rse in ReferSeries:
                Series_Instance_UID = rse[0x0020, 0x000e].value
                val_rse = ("'" + Referenced_SOP_Instance_UID+"'", "'" + Series_Instance_UID+"'")
                rse_query = "insert into ReferSeries (ReferStudy_Referenced_SOP_Instance_UID, Series_Instance_UID) values (%s,%s)" % val_rse
                cursor = cnx.cursor()
                cursor.execute(rse_query)
                cnx.commit()
                '''ContourImage'''
                ContourImage = rse[0x3006, 0x0016].value
                for ci in ContourImage:
                    Referenced_SOP_Class_UID_ci = ci[0x0008, 0x1150].value
                    Referenced_SOP_Instance_UID_ci = ci[0x0008, 0x1155].value
                    val_ci = ("'" + Series_Instance_UID+"'", "'" + Referenced_SOP_Class_UID_ci+"'","'" + Referenced_SOP_Instance_UID_ci+"'")
                    ci_query = "insert into ContourImage (ReferSeries_Series_Instance_UID, Referenced_SOP_Class_UID,Referenced_SOP_Instance_UID) values (%s,%s,%s)" % val_ci
                    cursor = cnx.cursor()
                    cursor.execute(ci_query)
                    cnx.commit()

