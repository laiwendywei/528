import Merge_Code
from flask import Flask, render_template, request
from flask_mysqldb import MySQL
import shutil
import cv2 as cv
import numpy as np
import pydicom
import base64
from PIL import Image
import io
from threading import Thread
from pydicom import dcmread

# 在网页上输入dicom的时候，一定要连着directory都输完整...qwq 不然no file found
app = Flask(__name__,static_folder='./static')

#app.config['MYSQL_HOST'] = '127.0.0.1'
#app.config['MYSQL_USER'] = 'root'
#app.config['MYSQL_PASSWORD'] = ''
#app.config['MYSQL_DB'] = 'BME'

#mysql = MySQL(app)

@app.route("/", methods=['GET', 'POST'])
def main():
    return render_template('index.html')

@app.route("/send_data", methods=['POST'])
def dcm(image_data=None):
    dcm = request.form['dcm']
    dc = dcmread(dcm)
    if dc.file_meta[0x0002, 0x0002].value == '1.2.840.10008.5.1.4.1.1.481.3':
        Merge_Code.struct_set(dc)
        # print ('inserted structure set dicom')
    elif dc.file_meta[0x0002, 0x0002].value == '1.2.840.10008.5.1.4.1.1.2':
        output, instance = Dicom_to_Image(dcm)
        instance = "dicom.jpg"
        cv.imwrite(instance, output)
        shutil.move('./' + instance, './static/' + instance)
        Merge_Code.ct_image(dc)
        # print('inserted ct dicom')
    elif dc.file_meta[0x0002, 0x0002].value == '1.2.840.10008.5.1.4.1.1.481.2':
        Merge_Code.RD_Dose(dc)
        # print ('inserted dose dicom')
    elif dc.file_meta[0x0002, 0x0002].value == '1.2.840.10008.5.1.4.1.1.481.5':
        Merge_Code.RT(dc)
    else:
        print('Cannot find a parser.')

def Dicom_to_Image(path):
    dcm = pydicom.read_file(path)
    rows = dcm.get(0x00280010).value
    cols = dcm.get(0x00280011).value

    instance = int(dcm.get(0x00200013).value)
    Window_Center = int(dcm.get(0x00281050).value)
    Window_width = int(dcm.get(0x00281051).value)

    Window_Max = int(Window_Center+Window_width / 2)
    Window_Min = int(Window_Center-Window_width / 2)

    if (dcm.get(0x00281052) is None):
        Rescale_Intercept = int(dcm.get(0x00281052).value)
    else:
        Rescale_Intercept = int(dcm.get(0x00281052).value)
    if dcm.get(0x00281053) is None:
        Rescale_Slope = 1
    else:
        Rescale_Slope = int(dcm.get(0x00281053).value)

    new = np.zeros((rows,cols),np.uint8)
    pixels = dcm.pixel_array
    for i in range(0,rows):
        for j in range(0,cols):
            pix_val = pixels[i][j]
            rescale_pix_val = pix_val*Rescale_Slope+Rescale_Intercept
            if rescale_pix_val > Window_Max:
                new[i][j] = 255
            elif rescale_pix_val < Window_Min:
                new[i][j] =0
            else:
                new[i][j] = int(((rescale_pix_val -Window_Min) / (Window_Max-Window_Min))*255)
    return new,instance

if __name__ == '__main__':
    app.run(debug=True)
