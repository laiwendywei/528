import shutil

import cv2 as cv
import numpy as np
import pydicom

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

def main():
    image = "./sample/006/CT.1.2.840.113619.2.55.3.51045121.848.1507151727.835.23-no-phi.dcm"
    output, instance = Dicom_to_Image(image)
    # instance_name = str(instance).zfill(3)+".jpg"
    instance = "dicom.jpg"
    cv.imwrite(instance, output)
    shutil.move('./'+instance, './static/'+instance)

if __name__ == "__main__":
    main()