import qrcode
from qreader import QReader
import cv2

class gen_qrcode():

    def __init__(self, data):
        self.data = data

    def genqr(self, fn):
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(self.data)
        qr.make(fit=True)

        img = qr.make_image(fill_color="black", back_color="white")
        img.save(fn)


class qr_reader():

    def __init__(self, img_file):
        self.fn = img_file

    def read(self):
        img = cv2.imread(self.fn)
        detect = cv2.QRCodeDetector()
        value, points, straight_qrcode = detect.detectAndDecode(img)
        return value



def test1():

    #text = 'Приготовление раствора окислителя: - в пробирку 1,5 мл на весах отмерить 10 - 30 мг окислителя. Затем в пробирку к окислителю добавить, обработанную DEPC воду. Количество воды рассчитать по формуле:  vol, мл = масса навески, мг / 85. - Перемешивать содержимое пробирки до полного растворения кристаллов окислителя. Раствор окислителя может хранится при комнатной температуре не более 5 дней с момента приготовления.'
    #text = 'CODE12345///Ацетонитрил ХЧ, Экос'
    #text = 'BASE_CODE_CONTENT_CODE_12345'
    #text = 'INIT_BASE_CODE_OLIGO_LAB_s0000001'
    text = 'INIT_BASE_CODE_OLIGO_LAB_s0000002'
    fn_list = [
        f'INIT_BASE_CODE_OLIGO_LAB_00001{i}'
        for i in range(41, 51)
    ]
    for f in fn_list:
        qr = gen_qrcode(f)
        qr.genqr(f'qr/{f}.png')

def test2():
    qr = qr_reader('test.png')
    print(qr.read())

if __name__ == '__main__':
    test1()


