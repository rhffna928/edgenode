import jpype
import jpype.imports
from jpype.types import *
from datetime import datetime
import json

# JVM 시작
jpype.startJVM()
jpype.addClassPath("../watosysEncrypt_not_otp_v1.0.0.jar")
jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")
json_message = {
    0 : {"topicName":"return/global_planning","plan":[{"STSW000001":[[22420232,[36.609286,127.287869],0],[22420261,[36.61033132,127.2848557],1],[22420265,[36.61033237,127.2848132],2],[22420264,[36.61036086,127.2848038],2],[22420061,[36.6103738,127.2855318],3]]}],"missionArea":"null","strategy":"MIN_TOTAL_TIME"},
    1 : {"vehicleId":"TRAC000001","vehicleType":"T","eventCode": "warning","eventDescription": "주변 장애물 감지","registDate":"2024-11-04 17:24:35"},
    2 : {"vehicleId":"STSW000001","eventCode":"warning"},
    3 : {'resultCd': 0, 'resultMssage': "globalpath write 성공"},
    4 : {'resultCd': 0, 'resultMssage': 'init 성공 응답 22', 'userId': '1', 'edgeList': [['TRAC000003', '트랙터03', 'T']]}
}




now = datetime.now()
now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
_edgeid = str("STSW000001")

for i in range(len(json_message)):
    data_message = json.dumps(json_message[i], ensure_ascii=False)

    data = jvm_msg_encrypt_class.encode(now_str,_edgeid,data_message)
    print(data,"\n")

# json_message = {"data":"WT2vIhz5G1rJXJ6gLqBvIS42U5galM0DTjyePz3SrAl20aLDiM5xQSBDWttJ3k4c0lzsPLORg794BP2U1J3oVg=="}
# data_message = json.dumps(json_message, ensure_ascii=False)
# try:
#     decode_data = jvm_msg_encrypt_class.decode(now_str,data_message)
    
#     print(decode_data)
# except Exception as err:
#     print(err)