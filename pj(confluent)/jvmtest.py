import jpype
import jpype.imports
from jpype.types import *
from datetime import datetime
import json

# JVM 시작
jpype.startJVM()
jpype.addClassPath("watosysEncrypt_not_otp_v1.0.0.jar")
jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")

json_message = {"topicName":"return/global_planning","plan":[{"STSW000001":[[22420232,[36.609286,127.287869],0],[22420261,[36.61033132,127.2848557],1],[22420265,[36.61033237,127.2848132],2],[22420264,[36.61036086,127.2848038],2],[22420061,[36.6103738,127.2855318],3]]}],"missionArea":"null","strategy":"MIN_TOTAL_TIME"}

data_message = json.dumps(json_message, ensure_ascii=False)

now = datetime.now()
now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
_edgeid = str("STSW000001")
data = jvm_msg_encrypt_class.encode(now_str,_edgeid,data_message)

print(data)