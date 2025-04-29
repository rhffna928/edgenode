import jpype
import datetime
# JVM 시작
jpype.startJVM()
jpype.addClassPath("watosysEncrypt_not_otp_v1.0.0.jar")
jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")

_edgeId = "STSW000001"
_timestamp = datetime.datetime.now().strftime("%Y:%m:%d-%H:%M:%S.%f")[:-3]
data_message = """
{
    "topicName":"return/global_planning",
    "plan":[
        {"STSW000001":[[22420232,[36.609286,127.287869],0],[22420261,[36.61033132,127.2848557],1],[22420265,[36.61033237,127.2848132],2],[22420264,[36.61036086,127.2848038],2],[22420061,[36.6103738,127.2855318],3]}
        ],
    "missionArea":null,
    "strategy":"MIN_TOTAL_TIME"
}
"""

try:
    sd = jvm_msg_encrypt_class.encode(_timestamp, _edgeId, str(data_message))
    print(sd)
except Exception as e:
    print("Error:", e)