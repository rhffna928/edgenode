import jpype
import datetime
# JVM 시작
jpype.startJVM()
jpype.addClassPath("watosysEncrypt_not_otp_v1.0.0.jar")
jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")

_edgeId = "STSW000001"
_timestamp = datetime.datetime.now().strftime("%Y:%m:%d-%H:%M:%S.%f")[:-3]
data_message = "WT2vIhz5G1rJXJ6gLqBvIS42U5galM0DTjyePz3SrAl20aLDiM5xQSBDWttJ3k4c0lzsPLORg794BP2U1J3oVg=="

try:
    sd = jvm_msg_encrypt_class.decode(_timestamp, _edgeId, str(data_message))
    print(sd)
except Exception as e:
    print("Error:", e)