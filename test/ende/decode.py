import jpype
import datetime
# JVM 시작
jpype.startJVM()
jpype.addClassPath("watosysEncrypt_not_otp_v1.0.0.jar")
jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")

_edgeId = "STSW000001"
_timestamp = datetime.datetime.now().strftime("%Y:%m:%d-%H:%M:%S.%f")[:-3]
data_message = "Pp/NSBnuDeQ1dBy8HDpJHvS4MHCYc3zmCC18nkTrcGxnlD2JpohTALGXr0aaCDMJNL8FEjktdHLMkx0SRpgfM0q2QVuQd6ComxH5hflnic3EbZoQDPoOwsL81PMtLeEQn+LrvYxJcPcufF8VvEo/1A=="

try:
    sd = jvm_msg_encrypt_class.decode(_timestamp, _edgeId, str(data_message))
    print(sd)
except Exception as e:
    print("Error:", e)