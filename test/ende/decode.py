import jpype
import datetime
# JVM 시작
jpype.startJVM()
jpype.addClassPath("watosysEncrypt_not_otp_v1.0.0.jar")
jvm_msg_encrypt_class = jpype.JClass("watosys.utils.eg.msg.MsgEncrypt")

_edgeId = "STSW000001"
_timestamp = datetime.datetime.now().strftime("%Y:%m:%d-%H:%M:%S.%f")[:-3]
data_message = "xLz6OnUg92wnaYwBLOlKWool7dwJXTWtXfH3eYKdKABxol10F1Kb+64RA+V3lgwbXHWF6TbC3L56BYJ1w4aO2C90AOqBmCZgKeh7hQ9UTYDPZ7RFO83DENZtua3petLkDDXS7jDVwDi9O9t28irFzNKX5L/Kj3rjs4Sde018SxpDGIukQ6Fwc3JvpzmA7K6z0uUP6yK3IBH0wVxhlhFBvyeMqhJv5t3iz+Z+ZBh48B5Iug/EAxbKoPgGh2nlcnkB5k6Tq5TEq7PJv7oZHpAZM498j0kZbLGJNuLLSahuvthpTbBPnQi7DFtRG1nLx8WRDzm7taH62bCuRB6g0aslj9pmILLXAKWbSKxV+vvWXQHbvggoaojaX2bX+rSFDA+1s7tdQBy3aZ8dJhDpw2rMbITKHreRC7BjefPM62oR73ncPs9FN+eB4VtCzCya/WItVGVij9BawPbu0f7fR1CVUg=="

try:
    sd = jvm_msg_encrypt_class.decode(_timestamp, _edgeId, str(data_message))
    print(sd)
except Exception as e:
    print("Error:", e)