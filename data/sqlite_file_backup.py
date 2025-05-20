# 1.  노드 sqlite.db 파일 가져오기

import subprocess
import datetime
import os

# 서버 정보
server_ip = "172.30.1.20"
username = "nmone"
remote_db_path = "/home/nmone/mk/python/kafka/egnode/pj_1(confluent)/sqlite_db/test.db"
remote_backup_dir = "/home/nmone/gts/backup"

# 백업 파일 이름 (날짜 포함)
timestamp = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
backup_filename = f"{timestamp}_test_backup.db"
remote_backup_path = f"{remote_backup_dir}/{backup_filename}"

# 로컬 저장 경로 (Windows)
local_dir = r"C:\Test\Goo\edgeCloud_241111(gts)\backup\node"

# 백업 명령어 (서버에서 실행)
backup_cmd = f'ssh {username}@{server_ip} "mkdir -p {remote_backup_dir}; sqlite3 \\"{remote_db_path}\\" \\".backup \'{remote_backup_path}\'\\""'

print(f"1️⃣ 서버 백업 실행 중...\n{backup_cmd}")
backup_result = subprocess.run(backup_cmd, shell=True)

if backup_result.returncode != 0:
    print("❌ 서버에서 백업 실패!")
    exit(1)
print(f"✅ 서버 백업 완료: {remote_backup_path}")

# SCP 명령어로 파일 다운로드
scp_cmd = f'scp {username}@{server_ip}:"{remote_backup_path}" "{local_dir}\\{backup_filename}"'
print(f"2️⃣ 로컬로 백업 파일 복사 중...\n{scp_cmd}")
scp_result = subprocess.run(scp_cmd, shell=True)

if scp_result.returncode != 0:
    print("❌ 로컬 다운로드 실패!")
    exit(2)
print(f"✅ 로컬 저장 완료: {local_dir}\\{backup_filename}")
