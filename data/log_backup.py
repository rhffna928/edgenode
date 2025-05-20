# 2. 백업DB 읽고 로그 DB에 record 기록

import sqlite3
import datetime
import os

# 경로 설정
backup_db_path = "C:/Test/Goo/edgeCloud_241111(gts)/backup/node/250520_100800_test_backup.db"
log_db_path = r"C:\Test\Goo\edgeCloud_241111(gts)\backup\node\log_result.db"

# 소스 DB 이름
source_name = os.path.basename(backup_db_path)
log_time = datetime.datetime.now().isoformat(timespec='seconds')

# 1. 백업 DB 연결 (읽기 전용)
backup_conn = sqlite3.connect(f"file:{backup_db_path}?mode=ro", uri=True)
backup_cursor = backup_conn.cursor()

# 2. 로그 DB 연결 (쓰기용)
log_conn = sqlite3.connect(log_db_path)
log_cursor = log_conn.cursor()

# 3. 로그 테이블 생성
# log_cursor.execute("""
# CREATE TABLE IF NOT EXISTS record_log (
#     log_time TEXT NOT NULL,
#     table_name TEXT NOT NULL,
#     record_count INTEGER NOT NULL,
#     source_db TEXT NOT NULL,
#     PRIMARY KEY (log_time, table_name, source_db)
# )
# """)

# 4. 테이블 목록 가져오기 (record_log 제외)
backup_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
tables = [row[0] for row in backup_cursor.fetchall() if row[0] != 'record_log']

# 5. 각 테이블 레코드 수 로깅
for table in tables:
    backup_cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = backup_cursor.fetchone()[0]

    log_cursor.execute("""
    INSERT OR IGNORE INTO record_log (log_time, table_name, record_count, source_db)
    VALUES (?, ?, ?, ?)
    """, (log_time, table, count, source_name))

print(f"✅ {len(tables)}개 테이블의 레코드 수를 log_result.db에 기록했습니다.")

# 마무리
log_conn.commit()
backup_conn.close()
log_conn.close()
