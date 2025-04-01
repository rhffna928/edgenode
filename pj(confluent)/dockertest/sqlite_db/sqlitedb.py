import os
import shutil
from datetime import datetime

def rename_db_file(old_db_path, new_db_path):
    try:
        # DB 파일이 존재하는지 확인
        if not os.path.exists(old_db_path):
            print(f"원본 DB 파일을 찾을 수 없습니다: {old_db_path}")
            return False
            
        # 새 파일 이름으로 복사
        shutil.copy2(old_db_path, new_db_path)
        print(f"DB 파일이 성공적으로 복사되었습니다: {new_db_path}")
        return True
    except Exception as e:
        print(f"DB 파일 복사 중 오류 발생: {str(e)}")
        return False

def main():
    # 현재 날짜를 YYYYMMDD 형식으로 가져옴
    current_date = datetime.now().strftime("%Y%m%d")
    
    # 원본 DB 파일 경로 (실제 DB 파일 경로로 수정 필요)
    old_db_path = "sample.db"
    
    # 새로운 DB 파일 이름 생성
    new_db_name = f"sample_{current_date}.db3"
    new_db_path = os.path.join(os.path.dirname(old_db_path), new_db_name)
    
    # DB 파일 이름 변경
    if rename_db_file(old_db_path, new_db_path):
        print("DB 파일 이름 변경이 완료되었습니다.")
    else:
        print("DB 파일 이름 변경에 실패했습니다.")

if __name__ == "__main__":
    main()