import pickle
import os
from datetime import datetime

def print_log_content(file_path):
    """打印日志文件的内容"""
    if os.path.exists(file_path):
        print(f"\n{'-'*20} {file_path} 内容 {'-'*20}")
        try:
            with open(file_path, 'rb') as f:
                while True:
                    try:
                        entry = pickle.load(f)
                        if isinstance(entry, (set, dict)):
                            if isinstance(entry, set):
                                print(f"事务ID集合: {entry}")
                            else:
                                print(f"事务ID: {entry.get('trans_id')}")
                                print(f"操作类型: {entry.get('operation', '未知')}")
                                print(f"表名: {entry.get('table_name')}")
                                print(f"记录: {entry.get('record')}")
                                print(f"时间: {datetime.fromtimestamp(entry.get('timestamp'))}")
                                print("-" * 50)
                    except EOFError:
                        break
        except Exception as e:
            print(f"读取文件出错: {e}")
    else:
        print(f"\n{file_path} 文件不存在")

if __name__ == "__main__":
    # 打印所有日志文件的内容
    print_log_content("before_image.log")
    print_log_content("after_image.log")
    print_log_content("active_trans.log")
    print_log_content("commit_trans.log")
