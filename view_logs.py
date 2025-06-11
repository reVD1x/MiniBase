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
            print(f"读取文件 {file_path} 时出错: {str(e)}")
    else:
        print(f"\n文件 {file_path} 不存在")

def main():
    """主函数，查看所有日志文件的内容"""
    print("欢迎使用日志查看器！")
    print("=" * 60)

    # 查看所有日志文件
    log_files = [
        'before_image.log',
        'after_image.log',
        'active_trans.log',
        'commit_trans.log'
    ]

    for log_file in log_files:
        print_log_content(log_file)

if __name__ == '__main__':
    main()
