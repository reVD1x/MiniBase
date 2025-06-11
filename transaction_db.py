import os
import pickle
import time
from common_db import *


# ----------------------------------------
# transaction_db.py
# B22042225
# -----------------------
class TransactionManager:
    def __init__(self):
        self.before_image_file = "before_image.log"
        self.after_image_file = "after_image.log"
        self.active_trans_file = "active_trans.log"
        self.commit_trans_file = "commit_trans.log"
        self.active_transactions = set()  # 活动事务表
        self.committed_transactions = set()  # 提交事务表
        self._init_files()
        self._load_transaction_states()

    def _init_files(self):
        # 初始化日志文件
        if not os.path.exists(self.before_image_file):
            with open(self.before_image_file, 'wb') as f:
                pass
        if not os.path.exists(self.after_image_file):
            with open(self.after_image_file, 'wb') as f:
                pass
        if not os.path.exists(self.active_trans_file):
            self._save_transaction_states()
        if not os.path.exists(self.commit_trans_file):
            with open(self.commit_trans_file, 'wb') as f:
                pickle.dump(self.committed_transactions, f)

    def _load_transaction_states(self):
        # 加载事务状态
        try:
            with open(self.active_trans_file, 'rb') as f:
                self.active_transactions = pickle.load(f)
        except:
            self.active_transactions = set()

        try:
            with open(self.commit_trans_file, 'rb') as f:
                self.committed_transactions = pickle.load(f)
        except:
            self.committed_transactions = set()

    def _save_transaction_states(self):
        # 保存事务状态
        with open(self.active_trans_file, 'wb') as f:
            pickle.dump(self.active_transactions, f)
        with open(self.commit_trans_file, 'wb') as f:
            pickle.dump(self.committed_transactions, f)

    def begin_transaction(self):
        # 开始新事务
        trans_id = len(self.active_transactions) + len(self.committed_transactions) + 1
        self.active_transactions.add(trans_id)
        self._save_transaction_states()
        return trans_id

    def write_before_image(self, trans_id, table_name, record):
        # 写入前像
        if not isinstance(table_name, str):
            table_name = table_name.decode('utf-8')

        log_entry = {
            'trans_id': trans_id,
            'table_name': table_name,
            'record': record,
            'timestamp': time.time(),
            'operation': 'DELETE'  # 因为前像只在DELETE和UPDATE操作时产生，这里先默认为DELETE，具体由after_image决定
        }
        with open(self.before_image_file, 'ab') as f:
            pickle.dump(log_entry, f)
            f.flush()  # 确保立即写入磁盘
            os.fsync(f.fileno())  # 强制同步到磁盘

    def write_after_image(self, trans_id, table_name, record, operation):
        # 写入后像，operation可以是'INSERT'、'UPDATE'或'DELETE'
        if not isinstance(table_name, str):
            table_name = table_name.decode('utf-8')

        if operation not in ['INSERT', 'UPDATE', 'DELETE']:
            raise ValueError(f"Invalid operation type: {operation}")

        log_entry = {
            'trans_id': trans_id,
            'table_name': table_name,
            'record': record,
            'timestamp': time.time(),
            'operation': operation
        }

        # 如果有对应的before_image，需要更新其operation
        if operation in ['UPDATE', 'DELETE']:
            try:
                with open(self.before_image_file, 'rb+') as f:
                    # 定位到最后一条相关记录
                    pos = f.seek(0, 2)  # 移到文件末尾
                    while pos > 0:
                        try:
                            f.seek(pos)
                            last_entry = pickle.load(f)
                            if (last_entry.get('trans_id') == trans_id and
                                    last_entry.get('table_name') == table_name):
                                # 找到相关记录，更新operation
                                last_entry['operation'] = operation
                                f.seek(pos)
                                pickle.dump(last_entry, f)
                                break
                        except:
                            pos -= 1
            except:
                pass  # 如果更新before_image失败，不影响after_image的写入

        with open(self.after_image_file, 'ab') as f:
            pickle.dump(log_entry, f)
            f.flush()
            os.fsync(f.fileno())

    def commit_transaction(self, trans_id):
        # 提交事务
        if trans_id in self.active_transactions:
            self.active_transactions.remove(trans_id)
            self.committed_transactions.add(trans_id)
            self._save_transaction_states()

    def rollback_transaction(self, trans_id):
        # 回滚事务
        if trans_id in self.active_transactions:
            # TODO: 实现回滚逻辑，通过前像恢复数据
            self.active_transactions.remove(trans_id)
            self._save_transaction_states()

    def recover(self):
        # 系统恢复
        # 读取所有日志，处理未完成的事务
        pass  # TODO: 实现系统恢复逻辑
