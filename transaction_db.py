import os
import pickle
import time
from common_db import *

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
        log_entry = {
            'trans_id': trans_id,
            'table_name': table_name,
            'record': record,
            'timestamp': time.time(),
            'operation': 'UPDATE'  # 前像只会在UPDATE操作时产生
        }
        with open(self.before_image_file, 'ab') as f:
            pickle.dump(log_entry, f)
            f.flush()  # 确保立即写入磁盘
            os.fsync(f.fileno())  # 强制同步到磁盘

    def write_after_image(self, trans_id, table_name, record, operation):
        # 写入后像，operation可以是'INSERT'或'UPDATE'
        log_entry = {
            'trans_id': trans_id,
            'table_name': table_name,
            'record': record,
            'timestamp': time.time(),
            'operation': operation
        }
        with open(self.after_image_file, 'ab') as f:
            pickle.dump(log_entry, f)
            f.flush()  # 确保立即写入磁盘
            os.fsync(f.fileno())  # 强制同步到磁盘

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
