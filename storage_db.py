# -----------------------------------------------------------------------
# storage_db.py
# Author: Jingyu Han  hjymail@163.com
# -----------------------------------------------------------------------
# the module is to store tables in files
# Each table is stored in a separate file with the suffix ".dat".
# For example, the table named movie_star is stored in file movie_star.dat
# -----------------------------------------------------------------------

# struct of the file is as follows, each block is 4096
# ---------------------------------------------------
# block_0|block_1|...|block_n
# ----------------------------------------------------------------
from common_db import BLOCK_SIZE

# structure of block_0, which stores the meta information and field information
# ---------------------------------------------------------------------------------
# block_id                                # 0
# number_of_dat_blocks                    # at first it is 0 because there is no data in the table
# number_of_fields or number_of_records   # the total number of fields for the table
# -----------------------------------------------------------------------------------------


# the data type is as follows
# ----------------------------------------------------------
# 0->str,1->varStr,2->int,3->bool
# ---------------------------------------------------------------


# structure of data block, whose block id begins with 1
# ----------------------------------------
# block_id       
# number of records
# record_0_offset         # it is a pointer to the data of record
# record_1_offset
# ...
# record_n_offset
# ....
# free space
# ...
# record_n
# ...
# record_1
# record_0
# -------------------------------------------

# structure of one record
# -----------------------------
# pointer                     #offset of table schema in block id 0
# length of record            # including record head and record content
# time stamp of last update  # for example,1999-08-22
# field_0_value
# field_1_value
# ...
# field_n_value
# -------------------------


import struct
import os
import ctypes


# --------------------------------------------
# the class can store table data into files
# functions include insert, delete and update
# --------------------------------------------

class Storage(object):

    # ------------------------------
    # constructor of the class
    # input:
    #       tableName
    # -------------------------------------
    def __init__(self, tableName):
        # print "__init__ of ",Storage.__name__,"begins to execute"
        self.open = False
        tableName=tableName.strip()

        self.record_list = []
        self.record_Position = []

        if not os.path.exists(tableName + '.dat'.encode('utf-8')):  # the file corresponding to the table does not exist
            print('table file '.encode('utf-8') + tableName + '.dat does not exists'.encode('utf-8'))
            self.f_handle = open(tableName + '.dat'.encode('utf-8'), 'wb+')
            self.f_handle.close()
            self.open = False
            print(tableName + '.dat has been created'.encode('utf-8'))

        self.f_handle = open(tableName + '.dat'.encode('utf-8'), 'rb+')
        print('table file '.encode('utf-8') + tableName + '.dat has been opened'.encode('utf-8'))
        self.open = True

        self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        self.f_handle.seek(0)
        self.dir_buf = self.f_handle.read(BLOCK_SIZE)

        self.dir_buf.strip()
        my_len = len(self.dir_buf)
        self.field_name_list = []
        beginIndex = 0

        if my_len == 0:  # there is no data in block 0, we should write metaData into block 0
            if isinstance(tableName, bytes):
                self.num_of_fields = input(
                    "please input the number of fields in table " + tableName.decode('utf-8') + ":")
            else:
                self.num_of_fields = input(
                    "please input the number of fields in table " + tableName + ":")
            if int(self.num_of_fields) > 0:

                self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
                self.block_id = 0
                self.data_block_num = 0
                struct.pack_into('!iii', self.dir_buf, beginIndex, 0, 0,
                                 int(self.num_of_fields))  # block_id,number_of_data_blocks,number_of_fields

                beginIndex = beginIndex + struct.calcsize('!iii')

                # the following is to write the field name,field type and field length into the buffer in turn
                for i in range(int(self.num_of_fields)):
                    field_name = input("please input the name of field " + str(i) + " :")

                    if len(field_name) < 10:
                        field_name = ' ' * (10 - len(field_name.strip())) + field_name

                    while True:
                        field_type = input(
                            "please input the type of field(0-> str; 1-> varStr; 2-> int; 3-> boolean) " + str(
                                i) + " :")
                        if int(field_type) in [0, 1, 2, 3]:
                            break

                    # to need further modification here
                    field_length = input("please input the length of field " + str(i) + " :")
                    temp_tuple = (field_name, int(field_type), int(field_length))
                    self.field_name_list.append(temp_tuple)
                    if isinstance(field_name, str):
                        field_name = field_name.encode('utf-8')

                    struct.pack_into('!10sii', self.dir_buf, beginIndex, field_name, int(field_type),
                                     int(field_length))
                    beginIndex = beginIndex + struct.calcsize('!10sii')

                self.f_handle.seek(0)
                self.f_handle.write(self.dir_buf)
                self.f_handle.flush()

        else:  # there is something in the file

            self.block_id, self.data_block_num, self.num_of_fields = struct.unpack_from('!iii', self.dir_buf, 0)

            print('number of fields is ', self.num_of_fields)
            print('data_block_num', self.data_block_num)
            beginIndex = struct.calcsize('!iii')

            # the followings are to read field name, field type and field length into main memory structures
            for i in range(self.num_of_fields):
                field_name, field_type, field_length = struct.unpack_from('!10sii', self.dir_buf,
                                                                          beginIndex + i * struct.calcsize(
                                                                              '!10sii'))  # 'i' means no memory alignment

                temp_tuple = (field_name, field_type, field_length)
                self.field_name_list.append(temp_tuple)
                print("the " + str(i) + "th field information (field name,field type,field length) is ", temp_tuple)
        # print self.field_name_list
        record_head_len = struct.calcsize('!ii10s')
        record_content_len = sum(map(lambda x: x[2], self.field_name_list))
        # print record_content_len

        Flag = 1
        while Flag <= self.data_block_num:
            self.f_handle.seek(BLOCK_SIZE * Flag)
            self.active_data_buf = self.f_handle.read(BLOCK_SIZE)
            self.block_id, self.Number_of_Records = struct.unpack_from('!ii', self.active_data_buf, 0)
            print('Block_ID=%s,   Contains %s data' % (self.block_id, self.Number_of_Records))
            # There exists a record
            if self.Number_of_Records > 0:
                for i in range(self.Number_of_Records):
                    self.record_Position.append((Flag, i))
                    offset = \
                        struct.unpack_from('!i', self.active_data_buf,
                                           struct.calcsize('!ii') + i * struct.calcsize('!i'))[
                            0]
                    record = struct.unpack_from('!' + str(record_content_len) + 's', self.active_data_buf,
                                                offset + record_head_len)[0]
                    tmp = 0
                    tmpList = []
                    for field in self.field_name_list:
                        t = record[tmp:tmp + field[2]].strip()
                        tmp = tmp + field[2]
                        if field[1] == 2:
                            t = int(t)
                        if field[1] == 3:
                            t = bool(t)
                        tmpList.append(t)
                    self.record_list.append(tuple(tmpList))
            Flag += 1

    # ------------------------------
    # return the record list of the table
    # input:
    #       
    # -------------------------------------
    def getRecord(self):
        return self.record_list

    # --------------------------------
    # to insert a record into table
    # param insert_record: list
    # return: True or False
    # -------------------------------
    def insert_record(self, insert_record):

        # example: ['xuyidan','23','123456']

        # step 1: to check the insert_record is True or False

        tmpRecord = []
        for idx in range(len(self.field_name_list)):
            insert_record[idx] = insert_record[idx].strip()
            if self.field_name_list[idx][1] == 0 or self.field_name_list[idx][1] == 1:
                if len(insert_record[idx]) > self.field_name_list[idx][2]:
                    return False
                tmpRecord.append(insert_record[idx])
            if self.field_name_list[idx][1] == 2:
                try:
                    tmpRecord.append(int(insert_record[idx]))
                except:
                    return False
            if self.field_name_list[idx][1] == 3:
                try:
                    tmpRecord.append(bool(insert_record[idx]))
                except:
                    return False
            insert_record[idx] = ' ' * (self.field_name_list[idx][2] - len(insert_record[idx])) + insert_record[idx]

        # step2: Add tmpRecord to record_list; change insert_record into inputStr
        inputStr = ''.join(insert_record)

        self.record_list.append(tuple(tmpRecord))

        # Step3: To calculate MaxNum in each Data Blocks
        record_content_len = len(inputStr)
        record_head_len = struct.calcsize('!ii10s')
        record_len = record_head_len + record_content_len
        MAX_RECORD_NUM = (BLOCK_SIZE - struct.calcsize('!i') - struct.calcsize('!ii')) / (
                record_len + struct.calcsize('!i'))

        # Step4: To calculate new record Position
        if not len(self.record_Position):
            self.data_block_num += 1
            self.record_Position.append((1, 0))
        else:
            last_Position = self.record_Position[-1]
            if last_Position[1] == MAX_RECORD_NUM - 1:
                self.record_Position.append((last_Position[0] + 1, 0))
                self.data_block_num += 1
            else:
                self.record_Position.append((last_Position[0], last_Position[1] + 1))

        last_Position = self.record_Position[-1]

        # Step5: Write new record into file xxx.dat
        # update data_block_num
        self.f_handle.seek(0)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, 0, self.data_block_num)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data block head
        self.f_handle.seek(BLOCK_SIZE * last_Position[0])
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, last_Position[0], last_Position[1] + 1)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data offset
        offset = struct.calcsize('!ii') + last_Position[1] * struct.calcsize('!i')
        beginIndex = BLOCK_SIZE - (last_Position[1] + 1) * record_len
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + offset)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!i'))
        struct.pack_into('!i', self.buf, 0, beginIndex)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data
        record_schema_address = struct.calcsize('!iii')
        update_time = '2016-11-16'  # update time
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + beginIndex)
        self.buf = ctypes.create_string_buffer(record_len)
        struct.pack_into('!ii10s', self.buf, 0, record_schema_address, record_content_len, update_time.encode('utf-8'))
        struct.pack_into('!' + str(record_content_len) + 's', self.buf, record_head_len, inputStr.encode('utf-8'))
        self.f_handle.write(self.buf.raw)
        self.f_handle.flush()

        return True

    # ------------------------------
    # show the data structure and its data
    # input:
    #       t
    # -------------------------------------

    def show_table_data(self):
        print('|    '.join(map(lambda x: x[0].decode('utf-8').strip(), self.field_name_list)))  # show the structure

        # the following is to show the data of the table
        for record in self.record_list:
            print(record)

    # --------------------------------
    # to delete the data file
    # input
    #       tableName
    # output
    #       True or False
    # -----------------------------------
    def delete_table_data(self, tableName):

        # step 1: identify whether the file is still open
        if self.open:
            self.f_handle.close()
            self.open = False

        # step 2: remove the file from os   
        tableName.strip()
        if os.path.exists(tableName + '.dat'.encode('utf-8')):
            os.remove(tableName + '.dat'.encode('utf-8'))

        return True

    # ------------------------------
    # get the list of field information, each element of which is (field name, field type, field length)
    # input:
    #       
    # -------------------------------------

    def getFieldList(self):
        return self.field_name_list

    # --------------------------------
    # update one record by keyword, support byte storage and string comparison
    # input:
    #       keyword_field: the field name to be used as keyword
    #       keyword_value: the value of the keyword field
    #       update_field: the field name to be updated
    #       new_value: the new value to be set
    # output:
    #       True or False
    # -----------------------------------

    def update_row_by_keyword(self, keyword_field, keyword_value, update_field, new_value):
        # 1. 规范化字段名（从字节解码为字符串）
        field_names = [f[0].decode('utf-8').strip() for f in self.field_name_list]

        # 2. 检查字段是否存在
        if keyword_field not in field_names or update_field not in field_names:
            print(f"Error: Valid fields are {field_names}")
            return False

        # 3. 获取字段索引和类型
        keyword_idx = field_names.index(keyword_field)
        update_idx = field_names.index(update_field)
        field_type = self.field_name_list[update_idx][1]
        field_length = self.field_name_list[update_idx][2]

        # 4. 类型转换处理
        try:
            if field_type == 2:  # int
                new_value = int(new_value)
            elif field_type == 3:  # bool
                new_value = bool(int(new_value))
        except ValueError:
            print(f"Type error: Cannot convert '{new_value}' to field type {field_type}")
            return False

        # 5. 查找并更新记录（字节存储，字符串比较）
        updated = False
        for i, record in enumerate(self.record_list):
            # 获取关键字字段的值（字节）
            keyword_bytes = record[keyword_idx]

            # 将字节解码为字符串（假设使用 UTF-8 编码）
            keyword_str = keyword_bytes.decode('utf-8').strip()

            # 使用字符串进行比较
            if keyword_str == str(keyword_value).strip():
                # 准备新值（根据字段类型处理）
                if field_type == 1:  # varStr 类型（可变长度字符串）
                    new_bytes = str(new_value).encode('utf-8')
                elif field_type == 0:  # str 类型（固定长度字符串）
                    # 截断或补全到固定长度
                    new_str = str(new_value)[:field_length]
                    new_bytes = new_str.ljust(field_length, ' ').encode('utf-8')
                elif field_type == 2:  # int 类型
                    # 转为字符串，右对齐并用0填充
                    new_bytes = str(new_value).rjust(field_length, '0').encode('utf-8')
                elif field_type == 3:  # bool 类型
                    # 存储为1或0，右对齐
                    new_bytes = ('1' if new_value else '0').rjust(field_length, '0').encode('utf-8')
                else:
                    print(f"Unsupported field type: {field_type}")
                    return False

                # 更新记录
                new_record = list(record)
                new_record[update_idx] = new_bytes
                self.record_list[i] = tuple(new_record)
                updated = True
                print(f"Updated: {record} -> {self.record_list[i]}")
                self._update_record_on_disk(i)

        if not updated:
            print(f"No record found where {keyword_field}={keyword_value}")
        return updated

    # --------------------------------
    # deal with the update of record on disk
    # input:
    #       record_index: the index of the record in self.record_list
    # ------------------------------------

    def _update_record_on_disk(self, record_index):
        block_id, pos_in_block = self.record_Position[record_index]
        record = self.record_list[record_index]

        # 构建记录内容（全部使用字节串）
        record_content = b''
        for i, field in enumerate(self.field_name_list):
            field_type = field[1]
            field_length = field[2]
            val = record[i]

            # 确保值是正确的字节表示
            if isinstance(val, bytes):
                # 如果已经是字节，直接使用
                byte_val = val
            else:
                # 否则转为字符串再编码
                str_val = str(val)

                if field_type == 0 or field_type == 1:  # 字符串类型
                    # 截断或补全到固定长度
                    str_val = str_val[:field_length]
                    byte_val = str_val.ljust(field_length, ' ').encode('utf-8')
                elif field_type == 2:  # int 类型
                    # 右对齐并用0填充
                    byte_val = str_val.rjust(field_length, '0').encode('utf-8')
                elif field_type == 3:  # bool 类型
                    # 存储为1或0，右对齐
                    byte_val = ('1' if val else '0').rjust(field_length, '0').encode('utf-8')
                else:
                    byte_val = str_val.encode('utf-8')

            # 确保长度正确（再次验证，防止上面的逻辑有遗漏）
            if len(byte_val) > field_length:
                byte_val = byte_val[:field_length]
            elif len(byte_val) < field_length:
                pad_char = b' ' if field_type in [0, 1] else b'0'
                byte_val = byte_val.ljust(field_length, pad_char) if field_type in [0, 1] else byte_val.rjust(
                    field_length, pad_char)

            record_content += byte_val

        # 写入磁盘
        head_len = struct.calcsize('!ii10s')
        self.f_handle.seek(BLOCK_SIZE * block_id +
                           struct.calcsize('!ii') +
                           pos_in_block * struct.calcsize('!i'))
        record_offset = struct.unpack('!i', self.f_handle.read(struct.calcsize('!i')))[0]

        self.f_handle.seek(BLOCK_SIZE * block_id + record_offset)
        buf = ctypes.create_string_buffer(head_len + len(record_content))
        struct.pack_into('!ii10s', buf, 0,
                         struct.calcsize('!iii'),
                         len(record_content),
                         '2023-01-01'.encode('utf-8'))
        struct.pack_into(f'!{len(record_content)}s', buf, head_len, record_content)
        self.f_handle.write(buf.raw)  # 使用 buf.raw 写入完整缓冲区
        self.f_handle.flush()

    # ----------------------------------------
    # destructor
    # ------------------------------------------------
    def __del__(self):  # write the metaHead information in the head object to file

        if self.open:
            self.f_handle.seek(0)
            self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
            struct.pack_into('!ii', self.buf, 0, 0, self.data_block_num)
            self.f_handle.write(self.buf)
            self.f_handle.flush()
            self.f_handle.close()

    # ------------------------------
    # 获取字段名称列表，用于查询执行
    # 返回与 getFieldList 格式相同的字段信息
    # ------------------------------
    def getfilenamelist(self):
        return self.getFieldList()
