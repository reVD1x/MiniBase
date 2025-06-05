# -----------------------
# main_db.py
# author: Jingyu Han   hjymail@163.com
# modified by: Ning Wang, Yidan Xu
# -----------------------------------
# This is the main loop of the program
# ---------------------------------------

import struct
import sys
import ctypes
import os

import head_db  # the main memory structure of table schema
import schema_db  # the module to process table schema
import storage_db  # the module to process the storage of instance

import query_plan_db  # for SQL clause of which data is stored in binary format
import lex_db  # for lex, where data is stored in binary format
import parser_db  # for yacc, where ddata is tored in binary format
import common_db  # the global variables, functions, constants in the program
import query_plan_db  # construct the query plan and execute it

PROMPT_STR = 'Input your choice  \n1:add a new table structure and data \n2:delete a table structure and data\
\n3:view a table structure and data \n4:delete all tables and data \n5:select from where clause\
\n6:delete a row according to field keyword \n7:update a row according to field keyword \n. to quit):\n'


# --------------------------
# the main loop, which needs further implementation
# ---------------------------

def main():
    # main loops for the whole program
    print('main function begins to execute')

    # The instance data of table is stored in binary format, which corresponds to chapter 2-8 of textbook

    schemaObj = schema_db.Schema()  # to create a schema object, which contains the schema of all tables
    dataObj = None
    choice = input(PROMPT_STR)

    while True:

        if choice == '1':  # add a new table and lines of data
            tableName = input('please enter your new table name:')
            if isinstance(tableName, str):
                tableName = tableName.encode('utf-8')
            #  tableName not in all.sch
            insertFieldList = []
            if tableName.strip() not in schemaObj.get_table_name_list():
                # Create a new table
                dataObj = storage_db.Storage(tableName)

                insertFieldList = dataObj.getFieldList()

                schemaObj.appendTable(tableName, insertFieldList)  # add the table structure
            else:
                dataObj = storage_db.Storage(tableName)

                # to the students: The following needs to be further implemented (many lines can be added)
                record = []
                Field_List = dataObj.getFieldList()
                for x in Field_List:
                    s = 'Input field name is: ' + str(x[0].strip()) + '  field type is: ' + str(x[1]) + \
                        ' field maximum length is: ' + str(x[2]) + '\n'
                    record.append(input(s))

                if dataObj.insert_record(record):  # add a row
                    print('OK!')
                else:
                    print('Wrong input!')

                del dataObj

            choice = input(PROMPT_STR)





        elif choice == '2':  # delete a table from schema file and data file

            table_name = input('please input the name of the table to be deleted:')
            if isinstance(table_name,str):
                table_name=table_name.encode('utf-8')
            if schemaObj.find_table(table_name.strip()):
                if schemaObj.delete_table_schema(
                        table_name):  # delete the schema from the schema file
                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.delete_table_data(table_name.strip())  # delete table content from the table file
                    del dataObj

                else:
                    print('the deletion from schema file fail')


            else:
                print('there is no table '.encode('utf-8') + table_name + ' in the schema file'.encode('utf-8'))


            choice = input(PROMPT_STR)



        elif choice == '3':  # view the table structure and all the data

            print(schemaObj.headObj.tableNames)
            table_name = input('please input the name of the table to be displayed:')
            if isinstance(table_name,str):
                table_name=table_name.encode('utf-8')
            if table_name.strip():
                if schemaObj.find_table(table_name.strip()):
                    schemaObj.viewTableStructure(table_name)  # to be implemented

                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.show_table_data()  # view all the data of the table
                    del dataObj
                else:
                    print('table name is None')

            choice = input(PROMPT_STR)



        elif choice == '4':  # delete all the table structures and their data
            table_name_list = list(schemaObj.get_table_name_list())
            # to be inserted here -> to delete from data files
            for i in range(len(table_name_list)):
                table_name = table_name_list[i]
                table_name.strip()

                if table_name:
                    stObj = storage_db.Storage(table_name)
                    stObj.delete_table_data(table_name.strip())  # delete table data
                    del stObj

            schemaObj.deleteAll()  # delete schema from schema file

            choice = input(PROMPT_STR)


        elif choice == '5':  # process SELECT FROM WHERE clause
            print('#        Your Query is to SQL QUERY                  #')
            sql_str = input('please enter the select from where clause:')
            lex_db.set_lex_handle()  # to set the global_lexer in common_db.py
            parser_db.set_handle()  # to set the global_parser in common_db.py

            try:
                common_db.global_syn_tree = common_db.global_parser.parse(
                    sql_str.strip(),
                    lexer=common_db.global_lexer
                )  # construct the global_syn_tree
                #reload(query_plan_db)
                query_plan_db.construct_logical_tree()
                query_plan_db.execute_logical_tree()
            except:
                print('WRONG SQL INPUT!')
            print('#----------------------------------------------------#')
            choice = input(PROMPT_STR)


        elif choice == '6':  # delete a line of data from the storage file given the keyword

            table_name = input('please input the name of the table to be deleted from:')
            field_name = input('please input the field name and the corresponding keyword (fieldname:keyword):')
            # to the students: to be inserted here, delete the line from data files
######
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')

            # 解析输入的字段名和关键字
            try:
                field_name, keyword = field_name.split(':')
                field_name = field_name.strip()
                keyword = keyword.strip()
            except:
                print('Invalid input format! Please use "fieldname:keyword"')
                choice = input(PROMPT_STR)
                continue

            # 检查表是否存在
            if schemaObj.find_table(table_name.strip()):
                dataObj = storage_db.Storage(table_name)
                records = dataObj.getRecord()
                field_list = dataObj.getFieldList()

                # 查找字段在记录中的索引位置
                field_index = -1
                for i, field in enumerate(field_list):
                    if field[0].strip().decode('utf-8') == field_name:
                        field_index = i
                        break

                if field_index == -1:
                    print('Field name not found in table!')
                else:
                    # 查找匹配的记录
                    found = False
                    new_records = []

                    for record in records:
                        # 检查记录中对应字段的值是否与关键字匹配
                        field_value = str(record[field_index])
                        if field_value.strip() == keyword:
                            found = True
                            print(f'Record found and will be deleted: {record}')
                        else:
                            new_records.append(record)

                    if found:
                        # 重建表并重新插入记录（除了要删除的记录）
                        dataObj.delete_table_data(table_name.strip())
                        del dataObj

                        # 重新创建表
                        if schemaObj.find_table(table_name.strip()):
                            dataObj = storage_db.Storage(table_name)

                            # 重新插入保留的记录
                            for record in new_records:
                                record_str = [str(item) for item in record]
                                dataObj.insert_record(record_str)

                            print('Record deleted successfully!')
                            del dataObj
                        else:
                            print('Error rebuilding table after deletion!')
                    else:
                        print('No record found with the specified keyword!')
                        del dataObj
            else:
                print(f'Table {table_name.decode("utf-8")} not found!')
#####
            choice = input(PROMPT_STR)

        elif choice == '7':  # update a line of data given the keyword

            table_name = input('please input the name of the table:')
            field_name = input('please input the field name:')
            field_name_value = input('please input the old value of the field:')
            # to the students: to be inserted here, update the line according to the user input
#####
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')

            field_name = field_name.strip()
            field_name_value = field_name_value.strip()

            # 检查表是否存在
            if schemaObj.find_table(table_name.strip()):
                dataObj = storage_db.Storage(table_name)
                records = dataObj.getRecord()
                field_list = dataObj.getFieldList()

                # 查找字段在记录中的索引位置
                field_index = -1
                for i, field in enumerate(field_list):
                    if field[0].strip().decode('utf-8') == field_name:
                        field_index = i
                        break

                if field_index == -1:
                    print('Field name not found in table!')
                else:
                    # 查找匹配的记录
                    found = False
                    new_records = []

                    for record in records:
                        # 检查记录中对应字段的值是否与关键字匹配
                        field_value = str(record[field_index])
                        if field_value.strip() == field_name_value:
                            found = True
                            print(f'Record found: {record}')
                            # 询问用户要更新哪个字段和新值
                            update_field = input('Input the field name you want to update: ')
                            update_value = input('Input the new value: ')

                            # 查找更新字段的索引
                            update_index = -1
                            for j, f in enumerate(field_list):
                                if f[0].strip().decode('utf-8') == update_field.strip():
                                    update_index = j
                                    break

                            if update_index == -1:
                                print('Update field name not found!')
                                new_records.append(record)
                            else:
                                # 创建更新后的记录
                                updated_record = list(record)
                                # 根据字段类型转换值
                                field_type = field_list[update_index][1]
                                if field_type == 2:  # int类型
                                    try:
                                        updated_record[update_index] = int(update_value)
                                    except:
                                        print('Invalid value for integer field!')
                                        new_records.append(record)
                                        continue
                                elif field_type == 3:  # boolean类型
                                    try:
                                        updated_record[update_index] = bool(update_value)
                                    except:
                                        print('Invalid value for boolean field!')
                                        new_records.append(record)
                                        continue
                                else:  # 字符串类型
                                    if len(update_value) > field_list[update_index][2]:
                                        print('Value too long for the field!')
                                        new_records.append(record)
                                        continue
                                    updated_record[update_index] = update_value

                                print(f'Record will be updated to: {tuple(updated_record)}')
                                new_records.append(tuple(updated_record))
                        else:
                            new_records.append(record)

                    if found:
                        # 重建表并重新插入更新后的记录
                        dataObj.delete_table_data(table_name.strip())
                        del dataObj

                        # 重新创建表
                        if schemaObj.find_table(table_name.strip()):
                            dataObj = storage_db.Storage(table_name)

                            # 重新插入记录
                            for record in new_records:
                                record_str = [str(item) for item in record]
                                dataObj.insert_record(record_str)

                            print('Record updated successfully!')
                            del dataObj
                        else:
                            print('Error rebuilding table after update!')
                    else:
                        print(f'No record found with the specified value "{field_name_value}" in field "{field_name}"!')
                        del dataObj
            else:
                print(f'Table {table_name.decode("utf-8")} not found!')
######
            choice = input(PROMPT_STR)



        elif choice == '.':
            print('main loop finishies')
            del schemaObj
            break

    print('main loop finish!')


if __name__ == '__main__':
    main()


