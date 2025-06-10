# -----------------------
# main_db.py
# author: Jingyu Han   hjymail@163.com
# modified by: Ning Wang, Yidan Xu
# -----------------------------------
# This is the main loop of the program
# ---------------------------------------
import importlib
import struct
import sys
import ctypes
import os

import head_db  # the main memory structure of table schema
import schema_db  # the module to process table schema
import storage_db  # the module to process the storage of instance

import query_plan_db  # for SQL clause of which data is stored in binary format
import lex_db  # for lex, where data is stored in binary format
import parser_db  # for yacc, where ddata is stored in binary format
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

    # The instance data of table is stored in binary format, which corresponds to chapter 2-8 of the textbook

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
                insertFieldList = dataObj.getFieldList()  # get the field list from the data file
                schemaObj.appendTable(tableName, insertFieldList)  # add the table structure to schema file
            else:
                dataObj = storage_db.Storage(tableName)

                try:
                    # 开始事务
                    dataObj.begin_transaction()

                    # 收集记录数据
                    record = []
                    Field_List = dataObj.getFieldList()
                    for x in Field_List:
                        s = 'Input field name is: ' + str(x[0].strip()) + '  field type is: ' + str(x[1]) + \
                            ' field maximum length is: ' + str(x[2]) + '\n'
                        record.append(input(s))

                    if dataObj.insert_record(record):  # add a row
                        # 提交事务
                        dataObj.commit_transaction()
                        print('OK!')
                    else:
                        # 回滚事务
                        dataObj.rollback_transaction()
                        print('Wrong input!')

                except Exception as e:
                    if dataObj:
                        dataObj.rollback_transaction()
                    print(f'Error: {str(e)}')
                finally:
                    if dataObj:
                        del dataObj

            choice = input(PROMPT_STR)





        elif choice == '2':  # delete a table from schema file and data file

            table_name = input('please input the name of the table to be deleted:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            if schemaObj.find_table(table_name.strip()):
                if schemaObj.delete_table_schema(table_name):  # delete the schema from the schema file
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
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
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
                table_name = table_name.strip()

                if table_name:
                    stObj = storage_db.Storage(table_name)
                    stObj.delete_table_data(table_name)  # delete table data
                    del stObj

            schemaObj.deleteAll()  # delete schema from schema file

            choice = input(PROMPT_STR)


        elif choice == '5':  # process SELECT FROM WHERE clause
            print('#        Your Query is to SQL QUERY                  #')
            sql_str = input('please enter the select from where clause:')

            try:
                # 确保先重载模块，以避免全局变量引用问题
                print("重新加载模块...")
                importlib.reload(common_db)
                importlib.reload(lex_db)
                importlib.reload(parser_db)
                importlib.reload(query_plan_db)

                # 初始化词法分析器和语法分析器
                print("初始化词法分析器...")
                lex_db.set_lex_handle()  # to set the global_lexer in common_db.py
                if common_db.global_lexer is None:
                    raise Exception("词法分析器初始化失败")

                print("初始化语法分析器...")
                parser_db.set_handle()  # to set the global_parser in common_db.py
                if common_db.global_parser is None:
                    raise Exception("语法分析器初始化失败")

                # 确保SQL字符串使用正确编码
                sql_str = sql_str.strip()

                # 解析SQL语句构建语法树
                print('解析SQL语句:', sql_str)
                common_db.global_syn_tree = common_db.global_parser.parse(
                    sql_str,
                    lexer=common_db.global_lexer
                )

                # 执行SQL语句
                if common_db.global_syn_tree:
                    print('语法树构建成功')
                    # 构建逻辑树
                    query_plan_db.construct_logical_tree()

                    # 执行查询
                    if common_db.global_logical_tree:
                        print('执行逻辑树...')
                        query_plan_db.execute_logical_tree()
                    else:
                        print('逻辑查询树生成失败')
                else:
                    print('SQL语句解析失败')

            except Exception as e:
                print('SQL输入错误: ', str(e))
                import traceback
                traceback.print_exc()
            print('#----------------------------------------------------#')
            choice = input(PROMPT_STR)


        elif choice == '6':  # delete a line of data from the storage file given the keyword

            table_name = input('please input the name of the table to be deleted from:')
            field_name = input('please input the field name and the corresponding keyword (fieldName:keyword):')
            # to the students: to be inserted here, delete the line from data files

            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')

            if table_name.strip():
                if not schemaObj.find_table(table_name.strip()):
                    print('table name is None')
                else:
                    try:
                        field_name, keyword = field_name.split(':', 1)
                        field_name = field_name.strip()
                        keyword = keyword.strip()
                    except ValueError:
                        print("wrong input format, please use 'fieldName:keyword'")
                        choice = input(PROMPT_STR)
                        continue

                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    field_list = dataObj.getFieldList()  # get the field list from the data file

                    # for field in field_list:
                    #   if field[0].strip() == field_name:

            choice = input(PROMPT_STR)

        elif choice == '7':  # update a line of data given the keyword
            table_name = input('please input the name of the table:').strip()
            keyword_field = input('please input the search field name:').strip()
            keyword_value = input('please input the search value:').strip()
            update_field = input('please input the field to update:').strip()
            new_value = input('please input the new value:').strip()

            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')

            try:
                storage = storage_db.Storage(table_name)
                # 开始事务
                storage.begin_transaction()

                if storage.update_row_by_keyword(keyword_field, keyword_value, update_field, new_value):
                    # 更新成功，提交事务
                    storage.commit_transaction()
                    print("Update successful!")
                else:
                    # 更新失败，回滚事务
                    storage.rollback_transaction()
                    print("Update failed (no matching record or invalid fields)")
            except Exception as e:
                if 'storage' in locals():
                    storage.rollback_transaction()
                print(f"Error: {str(e)}")
            finally:
                if 'storage' in locals():
                    del storage

            choice = input(PROMPT_STR)



        elif choice == '.':
            print('main loop finishes')
            del schemaObj
            break

    print('main loop finish!')


if __name__ == '__main__':
    main()
