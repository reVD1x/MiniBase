# ------------------------------------------------
# query_plan_db.py
# author: Jingyu Han  hjymail@163.com
# modified by:Shuting Guo shutingnjupt@gmail.com
# ------------------------------------------------


# ----------------------------------------------------------
# this module can turn a syntax tree into a query plan tree
# ----------------------------------------------------------

import common_db
import storage_db
import itertools


# --------------------------------
# 不直接导入global_syn_tree，而是在需要时通过common_db.global_syn_tree引用
# -------------------------------------------


class parseNode:
    def __init__(self):
        self.sel_list = []
        self.from_list = []
        self.where_list = []

    def get_sel_list(self):
        return self.sel_list

    def get_from_list(self):
        return self.from_list

    def get_where_list(self):
        return self.where_list

    def update_sel_list(self, self_list):
        self.sel_list = self_list

    def update_from_list(self, from_list):
        self.from_list = from_list

    def update_where_list(self, where_list):
        self.where_list = where_list


# --------------------------------
# Author: Shuting Guo shutingnjupt@gmail.com
# to extract data from global variable syn_tree
# output:
#       sel_list
#       from_list
#       where_list
# --------------------------------
def extract_sfw_data():
    print('extract_sfw_data begins to execute')
    if common_db.global_syn_tree is None:
        print('wrong')
    else:
        common_db.show(common_db.global_syn_tree)
        PN = parseNode()
        destruct(common_db.global_syn_tree, PN)
        return PN.get_sel_list(), PN.get_from_list(), PN.get_where_list()


# ---------------------------------
# Author: Shuting Guo shutingnjupt@gmail.com
# Query  : SFW
#   SFW  : SELECT SelList FROM FromList WHERE Condition
# SelList: TCNAME COMMA SelList
# SelList: TCNAME
#
# FromList:TCNAME COMMA FromList
# FromList:TCNAME
# Condition: TCNAME EQX CONSTANT
# B22042228
# ---------------------------------

def destruct(nodeObj, PN):
    if isinstance(nodeObj, common_db.Node):  # it is a Node object
        if nodeObj.children:
            # 确保比较的是字节字符串
            node_value = nodeObj.value
            if isinstance(node_value, str):
                node_value = node_value.encode('utf-8')

            # 使用字节字符串比较
            if node_value == b'SelList':
                tmpList = []
                show(nodeObj, tmpList)
                print("Found SelList, extracted:", tmpList)
                PN.update_sel_list(tmpList)
            elif node_value == b'FromList':
                tmpList = []
                show(nodeObj, tmpList)
                print("Found FromList, extracted:", tmpList)
                PN.update_from_list(tmpList)
            elif node_value == b'Cond':
                tmpList = []
                show(nodeObj, tmpList)
                print("Found Cond, extracted:", tmpList)
                PN.update_where_list(tmpList)
            else:
                for i in range(len(nodeObj.children)):
                    destruct(nodeObj.children[i], PN)


# B22042228
def show(nodeObj, tmpList):
    if isinstance(nodeObj, common_db.Node):
        if not nodeObj.children:
            # 确保添加到列表中的值是字节串
            if isinstance(nodeObj.value, str):
                tmpList.append(nodeObj.value.encode('utf-8'))
            else:
                tmpList.append(nodeObj.value)
        else:
            for i in range(len(nodeObj.children)):
                show(nodeObj.children[i], tmpList)
    if isinstance(nodeObj, str):
        tmpList.append(nodeObj.encode('utf-8'))
    elif isinstance(nodeObj, bytes):
        tmpList.append(nodeObj)


# ---------------------------
# input:
#       from_list
# output:
#       a tree
# -----------------------------------

def construct_from_node(from_list):
    if from_list:
        if len(from_list) == 1:
            temp_node = common_db.Node(from_list[0], None)
            return common_db.Node('X', [temp_node])
        elif len(from_list) == 2:
            temp_node_first = common_db.Node(from_list[0], None)
            temp_node_second = common_db.Node(from_list[1], None)

            return common_db.Node('X', [temp_node_first, temp_node_second])

        elif len(from_list) > 2:

            right_node = common_db.Node(from_list[len(from_list) - 1], None)

            return common_db.Node('X', [construct_from_node(from_list[0:len(from_list) - 1]), right_node])


# ---------------------------
# input:
#       where_list
#       from_node
# output:
#       a tree
# -----------------------------------
def construct_where_node(from_node, where_list):
    if from_node and len(where_list) > 0:
        return common_db.Node('Filter', [from_node], where_list)
    elif from_node and len(where_list) == 0:  # there is no where clause
        return from_node


# ---------------------------
# input:
#       sel_list
#       wf_node
# output:
#       a tree
# -----------------------------------
def construct_select_node(wf_node, sel_list):
    if wf_node and len(sel_list) > 0:
        return common_db.Node('Proj', [wf_node], sel_list)


# ----------------------------------
# Author: Shuting Guo shutingnjupt@gmail.com
# to execute the query plan and return the result
# input
#       global logical tree
# B22042228
# ---------------------------------------------

def execute_logical_tree():
    if common_db.global_logical_tree:
        def execute_tree():

            idx = 0
            dict_ = {}

            def show(node_obj, idx, dict_):
                if isinstance(node_obj, common_db.Node):  # it is a Node object
                    dict_.setdefault(idx, [])
                    dict_[idx].append(node_obj.value)
                    if node_obj.var:
                        dict_[idx][-1] = tuple((dict_[idx][-1], node_obj.var))
                    if node_obj.children:
                        for i in range(len(node_obj.children)):
                            show(node_obj.children[i], idx + 1, dict_)

            show(common_db.global_logical_tree, idx, dict_)
            idx = sorted(dict_.keys(), reverse=True)[0]

            def GetFilterParam(tableName_Order, current_field, param):
                # 确保param是字节串
                if isinstance(param, str):
                    param = param.encode('utf-8')

                # 确保使用字节串进行所有操作
                dot = b'.'
                if dot in param:
                    tableName = param.split(dot)[0]
                    FieldName = param.split(dot)[1]

                    # 确保tableName_Order中的表名也是字节串以进行一致比较
                    tableName_Order_bytes = []
                    for name in tableName_Order:
                        if isinstance(name, str):
                            tableName_Order_bytes.append(name.encode('utf-8'))
                        else:
                            tableName_Order_bytes.append(name)

                    if tableName in tableName_Order_bytes:
                        TableIndex = tableName_Order_bytes.index(tableName)
                    else:
                        return 0, 0, 0, False
                elif len(tableName_Order) == 1:
                    TableIndex = 0
                    FieldName = param
                else:
                    return 0, 0, 0, False

                # 确保字段名比较一致性
                tmp = []
                for field in current_field[TableIndex]:
                    if isinstance(field[0], str):
                        tmp.append(field[0].strip().encode('utf-8'))
                    else:
                        tmp.append(field[0].strip())

                if FieldName in tmp:
                    FieldIndex = tmp.index(FieldName)
                    FieldType = current_field[TableIndex][FieldIndex][1]
                    return TableIndex, FieldIndex, FieldType, True
                else:
                    return 0, 0, 0, False

            current_field = []
            current_list = []
            # print dict_
            while idx >= 0:
                if idx == sorted(dict_.keys(), reverse=True)[0]:
                    if len(dict_[idx]) > 1:
                        a_1 = storage_db.Storage(dict_[idx][0])
                        a_2 = storage_db.Storage(dict_[idx][1])
                        current_list = []
                        tableName_Order = [dict_[idx][0], dict_[idx][1]]
                        current_field = [a_1.getFileNameList(), a_2.getFileNameList()]
                        for x in itertools.product(a_1.getRecord(), a_2.getRecord()):
                            current_list.append(list(x))
                    else:
                        a_1 = storage_db.Storage(dict_[idx][0])
                        current_list = a_1.getRecord()

                        tableName_Order = [dict_[idx][0]]
                        current_field = [a_1.getFileNameList()]
                        # print current_list

                elif 'X' in dict_[idx] and len(dict_[idx]) > 1:
                    a_2 = storage_db.Storage(dict_[idx][1])
                    tableName_Order.append(dict_[idx][1])
                    current_field.append(a_2.getFileNameList())
                    tmp_List = current_list[:]
                    current_list = []
                    for x in itertools.product(tmp_List, a_2.getRecord()):
                        current_list.append(list((x[0][0], x[0][1], x[1])))

                elif 'X' not in dict_[idx]:
                    if 'Filter' in dict_[idx][0]:
                        FilterChoice = dict_[idx][0][1]

                        # 处理条件列表，可能包含多个AND条件
                        temp_List = current_list[:]
                        current_list = []

                        # 识别AND条件
                        has_and = False
                        and_conditions = []

                        # 首先检查条件是否包含AND结构
                        i = 0
                        while i < len(FilterChoice):
                            if isinstance(FilterChoice[i], bytes) and FilterChoice[i] == b'AND':
                                has_and = True
                                # 第一个条件: [字段, '=', 值]
                                first_cond = [FilterChoice[i-3], FilterChoice[i-2], FilterChoice[i-1]]
                                # 第二个条件: [字段, '=', 值]
                                second_cond = [FilterChoice[i+1], FilterChoice[i+2], FilterChoice[i+3]]

                                # 如果之前没有添加过第一个条件，则添加
                                if not and_conditions:
                                    and_conditions.append(first_cond)

                                # 添加第二个条件
                                and_conditions.append(second_cond)
                                i += 4  # 跳过已处理的元素
                            else:
                                i += 1

                        # 如果没有识别到AND结构，则按照传统方式处理单个条件
                        if not has_and:
                            and_conditions = [[FilterChoice[0], FilterChoice[1], FilterChoice[2]]]

                        print("识别的条件:", and_conditions)

                        # 处理每个条件
                        for condition in and_conditions:
                            condition_field, condition_op, condition_value = condition

                            # 获取字段信息
                            TableIndex, FieldIndex, FieldType, isTrue = GetFilterParam(
                                tableName_Order, current_field, condition_field)

                            if not isTrue:
                                print(f"条件 {condition_field} 无效")
                                return [], [], False

                            # 处理条件值
                            if FieldType == 2:  # 整数
                                FilterParam = int(condition_value.strip())
                            elif FieldType == 3:  # 布尔
                                FilterParam = bool(condition_value.strip())
                            else:  # 字符串
                                if isinstance(condition_value, bytes):
                                    FilterParam = condition_value.strip().replace(b"'", b"").replace(b'"', b"")
                                else:
                                    FilterParam = str(condition_value).strip().replace("'", "").replace('"', "").encode('utf-8')

                            print(f"条件比较: 字段={condition_field.decode('utf-8') if isinstance(condition_field, bytes) else condition_field}, 类型={FieldType}, 值={FilterParam}")

                            # 应用过滤条件
                            filtered_list = []
                            for record in (current_list if current_list else temp_List):
                                if len(current_field) == 1:
                                    field_value = record[FieldIndex]
                                else:
                                    field_value = record[TableIndex][FieldIndex]

                                # 确保类型一致
                                if FieldType == 0 or FieldType == 1:  # 字符串类型
                                    if isinstance(field_value, bytes):
                                        field_value = field_value.strip()
                                    else:
                                        field_value = str(field_value).strip().encode('utf-8')

                                # 比较并过滤
                                if field_value == FilterParam:
                                    filtered_list.append(record)

                            # 更新结果集为过滤后的记录
                            current_list = filtered_list

                        # 如果处理完所有条件后没有记录，输出提示
                        if not current_list:
                            print("没有满足所有条件的记录")

                    if 'Proj' in dict_[idx][0]:
                        SelIndexList = []
                        for i in range(len(dict_[idx][0][1])):
                            TableIndex, FieldIndex, FieldType, isTrue = GetFilterParam(tableName_Order, current_field,
                                                                                       dict_[idx][0][1][i])
                            if not isTrue:
                                return [], [], False
                            SelIndexList.append((TableIndex, FieldIndex))
                        tmp_List = current_list[:]
                        current_list = []
                        # print SelIndexList,current_field
                        for tmpRecord in tmp_List:
                            # print tmpRecord
                            if len(current_field) == 1:
                                tmp = []
                                for x in list(map(lambda x: x[1], SelIndexList)):
                                    tmp.append(tmpRecord[x])
                                current_list.append(tmp)
                            else:
                                tmp = []
                                for x in SelIndexList:
                                    tmp.append(tmpRecord[x[0]][x[1]])
                                current_list.append(tmp)
                        outPutField = []
                        for xi in SelIndexList:
                            # 确保连接的内容是相同类型，此处统一转为字节串
                            tableName = tableName_Order[xi[0]]
                            if isinstance(tableName, str):
                                tableName = tableName.encode('utf-8')

                            fieldName = current_field[xi[0]][xi[1]][0]
                            if isinstance(fieldName, str):
                                fieldName = fieldName.encode('utf-8')

                            # 使用字节串连接
                            outPutField.append(tableName.strip() + b'.' + fieldName.strip())
                        return outPutField, current_list, True
                idx -= 1

        outPutField, current_list, isRight = execute_tree()

        if isRight:
            # 显示字段名
            print("查询字段:", [field.decode('utf-8') if isinstance(field, bytes) else field for field in outPutField])

            # 显示查询结果
            print("\n查询结果:")
            if current_list:
                for record in current_list:
                    # 处理不同类型的记录值
                    formatted_record = []
                    for val in record:
                        if isinstance(val, bytes):
                            formatted_record.append(val.decode('utf-8').strip())
                        else:
                            formatted_record.append(val)
                    print(formatted_record)
            else:
                print("没有找到满足条件的记录")
        else:
            print('SQL查询输入错误!')
    else:
        print('没有可执行的查询计划树')


# --------------------------------
# Author: Shuting Guo shutingnjupt@gmail.com
# to construct a logical query plan tree
# output:
#       global_logical_tree
# B22042228
# ---------------------------------
def construct_logical_tree():
    print('Constructing logical tree...')
    if common_db.global_syn_tree:
        print('Syntax tree exists, extracting SFW data...')
        sel_list, from_list, where_list = extract_sfw_data()

        comma = b','
        sel_list = [i for i in sel_list if i != comma]
        from_list = [i for i in from_list if i != comma]

        # 确保 where_list 是一个元组，即使是空的
        if where_list:
            where_list = tuple(where_list)
        else:
            where_list = tuple()  # 空元组表示没有 WHERE 条件
            print('No WHERE clause detected')

        print('Selection list:', sel_list)
        print('From list:', from_list)
        print('Where list:', where_list)

        if not from_list:
            print('Warning: From list is empty')
            return

        # 检查选择列表中是否包含星号（*）
        has_star = False
        for item in sel_list:
            if isinstance(item, bytes) and item.strip() == b'*':
                has_star = True
                break
            elif isinstance(item, str) and item.strip() == '*':
                has_star = True
                break

        # 如果包含星号，则展开为所有字段
        if has_star:
            print('发现星号查询，展开为所有字段...')
            expanded_sel_list = []

            for table_name in from_list:
                try:
                    # 确保表名是字节型
                    if isinstance(table_name, str):
                        table_name_bytes = table_name.encode('utf-8')
                    else:
                        table_name_bytes = table_name

                    # 创建Storage对象获取表的所有字段
                    storage_obj = storage_db.Storage(table_name_bytes)
                    field_list = storage_obj.getFileNameList()

                    # 为每个字段生成完整的字段引用（表名.字段名）
                    for field in field_list:
                        field_name = field[0].strip()
                        if isinstance(field_name, str):
                            field_name = field_name.encode('utf-8')

                        # 创建表名.字段名格式的引用
                        qualified_field = table_name_bytes.strip() + b'.' + field_name
                        expanded_sel_list.append(qualified_field)

                except Exception as e:
                    print(f"获取表 {table_name} 的字段时出错: {str(e)}")

            # 用展开后的字段列表替换原始的选择列表
            if expanded_sel_list:
                print(f"星号已展开为以下字段: {expanded_sel_list}")
                sel_list = expanded_sel_list
            else:
                print("警告: 无法展开星号，没有找到可用字段")
                return

        if not sel_list:
            print('Warning: Selection list is empty')
            return

        from_node = construct_from_node(from_list)
        if from_node:
            print('From node constructed successfully')
        else:
            print('Failed to construct From node')
            return

        where_node = construct_where_node(from_node, where_list)
        if where_node:
            print('Where/From node constructed successfully')
            if len(where_list) == 0:
                print('查询不包含 WHERE 子句，将显示所有记录')
        else:
            print('Failed to construct Where/From node')
            return

        common_db.global_logical_tree = construct_select_node(where_node, sel_list)

        if common_db.global_logical_tree:
            print('Logical tree constructed successfully')
            common_db.show(common_db.global_logical_tree)
        else:
            print('Failed to construct logical tree')
    else:
        print('there is no data in the syntax tree in the construct_logical_tree')


'''
# the following is to test the code
from_list1=['a','b','c','d','e','f','g']
tree_from=construct_from_node(from_list1)
where_list1=[('x.c','=','y.c'),('z','=','w')]
tree_where=construct_where_node(tree_from,where_list1)
sel_list1=['f1','f2']
syn_tree=construct_select_node(tree_where,sel_list1)
print extract_sfw_data()
'''
