#-----------------------------
# parser_db.py
# author: Jingyu Han   hjymail@163.com
# modified by:
#-------------------------------
# the module is to construct a syntax tree for a "select from where" SQL clause
# the output is a syntax tree
#----------------------------------------------------
import common_db

# the following two packages need to be installed by yourself
import ply.yacc as yacc 
import ply.lex as lex



from lex_db import tokens



#---------------------------------
# Query  : SFW
#   SWF  : SELECT SelList FROM FromList WHERE Condition
# SelList: TCNAME COMMA SelList
# SelList: TCNAME
#
# FromList:TCNAME COMMA FromList
# FromList:TCNAME
# Condition: TCNAME EQX CONSTANT
#---------------------------------



#------------------------------
# check the syntax tree
# input:
#       syntax tree
# output:
#       true or false
#-----------------------------
def check_syn_tree(syn_tree):
    if syn_tree:
        pass



#------------------------------
#(1) construct the node for query expression
#(2) check the tree
#(3) view the data in the tree
# input:
#       
# output:
#       the root node of syntax tree
#--------------------------------------      
def p_expr_query(t):
    """Query : SFW"""

    if isinstance(t[1], str):
        t[1] = t[1].encode('utf-8')

    t[0]=common_db.Node(b'Query',[t[1]])
    common_db.global_syn_tree=t[0]
    check_syn_tree(common_db.global_syn_tree)
    common_db.show(common_db.global_syn_tree)
    
    return t

#------------------------------
#(1) construct the node for SFW expression
# input:
#       
# output:
#       the nodes
#--------------------------------------   
def p_expr_sfw(t):
    """SFW : SELECT SelList FROM FromList WHERE Cond"""
    t[1]=common_db.Node(b'SELECT',None)
    t[3]=common_db.Node(b'FROM',None)
    t[5]=common_db.Node(b'WHERE',None)

    t[0]=common_db.Node(b'SFW',[t[1],t[2],t[3],t[4],t[5],t[6]])

    return t

#------------------------------
#construct the node for select list
# input:
#       
# output:
#       the nodes
#--------------------------------------   

def p_expr_sel_list_first(t):
    """SelList : TCNAME COMMA SelList"""
    
    if isinstance(t[1], str):
        t[1] = t[1].encode('utf-8')

    t[1]=common_db.Node(b'TCNAME',[t[1]])

    t[2]=common_db.Node(b',',None)
    t[0]=common_db.Node(b'SelList',[t[1],t[2],t[3]])

    return t

#------------------------------
#construct the node for select list expression
# input:
#       
# output:
#       the nodes
#--------------------------------------   
def p_expr_sel_list_second(t):
    """SelList : TCNAME"""
   
    if isinstance(t[1], str):
        t[1] = t[1].encode('utf-8')

    t[1]=common_db.Node(b'TCNAME',[t[1]])
    t[0]=common_db.Node(b'SelList',[t[1]])

    return t


#---------------------------
#construct the node for from expression
# input:
#       
# output:
#       the nodes
#--------------------------------------   
def p_expr_fromlist_first(t):
    """FromList : TCNAME COMMA FromList"""

    if isinstance(t[1], str):
        t[1] = t[1].encode('utf-8')

    t[1]=common_db.Node(b'TCNAME',[t[1]])
    t[2]=common_db.Node(b',',None)
    t[0]=common_db.Node(b'FromList',[t[1],t[2],t[3]])

    return t


#------------------------------
#(1) construct the node for from expression
# input:
#       
# output:
#       the nodes
#--------------------------------------           
def p_expr_fromlist_second(t):
    """FromList : TCNAME"""

    if isinstance(t[1], str):
        t[1] = t[1].encode('utf-8')

    t[1]=common_db.Node(b'TCNAME',[t[1]])
    t[0]=common_db.Node(b'FromList',[t[1]])
    return t
        
#------------------------------
#construct the node for condition expression
# input:
#       
# output:
#       the nodes
#--------------------------------------   
def p_expr_condition(t):
    """Cond : TCNAME EQX CONSTANT"""

    if isinstance(t[1], str):
        t[1] = t[1].encode('utf-8')

    # 处理常量值 - 确保是字节字符串
    if not isinstance(t[3], bytes):
        if isinstance(t[3], (int, float)):
            t[3] = str(t[3]).encode('utf-8')
        elif isinstance(t[3], str):
            t[3] = t[3].encode('utf-8')
        else:
            t[3] = str(t[3]).encode('utf-8')

    t[1]=common_db.Node(b'TCNAME',[t[1]])
    t[2]=common_db.Node(b'=',None)
    t[3]=common_db.Node(b'CONSTANT',[t[3]])

    t[0]=common_db.Node(b'Cond',[t[1],t[2],t[3]])

    return t 


    
#------------------------------
# for error
# input:
#       
# output:
#       the error messages
#--------------------------------------   
def p_error(t):
    print ('wrong at %s'% t.value)


#------------------------------------------
# to set the global_parser handle in common_db.py
#---------------------------------------------    
def set_handle():    
    try:
        # 确保 tokens 和语法规则已经准备好
        print("初始化解析器...")
        common_db.global_parser = yacc.yacc(write_tables=0, debug=True)
        if common_db.global_parser is None:
            print('错误：解析器对象创建失败')
        else:
            print('解析器成功初始化')
    except Exception as e:
        print('解析器初始化错误:', str(e))
        import traceback
        traceback.print_exc()

# the following is to test
'''
# the following is to test
my_str="select f1,f2 from t1,t2 where f1=9"
my_parser=yacc.yacc(write_tables=0)# the tabl does not cache
my_parser.parse(my_str)
'''
