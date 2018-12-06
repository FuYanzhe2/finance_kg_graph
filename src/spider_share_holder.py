# -*- coding: utf-8 -*-
import json
from DataDb_cfg import MongoDb_config,MysqlDb_config
from Parser_finance_info import Parser_finance_info

#最新年报主页
root_html = "http://data.cfi.cn/data_ndkA0A1934A1939A1940A5526.html"

mongo_db = MongoDb_config()
mysql_db = MysqlDb_config()

parser_tool = Parser_finance_info(mysql_db)

def load_json_file(file_path):
    with open(file_path,'r',encoding='utf-8') as f:
        tokens = json.load(f)
    return tokens

def write_json_file(file_path,json_file):
    with open(file_path,'a',encoding='utf-8') as f:
        json.dump(json_file, f)



def save_html_source(html_tree,key_num,mongo_table):
    """
    所需爬取的所有网页源代码，存入mongodb中
    :param html_tree:所要爬取的所有网页的网址
        格式：{stock_id1:[html1,html2......],stock_id2:[html1,html2......],stock_id3:[html1,html2......]}

    :param key_num:针对每个id里的html分开爬取，0->html1,1->html2......
        格式：int

    :param mongo_table:mongo表的名称
        格式：string
    """
    for key in html_tree.keys():
        if key=='_id':continue
        firm_info_html= html_tree[key][key_num]
        tmp_dict = {}
        try:
            page_source = parser_tool.get_page_source(firm_info_html,False)
        except:
            page_source = parser_tool.get_page_source(firm_info_html,False)
        tmp_dict[key] = page_source
        mongo_db.Insert_data(tmp_dict,mongo_table)
    print("{} save completed......".format(mongo_table))

def process_info_tokens(tokens,table_index,parser_function,TableName):
    """
    :param tokens:存储解析后的网页源代码
        格式dict {证券号1:网页源码,证券号2:网页源码,证券号3:网页源码......}
    :param table_index:表的表头信息
        格式list
    :param parser_function:解析网页源代码的回调函数
    :param TableName:表名称
        格式string
    :return:
    """
    print("读取数据.....")
    total_info = []
    for html_text in tokens:
        stock_id = [key for key in html_text.keys()][1]
        print('{}'.format(stock_id))
        try:
            html_source = html_text[stock_id]
            sub_info = parser_function(html_source,stock_id)
            total_info += sub_info
        except:
            print("{} error".format(stock_id))
    print("导入数据到mysql......")
    mysql_db.Insert_listData(TableName=TableName,table_index=table_index,dataset=total_info)

def main():
    dict_tokens = mongo_db.Get_data('html_tree')[0]
    saving_html = True
    #分开访问速度会快一些
    if saving_html:
        save_html_source(dict_tokens,0,'gsda')
        save_html_source(dict_tokens,1,'gdmd')
        save_html_source(dict_tokens,2,'ggyl')
        save_html_source(dict_tokens,3,'zyfb')

    firm_tokens = mongo_db.Get_data('gsda')
    firm_info_index = ['stock_id','firm_name','firm_address','e_mail','introduction','firm_id']
    share_info_TableName = "new_firm_info"
    process_info_tokens(firm_tokens,firm_info_index,parser_tool.parser_firm_info,share_info_TableName)

    gd_tokens = mongo_db.Get_data('gdmd')
    share_info_index = ['stock_id','holders_name','per_shares','table_class','table_class_id','classfiy','fund_id','holder_person_id','firm_name','firm_id']
    share_info_TableName = "new_shareholder_info"
    process_info_tokens(gd_tokens,share_info_index,parser_tool.parser_gd_info,share_info_TableName)

    mysql_db.Insert_listData("unlisted_firm",['firm_name','firm_id'],parser_tool.unlisted_firm)
    mysql_db.cur.execute('create table main_shareholder_info (select distinct * from new_shareholder_info where new_shareholder_info.table_class_id=\'0\')')
    mysql_db.cur.execute('create table circulate_shareholder_info (select distinct * from new_shareholder_info where new_shareholder_info.table_class_id=\'1\')')
    mysql_db.cur.execute('create table fund_holder_info (select Distinct holders_name,fund_id from new_shareholder_info a where a.classfiy=\'基金、产品\')')
    mysql_db.cur.execute('create table person_holder_info (select Distinct holders_name,holder_person_id from new_shareholder_info a where a.classfiy=\'个人\')')
    mysql_db.cur.execute('create table tmp_mainshare_holder (select Distinct stock_id from new_finance_report)')
    mysql_db.cur.execute('alter table tmp_mainshare_holder add holder_type varchar(20) default \'主要股东\'')

    mysql_db.cur.execute('create table tmp_cirshare_holder (select Distinct stock_id from new_finance_report)')
    mysql_db.cur.execute('alter table tmp_cirshare_holder add holder_type varchar(20) default \'流通股东\'')

    gg_tokens = mongo_db.Get_data('ggyl')
    manager_info_index = ['stock_id','person_name','position_id','position_name','salary']
    manager_info_TableName = "new_manager_info"
    process_info_tokens(gg_tokens,manager_info_index,parser_tool.parser_manager_info,manager_info_TableName)

    print("将position字典导入MySQL......")
    mysql_db.Insert_jsonData("position_dict",parser_tool.position_dict)
    print("更新firm字典......")
    write_json_file('./data/firm.json',parser_tool.firm_dict)

    gd_tokens = mongo_db.Get_data('zyfb')
    share_info_index = ['stock_id','business_name','business_id','gross_porfit']
    share_info_TableName = "new_firm_business"
    process_info_tokens(gd_tokens,share_info_index,parser_tool.parser_business_info,share_info_TableName)

if __name__ == '__main__':
    main()