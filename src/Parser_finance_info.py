# -*- coding: utf-8 -*-
from selenium.common.exceptions import TimeoutException
from pyquery import PyQuery as pq
from selenium import webdriver
import json
import re


def load_json_file(file_path):
    with open(file_path,'r',encoding='utf-8') as f:
        tokens = json.load(f)
    return tokens

def write_json_file(file_path,json_file):
    with open(file_path,'w',encoding='utf-8') as f:
        json.dump(json_file, f)

class Parser_finance_info():

    def __init__(self,mysql_db):
        self.mysql_db = mysql_db
        self.firm_dict = {}
        self.position_dict = {}
        self.unlisted_firm = []
        self.business_dict = {}
        self.fund_dict = {}
        self.holder_person = {}
        self.browers = webdriver.Chrome()

    def updata_ditionary(self):
        """
        该函数必须在获取玩firm_info表并设置完自增id后使用
        """
        position_sql = "SELECT DISTINCT position_name,position_id from new_manager_info"
        firm_sql = "SELECT DISTINCT firm_name,firm_id from new_firm_info"
        self.firm_dict = self._get_dictionary(firm_sql)
        self.position_dict = self._get_dictionary(position_sql)

    def _get_dictionary(self,sql):
        sql_dict={}
        result = self.mysql_db.get_check_result(sql)
        for token in result:
            sql_dict[token[0]]=token[1]
        return sql_dict

    def get_page_source(self,html,if_frame):
        try:
            self.browers.get(html)
            if if_frame:
                self.browers.switch_to.frame('content')
            return self.browers.page_source

        except TimeoutException:
            self.get_page_source(html,if_frame)

    def get_firm_id(self,firm_name,add_unlisted ):
        if firm_name in self.firm_dict.keys():
            id = self.firm_dict[firm_name]
        else:
            id = len(self.firm_dict)+1
            self.firm_dict[firm_name] = id
            if add_unlisted :
                self.unlisted_firm.append(([firm_name,id]))
        return str(id)

    def parser_gd_info(self,page_source,key):
        pattern1 = re.compile('table id="tabh".*?</table>',re.I)
        main_table,circulate_table= re.findall(pattern1,page_source)
        def classfy_holders(holders_name):
            pattern = re.compile('^.*?公司$')
            if re.match(pattern,holders_name):
                flag = "企业"
            elif len(holders_name)<=4:
                flag = "个人"
            elif '-' in holders_name:
                flag = "基金、产品"
            else:
                flag = "企业"
            return flag

        def get_sharehold_info(table_info,table_class,table_class_id):
            pattern2 = re.compile('<tr.*?</tr>',re.I)
            shareholders_info= [pq(tokens).text().split('\n') for tokens in re.findall(pattern2,table_info)]
            shareholders = []
            if table_class_id==0 :
                range_list =shareholders_info[2:-1]
            else:
                range_list =shareholders_info[2:]
            for info_token in range_list:
                flag = classfy_holders(info_token[1])
                shareholder = (key,info_token[1],info_token[3],table_class,str(table_class_id),flag)
                if flag=="基金、产品":
                    if info_token[1] not in self.fund_dict:
                        fund_id = len(self.fund_dict)+1
                        self.fund_dict[info_token[1]] = fund_id
                    else:
                        fund_id = self.fund_dict[info_token[1]]
                    holder_person_id = "None"
                    shareholder += (fund_id,holder_person_id)
                    holder_name = info_token[1].split('-')
                    pattern3 = re.compile('^.*?公司$')
                    pattern4 = re.compile('^.*?银行$')
                    firm_name = re.match(pattern3,holder_name[0])
                    bank_name = re.match(pattern4,holder_name[1])
                    if firm_name:
                        firm_name = firm_name.group()
                        shareholder+=(firm_name,self.get_firm_id(firm_name,True))
                    elif bank_name:
                        bank_name = bank_name.group()
                        shareholder+=(bank_name,self.get_firm_id(bank_name,True))
                    else:
                        shareholder+=("None","None")
                elif flag=="企业":
                    fund_id = "None"
                    holder_person_id = "None"
                    shareholder+=(fund_id,holder_person_id,info_token[1],self.get_firm_id(info_token[1],True))

                else:
                    if info_token[1] not in self.holder_person:
                        holder_person_id = len(self.holder_person)+1
                        self.holder_person[info_token[1]] = holder_person_id
                    else:
                        holder_person_id = self.holder_person[info_token[1]]
                    fund_id = "None"
                    shareholder+=(fund_id,holder_person_id,"None","None")
                shareholders.append(shareholder)
            return shareholders
        stock_main_holders = get_sharehold_info(main_table,"主要股东",0)
        stock_circulate_holders = get_sharehold_info(circulate_table,"流通股东",1)
        stock_holders = stock_main_holders+stock_circulate_holders
        return stock_holders

    def parser_manager_info(self,page_source,stock_id):
        doc = pq(page_source)
        firm_position_list = []
        tokens = doc('#tabh').text().split('\n')[5:]
        for index in range(0,len(tokens),4):
            position_name = tokens[index+2]
            position_name = re.sub('、|/|,|；|，|;','and',position_name)
            position_name = re.sub('\\(.*?\\)|（.*?）|\\(.*?）','',position_name)
            position_name = re.sub('－','_',position_name)
            position_name = re.sub(' |。','',position_name)
            if position_name not in self.position_dict:
                position_id = len(self.position_dict)+1
                self.position_dict[position_name]=position_id

            else:
                position_id = self.position_dict[position_name]
            if tokens[index+3]=='--':tokens[index+3]="None"

            firm_position_list.append((str(stock_id),tokens[index+1],str(position_id),
                                       position_name,str(tokens[index+3])))
        return firm_position_list

    def parser_firm_info(self,page_source,stock_id):
        doc = pq(page_source)
        tr_tags = doc('.vertical_table').text().split('\n')
        firm_id = self.get_firm_id(tr_tags[2],False)
        firm_info = [(stock_id,tr_tags[2],tr_tags[34],tr_tags[42],tr_tags[82],firm_id)]
        return firm_info

    def parser_business_info(self,page_source,stock_id):
        pattern1 = re.compile('table id="tabh".*?</table>',re.I)
        main_table= re.findall(pattern1,page_source)[0]
        pattern2 = re.compile('<tr.*?</tr>',re.I)
        table_tokens= [pq(tokens).text().split('\n') for tokens in re.findall(pattern2,main_table)]
        target_tokens = table_tokens[2:-1]
        one_business_tokens = []
        for token in target_tokens:
            if token[1] not in self.business_dict:
                id = len(self.business_dict)+1
                self.business_dict[token[1]] = id
            else:
                id = self.business_dict[token[1]]
            one_business_tokens.append((stock_id,token[1],str(id),str(token[4])))
        return one_business_tokens

