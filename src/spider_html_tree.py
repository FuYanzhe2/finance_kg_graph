from selenium.common.exceptions import TimeoutException
from pyquery import PyQuery as pq
from selenium import  webdriver
import json
import re

root = 'http://quote.cfi.cn'

def get_page_html(browers,html):
    try:
        browers.get(html)
        return browers.page_source
    except TimeoutException:
        get_page_html(browers,html)

def parse_page_source(page_source):
    doc = pq(page_source)
    firm_detail = doc('.vertical_table a').attr('href')
    firm_detail = root+firm_detail
    gd_info = re.search('(/gdtj.*?html)',str(doc)).group(0)
    gd_info = root+gd_info
    ggyl_info =  re.search('(/ggyl.*?html)',str(doc)).group(0)
    ggyl_info = root + ggyl_info
    zyfb_info =  re.search('(/zyfb.*?html)',str(doc)).group(0)
    zyfb_info = root+zyfb_info
    return [firm_detail,gd_info,ggyl_info,zyfb_info]

def main():
    firm_info_tree = {}
    browers = webdriver.Chrome()
    def process_one_page(browers,html):
        html = html.strip()
        id = re.search('(\d+)',html).group(0)
        print(id)
        page_source = get_page_html(browers,html)
        id_html_list = parse_page_source(page_source)
        firm_info_tree[id] = id_html_list

    with open("../data/firm_main_page.txt",'r') as f:
        for html in f.readlines():
            try:
                process_one_page(browers,html)
            except:
                print("html")
                print(html)
                browers = webdriver.Chrome()
                process_one_page(browers,html)
    with open('../data/html_tree1.json','a',encoding='utf-8') as f:
        json.dump(firm_info_tree, f)

if __name__ == '__main__':
    main()
