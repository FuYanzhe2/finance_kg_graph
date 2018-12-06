from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from pyquery import PyQuery as pq
import time
import json
browers = webdriver.Chrome()
wait = WebDriverWait(browers,10)
main_page = open("../data/firm_main_page.txt",'a')

def save_json_file(content):
    with open('data/finance_report.txt','a',encoding='utf-8') as f:
        f.write(json.dumps(content,ensure_ascii=False))
        f.close()

def get_page_source(html):
    try:
        browers.get(html)
        browers.switch_to.frame('content')
        return browers.page_source
    except TimeoutException:
        get_page_source(html)

def parse_one_page_source(page_source):

    doc = pq(page_source)
    tokens = doc('.table_data tbody tr').items()
    i = 0
    page_list = []
    for item in tokens:
        if i == 0:
            title_list = []
            str1_list = item.text().split('\n')
            title_list.extend(str1_list)
            i = i + 1
        else:
            token_list = []
            url = item.find('a').attr('href')
            main_page.write(url+'\n')
            str2_list = item.text().split('\n')
            token_list.extend(str2_list)
            assert len(title_list) == len(token_list)
            token_dict = dict(zip(title_list, token_list))
            page_list.append(token_dict)
    return page_list

def next_page():
    try:
        submit = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR,
                                        "#content > div:nth-child(6) > a:nth-child(74)")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,'.table_data tbody tr')))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,'#content > div:nth-child(6) > font')))
        submit.click()
        time.sleep(3)
        html = browers.page_source
        return html
    except TimeoutException:
        next_page()

def main():
    html = 'http://data.cfi.cn/data_ndkA0A1934A1939A1940A5526.html'
    page_source = get_page_source(html)
    all_page_source = []
    one_page_source = parse_one_page_source(page_source)
    all_page_source.extend(one_page_source)
    for offset in range(2,74):
        next_page_source = next_page()
        one_page_source = parse_one_page_source(next_page_source)
        print(offset)
        all_page_source.extend(one_page_source)
    main_page.close()
    save_json_file(all_page_source)
if __name__ == '__main__':
    main()

