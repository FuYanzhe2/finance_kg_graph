# finance_kg_graph
#中财网爬虫代码1.0

requirement:

    - 环境：win7 python3
    - 数据库：neo4j,mysql,mongodb

1.爬取主页信息获得所有公司主页网址 spider_main_page

    - python spider_main_page.py

2.获取所有公司相关信息网址 spider_html_tree

    - python spider_html_tree.py

3.根据2中的信息网址爬取网页源代码并存入mongodb,解析源代码存入
mysql生成表单文件

    -python spider_share_holder.py

4.读取mysql表单文件导入neo4j中，并创建节点关系

    -python Mysql2neo4j.py

注意：数据库配置文件gol.cfg

详细使用即代码讲解见我的博客:https://www.jianshu.com/p/d35f63cdd039
