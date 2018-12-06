from DataDb_cfg import MysqlDb_config,Neo4j_config
mysql_db = MysqlDb_config()
finance_graph = Neo4j_config(LOAD_DATA=True).neo_graph
finance_sql = MysqlDb_config()
DB_path = "C:/Users/Nest/.Neo4jDesktop/neo4jDatabases/database-266cbc2c-81f1-4f1b-9688-ba3ff3815211/installation-3.3.5/import/"

def Load_config(sql_table,csv_file,node_name):
    print("导入{}表.....".format(sql_table))
    cypher = """
    LOAD CSV WITH HEADERS FROM "file:///{}" AS row
	Create (n:{})
	SET n = row
    """.format(csv_file,node_name)
    sql = "SELECT * from {}".format(sql_table)
    finance_sql.read_mysql_to_csv(DB_path+"{}".format(csv_file),sql)
    finance_graph.run(cypher)

def load_table():
    Load_config("Industry_info","industry.csv","Industry")
    Load_config("new_firm_info","new_firm_info.csv","firm")
    Load_config("unlisted_firm_info","unlisted_firm_info.csv","firm")
    Load_config("new_finance_report","new_finance_report.csv","stock")
    Load_config("main_shareholder_info","main_shareholder_info.csv","main_shareholder")
    Load_config("circulate_shareholder_info","circulate_shareholder_info.csv","circulate_shareholder")
    Load_config("business_info","business_info.csv","business")
    Load_config("new_manager_info","manager.csv","manager")
    Load_config("tpm_manager","tmp_manager.csv","tmp_manager")
    Load_config("market_info","market_info.csv","market")
    Load_config("new_firm_business","firm_business.csv","firm_business")
    Load_config("tmp_mainshare_holder","tmp_mainshare_holder.csv","main_holder")
    Load_config("tmp_cirshare_holder","tmp_cirshare_holder.csv","circulate_holder")
    Load_config("person_holder_info","person_holder_info.csv","person_holder")
    Load_config("fund_holder_info","fund_holder_info.csv","fund_holder")

def main():

    load_table()

    #行业与股票的关系
    print("创建行业与股票的关系")
    cypher_text1 = "match (n:stock),(m:Industry) where n.Industry_id=m.industry_id Merge (n)-[:所属行业]->(m)"
    finance_graph.run(cypher_text1)
    #公司与股票的关系
    print("创建公司与股票的关系")
    cypher_text2 = "match (n:firm),(m:stock) where n.stock_id=m.stock_id Merge (n)-[:股票]->(m)"
    finance_graph.run(cypher_text2)
    #公司与高管关系
    print("创建公司与高管关系")
    cypher_text3 = "match (n:tmp_manager),(m:stock),(p:firm) where n.stock_id=m.stock_id and m.stock_id=p.stock_id Merge (p)-[:manager]-(n)"
    finance_graph.run(cypher_text3)
    sql_text = "select distinct position_id,position_name from position_info"
    _,position_info = mysql_db.read_sql_data(sql_text)
    for tokens in  position_info:
        position_id = tokens[0]
        position_name = tokens[1]
        cypher_text4_template = "match (n:manager),(m:tmp_manager),(p:firm) where n.stock_id=m.stock_id and m.stock_id=p.stock_id and n.position_id='%s' Merge (m)-[r:manager{position_name:'%s',position_id:'%s'}]->(n)"%(str(position_id),position_name,str(position_id))
        finance_graph.run(cypher_text4_template)
    #股票与市场关系
    print("创建股票与市场关系")
    cypher_text3 = "match (n:stock),(m:market) where n.market=m.market_name  Merge (n)-[:sub_of]->(m)"
    finance_graph.run(cypher_text3)

    #公司与业务关系
    #print("创建公司与业务关系")
    #cypher_text4 = "match (n:stock),(m:firm_business),(p:firm) where n.stock_id=m.stock_id and m.stock_id=p.stock_id  Merge (p)-[:主要业务]->(m)"
    #finance_graph.run(cypher_text4)
    #业务与业务关系
    #print("创建公司与业务关系")
    #cypher_text5 = "match (n:Business),(m:firm_business) where n.business_id=m.business_id Merge (m)-[:业务所属]->(n)"
    #finance_graph.run(cypher_text5)
    #公司与业务关系


    print("创建公司与业务关系")
    cypher_text4 = "match (n:stock),(m:firm_business),(p:firm),(q:business) where n.stock_id=m.stock_id and m.stock_id=p.stock_id and m.business_id=q.business_id" \
                   " Merge (p)-[:主要业务]->(q)"
    finance_graph.run(cypher_text4)


    Load_config("new_shareholder_info","main_shareholder_info.csv","main_shareholder")
    #1.创建主要股东
    print("创建主要股东关系")

    cypher_text8 = "match (n:firm),(p:firm),(m:main_shareholder) where m.classfiy='企业' " \
                   "and p.firm_id=m.firm_id and m.stock_id=n.stock_id " \
                   "Merge (p)-[:参股]->(n)"
    finance_graph.run(cypher_text8)
    cypher_text9 = "match (n:firm),(p:person_holder),(m:main_shareholder) where m.classfiy='个人' " \
                   "and p.holder_person_id=m.holder_person_id and m.stock_id=n.stock_id " \
                   "Merge (p)-[:参股]->(n)"
    finance_graph.run(cypher_text9)
    cypher_text10 = "match (n:firm),(p:fund_holder),(m:main_shareholder) where m.classfiy='基金、产品' " \
                    "and p.fund_id=m.fund_id and m.stock_id=n.stock_id " \
                    "Merge (p)-[:参股]->(n)"
    finance_graph.run(cypher_text10)
    """
    print("创建流通股东关系")
    cypher_text8 = "match (n:firm),(p:firm),(m:circulate_shareholder) where m.classfiy='企业' " \
                   "and p.firm_id=m.firm_id and m.stock_id=n.stock_id " \
                   "Merge (p)-[:参股]->(n)"
    finance_graph.run(cypher_text8)
    cypher_text9 = "match (n:firm),(p:person_holder),(m:circulate_shareholder) where m.classfiy='企业' " \
                   "and p.firm_id=m.firm_id and m.stock_id=n.stock_id " \
                   "Merge (p)-[:参股]->(n)"
    finance_graph.run(cypher_text9)
    cypher_text10 = "match (n:firm),(p:fund_holder),(m:circulate_shareholder) where m.classfiy='企业' " \
                   "and p.firm_id=m.firm_id and m.stock_id=n.stock_id " \
                   "Merge (p)-[:参股]->(n)"
    finance_graph.run(cypher_text10)

    #1.创建主要股东
    print("创建主要股东关系")
    cypher_text7 = "match (n:main_holder),(m:stock),(p:firm) where n.stock_id=m.stock_id " \
                   "and m.stock_id=p.stock_id Merge (p)-[:十大主要股东]->(n)"
    finance_graph.run(cypher_text7)
    cypher_text8 = "match (n:main_holder),(p:firm),(m:main_shareholder) where m.classfiy='企业' " \
                   "and p.firm_id=m.firm_id and m.stock_id=n.stock_id" \
                   " Merge (n)-[r:主要股东{classify:'%s'}]->(p) Merge (p)-[:参股]->(n)"%('企业')
    finance_graph.run(cypher_text8)
    cypher_text9 = "match (n:main_holder),(p:person_holder),(m:main_shareholder) where m.classfiy='个人' " \
                   "and p.holder_person_id=m.holder_person_id and m.stock_id=n.stock_id " \
                   "Merge (n)-[r:主要股东{classify:'%s'}]->(p) Merge (p)-[:参股]->(n)"%('个人')
    finance_graph.run(cypher_text9)
    cypher_text10 = "match (n:main_holder),(p:fund_holder),(m:main_shareholder) where m.classfiy='基金、产品' " \
                   "and p.fund_id=m.fund_id and m.stock_id=n.stock_id " \
                   "Merge (n)-[r:主要股东{classify:'%s'}]->(p) Merge (p)-[:参股]->(n)"%('基金、产品')
    finance_graph.run(cypher_text10)

    cypher_text11 = "match (p:fund_holder),(m:main_shareholder),(q:firm) where p.fund_id=m.fund_id and q.firm_id=m.firm_id Merge (p)-[:所属公司]->(q)"
    finance_graph.run(cypher_text11)


    print("创建流通股东关系")
    cypher_text7 = "match (n:circulate_holder),(m:stock),(p:firm) where n.stock_id=m.stock_id " \
                   "and m.stock_id=p.stock_id Merge (p)-[:十大流通股东]->(n)"
    finance_graph.run(cypher_text7)
    cypher_text8 = "match (n:circulate_holder),(p:firm),(m:circulate_shareholder) where m.classfiy='企业' " \
                   "and p.firm_id=m.firm_id and m.stock_id=n.stock_id" \
                   " Merge (n)-[r:流通股东{classify:'%s'}]->(p) Merge (p)-[:参股]->(n)"%('企业')
    finance_graph.run(cypher_text8)
    cypher_text9 = "match (n:circulate_holder),(p:person_holder),(m:circulate_shareholder) where m.classfiy='个人' " \
                   "and p.holder_person_id=m.holder_person_id and m.stock_id=n.stock_id " \
                   "Merge (n)-[r:流通股东{classify:'%s'}]->(p) Merge (p)-[:参股]->(n)"%('个人')
    finance_graph.run(cypher_text9)
    cypher_text10 = "match (n:circulate_holder),(p:fund_holder),(m:circulate_shareholder) where m.classfiy='基金、产品' " \
                   "and p.fund_id=m.fund_id and m.stock_id=n.stock_id " \
                   "Merge (n)-[r:流通股东{classify:'%s'}]->(p) Merge (p)-[:参股]->(n)"%('基金、产品')
    finance_graph.run(cypher_text10)
    """

    cypher_text11 = "match (p:fund_holder),(m:main_shareholder),(q:firm) where p.fund_id=m.fund_id and q.firm_id=m.firm_id " \
                    "Merge (p)-[:所属公司]->(q) Merge (q)-[:基金计划]->(p)"
    finance_graph.run(cypher_text11)
    cypher_text12 = "match (p:fund_holder),(m:circulate_shareholder),(q:firm) where p.fund_id=m.fund_id and q.firm_id=m.firm_id " \
                    "Merge (q)-[:基金计划]->(p)"
    finance_graph.run(cypher_text12)

if __name__ == '__main__':
    main()

