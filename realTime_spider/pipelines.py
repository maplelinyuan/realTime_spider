# -*- coding: utf-8 -*-

# 数据库结构说明:
# 每天建一个数据库，如：realTime_2018_01_01
# 当天数据库每场比赛建一个集合（表）, 如：match_2373123
# 文档结构：
# {
#     '_id': '',
#     'league_name': '',
#     'home_name': '',
#     'away_name': '',
#     'start_time': '',
#     'support_direction': '',
#     'company_list': ['', ''],   # 其中为match_id + '_' + company_id
# }
# 每场比赛每个公司再建一个赔率表，如：match_id_company_id
# 文档结构：
# {
#     '_id': '',
#     'company_name': '',
#     'home_odd': '',
#     'draw_odd': '',
#     'away_odd': '',
#     'update_time': '',
#     'count_index': '',
# }



from pymongo import MongoClient
import datetime
import json
import pdb

class RealtimeSpiderPipeline(object):
    def __init__(self):
        # 链接数据库
        self.client = MongoClient(host='localhost', port=27017)
        # self.client.admin.authenticate(settings['MINGO_USER'], settings['MONGO_PSW'])     #如果有账户密码

    def process_item(self, item, spider):
        if spider.name == 'realTime_spider':
            # 这里写爬虫 realTime_spider 的逻辑
            db_name = 'realTime_matchs'
            self.db = self.client[db_name]  # 获得数据库的句柄
            col_name = 'matchs_' + item['current_search_date']
            # 如果match_name（集合名称） 在 该数据中，则使用update更新，否则insert
            self.coll = self.db[col_name]  # 获得collection的句柄
            match_id = item['match_id']
            if self.coll.find({"match_id": match_id}).count() > 0:
                col_exist = True
            else:
                col_exist = False
            try:
                company_id = item['company_id']  # 比赛id_公司id
                try:
                    league_name = item['league_name']
                    home_name = item['home_name']
                    away_name = item['away_name']
                    start_time = item['start_time']
                    support_direction = ''

                    # 如果col_exist，则update，否则insert
                    if col_exist:
                        updateItem = dict(company_id_list=company_id)
                        self.coll.update({"match_id": match_id}, {'$addToSet': updateItem})
                    else:
                        insertItem = dict(match_id=match_id, league_name=league_name, home_name=home_name, away_name=away_name,
                                          start_time=start_time, support_direction=support_direction,
                                          company_id_list=[company_id])
                        self.coll.insert(insertItem)
                except Exception as err:
                    print(err)
                    pass

                col_2_name = "match_" + match_id
                self.coll_2 = self.db[col_2_name]  # 获得collection 2的句柄
                try:
                    company_id = item['company_id']
                    company_name = item['company_name']
                    original_home_odd = item['original_home_odd']
                    original_draw__odd = item['original_draw__odd']
                    original_away_odd = item['original_away_odd']
                    last_home_odd = item['last_home_odd']
                    last_draw_odd = item['last_draw_odd']
                    last_away_odd = item['last_away_odd']

                    # 不管col_2_exist，都insert
                    insertItem = dict(company_id=company_id, company_name=company_name, home_odd=original_home_odd, draw_odd=original_draw__odd, away_odd=original_away_odd,
                                      last_home_odd=last_home_odd, last_draw_odd=last_draw_odd, last_away_odd=last_away_odd)
                    self.coll_2.insert(insertItem)
                except Exception as err:
                    print(err)
                    pass
            finally:
                self.client.close()

        return item

