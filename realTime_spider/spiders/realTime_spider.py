# -*- coding: utf-8 -*-
# import os
import scrapy
import pdb
import datetime, time
# import re
# import json
# from lxml import etree
# import requests
from realTime_spider.items import RealtimeSpiderItem
from scrapy_splash import SplashRequest
from realTime_spider.spiders.tools import MyTools
from scrapy_redis.spiders import RedisSpider
from pymongo import MongoClient
import regex

# 设定参数
need_company_id = '156'     # pinnacle
first_limit_mktime = 198000     # 初盘限制时间
first_limit_min_mktime = 165600     # 初盘最小时间
limit_mktime = 15600    # 以赛前这么多时间为终盘赔率

# class OddSpider(scrapy.Spider):
class OddSpider(RedisSpider):
    name = 'realTime_spider'
    allowed_domains = ['http://bf.7m.com.cn/']
    download_delay = 2
    # 包装url
    start_urls = []
    # url = 'http://bf.7m.com.cn/'
    # start_urls.append(url)

    redis_key = 'OddSpider:start_urls'

    global splashurl
    splashurl = "http://192.168.99.100:8050/render.html";
    # 此处是重父类方法，并使把url传给splash解析
    def make_requests_from_url(self, url):
        global splashurl;
        url = splashurl + "?url=" + url;
        # 使用代理访问
        proxy = MyTools.get_proxy()
        LUA_SCRIPT = """
                    function main(splash)
                        splash:on_request(function(request)
                            request:set_proxy{
                                host = "%(host)s",
                                port = %(port)s,
                                username = '', password = '', type = "HTTPS",
                            }
                        end)
                        assert(splash:go(args.url))
                        assert(splash:wait(0.5))
                        return {
                            html = splash:html(),
                        }
                    end
                    """
        proxy_host = proxy.strip().split(':')[0]
        proxy_port = int(proxy.strip().split(':')[-1])
        LUA_SCRIPT = LUA_SCRIPT % {'host': proxy_host, 'port': proxy_port}
        try:
            print('line72,当前代理为：', "http://{}".format(proxy))
            return SplashRequest(url, self.parse,
                                args={'wait': 0.5, 'images': 0, 'timeout': 30, 'lua_source': LUA_SCRIPT},
                                dont_filter=True)
        except Exception as err:
            MyTools.delete_proxy(proxy)

    def start_requests(self):
        for url in self.start_urls:
            # 使用代理访问
            proxy = MyTools.get_proxy()

            LUA_SCRIPT = """
                        function main(splash)
                            splash:on_request(function(request)
                                request:set_proxy{
                                    host = "%(host)s",
                                    port = %(port)s,
                                    username = '', password = '', type = "HTTPS",
                                }
                            end)
                            assert(splash:go(args.url))
                            assert(splash:wait(0.5))
                            return {
                                html = splash:html(),
                            }
                        end
                        """
            proxy_host = proxy.strip().split(':')[0]
            proxy_port = int(proxy.strip().split(':')[-1])
            LUA_SCRIPT = LUA_SCRIPT % {'host': proxy_host, 'port': proxy_port}
            try:
                print('初始代理为：', "http://{}".format(proxy))
                yield SplashRequest(url, self.parse, args={'wait': 0.5, 'images': 0, 'timeout': 30, 'lua_source': LUA_SCRIPT}, dont_filter=True)
            except Exception as err:
                MyTools.delete_proxy(proxy)

    '''
        redis中存储的为set类型的公司名称，使用SplashRequest去请求网页。
        注意：不能在make_request_from_data方法中直接使用SplashRequest（其他第三方的也不支持）,会导致方法无法执行，也不抛出异常
        但是同时重写make_request_from_data和make_requests_from_url方法则可以执行
    '''

    # 分析当天所有比赛信息
    def parse(self, response):
        current_hour = time.localtime()[3]  # 获取当前的小时数，如果小于12则应该选择yesterday
        current_minute = time.localtime()[4]  # 获取当前的小时数，如果小于12则应该选择yesterday
        nowadays = datetime.datetime.now().strftime("%Y-%m-%d")  # 获取当前日期 格式2018-01-01
        yesterdy = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")  # 获取昨天日期
        if current_hour < 12:
            self.current_search_date = yesterdy  # str
        else:
            self.current_search_date = nowadays  # str

        # 链接数据库
        client = MongoClient(host='localhost', port=27017)
        # db_name = 'realTime_' + self.current_search_date
        # db = client[db_name]  # 获得数据库的句柄
        # coll_match_list = db.collection_names()
        self.completed_match_id_list = []  # 记录已经抓取了赔率信息的公司ID列表
        # for coll_match_name in coll_match_list:
        #     # 找到是整场比赛的集合
        #     if len(regex.findall(r'match_', coll_match_name)) == 0:
        #         continue
        #     coll = db[coll_match_name]  # 获得collection的句柄
        #     match_id = coll_match_name.split('_')[-1]
        #     self.completed_match_id_list.append(match_id)
        # 切换到记录已经爬取的match_id文档，将其中ID添加到已爬取id列表
        db = client['realTime_info']
        info_coll = db['realTime_crawled']
        if info_coll.find({"crawled": 1}).count() > 0:
            if 12 < current_hour < 13 and 0 < current_minute < 30:
                # 每天12点-12：30内清空crawled matchId list
                info_coll.update({"crawled": 1}, {"$unset": {"matchId_list": 1}})
            for item in [data['matchId_list'] for data in info_coll.find({"crawled": 1}, {"matchId_list": {"$exist": True}})][0]:
                self.completed_match_id_list.append(str(item).strip())
        if info_coll.find({"no_need_company": 1}).count() > 0:
            if 12 < current_hour < 13 and 0 < current_minute < 30:
                # 每天12点-12：30内清空crawled matchId list
                info_coll.update({"no_need_company": 1}, {"$unset": {"matchId_list": 1}})
            for item in [data['matchId_list'] for data in info_coll.find({"no_need_company": 1}, {"matchId_list": {"$exist": True}})][0]:
                self.completed_match_id_list.append(str(item).strip())
        self.completed_match_id_list = list(set(self.completed_match_id_list))
        # 切换到记录当前正在爬取的match_id 文档， 清空之前的信息， 解锁分析这些比赛
        info_coll = db['realTime_crawling']
        info_coll.update({"crawling": 1}, {"$unset": {"matchId_list": 1}})
        client.close()

        print('开始分析当天所有比赛信息')
        need_step = False   # 标志是否要跳过
        current_start_date = ''
        for tr in response.xpath('//table[@id="live_Table"]/tbody/tr'):
            # 如果ID不存在要跳过
            try:
                tr_id = tr.xpath('@id').extract()[0]
            except Exception as e:
                # 说明不存在class，这时也要跳过
                need_step = False
                continue
            # 找到ID前四个字符为date的tr,拿到日期
            if tr_id[:4] == 'date' and len(tr.xpath('@style').extract()) == 0:
                current_start_date = tr.xpath('td')[0].xpath('text()').extract()[0][0:11].replace('年', '-').replace('月', '-').replace('日', '')
            # 如果前两个字符不是bh说明不是比赛line，要跳过
            if tr_id[:2] != 'bh' or need_step:
                need_step = False
                continue
            if len(tr.xpath('td')[8].xpath('a')) < 3:
                # 有的比赛不存在欧赔，需要跳过
                continue
            if len(tr.xpath('td')[3].xpath('span')[-1].xpath('text()')) != 0:
            # 不等于空说明已经开赛，就跳过
                continue

            start_time_text = current_start_date + ' ' + tr.xpath('td')[2].xpath('text()').extract()[0]
            start_mktime = time.mktime(time.strptime(start_time_text, "%Y-%m-%d %H:%M")) + 3600*8   # 东八区 + 增加时间
            time_local = time.localtime(start_mktime)
            start_time_text = time.strftime("%Y-%m-%d %H:%M", time_local)
            # 如果当前时间比开始时间相对现在大于分析时间,则结束遍历，不再往下查找
            now_mktime = time.time()
            if (start_mktime - now_mktime) > limit_mktime:
                print('时间未到，暂不爬取')
                continue
            # 判断结束

            # 当前页面一场比赛一行
            match_id = tr.xpath('td')[0].xpath('input/@value').extract()[0]  # 该场比赛ID
            if match_id in self.completed_match_id_list:
                print('已经抓取了赔率，跳过 %s' % match_id)
                continue
            all_odds_href = 'http://1x2.7m.hk/list_big.shtml?id=' + match_id  # 跳转到单场比赛全赔率页面的链接
            single_match_dict = {
                'start_time_text': start_time_text
            }

            # 使用代理访问
            proxy = MyTools.get_proxy()
            LUA_SCRIPT = """
                        function main(splash)
                            splash:on_request(function(request)
                                request:set_proxy{
                                    host = "%(host)s",
                                    port = %(port)s,
                                    username = '', password = '', type = "HTTPS",
                                }
                            end)
                            assert(splash:go(args.url))
                            assert(splash:wait(0.5))
                            return {
                                html = splash:html(),
                            }
                        end
                        """
            proxy_host = proxy.strip().split(':')[0]
            proxy_port = int(proxy.strip().split(':')[-1])
            LUA_SCRIPT = LUA_SCRIPT % {'host': proxy_host, 'port': proxy_port}
            try:
                print('单场比赛代理为：', "http://{}".format(proxy))
                yield SplashRequest(all_odds_href, self.all_odds_parse, meta=single_match_dict,
                                    args={'wait': 0.5, 'images': 0, 'timeout': 30, 'lua_source': LUA_SCRIPT},
                                    dont_filter=True)
            except Exception as err:
                MyTools.delete_proxy(proxy)

    # 分析单场比赛所有赔率信息
    def all_odds_parse(self, response):
        print('开始分析单场比赛所有赔率信息')
        match_id = response.url.split('=')[-1]
        # 如果当前比赛没有需要包含的公司ID，就不爬取
        current_company_id_list = []
        original_current_company_id_list = [item.xpath('td')[0].xpath('input/@value').extract() for item in
                                            response.xpath('//div[@id="odds_tb"]/table/tbody/tr')]
        for item in original_current_company_id_list:
            if item != []:
                current_company_id_list.append(item[0])
        if not need_company_id in current_company_id_list:
            print('没有必须的赔率公司，跳过')
            # 链接数据库
            client = MongoClient(host='localhost', port=27017)
            db = client['realTime_info']
            info_coll = db['realTime_crawled']
            info_coll.update({"no_need_company": 1}, {"$addToSet": {"matchId_list": match_id}}, True)
            client.close()
            return False
        # 判断结束

        # 将当前match_id添加进正在爬取列表中
        client = MongoClient(host='localhost', port=27017)
        db = client['realTime_info']
        # 保存到正在爬取的id list
        info_coll = db['realTime_crawling']
        info_coll.update({"crawling": 1}, {"$addToSet": {"matchId_list": match_id}}, True)
        # 保存到已经爬取的id list
        info_coll = db['realTime_crawled']
        info_coll.update({"crawled": 1}, {"$addToSet": {"matchId_list": match_id}}, True)
        client.close()
        # 断开数据库

        start_time_text = response.meta['start_time_text']
        try:
            if len(response.xpath('//div[@id="mn"]/span')) != 0:
                league_name = response.xpath('//div[@id="mn"]/span/text()').extract()[0].strip()
            else:
                league_name = response.xpath('//div[@id="mn"]/a/text()').extract()[0].strip()
        except:
            print('获取联赛名称出错！')
        home_name = response.xpath('//div[@class="team_name1"]/a/text()').extract()[0].strip()
        away_name = response.xpath('//div[@class="team_name2"]/a/text()').extract()[0].strip()
        need_step = False  # 标志是否要跳过
        for tr in response.xpath('//div[@id="odds_tb"]/table/tbody/tr'):
            try:
                tr_class = tr.xpath('@class').extract()[0]  # 如果前三个字符不是ltd说明不是比赛line，要跳过
            except Exception as e:
                # 说明不存在class，这时也要跳过
                need_step = False
                continue
            if tr_class[:3] != 'ltd' or need_step:
                need_step = False
                continue

            td_len = len(tr.xpath('td'))    # 如果是该公司的初赔行，td为14个，当前赔率行为7个
            # 如果下面成立说明是最新赔率行
            if td_len == 9:
                single_match_tr_index = 1
            elif td_len == 7:
                single_match_tr_index = 2
            else:
                print('td_len出错')
                single_match_tr_index = 0
                # pdb.set_trace()
            # 如果是初赔行，则拿取公司名称和最后更新时间
            if single_match_tr_index == 1:
                company_id = tr.xpath('td')[0].xpath('input/@value').extract()[0]  # 公司ID
                company_name = tr.xpath('td')[1].xpath('a/text()').extract()[0]  # 公司名称
                all_odds_href = 'http://1x2.7m.hk/log_en.shtml?id=' + match_id + '&cid=' + company_id  # 跳转到单场比赛全赔率页面的链接
                single_meta = {}
                single_meta['match_id'] = match_id
                single_meta['company_id'] = company_id
                single_meta['league_name'] = league_name
                single_meta['home_name'] = home_name
                single_meta['away_name'] = away_name
                single_meta['start_time_text'] = start_time_text
                single_meta['company_name'] = company_name

                # 使用代理访问
                proxy = MyTools.get_proxy()
                LUA_SCRIPT = """
                            function main(splash)
                                splash:on_request(function(request)
                                    request:set_proxy{
                                        host = "%(host)s",
                                        port = %(port)s,
                                        username = '', password = '', type = "HTTPS",
                                    }
                                end)
                                assert(splash:go(args.url))
                                assert(splash:wait(0.5))
                                return {
                                    html = splash:html(),
                                }
                            end
                            """
                proxy_host = proxy.strip().split(':')[0]
                proxy_port = int(proxy.strip().split(':')[-1])
                LUA_SCRIPT = LUA_SCRIPT % {'host': proxy_host, 'port': proxy_port}
                try:
                    print('单家公司代理为：', "http://{}".format(proxy))
                    yield SplashRequest(all_odds_href, self.single_company_odds_parse, meta=single_meta,
                                        args={'wait': 0.5, 'images': 0, 'timeout': 30, 'lua_source': LUA_SCRIPT},
                                        dont_filter=True)
                except Exception as err:
                    MyTools.delete_proxy(proxy)

    # 获取单家公司的所有赔率
    def single_company_odds_parse(self, response):
        league_name = response.meta['league_name']
        match_id = response.meta['match_id']
        company_id = response.meta['company_id']
        company_name = response.meta['company_name']
        home_name = response.meta['home_name']
        away_name = response.meta['away_name']
        start_time_text = response.meta['start_time_text']
        print('开始解析 %s 赔率：' % company_name)

        now_mktime = time.time()
        need_step = False  # 标志是否要跳过
        last_home_odd = ''
        last_draw_odd = ''
        last_away_odd = ''
        original_home_odd = ''
        original_draw__odd = ''
        original_away_odd = ''
        prev_home_odd = ''
        prev_draw_odd = ''
        prev_away_odd = ''
        prev_update_mktime = ''
        for tr in response.xpath('//div[@id="log_tb"]/table/tbody/tr'):
            # 如果tr有class存在，说明是头部需要跳过
            if len(tr.xpath('@class').extract()) > 0 or need_step:
                need_step = False
                continue
            home_odd = float(tr.xpath('td')[0].xpath('text()').extract()[0])
            draw_odd = float(tr.xpath('td')[1].xpath('text()').extract()[0])
            away_odd = float(tr.xpath('td')[2].xpath('text()').extract()[0])
            try:
                update_time = datetime.datetime.strptime(tr.xpath('td')[3].xpath('text()').extract()[0].replace('(Early)',''), '%d-%m-%Y %H:%M')
            except:
                print('获取单行赔率更新时间出错')
            update_mktime = time.mktime(update_time.timetuple())
            start_mktime = time.mktime(time.strptime(start_time_text, "%Y-%m-%d %H:%M"))
            if (start_mktime - update_mktime) > limit_mktime and last_home_odd == '':
                # 如果大于最近几小时的限定时间，且是第一个大于限定时间，就开始计算概率比较平均概率
                last_home_odd = home_odd
                last_draw_odd = draw_odd
                last_away_odd = away_odd
                continue
            if (start_mktime - update_mktime) > first_limit_mktime:
                if prev_update_mktime == '':
                    print('更新时间不满足条件，跳过！')
                    return False
                elif (start_mktime - prev_update_mktime) > first_limit_min_mktime:
                    original_home_odd = prev_home_odd
                    original_draw__odd = prev_draw_odd
                    original_away_odd = prev_away_odd
                    # 如果当前更新时间距离开场小于限定初盘时间，并且时间差满足最低条件，则记录初赔
                    break
                else:
                    print('更新时间不满足条件2，跳过！')
                    return False
            prev_home_odd = home_odd
            prev_draw_odd = draw_odd
            prev_away_odd = away_odd
            prev_update_mktime = update_mktime
        if last_home_odd == '' or original_home_odd == '':
            return False
        odd_Item = RealtimeSpiderItem()
        odd_Item['league_name'] = league_name   # str
        odd_Item['match_id'] = match_id     # str
        odd_Item['home_name'] = home_name   # str
        odd_Item['away_name'] = away_name   # str
        odd_Item['start_time'] = start_time_text  # str
        odd_Item['company_id'] = company_id     # str
        odd_Item['company_name'] = company_name     # str
        odd_Item['original_home_odd'] = original_home_odd     # float型
        odd_Item['original_draw__odd'] = original_draw__odd     # float型
        odd_Item['original_away_odd'] = original_away_odd     # float型
        odd_Item['last_home_odd'] = last_home_odd     # float型
        odd_Item['last_draw_odd'] = last_draw_odd  # float型
        odd_Item['last_away_odd'] = last_away_odd  # float型
        # odd_Item['update_time'] = time.strftime("%Y-%m-%d %H:%M", time.localtime(update_mktime))    # str
        odd_Item['current_search_date'] = self.current_search_date  # str
        print('满足条件的赔率产生！')
        yield odd_Item










