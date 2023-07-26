# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class PotsaveSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class PotsaveDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ProxyMiddleware(object):
    def process_request(self, request, spider):
        # from fp.fp import FreeProxy
        # prox = FreeProxy(timeout=0.3,country_id=['US', 'CA','GB']).get()
        proxy_list = [
            "107.150.42.74:17046",
            "173.208.246.186:19001",
            "192.187.111.82:17057",
            "173.208.186.10:17060",
            "63.141.236.210:19014",
        ]
        import random

        prox = random.choice(proxy_list)
        request.meta["proxy"] = "http://" + prox


from scrapy.extensions.httpcache import DummyPolicy


class CachePolicy(DummyPolicy):
    def should_cache_response(self, response, request):
        return response.status == 200


from scrapy.statscollectors import StatsCollector


class MyStatsCollector(StatsCollector):
    def _persist_stats(self, stats, spider):
        import datetime

        # import mysql.connector
        # import pdb;pdb.set_trace()
        crawler_name = spider.name
        runtime = stats["elapsed_time_seconds"]
        runstatus = True
        records = spider.stats["products_count"]
        with open('test_stats.txt','w+') as f:
            f.write(str(spider.stats["products_count"]))
        if spider.stats["products_count"]==0:
            import requests

            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Postmark-Server-Token": "eacd4a20-ac05-455d-a934-7698ccf3d770",
            }

            json_data = {
                "From": "info@potsave.com",
                "To": "alex@redlzardstudioz.com",
                "Subject": "<strong>Alert!</strong> " + crawler_name + " failed to run",
                "HtmlBody": "<strong>Alert!</strong> "
                + crawler_name
                + " failed to run",
                "MessageStream": "outbound",
            }

            response = requests.post(
                "https://api.postmarkapp.com/email", headers=headers, json=json_data
            )
        try:
            get_id = ("Select * from crawlers_info where crawler_name='"+ crawler_name+ "'")
            spider.cursor.execute(get_id)
            value = spider.cursor.fetchone()
            #import pdb;pdb.set_trace()
            total_runs = int(value[11]) + 1 if value else None
            last_run = value[6] if value else None
            next_run = value[6] if value else None
            if len(value) > 1:
                if value[4] == "daily":
                    next_run = last_run + datetime.timedelta(days=1)
                elif value[4] == "weekly":
                    next_run = last_run + datetime.timedelta(days=7)
                elif value[4] == "monthly":
                    next_run = last_run + datetime.timedelta(days=30)
                elif value[4] == "annually":
                    next_run = last_run + datetime.timedelta(days=365)
            add_dsp = "update crawlers_info SET crawler_discreption=%s,location=%s,frequency=%s,last_run=%s,next_run=%s,runtime=%s,runstatus=%s,status=%s,records=%s,total_runs=%s,created_at=%s,updated_at=%s where crawler_name=%s"
            spider.cursor.execute(
                add_dsp,
                (
                    value[2],
                    value[3],
                    value[4],
                    last_run,
                    next_run,
                    runtime,
                    runstatus,
                    value[9],
                    records,
                    total_runs,
                    str(datetime.datetime.now()),
                    str(datetime.datetime.now()),
                    value[1]
                ),
            )
            spider.cnx.commit()
        except:
            pass
        try:
            add_dsp = "insert into crawler_history (crawlerid,frequency,last_run,runtime,runStatus,records,totalRuns,createdAt,updatedAt) values (%s, %s, %s, %s,%s, %s, %s, %s,%s)"
            spider.cursor.execute(
                add_dsp,
                (
                    value[0],
                    value[4],
                    last_run,
                    runtime,
                    'success' if runstatus else 'failed',
                    records,
                    total_runs,
                    str(datetime.datetime.now()),
                    str(datetime.datetime.now())
                ),
            )
            spider.cnx.commit()
            spider.cursor.close()
            spider.cnx.close()
        except:
            pass
