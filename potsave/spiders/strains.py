import scrapy
from inline_requests import inline_requests
from scrapy.utils.gz import gunzip
from xml.etree.cElementTree import fromstring, ElementTree

# from geopy.geocoders import Nominatim
import datetime
import json
import mysql.connector
import traceback


class WeedmapsSpider(scrapy.Spider):
    name = "weedmaps_strains"
    # start_urls = ['http://s3-us-west-2.amazonaws.com/weedmaps-production-sitemap/dispensary.xml.gz']

    def __init__(self, *args, **kwargs):
        self.stats = {"disp_count": 0, "products_count": 0}
        super().__init__(**kwargs)
        with open("config.json", "r") as f:
            self.config = eval(f.read())
        self.cnx = mysql.connector.connect(**self.config)
        self.cursor = self.cnx.cursor(buffered=True)

    def start_requests(self):
        url = "http://s3-us-west-2.amazonaws.com/weedmaps-production-sitemap/strains.xml.gz"

        headers = {
            "authority": "weedmaps.com",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "dnt": "1",
            "sec-ch-ua": '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        }
        yield scrapy.Request(
            url=url,
            headers=headers,
            callback=self.parse,
            dont_filter=True
        ) 

    @inline_requests
    def parse(self, response):
        try:
            val = gunzip(response.body)
            tree = ElementTree(fromstring(val))
            root = tree.getroot()
            for url in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
                loc = url.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc").text
                headers = {
                    "authority": "weedmaps.com",
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                    "dnt": "1",
                    "sec-ch-ua": '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"macOS"',
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "none",
                    "sec-fetch-user": "?1",
                    "upgrade-insecure-requests": "1",
                    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
                }
                yield scrapy.Request(
                    url=loc,
                    headers=headers,
                    meta={"url": loc},
                    callback=self.dsp_data,
                    dont_filter=True,
                )
        except Exception as e:
            add_dsp = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
            log_data = "".join(traceback.TracebackException.from_exception(e).format())
            # self.cursor.execute(
            #     add_dsp,
            #     (
            #         log_data,
            #         "weedmaps_stains",
            #         str(datetime.datetime.now()),
            #         str(datetime.datetime.now()),
            #     ),
            # )
            add_dsp1 = "INSERT INTO Products_info (log_data, source, created_at, updated_at) VALUES (%s, %s, %s, %s)"
            self.cursor.executemany(add_dsp1, scrap2_except)
            self.cnx.commit()

    def dsp_data(self, response):
        #import pdb;pdb.set_trace()
        self.stats["products_count"] = self.stats["products_count"] + 1
        data_info = {}
        try:
            json_data = response.xpath(
                "//script[contains(@type,'application/json')]/text()"
            ).extract()
            strain = json.loads(json_data[0])
            # geolocator = Nominatim(user_agent='google-bot')
            strain_data =  strain['props']["dehydratedState"]["queries"][0]['state']['data']['data']['attributes']

            data_info["name"] = strain_data['name']
            data_info["description"] = strain_data['description']
            effects = strain_data["effects"]
            _effect = ''
            for effect in effects:
                if _effect:
                    _effect = _effect +','+effect['name']
                else:
                    _effect = effect['name']
            data_info['effects'] = _effect
            falvours = strain_data["flavors"]
            _falvour = ''
            for falvour in falvours:
                if _falvour:
                    _falvour = _falvour +','+falvour['name']
                else:
                    _falvour = falvour['name']
            data_info['flavors'] = _falvour
            data_info["created_at"] = str(datetime.datetime.now())
            data_info["updated_at"] = str(datetime.datetime.now())
            try:
                add_dsp = (
                    "INSERT INTO strains ("
                    + ",".join(data_info.keys())
                    + ") VALUES (%s, %s, %s, %s, %s, %s)"
                )
                # import pdb;pdb.set_trace()
                self.cursor.execute(
                    add_dsp,
                    (
                        data_info["name"],
                        data_info["description"],
                        data_info["effects"],
                        data_info["flavors"],
                        data_info["created_at"],
                        data_info["updated_at"],
                    ),
                )
                self.cnx.commit()
            except Exception as e:
                import pdb;pdb.set_trace()
                pass
            self.stats["disp_count"] = self.stats["disp_count"] + 1
            try:
                with open("config_historical.json", "r") as f:
                    config1 = eval(f.read())
                cnx = mysql.connector.connect(**config1)
                cursor = cnx.cursor(buffered=True)
                cursor.execute(
                    add_dsp,
                    (
                        data_info["name"],
                        data_info["description"],
                        data_info["effects"],
                        data_info["flavors"],
                        data_info["created_at"],
                        data_info["updated_at"],
                    ),
                )
                cnx.commit()
                cnx.close()
                cursor.close()
            except:
                pass
        except Exception as e:
            add_dsp = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
            log_data = "".join(traceback.TracebackException.from_exception(e).format())
            # self.cursor.execute(
            #     add_dsp,
            #     (
            #         log_data,
            #         "weedmaps_strains",
            #         str(datetime.datetime.now()),
            #         str(datetime.datetime.now()),
            #     ),
            # )
            add_dsp1 = "INSERT INTO Products_info (log_data, source, created_at, updated_at) VALUES (%s, %s, %s, %s)"
            self.cursor.executemany(add_dsp1, scrap2_except)
            self.cnx.commit()
