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
    name = "weedmaps"
    scrap1_data_info_keys = {}

    # start_urls = ['http://s3-us-west-2.amazonaws.com/weedmaps-production-sitemap/dispensary.xml.gz']

    def __init__(self, *args, **kwargs):
        self.stats = {"disp_count": 0, "products_count": 0}
        super().__init__(**kwargs)
        with open("config.json", "r") as f:
            self.config = eval(f.read())
        self.cnx = mysql.connector.connect(**self.config)
        self.cursor = self.cnx.cursor(buffered=True)

    def start_requests(self):
        url = "http://s3-us-west-2.amazonaws.com/weedmaps-production-sitemap/dispensary.xml.gz"

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
            dont_filter=True,
            meta={"dont_cache": True},
        )  # add meta={'dont_cache': True},

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
                    url=loc + "/about",
                    headers=headers,
                    meta={"url": loc},
                    callback=self.dsp_data,
                    dont_filter=True,
                )
            add_dsp = (
            "INSET INT dispensaries ("
            + ",".join(scrap1_data_info_keys)
            + ") VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, ST_GeomFromText(%s), %s, %s,%s, %s, %s, %s, %s,%s, %s)"
            )
            self.cursor.executemany(add_dsp, scrap1)
        except Exception as e:
            add_dsp = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
            log_data = "".join(traceback.TracebackException.from_exception(e).format())
            self.cursor.execute(
                add_dsp,
                (
                    log_data,
                    "weedmaps",
                    str(datetime.datetime.now()),
                    str(datetime.datetime.now()),
                ),
            )
            self.cnx.commit()

    @inline_requests
    def dsp_data(self, response):
        #import pdb;pdb.set_trace()
        data_info = {}
        try:
            disp_data = response.xpath(
                "//script[contains(@type,'application/ld+json')]/text()"
            ).extract()
            disp = json.loads(disp_data[0])
            json_data = response.xpath(
                "//script[contains(@type,'application/json')]/text()"
            ).extract()
            product = json.loads(json_data[0])
            # geolocator = Nominatim(user_agent='google-bot')
            data_info["Name"] = disp["name"]
            try:
                data_info["Type"] = product["props"]["storeInitialState"]["listing"][
                    "listing"
                ]["online_ordering"]["enabled_for_delivery"]
            except:
                data_info["Type"] = False
            try:
                data_info["Tag"] = ','.join(product["props"]["storeInitialState"]['listing']['listing']['retailer_services'])
            except:
                data_info['Tag'] = None
            data_info["logoImage"] = disp["logo"]
            data_info["description"] = (
                disp["description"] if disp.get("description") else " "
            )
            data_info["Address"] = disp["address"]["streetAddress"].replace("'", "")
            data_info["Address2"] = None
            data_info["City"] = disp["address"]["addressLocality"]
            data_info["State"] = disp["address"]["addressRegion"]
            data_info["Country"] = "USA"
            data_info["Zip"] = disp["address"].get("postalCode")
            data_info["Longitude"] = disp["geo"].get("longitude")
            data_info["Latitude"] = disp["geo"].get("latitude")
            data_info['point'] = "POINT ({} {})".format(data_info["Longitude"],data_info["Latitude"])
            data_info["Google_Ratings"] = None
            data_info["Phone"] = disp["telephone"]
            data_info["Website"] = (
                response.xpath("//div[contains(@class,'Website')]/a/@href").extract()[0]
                if response.xpath("//div[contains(@class,'Website')]/a/@href").extract()
                else None
            )
            data_info["Email"] = disp["email"]
            data_info["Source"] = "weedmaps"
            data_info["google_map_links"] = (
                response.xpath(
                    "//div/a[contains(@class,'AddressLink-')]/@href"
                ).extract()[0]
                if response.xpath(
                    "//div/a[contains(@class,'AddressLink-')]/@href"
                ).extract()
                else None
            )
            data_info["timings"] = str(disp["openingHoursSpecification"])
            data_info["created_at"] = str(datetime.datetime.now())
            data_info["updated_at"] = str(datetime.datetime.now())
            try:
                add_dsp = (
                    "INSERT INTO dispensaries ("
                    + ",".join(data_info.keys())
                    + ") VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, ST_GeomFromText(%s), %s, %s,%s, %s, %s, %s, %s,%s, %s)"
                )
                scrap1_data_info_keys = data_info.keys()
                temp_turple = (
                    data_info["Name"],
                    data_info["Type"],
                    data_info["Tag"],
                    data_info["logoImage"],
                    data_info["description"],
                    data_info["Address"],
                    data_info["Address2"],
                    data_info["City"],
                    data_info["State"],
                    data_info["Country"],
                    data_info["Zip"],
                    data_info["Longitude"],
                    data_info["Latitude"],
                    data_info['point'],
                    data_info["Google_Ratings"],
                    data_info["Phone"],
                    data_info["Website"],
                    data_info["Email"],
                    data_info["Source"],
                    data_info["google_map_links"],
                    data_info["timings"],
                    data_info["created_at"],
                    data_info["updated_at"],
                )
                scrap1.append(temp_turple)
                # import pdb;pdb.set_trace()
                # self.cursor.execute(
                #     add_dsp,
                #     (
                #         data_info["Name"],
                #         data_info["Type"],
                #         data_info["Tag"],
                #         data_info["logoImage"],
                #         data_info["description"],
                #         data_info["Address"],
                #         data_info["Address2"],
                #         data_info["City"],
                #         data_info["State"],
                #         data_info["Country"],
                #         data_info["Zip"],
                #         data_info["Longitude"],
                #         data_info["Latitude"],
                #         data_info['point'],
                #         data_info["Google_Ratings"],
                #         data_info["Phone"],
                #         data_info["Website"],
                #         data_info["Email"],
                #         data_info["Source"],
                #         data_info["google_map_links"],
                #         data_info["timings"],
                #         data_info["created_at"],
                #         data_info["updated_at"],
                #     ),
                # )
                # self.cnx.commit()
            except:
                pass
            self.stats["disp_count"] = self.stats["disp_count"] + 1
            try:
                config = {
                    "user": "linroot",
                    "password": "J.2Bmo9dR9dR9BjH",
                    "host": "lin-15502-9116-mysql-primary.servers.linodedb.net",
                    "database": "Potsaver_historical",
                }
                cnx = mysql.connector.connect(**config)
                cursor = cnx.cursor(buffered=True)
                cursor.execute(
                    add_dsp,
                    (
                        data_info["Name"],
                        data_info["Type"],
                        data_info["Tag"],
                        data_info["logoImage"],
                        data_info["description"],
                        data_info["Address"],
                        data_info["Address2"],
                        data_info["City"],
                        data_info["State"],
                        data_info["Country"],
                        data_info["Zip"],
                        data_info["Longitude"],
                        data_info["Latitude"],
                        data_info['point'],
                        data_info["Google_Ratings"],
                        data_info["Phone"],
                        data_info["Website"],
                        data_info["Email"],
                        data_info["Source"],
                        data_info["google_map_links"],
                        data_info["timings"],
                        data_info["created_at"],
                        data_info["updated_at"],
                    ),
                )
                cnx.commit()
                cnx.close()
                cursor.close()
            except:
                pass
            # yield data_info
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
            url = response.meta["url"]
            try:
                pages = response.xpath(
                    "//a[contains(@class,'pagination')]/text()"
                ).extract()[-1]
            except IndexError as e:
                pages = 1
            for i in range(1, int(pages) + 1):
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
                url = response.url.replace("/about", "") + "?page=" + str(i)
                yield scrapy.Request(
                    url=url,
                    headers=headers,
                    meta={"store": data_info},
                    callback=self.product_urls,
                    dont_filter=True,
                )
        except Exception as e:
            add_dsp = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
            log_data = "".join(traceback.TracebackException.from_exception(e).format())
            self.cursor.execute(
                add_dsp,
                (
                    log_data,
                    "weedmaps",
                    str(datetime.datetime.now()),
                    str(datetime.datetime.now()),
                ),
            )
            self.cnx.commit()

    @inline_requests
    def product_urls(self, response):
        # import pdb;pdb.set_trace()
        try:
            products_list = []
            # import pdb;pdb.set_trace()
            json_data = response.xpath(
                "//script[contains(@type,'application/json')]/text()"
            ).extract()
            product = json.loads(json_data[0])
            val = [*product["props"]["storeInitialState"]["listing"]["menus"]][0]
            urls = product["props"]["storeInitialState"]["listing"]["menus"][val][
                "data"
            ]
            if urls:
                # import pdb;pdb.set_trace()
                for url in urls:
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
                    product_url = (
                        "https://api-g.weedmaps.com/discovery/v1/products/"
                        + url["catalogSlug"]
                    )
                    data = yield scrapy.Request(
                        url=product_url, headers=headers, dont_filter=True
                    )

                    try:
                        store = response.meta["store"]
                        # import pdb;pdb.set_trace()
                        product_desc = json.loads(data.text)
                        product_info = {}
                        # dsp_id int [ref: > discrepancies.id]
                        product_info["dsp"] = store["Name"]
                        product_info["img_url"] = url["avatarImage"]["originalUrl"]
                        product_info["product_Name"] = url["name"]
                        product_info["Brand_Manufacturer"] = (
                            str(url["brandEndorsement"])
                            if url.get("brandEndorsement")
                            else None
                        )
                        product_info["description"] = product_desc["data"]["product"][
                            "description"
                        ]
                        product_info["Strain"] = None
                        if url.get("tags"):
                            for tag in url.get("tags"):
                                product_info["Strain"] = (
                                    tag["source"].get("slug")
                                    if tag.get("source")
                                    else None
                                )
                        if not product_info["Strain"]:
                            if product_desc["data"]["product"].get("tags"):
                                for tag in product_desc["data"]["product"].get("tags"):
                                    if "Strains/" in tag.get("group_name_and_name"):
                                        product_info["Strain"] = tag["name"]
                        product_info["terps"] = (
                            str(url["metrics"].get("terps"))
                            if url["metrics"].get("terps")
                            else None
                        )
                        product_info["THC"] = url["metrics"]["aggregates"]["thc"]
                        product_info["CBD"] = url["metrics"]["aggregates"]["cbg"]
                        product_info["Amount"] = (
                            url["price"]["quantity"] if url["price"] else None
                        )
                        product_info["Weight"] = (
                            url["price"]["quantity"] if url["price"] else None
                        )
                        product_info["Unit"] = (
                            url["price"]["unit"] if url["price"] else None
                        )
                        try:
                            product_info["Price"] = (
                                url["prices"]["ounce"][0]["price"]
                                if url["prices"].get("ounce")
                                else url["prices"]["unit"]["price"]
                            )
                            product_info["Price_gm"] = (
                                url["prices"]["ounce"][0]["gramUnitPrice"]
                                if url["prices"].get("ounce")
                                else url["prices"]["unit"]["price"]
                            )
                        except:
                            product_info["Price"] = (
                                url["prices"]["gram"][0]["price"]
                                if url["prices"].get("gram")
                                else None
                            )
                            product_info["Price_gm"] = (
                                url["prices"]["gram"][0]["gramUnitPrice"]
                                if url["prices"].get("gram")
                                else None
                            )
                        product_info["Category"] = (
                            url["edgeCategory"].get("name")
                            if url["edgeCategory"]
                            else None
                        )
                        product_info["Business_Name"] = (
                            url["brandEndorsement"].get("brandName")
                            if url.get("brandEndorsement")
                            else None
                        )
                        product_info["created_at"] = str(datetime.datetime.now())
                        product_info["updated_at"] = str(datetime.datetime.now())
                        lis = [
                            product_info["img_url"],
                            product_info["product_Name"],
                            product_info["Brand_Manufacturer"],
                            product_info["description"],
                            product_info["Strain"],
                            product_info["terps"],
                            product_info["THC"],
                            product_info["CBD"],
                            product_info["Amount"],
                            product_info["Weight"],
                            product_info["Unit"],
                            product_info["Price"],
                            product_info["Price_gm"],
                            product_info["Category"],
                            product_info["Business_Name"],
                            product_info["created_at"],
                            product_info["updated_at"],
                        ]
                        self.stats["products_count"] = self.stats["products_count"] + 1
                        products_list.append(lis)
                    except Exception as e:
                        add_dsp = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
                        log_data = "".join(
                            traceback.TracebackException.from_exception(e).format()
                        )
                        self.cursor.execute(
                            add_dsp,
                            (
                                log_data,
                                "weedmaps",
                                str(datetime.datetime.now()),
                                str(datetime.datetime.now()),
                            ),
                        )
                        self.cnx.commit()
            if products_list:
                try:
                    # import pdb;pdb.set_trace()
                    get_id = (
                        "Select id from dispensaries where Name='"
                        + store["Name"]
                        + "' and Address='"
                        + store["Address"]
                        + "' and Source='weedmaps'"
                    )
                    self.cursor.execute(get_id)
                    value = self.cursor.fetchone()[0]
                    product_list_comp = [[value] + i for i in products_list]
                    add_dsp = "INSERT INTO Products_info (dsp_id, img_url, product_Name, Brand_Manufacturer, description, Strain, terps, THC, CBD, Amount, Weight, Unit, Price, Price_gm, Category, Business_Name, created_at, updated_at) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)"
                    self.cursor.executemany(add_dsp, product_list_comp)
                    self.cnx.commit()
                except:
                    pass
            if products_list:
                # import pdb;pdb.set_trace()
                try:
                    with open("config_historical.json", "r") as f:
                        config1 = eval(f.read())
                    cnx = mysql.connector.connect(**config1)
                    cursor = cnx.cursor(buffered=True)
                    get_id = (
                        "Select id from dispensaries where Name='"
                        + store["Name"]
                        + "' and Address='"
                        + store["Address"]
                        + "' and Source='weedmaps'"
                    )
                    cursor.execute(get_id)
                    value = cursor.fetchone()[0]
                    product_list_comp = [[value] + i for i in products_list]
                    add_dsp = "INSERT INTO Products_info (dsp_id, img_url, product_Name, Brand_Manufacturer, description, Strain, terps, THC, CBD, Amount, Weight, Unit, Price, Price_gm, Category, Business_Name, created_at, updated_at) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)"
                    cursor.executemany(add_dsp, product_list_comp)
                    cnx.commit()
                    cnx.close()
                    cursor.close()
                except:
                    pass
        except Exception as e:
            # import pdb;pdb.set_trace()
            add_dsp = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
            log_data = "".join(traceback.TracebackException.from_exception(e).format())
            self.cursor.execute(
                add_dsp,
                (
                    log_data,
                    "weedmaps",
                    str(datetime.datetime.now()),
                    str(datetime.datetime.now()),
                ),
            )
            self.cnx.commit()
