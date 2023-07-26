import scrapy
import json
import datetime
import mysql.connector
import traceback
from inline_requests import inline_requests


class LeaflySpider2(scrapy.Spider):
    name = "leaflyus"
    scrap1_data_info_keys = {}
    scrap1 = []
    scrap1_except = []
    scrap2_except = []
    # start_urls = ['https://www.leafly.com/sitemaps/sitemap-index.xml']

    def __init__(self, *args, **kwargs):
        self.stats = {"disp_count": 0, "products_count": 0}
        with open("config.json", "r") as f:
            self.config = eval(f.read())
        self.cnx = mysql.connector.connect(**self.config)
        self.cursor = self.cnx.cursor(buffered=True)

    def start_requests(self):
        for page in range(1, 101):
            url = (
                "https://finder-service.leafly.com/v3/dispensaries?filter[]=dispensary&geo_query_type=point&promote_new_stores=true&lat=37.09024&limit=100&lon=-95.712891&page="
                + str(page)
                + "&radius=1608mi&return_facets=true&sort=default&sort_version=default"
            )

            headers = {
                "authority": "finder-service.leafly.com",
                "accept": "application/json",
                "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                "dnt": "1",
                "if-none-match": 'W/"1dd02-aUMiVauDiUiYrCsAUdWVgt+ruzY"',
                "origin": "https://www.leafly.com",
                "referer": "https://www.leafly.com/",
                "sec-ch-ua": '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "cross-site",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
                "x-app": "web-web",
                "x-country-code": "CA",
                "x-environment": "prod",
            }
            yield scrapy.Request(
                url=url,
                headers=headers,
                meta={"dont_cache": True},
                callback=self.dsp_iterate,
                dont_filter=True,
            )

    def dsp_iterate(self, response):
        stores = json.loads(response.text)["stores"]
        for store in stores:
            url = "https://www.leafly.com/cannabis-store/" + store["slug"]
            headers = {
                "authority": "www.leafly.ca",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                "cache-control": "max-age=0",
                "dnt": "1",
                "referer": "https://www.leafly.com/dispensaries",
                "sec-ch-ua": '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
            }
            yield scrapy.Request(
                url=url,
                headers=headers,
                meta={"store": store},
                callback=self.dsp_data,
                dont_filter=True,
            )

    @inline_requests
    def dsp_data(self, response):
        if response.status == 301:
            response = yield scrapy.Request(response.url)
        store = response.meta["store"]
        json_data = response.xpath(
            "//script[contains(@type,'application/ld+json')]/text()"
        )
        data = json_data[0].extract()
        js = json.loads(data)
        data_info = {}
        #import pdb;pdb.set_trace()
        try:
            data_info["Name"] = store["name"]
            data_info["Type"] = "True" if store["deliveryEnabled"] else "False"
            data_info["Tag"] = ",".join(store["flags"])
            data_info["logoImage"] = store["logoImage"]
            data_info["description"] = js.get("description")
            data_info["Address"] = store["address1"]
            data_info["Address2"] = store["address2"]
            data_info["City"] = store["city"]
            data_info["State"] = store["state"]
            data_info["Country"] = store["country"]
            data_info["Zip"] = store["zip"]
            data_info["Longitude"] = (
                store["locations"][0]["lon"] if store["locations"] else None
            )
            data_info["Latitude"] = (
                store["locations"][0]["lat"] if store["locations"] else None
            )
            data_info['point'] = "POINT ({} {})".format(data_info["Longitude"],data_info["Latitude"])
            data_info["Google_Ratings"] = None
            data_info["Phone"] = store["phone"]
            data_info["Website"] = js["url"] if js.get("url") else None
            data_info["Email"] = js.get("email") if js.get("email") else None
            data_info["Source"] = "leafly"
            data_info["google_map_links"] = None
            data_info["timings"] = str(js["openingHoursSpecification"])
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
                scrapy1.append(temp_turple)
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
            except Exception as e:
                print(e)
                pass
            self.stats["disp_count"] = self.stats["disp_count"] + 1
            url = (
                "https://consumer-api.leafly.com/api/dispensaries/v1/"
                + store["slug"]
                + "/menu_items?take=36"
            )
            try:
                config = {
                    "user": "linroot",
                    "password": "J.2Bmo9dR9dR9BjH",
                    "host": "lin-15502-9116-mysql-primary.servers.linodedb.net",
                    "database": "Potsaver_historical",
                }
                cnx = mysql.connector.connect(**config)
                cursor = cnx.cursor(buffered=True)
                self.cursor.execute(
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
            except Exception as e:
                print(e)
                pass

            headers = {
                "authority": "consumer-api.leafly.com",
                "accept": "application/json",
                "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                "dnt": "1",
                "origin": "https://www.leafly.com",
                "referer": "https://www.leafly.com/",
                "sec-ch-ua": '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
                "x-app": "web-web",
                "x-environment": "prod",
            }
            yield scrapy.Request(
                url=url,
                headers=headers,
                meta={"store": store, "dsp": js},
                callback=self.pages_data,
                dont_filter=True,
            )
        except Exception as e:
            add_dsp = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
            log_data = "".join(traceback.TracebackException.from_exception(e).format())
            # self.cursor.execute(
            #     add_dsp,
            #     (
            #         log_data,
            #         "leafly",
            #         str(datetime.datetime.now()),
            #         str(datetime.datetime.now()),
            #     ),
            # )
            temp_turple1 = (log_data, "leafly", str(datetime.datetime.now()), str(datetime.datetime.now()))
            scrap1_except.append(temp_turple1)
            add_dsp1 = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
            self.cursor.executemany(add_dsp1, scrap1_except)
            self.cnx.commit()

    def pages_data(self, response):
        try:
            # import pdb;pdb.set_trace()
            store = response.meta["store"]
            dsp = response.meta["dsp"]
            total_products = json.loads(response.text)["metadata"]["totalCount"]
            for i in range(0, int(total_products / 60) + 1):
                v = i * 60
                url = (
                    "https://consumer-api.leafly.com/api/dispensaries/v1/"
                    + store["slug"]
                    + "/menu_items?skip="
                    + str(v)
                    + "&take=60"
                )
                headers = {
                    "authority": "consumer-api.leafly.com",
                    "accept": "application/json",
                    "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                    "dnt": "1",
                    "origin": "https://www.leafly.com",
                    "referer": "https://www.leafly.com/",
                    "sec-ch-ua": '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"macOS"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-site",
                    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
                    "x-app": "web-web",
                    "x-environment": "prod",
                }
                yield scrapy.Request(
                    url=url,
                    headers=headers,
                    meta={"store": store, "dsp": dsp},
                    callback=self.product_urls,
                    dont_filter=True,
                )
        except Exception as e:
            add_dsp = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
            log_data = "".join(traceback.TracebackException.from_exception(e).format())
            # self.cursor.execute(
            #     add_dsp,
            #     (
            #         log_data,
            #         "leafly",
            #         str(datetime.datetime.now()),
            #         str(datetime.datetime.now()),
            #     ),
            # )
            temp_turpl1 = (log_data, "leafly", str(datetime.datetime.now()), str(datetime.datetime.now()))
            scrap2_except.append(temp_turpl1)
        add_dsp1 = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
        self.cursor.executemany(add_dsp1, scrap2_except)
        self.cnx.commit()

    def product_urls(self, response):
        store = response.meta["store"]
        dsp = response.meta["dsp"]
        products = json.loads(response.text)["data"]
        # import pdb;pdb.set_trace()
        product_list = []
        for product in products:
            try:
                product_info = {}
                product_info["dsp_id"] = product["dispensaryName"]
                product_info["img_url"] = product["imageUrl"]
                product_info["product_Name"] = product["name"]
                product_info["Brand_Manufacturer"] = str(product["brand"])
                product_info["description"] = (
                    product["name"] + "," + product["productCategory"]
                )
                product_info["Strain"] = product["strainName"]
                product_info["terps"] = (
                    str(product["strain"]["terps"]) if product["strain"] else None
                )
                product_info["THC"] = product["thcContent"]
                product_info["CBD"] = product["cbdContent"]
                product_info["Amount"] = product["displayQuantity"]
                product_info["Weight"] = product["normalizedQuantity"]
                product_info["Unit"] = product["quantity"]
                product_info["Price"] = product["price"]
                product_info["Price_gm"] = product["pricePerUnit"]
                product_info["Category"] = product["productCategory"]
                product_info["Business_Name"] = product["brandName"]
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
                product_list.append(lis)
            except Exception as e:
                add_dsp = "INSERT INTO logs_info (log_data,source,created_at,updated_at) VALUES (%s, %s, %s, %s)"
                log_data = "".join(
                    traceback.TracebackException.from_exception(e).format()
                )
                # self.cursor.execute(
                #     add_dsp,
                #     (
                #         log_data,
                #         "leafly",
                #         str(datetime.datetime.now()),
                #         str(datetime.datetime.now()),
                #     ),
                # )
        temp_turpl1 = (log_data, "leafly", str(datetime.datetime.now()), str(datetime.datetime.now()))
        self.cnx.commit()
        if products:
            # import pdb;pdb.set_trace()
            try:
                get_id = (
                    "Select id from dispensaries where Name='"
                    + store["name"]
                    + "' and Address='"
                    + store["address1"]
                    + "' and Source='leafly'"
                )
                self.cursor.execute(get_id)
                value = self.cursor.fetchone()[0]
                product_list_comp = [[value] + i for i in product_list]
                add_dsp = "INSERT INTO Products_info (dsp_id, img_url, product_Name, Brand_Manufacturer, description, Strain, terps, THC, CBD, Amount, Weight, Unit, Price, Price_gm, Category, Business_Name, created_at, updated_at) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)"
                self.cursor.executemany(add_dsp, product_list_comp)
                self.cnx.commit()
            except:
                pass

        if products:
            # import pdb;pdb.set_trace()
            try:
                with open("config_historical.json", "r") as f:
                    config1 = eval(f.read())
                cnx = mysql.connector.connect(**config1)
                cursor = cnx.cursor(buffered=True)
                get_id = (
                    "Select id from dispensaries where Name='"
                    + store["name"]
                    + "' and Address='"
                    + store["address1"]
                    + "' and Source='leafly'"
                )
                cursor.execute(get_id)
                value = cursor.fetchone()[0]
                product_list_comp = [[value] + i for i in product_list]
                add_dsp = "INSERT INTO Products_info (dsp_id, img_url, product_Name, Brand_Manufacturer, description, Strain, terps, THC, CBD, Amount, Weight, Unit, Price, Price_gm, Category, Business_Name, created_at, updated_at) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)"
                cursor.executemany(add_dsp, product_list_comp)
                cnx.commit()
                cnx.close()
                cursor.close()
            except:
                pass
