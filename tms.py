"""
Trufl Map Scraper for Google Maps
"""
import time
from multiprocessing import Pool, cpu_count
from collections import defaultdict
from datetime import datetime
import logging
import os
import random
import pandas as pd
import numpy as np
from timezonefinder import TimezoneFinder
import pytz
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    ElementNotInteractableException,
    NoSuchElementException,
    TimeoutException,
)
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from webdriver_manager.chrome import ChromeDriverManager
from geopy.geocoders import Nominatim
from geopy import Point
from faker import Faker
import langid
import translators as ts

logging.basicConfig(
    format="%(asctime)s | %(levelname)s: %(message)s",
    # level=logging.NOTSET,
    filename="tms.log",
)
logger = logging.getLogger("tms")
os.environ["WDM_LOG_LEVEL"] = "0"


"""Class Implementation for TMS with modules for scraping service """


class TMS:
    def __init__(
        self,
        database_table,
        search_term,
        search_scope,
        num_bots=cpu_count(),
        headless=True,
    ):
        """Create the headless information and initialize states data from csv"""
        self.headless = headless
        self.search_term = search_term
        self.database_table = database_table
        search_scopes = ["world", "us"]
        self.search_scope = search_scope
        if self.search_scope not in search_scopes:
            raise ValueError("Search term must be one of 'world', 'us'")
        self.num_bots = num_bots
        self.fake = Faker()

    def get_driver(self, images=True):
        """Get the driver with parameters"""
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument("--headless")
        options.add_argument("--start-maximized")
        # options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        # options.add_argument("--no-default-browser-check")
        # options.add_argument("--disable-extensions")
        # options.add_argument("--disable-default-apps")
        # added extra chrome options
        # options.add_argument("--disable-dev-shm-usage")
        options.add_argument("disable-infobars")

        if not images:
            prefs = {
                "profile.default_content_settings.popups": 0,
                "profile.default_content_setting_values": {"images": 2},
                "profile.managed_default_content_setting_values": {"images": 2},
            }
            options.add_experimental_option("prefs", prefs)

        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

        driver.implicitly_wait(3)
        return driver

    def tear_down(self, driver):
        """Exit the browser and end the session"""
        driver.quit()

    def connect_db(self, database="trufl-data-dev", fast_execute=True):
        """Method for connecting to Azure Hosted Database"""
        cnxn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=trufl-data.database.windows.net;"
            f"Database={database};"
            "UID=trufldataadmin;"
            "PWD=Trufl@123;"
        )
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": cnxn_str})

        if not fast_execute:
            engine = create_engine(connection_url, fast_executemany=False)
        else:
            engine = create_engine(connection_url, fast_executemany=True)
        return engine

    # double check this process in the future, talk with Joseph afterwards
    def extract_point(self, page_source):
        """Extracts latitude and longitude from Google source html code on a location page"""
        idx = page_source.find("https://www.google.com/maps/place/")
        test = page_source[idx:]
        lat = test.split("!3d")
        lat = lat[1].split("!4d")
        long = lat[1].split("!16")[0]
        if len(long) > 20:
            long = lat[1].split("\\u00")[0]
        lat = lat[0]
        try:
            lat = float(lat)
            long = float(long)
        except Exception as exc:
            logging.warning("lat float conversion failed, descend to step 2: %s", exc)
            long = long[:12]
            long = long.replace("!", "")
            long = long.replace(r"\\", "")
        if lat is None or lat == "":
            lat = "need lat"
        if long is None or long == "":
            long = "need long"
        return (lat, long)

    def scroll_results(self, driver):
        """Find all locations in search page, scroll to last listing"""
        try:
            results = driver.find_elements(
                By.XPATH, '//div[contains(@aria-label, "Results for")]/div/div[./a]'
            )
            option = results[-1]

            clickable = EC.element_to_be_clickable(option)
            presence = EC.presence_of_element_located(option)

            if clickable and presence:
                driver.execute_script("arguments[0].scrollIntoView(true);", option)
                time.sleep(2)
            return results
        except Exception as exc:
            logging.warning("Scroll results exception: %s", exc)

    def extract_times(self, driver):
        """Find all locations in search page, scroll to last listing"""
        closed_text = False
        found = False
        if not found:
            try:
                closed_text = (
                    WebDriverWait(driver, 3)
                    .until(
                        EC.presence_of_element_located(
                            (By.XPATH, "//span[contains(text(), 'Temporarily closed')]")
                        )
                    )
                    .text
                )
                if closed_text == "Temporarily closed":
                    closed_text = True
            except Exception as exc:
                logging.info("Extract times no temp closed element %s", exc)

        if not found:
            try:
                driver.find_element(By.XPATH, '//button[@data-item-id = "oh"]').click()
                found = True
            except NoSuchElementException as exc:
                logging.info("Extract Times NoSuchElementException %s", exc)

        if not found:
            try:
                driver.find_element(
                    By.XPATH,
                    '//div[contains(@jsaction, "pane.openhours.119.dropdown")]',
                ).click()
                found = True
            except (NoSuchElementException, ElementNotInteractableException) as exc:
                logging.info("Extract Times Exception %s", exc)

        # time.sleep(0.5)
        if closed_text:
            return "closed"

        if not closed_text:
            divs = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//div[contains(@aria-label, 'Hide open hours for')]")
                )
            )
            open_hours = divs.get_attribute("aria-label")

            open_hours = " ".join(open_hours.replace(".", "").split()[:-6])
            hours_dict = dict(hours.split(", ", 1) for hours in open_hours.split("; "))

            for i in list(hours_dict.keys()):
                hours_dict[i] = [hours_dict[i]]

            hours_df = pd.DataFrame(hours_dict)

            hours_df.columns = [day.split()[0] for day in list(hours_df.columns)]
            dow = [
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ]
            hours_df = hours_df[dow]
            hours = [x + "_hours" for x in dow]
            dow_hours = dict(zip(dow, hours))
            hours_df.rename(columns=dow_hours, inplace=True)
            return hours_df

    def extract_busy_times(self, driver, link):
        """Scrape busy time today from a Google Place"""
        # checkmark
        time.sleep(1)
        busy = driver.find_elements(By.XPATH, '//div[contains(@aria-label, "busy")]')
        times = []
        for x in busy:
            times.append(x.get_attribute("aria-label"))
        point = self.extract_point(driver.page_source)
        tf = TimezoneFinder()
        lat = float(point[0])
        long = float(point[1])
        tz = tf.timezone_at(lng=long, lat=lat)
        timezone = pytz.timezone(tz)
        now = datetime.now(timezone)
        this_week = datetime.today().isocalendar()[1]
        hour = now.strftime("%#I %p")
        hour = hour.replace(" ", "") + "."
        hours = []
        tod = []
        for x in times:
            idx = x.find("at")
            if idx == -1:
                idx2 = x.find("usually")
                x = x.replace("usually", "")
                t1 = x[idx2:]
                t2 = x[:idx2]
                t2 = t2.replace("Currently ", "")
                final = t2 + t1
                final = final.replace(" busy", "")
                final = final.replace(", ", ":")
                final = final.replace(".", "")
                final = final.replace(" ", "")
                hour = hour.replace(".", "")
                hours.append(hour)
                tod.append(final)
            else:
                busy = x[:idx]
                busy = busy.replace("busy", "")
                busy = busy.strip()
                if busy == "%":
                    busy = None
                t = x[idx:]
                t = t.replace("at", "")
                t = t.replace(" ", "")
                t = t.replace(".", "")
                if t == "":
                    t = "closed"
                hours.append(t)
                tod.append(busy)
        indexes = []
        for idx, x in enumerate(hours):
            if x == "closed":
                indexes.append(idx)
            i = x.find("6AM")
            if i != -1:
                indexes.append(idx)
        dow = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ]
        data = defaultdict(list)
        for num, day in enumerate(dow):
            if indexes[num] == indexes[-1]:
                h = hours[indexes[num - 1] : indexes[num]]
                busy_times = tod[indexes[num - 1] : indexes[num]]
                for i, x in enumerate(h):
                    data[day].append({h[i]: busy_times[i]})
            else:
                h = hours[indexes[num] : indexes[num + 1]]
                busy_times = tod[indexes[num] : indexes[num + 1]]
                for i, x in enumerate(h):
                    data[day].append({h[i]: busy_times[i]})
        data.default_factory = None
        new = dict(data)
        df = pd.DataFrame(columns=new.keys(), data=[new.values()])
        df["link"] = link
        col = df.pop("link")
        df.insert(0, col.name, col)
        df["week_num"] = this_week
        col = df.pop("week_num")
        df.insert(0, col.name, col)
        df.rename(columns={"Monday (Labor Day)_hours": "Monday_hours"}, inplace=True)

        return df

    def reverse_geocode(self, lat, long):
        """Async Method to reverse geocode locations"""
        with Nominatim(
            user_agent=self.fake.name(),
            # adapter_factory=AioHTTPAdapter,
        ) as geolocator:
            location = geolocator.reverse(Point(lat, long))
        return location.raw

    def get_attributes(self, driver):
        """Retrieve location attributes"""
        driver.find_element(
            By.XPATH,
            '//button[contains(@jsaction, "pane.attributes.expand")]',
        ).click()
        time.sleep(0.5)

        headers = []
        list_attr = []

        headers.append("Description")
        try:
            desc = driver.find_element(By.XPATH, '//span[@class = "HlvSq"]').text
            list_attr.append(desc)
        except:
            list_attr.append(None)

        eles = driver.find_elements(By.XPATH, '//div[contains(@role, "region")]')
        for x in eles:
            attr = []
            if "Available search" in x.get_attribute("aria-label"):
                continue
            headers.append(x.get_attribute("aria-label"))
            sub_eles = x.find_elements(By.XPATH, ".//span")
            for s in sub_eles:
                attr.append(s.get_attribute("aria-label"))
            list_attr.append([attr])

        attr_dict = dict(zip(headers, list_attr))
        attr_cols = [
            "Description",
            "Accessibility",
            "Activities",
            "Amenities",
            "Atmosphere",
            "Crowd",
            "Dining options",
            "Highlights",
            "Offerings",
            "Offerings: languages spoken",
            "Payments",
            "Planning",
            "Popular for",
            "Service options",
        ]
        attr_df = pd.DataFrame(attr_dict, columns=attr_cols)

        driver.find_element(
            By.XPATH, '//button[contains(@jsaction, "pane.header.back")]'
        ).click()
        # time.sleep(0.3)
        return attr_df

    def loc_basic_info(self, loc_data, tmp, assign, info):
        """Retrieving location basic information"""
        if langid.classify(loc_data["address"][info])[0] != "en":
            tmp[assign] = ts.google(loc_data["address"][info])
        else:
            tmp[assign] = loc_data["address"][info]

    def extract_restaurant_data(self, driver, link):
        """Main method for extracting individual location information"""
        driver.get(link)
        time.sleep(np.random.random(1)[0])
        page_source = driver.page_source

        # find coordinates
        try:
            lat, long = self.extract_point(page_source)
        except Exception as exc:
            driver.refresh()
            logging.warning("Extract Point Exception: %s", exc)
        try:
            lat, long = self.extract_point(page_source)
        except Exception as exc:
            logging.warning("Input String as lat Convert Point Exception: %s", exc)
            lat = "needs lat"
            long = "needs long"
        tmp = {}
        tmp["lat"] = lat
        tmp["long"] = long
        tmp["link"] = link
        tmp["title"] = None
        tmp["rating"] = None
        tmp["num_reviews"] = None
        tmp["booking"] = None
        tmp["category"] = None
        tmp["address"] = None
        tmp["number"] = None
        tmp["website"] = None

        # find title
        try:
            title = driver.find_element(
                By.XPATH, '//h1[@class = "DUwDvf fontHeadlineLarge"]'
            ).text
            tmp["title"] = title
        except:
            tmp["title"] = None

        try:
            loc_data = self.reverse_geocode(lat, long)
        except Exception as excep:
            logging.warn("Reverse Geocode Error: %s", excep)

        # Display Name Parse
        try:
            if langid.classify(loc_data["display_name"])[0] != "en":
                tmp["display_name"] = ts.google(loc_data["display_name"])
            else:
                tmp["display_name"] = loc_data["display_name"]
        except:
            tmp["display_name"] = None

        # City Parse
        try:
            tmp["city"] = None
            dict_keys = list(loc_data["address"].keys())
            if "city" in dict_keys:
                self.loc_basic_info(loc_data, tmp, "city", "city")
            elif "town" in dict_keys:
                self.loc_basic_info(loc_data, tmp, "city", "town")
            elif "village" in dict_keys:
                self.loc_basic_info(loc_data, tmp, "city", "village")
        except:
            tmp["city"] = None

        # Country Parse
        try:
            self.loc_basic_info(loc_data, tmp, "country", "country")
        except:
            tmp["country"] = None

        # State Parse
        try:
            self.loc_basic_info(loc_data, tmp, "state", "state")
        except:
            tmp["state"] = None

        # Postcode Parse
        try:
            self.loc_basic_info(loc_data, tmp, "postcode", "postcode")
        except:
            tmp["postcode"] = None

        # find category
        try:
            category = WebDriverWait(driver, 5).until(
                EC.visibility_of_element_located(
                    (By.XPATH, '//button[contains(@jsaction, "pane.rating.category")]')
                )
            )
            cat_text = category.text
            tmp.update({"category": cat_text})
        except Exception as exc:
            logging.info("Catgeory Extract and Dict Update Exception: %s", exc)

        # find address
        try:
            address = driver.find_element(By.CSS_SELECTOR, "[data-item-id='address']")
            address = address.get_attribute("aria-label")
            address = address.split(" ", 1)[1]
            tmp.update({"address": address})
        except Exception as exc:
            logging.info("Address Extract error: %s", exc)

        # find phone number
        try:
            phone_number = driver.find_element(
                By.CSS_SELECTOR, "[data-tooltip='Copy phone number']"
            )
            phone_number = phone_number.get_attribute("data-item-id")
            phone_number = phone_number.split("+", 1)[1]
            tmp.update({"number": phone_number})
        except:
            pass

        # find website
        try:
            website = driver.find_element(By.CSS_SELECTOR, "[data-item-id='authority']")
            website = website.get_attribute("aria-label")
            website = website.split()[1]
            tmp.update({"website": website})
        except Exception as exc:
            logging.info("Website Extract Error: %s", exc)

        # find booking company
        try:
            reserve = driver.find_element(
                By.XPATH, '//div[contains(@class, "m6QErb tLjsW UhIuC")]'
            )
            if reserve.text == "RESERVE A TABLE":
                try:
                    reserve.click()
                    # time.sleep(0.5)
                    book = []
                    bookings = WebDriverWait(driver, 5).until(
                        EC.presence_of_all_elements_located(
                            (By.XPATH, '//div[@class = "NGLLDf"]')
                        )
                    )
                    for b in bookings:
                        book.append(b.text)
                    tmp.update({"booking": book})
                    driver.find_element(
                        By.XPATH, '//button[contains(@aria-label, "Back")]'
                    ).click()
                    # time.sleep(0.5)
                except Exception as exc:
                    logging.warning(
                        f"Failed to extract booking: URL: {link} Exception{exc}"
                    )
        except:
            pass

        # find rating and number of reviews
        try:
            ratings_and_num_reviews = (
                WebDriverWait(driver, 5)
                .until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            "//div[contains(@jsaction, 'pane.rating.moreReviews')]",
                        )
                    )
                )
                .text
            )
            ratings_and_num_reviews = ratings_and_num_reviews.split("\n")
        except:
            pass
        try:
            rating = ratings_and_num_reviews[0]
            tmp.update({"rating": rating})
        except:
            pass
        try:
            num_reviews = ratings_and_num_reviews[1].split(" ")[0]
            tmp.update({"num_reviews": num_reviews})
        except:
            pass

        d = pd.DataFrame([tmp])

        # find busy times
        try:
            busy_df = self.extract_busy_times(driver, link)
            # busy_df = busy_df.astype(str)
            d = pd.merge(d, busy_df, on="link")
        except Exception as exc:
            logging.warning("Busy time and hour failed: %s", exc)

        # find attributes
        try:
            attr_df = self.get_attributes(driver)
            attr_df = attr_df.astype(str)
            d = pd.concat([d, attr_df], axis=1)
        except Exception as exc:
            logging.warning("Attributes not found: %s", exc)

        # find business hours
        try:
            hours = self.extract_times(driver)
            hours = hours.astype(str)
            if isinstance(hours, pd.DataFrame):
                d = pd.concat([d, hours], axis=1)
                d["open_status"] = "Open"
            elif isinstance(hours, str):
                d["open_status"] = "Temporarily Closed"
        except (NoSuchElementException, TimeoutException) as exc:
            d["open_status"] = None
            print("No business hours: %s", exc)

        if "week_num" not in d.columns:
            d["week_num"] = None
            d["Sunday"] = None
            d["Monday"] = None
            d["Tuesday"] = None
            d["Wednesday"] = None
            d["Thursday"] = None
            d["Friday"] = None
            d["Saturday"] = None
        if "Monday_hours" not in d.columns:
            d["Monday_hours"] = None
            d["Tuesday_hours"] = None
            d["Wednesday_hours"] = None
            d["Thursday_hours"] = None
            d["Friday_hours"] = None
            d["Saturday_hours"] = None
            d["Sunday_hours"] = None

        d["search_term"] = self.search_term
        d = d.astype(str)
        return d

    def check_eol(self, driver):
        """Check End of Results"""
        get_source = driver.page_source
        search_text = "You've reached the end of the list"
        return search_text in get_source

    def scrape_links(self, search):
        """Collect urls from search page"""
        search = search.replace("'", "''")
        driver = self.get_driver(images=True)
        driver.get(search)
        eol = self.check_eol(driver)
        count = 0
        while not eol:
            count += 1
            try:
                search_results_len = len(self.scroll_results(driver))
            except:
                search_results_len = 0
            eol = self.check_eol(driver)
            new_results_len = len(
                driver.find_elements(
                    By.XPATH, '//div[contains(@aria-label, "Results for")]/div/div[./a]'
                )
            )
            if new_results_len == search_results_len:
                time.sleep(2)
                driver.find_elements(
                    By.XPATH,
                    '//div[contains(@aria-label, "Results for")]/div/div[./a]',
                )[np.random.randint(new_results_len)].click()
                time.sleep(1)
            if count >= 300:
                break
        results = driver.find_elements(
            By.XPATH, '//div[contains(@aria-label, "Results for")]/div/div[./a]'
        )
        total_list = []
        for x in results:
            e = x.find_element(By.XPATH, ".//*")
            url = e.get_attribute("href")
            total_list.append(url)
        return total_list

    def get_current_links(self, search: str) -> list:
        list_of_links = pd.read_sql(
            f"SELECT DISTINCT LINK from {self.database_table} where search = '{search}'",
            self.connect_db(),
        )
        return list_of_links["LINK"].values.tolist()

    def get_web_results(self, search: str) -> list:
        new_list = self.scrape_links(search)
        print("New list: {}".format(len(new_list)))
        old_list = self.get_current_links(search)
        print("Old List: {}".format(len(old_list)))
        new_links = list(set(new_list) - set(old_list))
        logging.warning("Number Searches Remaining: %s", len(new_links))
        return new_links

    def add_table_data(self, search: str, link: str) -> None:
        engine = self.connect_db()
        driver = self.get_driver(images=True)
        df = self.extract_restaurant_data(driver, link)
        df["search"] = search
        columns_order = [
            "lat",
            "long",
            "link",
            "rating",
            "num_reviews",
            "booking",
            "category",
            "address",
            "number",
            "website",
            "display_name",
            "city",
            "country",
            "state",
            "postcode",
            "week_num",
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Description",
            "Accessibility",
            "Activities",
            "Amenities",
            "Atmosphere",
            "Crowd",
            "Dining options",
            "Highlights",
            "Offerings",
            "Offerings: languages spoken",
            "Payments",
            "Planning",
            "Popular for",
            "Service options",
            "title",
            "search",
            "Sunday_hours",
            "Monday_hours",
            "Tuesday_hours",
            "Wednesday_hours",
            "Thursday_hours",
            "Friday_hours",
            "Saturday_hours",
            "open_status",
            "search_term",
        ]
        df = df[columns_order]
        df = df.astype(str)
        df["scraped_dt"] = datetime.now()
        try:
            df.to_sql(self.database_table, engine, if_exists="append", index=False)
            logging.warning("Wrote to DB: %s", df)
        except Exception as exc:
            logging.warning("Write to DB failed: %s", exc)

    def update_table_master(self, search: str) -> None:
        new_links = self.get_web_results(search)
        for link in new_links:
            self.add_table_data(search, link)

    # def add_table_master(self, search: str) -> None:
    #     new_links = self.scrape_links(search)
    #     for link in new_links:
    #         self.add_table_data(search, link)

    def us_loop_searches(self):
        """Method for creating google url search strings from cities and states"""
        cities_df = pd.read_csv(
            "https://raw.githubusercontent.com/joseph-davis-trufl/files/main/uscities.csv"
        )
        searches = []
        for city, state in cities_df[["city", "state_name"]].values:
            city = city.replace(" ", "+")
            state = state.replace(" ", "+")
            search = (
                f"https://www.google.com/maps/search/{city}+{state}+{self.search_term}"
            )
            search = search.replace(" ", "+")
            searches.append(search)
        search_list = np.unique(searches)
        random.shuffle(search_list)
        return search_list

    def world_loop_searches(self):
        """Method for creating google url search strings from cities and states"""
        world_df = pd.read_csv(
            "https://raw.githubusercontent.com/joseph-davis-trufl/files/main/worldcities.csv"
        )
        searches = []
        for city, country in world_df[["city", "country"]].values:
            city = city.replace(" ", "+")
            country = country.replace(" ", "+")
            search = f"https://www.google.com/maps/search/{city}+{country}+{self.search_term}"
            search = search.replace(" ", "+")
            searches.append(search)
        search_list = np.unique(searches)
        random.shuffle(search_list)
        return search_list

    def error_handler(self, e):
        """Bot Error Callback"""
        logging.warning("Error handler %s", e.__cause__)


# taskkill /F /IM "chromedriver.exe" /T ; taskkill /F /IM "chrome.exe" /T ; taskkill /F /IM "python.exe" /T
