import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    ElementNotInteractableException,
    NoSuchElementException,
    TimeoutException,
)
from webdriver_manager.chrome import ChromeDriverManager
import logging
import time


class GMS:
    """Google Based Frontend Selenium Process and WebDriver Managagement"""

    def __init__(self, search_term, headless=True):
        self.headless = headless
        self.search_term = search_term

    def get_driver(self, images=False):
        """Get the driver with parameters"""
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument("--headless")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-default-apps")
        # added extra chrome options
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

    def check_owner(self, driver):
        eles = driver.find_elements(By.XPATH, "//span")
        women_owned = "False"
        for x in eles:
            if "women-owned" in x.text:
                women_owned = "Listed on Google"
        return women_owned

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
        d["women_owned"] = self.check_owner(driver)

        # find busy times
        # try:
        #     busy_df = self.extract_busy_times(driver, link)
        #     # busy_df = busy_df.astype(str)
        #     d = pd.merge(d, busy_df, on="link")
        # except Exception as exc:
        #     logging.warning("Busy time and hour failed: %s", exc)

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
        driver = self.get_driver(images=False)
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
