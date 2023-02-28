from multiprocessing import Pool, cpu_count
import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path
from uszipcode import SearchEngine
import logging
from gms import GMS


class Finder(GMS):
    """TMS scraping service rewritten for better abstraction and more granular searches"""

    def __init__(
        self,
        search_term,
        city,
        state,
        num_bots=cpu_count(),
        headless=True,
    ):
        """Create the headless information and initialize states data from csv"""
        super().__init__(headless=headless)

        self.search_term = search_term.replace(" ", "_")
        self.city = city
        self.state = state
        self.num_bots = num_bots
        ### Create SearchEngine Connection and Class Instance
        self.db_file_path = Path.cwd() / "db" / "simple_db.sqlite"
        self.search_file_path = Path.cwd() / "db" / f"{self.search_term}.sqlite"
        if not self.db_file_path.exists():
            print("Downloading USZIPCODE DB")
            _ = self.create_search_engine()
        # self.engine = sqlite3.connect(self.search_file_path)
        self.zdf = pd.read_sql_query(
            "select  * from simple_zipcode",
            sqlite3.connect(self.db_file_path),
        )
        self.zip_list = self.create_zip_list()

    def create_zip_list(self):
        """Create list of zip codes"""
        df = self.zdf[
            (self.zdf.post_office_city.str.contains(self.city, na=False))
            & (self.zdf.state.str.contains(self.state, na=False))
        ]
        zlist = df.zipcode.values.tolist()
        searches = []
        for z in zlist:
            search = f"https://www.google.com/maps/search/{z}+{self.search_term.replace('_', '+')}"
            searches.append(search)
        search_list = np.unique(searches)
        return search_list

    def create_search_engine(self):
        """Create Search Engine Attribute"""
        return SearchEngine(
            db_file_path=self.db_file_path,
            # simple_or_comprehensive=SearchEngine.SimpleOrComprehensiveArgEnum.comprehensive,
        )

    def error_handler(self, e):
        """Bot Error Callback"""
        logging.warning("Error Handler %s", e)

    def add_tasks(self, search):
        links = self.scrape_links(search)
        df = pd.DataFrame(columns=["link"], data=links)
        df.to_sql(
            "links", if_exists="append", con=sqlite3.connect(self.search_file_path)
        )
        print(f"Added {len(df)} tasks")

    def process_tasks(self):
        search_list = self.create_zip_list()
        with Pool(processes=cpu_count()) as pool:
            for i, _ in enumerate(search_list):
                pool.apply_async(
                    self.add_tasks,
                    args={search_list[i]},
                    error_callback=self.error_handler,
                )
            pool.close()
            pool.join()

    def add_locations(self, link: str) -> None:
        driver = self.get_driver(images=True)
        df = self.extract_restaurant_data(driver, link)
        try:
            df.to_sql(
                self.search_term,
                sqlite3.connect(self.search_file_path),
                if_exists="append",
                index=False,
            )
            logging.warning("Wrote to DB: %s", df)
        except Exception as exc:
            logging.warning("Write to DB failed: %s", exc)

    def process_locations(self):
        loc_list = pd.read_sql_query(
            "SELECT DISTINCT LINK FROM links",
            sqlite3.connect(f.search_file_path),
        )["link"].values.tolist()
        with Pool(cpu_count()) as p:
            for i, _ in enumerate(loc_list):
                p.apply_async(
                    self.add_locations,
                    args={loc_list[i]},
                    error_callback=self.error_handler,
                )
            p.close()
            p.join()


if __name__ == "__main__":
    f = Finder(
        search_term="women owned business", city="Pasadena", state="CA", headless=False
    )
    f.process_tasks()
    f.process_locations()
