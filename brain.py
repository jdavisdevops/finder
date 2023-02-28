import logging
from multiprocessing import Pool, Manager, cpu_count
import numpy as np
import time
import pandas as pd
from tms import TMS
import random


class Brain(TMS):
    def __init__(self):
        super().__init__(
            headless=True,
            search_scope="us",
            database_table="saul_request",
            search_term="restaurants",
        )

    def error_handler(self, e):
        """Bot Error Callback"""
        print("Error handler %s", e.__cause__)

    def us_loop_searches(self, search_term):
        """Method for creating google url search strings from cities and states"""
        cities_df = pd.read_csv(
            "https://raw.githubusercontent.com/joseph-davis-trufl/files/main/uscities.csv"
        )
        searches = []
        for city, state in cities_df[["city", "state_name"]].values:
            city = city.replace(" ", "+")
            state = state.replace(" ", "+")
            search = f"https://www.google.com/maps/search/{city}+{state}+{search_term}"
            search = search.replace(" ", "+")
            searches.append(search)
        search_list = np.unique(searches)
        random.shuffle(search_list)
        return search_list

    def add_tasks(self, search, task_queue):
        links = self.get_web_results(search)
        try:
            for i in links:
                task_queue.put((i, search))
        except Exception as excep:
            logging.warning("Failed to add to queue %s", excep)
        # return task_queue

    def process_tasks(self, func, task_queue, search_list=None):
        if func == self.add_tasks:
            print("add tasks")
            with Pool(5) as p:
                for i in search_list:
                    print(i)
                    p.apply_async(
                        func,
                        args=(
                            i,
                            task_queue,
                        ),
                    )
                p.close()
                p.join()
        elif func == self.add_table_data:
            print("process locations")
            with Pool(5) as p:
                while not task_queue.empty():
                    new_search = task_queue.get()
                    link = new_search[0]
                    search = new_search[1]
                    p.apply_async(
                        func,
                        args=(
                            search,
                            link,
                        ),
                    )
                p.close()
                p.join()

    def main(self, task_queue):
        while True:
            search
            if len(search) == 0:
                break
            if task_queue.empty():
                self.process_tasks(self.add_tasks, task_queue, search_list=search)
            if not task_queue.empty():
                self.process_tasks(self.add_table_data, task_queue)


if __name__ == "__main__":
    t = Brain()
    manager = Manager()
    loc_queue = manager.Queue()
    t.main(loc_queue)
