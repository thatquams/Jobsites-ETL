from bs4 import BeautifulSoup
import requests
from requests_html import HTMLSession
from airflow.models import Variable
import pandas as pd

# SET MAX PAGES TO SCRAPE
MAX_PAGES = 5
CURRENT_PAGE = 1



def connectToJobSite(func):
    """
        Decorator that handles HTTP request and HTML parsing for a job site URL.

        Args:
            func (function): A function that accepts a BeautifulSoup object and returns structured data.

        Returns:
            function: The wrapped function with HTML content from the provided URL.
    """
    def inner(URL, currentPage):
        try:
            
            if "jobberman" in URL:
                # URL = f"https://www.jobberman.com/jobs?page={currentPage}"
                URL = f"{Variable.get('jobberman_base_url')}?page={currentPage}"
            elif "myjobmag" in URL:
                # URL = f"https://www.myjobmag.com/jobs/page/{currentPage}"
                # URL = f"{Variable.get('myjobmag_base_url')}/{currentPage}"
                URL = ''.join(f"https://www.myjobmag.com/jobs/page/{currentPage}".split("#")[0])

            
            page = requests.get(URL)
            content = BeautifulSoup(page.content, "html.parser")
            scraper = func(content) 
            print(f"The {func.__name__} is executed successfully")
            
            # result_df = pd.DataFrame(scraper)

            return scraper
            
        except requests.exceptions.Timeout:
            print("Connecting to the URL timed out. Please try again later.")
            
        except requests.exceptions.TooManyRedirects:
            print("Too many redirects. Please check the URL and try again.")
            
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while making the request: {e}")
    return inner
