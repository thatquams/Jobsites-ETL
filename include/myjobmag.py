from .extension import connectToJobSite, MAX_PAGES
import pandas as pd 
import requests
import json
from bs4 import BeautifulSoup
pd.set_option('display.max_columns', 10)

@connectToJobSite
def myJobMag(content, currentPage=1):
    
    """
        Scrapes job listings from MyJobMag.com for up to 5 pages.

        This function iterates through the first 5 pages of job listings on MyJobMag, extracting
        job titles, companies, locations, posted dates, and job links. It uses BeautifulSoup to
        parse both the listing page and the individual job detail pages.

        Args:
            content (BeautifulSoup): Parsed HTML content of the first job listings page.
            currentPage (int, optional): Starting page number for scraping. Defaults to 1.

        Returns:
            pd.DataFrame: A DataFrame containing job details including title, company, 
                        posted date, location, job link, and source.
    """
    
    jobListings = content.find("ul", class_="job-list")
    jobs = []
    
    while currentPage <= 1:
        
        try:
            
            source = "https://www.myjobmag.com"
            # print(source)
            
            if jobListings:
                listings = jobListings.find_all("li", class_="job-list-li")
                for lst in listings:
                    lst_links = lst.find_all("li", "mag-b")
                    for link in lst_links:
                        href = link.find("a", href=True)["href"]
                        
                        response = requests.get(f"{source}{href}")
                        job_soup = BeautifulSoup(response.content, "html.parser")
                
                        titleElement = job_soup.find("ul", class_="read-h1").find("li").find("h1").text 
                        title = titleElement.split(" at")[0].strip()
                        company = titleElement.split("at ", 1)[-1].strip()
                        location = job_soup.find_all("span", class_="jkey-info")[3].text.strip()
                        posted_at = job_soup.find("div", id="posted-date").text.split(":")[-1].strip()
                        source = source
                        
                        jobs.append({
                            "title": title,
                            "company": company,
                            "posted_at": posted_at, 
                            "location": location,
                            "href": href,
                            "source": source
                        })
            
        except requests.exceptions.HTTPError as e:
            print(f"Error parsing job card: {e}")
        
        currentPage += 1    
        
    return jobs

