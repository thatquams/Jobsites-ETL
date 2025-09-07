from .extension import connectToJobSite, MAX_PAGES
import pandas as pd 
import requests
import json

@connectToJobSite
def jobberMan(content, currentPage=1):
    
    """
        Scrapes job listings from the JobberMan website.

        Args:
            content (BeautifulSoup): Parsed HTML content of the JobberMan jobs page.

        Returns:
            pd.DataFrame: A DataFrame containing job title, company, location, 
                        posting date, job URL, and source for each job listed.
    """

    job_cards = content.find_all(attrs={"data-cy" : "listing-cards-components"})
    jobs = []

    while currentPage <= MAX_PAGES:
        try:
            for job in job_cards:
                posted_at = job.find("p", class_="ml-auto text-sm font-normal text-gray-700 text-loading-animate")

                jobs_info = job.find("div", class_="w-full")
                title_et_company = jobs_info.find_all("p")
                title = title_et_company[0].text.strip()
                company = title_et_company[1].text.strip()
                href = jobs_info.find("a", href=True)["href"].strip()
                location = job.find("span", class_="bg-brand-secondary-100")
            
                jobs.append({
                        "title": title,
                        "company": company,
                        "posted_at": "".join(posted_at),
                        "location": "".join(location).strip(),
                        "href": href,
                        "source": "JobberMan"
                    })
            
        except requests.exceptions.HTTPError as e:
            print(f"Error parsing job card: {e}")
            
        currentPage += 1
        # if currentPage == MAX_PAGES:
        #     break

    return jobs
        