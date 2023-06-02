# IMPORTS
import pandas as pd # to store the data
from bs4 import BeautifulSoup # to get the data from the front end of the website
import re # To clean up data
import asyncio # To runs the browser
import pyppeteer # Puppeteer but for python, kind of the same

# Needed Variables
role = "data+analyst"
city = "Birmingham"

BDlogin = "your brightdata login:password"
BDEndPoint = f"wss://{BDlogin}@your bright data proxy"

# FUNCTIONS
def main(role, city, BDEndPoint):

    pages = asyncio.get_event_loop().run_until_complete(pagesSetup(BDEndPoint, role, city)) # make the list of pages we need to visit
    html_list = [] # a list to store the html data of each page

    for i in pages: # for each page get html data
        html_list.append(asyncio.get_event_loop().run_until_complete(gethtml(BDEndPoint, role, city, i))) # storing the data

    # use the list of html data to get the information we are looking for, into a pandas dataframe
    df = scrape_job_details(html_list) 
    # append the job descrition and data posted into the dataframe
    df = asyncio.get_event_loop().run_until_complete(jobdesc(BDEndPoint, df))
    # save it to a json file
    df.to_json(f"./docs/{role}.json", orient = 'table')

# This is to set up the number of pages for the job listings, there are about 10 listings per page, so total jobs/10
async def pagesSetup(endpoint, role, city):
    # Opens up a new browser
    browser =  await pyppeteer.launch(headless = False, executablePath ="./chrome-win/chrome.exe")
    await pyppeteer.connect(browserWSEndpoint = endpoint) # Connects to brightdata proxy
    pages = [0] # stores each page number so we can put it in the link
    html_pages = [] # stores the html for the page
    url = f'https://uk.indeed.com/jobs?q={role}&l={city}&sort=date&start={pages[0]}'
    # open a new tab in the browser
    page = await browser.newPage()
    page.setDefaultNavigationTimeout(2*60*1000)
    # goes to the premade url
    await page.goto(url)
    await page.waitFor(4000) # just to make sure its loaded up fully
    html_pages.append (await page.content()) # get the html of the page
    await browser.close() # close the browser
    soup = BeautifulSoup(html_pages[0], "html.parser") # load up the html into beatifulsoup
    # scrape the total jobs number and make it an integer, find the div with a class of V, as there is only one in the page it will be the first in the list.
    listingN = int(re.findall(r'\d+', soup.find("div", class_ ='jobsearch-JobCountAndSortPane-jobCount').text)[0])
    pages = list(range(0, listingN, 10)) # create a list of pages
    print(pages) # print the list, so we can make sure its correct
    return pages

# This function gets the html of all pages before we scrape them
async def gethtml(endpoint, role, city, page):
    # launch chromium browser in the background
    print(f"Opening page: {page}")
    url = f'https://uk.indeed.com/jobs?q={role}&l={city}&sort=date&start={page}'
    browser =  await pyppeteer.launch(headless = False, executablePath ="./chrome-win/chrome.exe")
    await pyppeteer.connect(browserWSEndpoint = endpoint)
    # open a new tab in the browser
    page = await browser.newPage()
    page.setDefaultNavigationTimeout(2*60*1000)
    # add URL to a new page and then open it
    await page.goto(url)
    await page.waitFor(4000)
    # close the browser
    html_pages = await page.content()
    await browser.close()
    return html_pages

# This function scrapes all the job postings of all pages from the previous function
def scrape_job_details(content):
    jobs_list = [] # where we will store all the data

    for n in content: # for each html data in the list we made previously
        soup = BeautifulSoup(n, "html.parser") # load up soup
        for post in soup.find_all(class_ ='job_seen_beacon'): #find all the job posts visible on the page
            try:
                data = {
                    "job_title":post.find("h2", class_='jobTitle').text, # get title
                    "job_id":re.findall("\_(.*)",post.find("a", class_='jcs-JobTitle').attrs["id"])[0], # get the id so we can use it later to get full description
                    "company":post.find("span", class_='companyName').text, # the company name
                    "company_location":post.find("div", class_='companyLocation').text, # the location so we can filter it later
                }
            except IndexError:
                continue   # if there is any errors, ignore it
            jobs_list.append(data) # add it to the list

    df = pd.DataFrame(jobs_list) # turn it into panda's data frame
    newdf = df.drop_duplicates() # remove any duplicates as there may be overlapping posts
    return newdf

# As we now have all the id's and listing details, we need to open them all up and get the full descriptions
# this is the slow part as it opens up all job posts one by one to get the descriptions
# plus headless browser doesnt seem to work so it is also a little annoying as it keeps flashing when opening up new pages
async def jobdesc(endpoint, df):

    newdf = df # making a copy of the data frame so we can edit the copy instead
    tracker = 1 
    for id in df["job_id"]: # for each job id
        # using the tracker and the total job posts to visualise the progress
        print(f"Getting description {tracker}/{len(df.index)}")
        # opening up a new browser each time as indeed blocks it otherwise
        browser =  await pyppeteer.launch(headless = False, executablePath ="./chrome-win/chrome.exe")
        await pyppeteer.connect(browserWSEndpoint = endpoint)
        # create the new url with the job id we got from before
        url = f'https://uk.indeed.com/viewjob?jk={id}'
        page = await browser.newPage()
        page.setDefaultNavigationTimeout(2*60*1000)
        await page.goto(url)
        await page.waitFor(4000)
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        # clean up the data before storing it as the /n may cause problems in the future
        desc = re.sub("/n", "", soup.find("div", class_="jobsearch-jobDescriptionText").text)
        # get the time of posting so we can filter out posts that are too old
        date = soup.find("span", class_ = "css-kyg8or").text
        await browser.close() # making sure to close each browser before opening new one, it will be a problem if we don't
        newdf.loc[newdf.index[newdf["job_id"] == id], 'job_desc'] = desc # making a new collumn and adding to it
        newdf.loc[newdf.index[newdf["job_id"] == id], 'date_posted'] = date # making a new collumn and adding to it
        tracker += 1

    return newdf

# RUN
main(role, city, BDEndPoint)