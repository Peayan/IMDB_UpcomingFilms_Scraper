from bs4 import BeautifulSoup as soup
import xlsxwriter
import time
import requests
import lxml
import aiohttp
import asyncio

########################################################

def get_film_dates_info(page_soup):
    """scrapes the film release date attribute for each film shown on the webpage"""
    film_dates = []
    for date in page_soup.findAll("h4"):
        film_date = date.string
        film_dates.append(film_date)
    return film_dates

########################################################

def get_film_names_info(page_soup):
    """scrapes the film name attribute for each film shown on the webpage"""
    films_names = []
    count = page_soup.findAll("ul")
    for name in count[7:len(count) - 4]:
        film_name = name.find('a').string
        films_names.append(film_name)
    return films_names

########################################################

def get_film_urls_info(page_soup):
    """scrapes the url attribute for each film shown on the webpage"""
    film_urls = []
    count = page_soup.findAll("ul")
    for url in count[7:len(count) - 4]:
        film_url = url.find('a')["href"]
        film_urls.append(f"https://imdb.com{film_url}")
    return film_urls

########################################################
async def fetch(url):
    """creates a session and request for the specified url to run asynchronously with other requests"""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

########################################################

async def add_to_directors(index, url):
    """converts film url request into html to be checked for a 'director' attribute to log"""
    page_soup = soup(url, 'html.parser')
    global __directors__
    __directors__[index] = (load_director_names(page_soup))

########################################################

async def collect_request_text(index, url):
    """passes the returned asynched web request to be parsed into html"""
    content = await fetch(url)
    await add_to_directors(index, content)

########################################################

async def main(film_urls):
    """handles the asynchronous tasks of grabbing the director values from each unique film url page"""
    tasks = []
    for index, url in enumerate(film_urls):
        tasks.append(collect_request_text(index, url))
    await asyncio.wait(tasks)

########################################################

def load_director_names(page_soup):
    """Itterates all credit data on film page and returns the director's name if one exists"""
    for index, director in enumerate(page_soup.findAll("div", {"class": "credit_summary_item"})):
        if director.h4.string == "Directors:" or director.h4.string == "Director:":
            return director.a.string
    return " - NA - "

########################################################

def write_data_to_file(film_names, film_release_dates, film_urls, film_directors=0):
    """Writes all of the scraped film information into an .xlsx file"""
    # Location we want to replace the outputted .xlsx file containing film info
    path = "IMDB_Releases.xlsx"

    # Create .xlsx document to store data
    outWorkbook = xlsxwriter.Workbook(path)
    outSheet = outWorkbook.add_worksheet()

    # Output to excel files
    outSheet.write(0, 0, "Film Name")
    for index, name in enumerate(film_names):
        outSheet.write(index + 2, 0, name)

    outSheet.write(0, 1, "Release Date")
    for index, date in enumerate(film_release_dates):
        outSheet.write(index + 2, 1, date)

    outSheet.write(0, 3, "Film Webpage")
    for index, url in enumerate(film_urls):
        outSheet.write_url(index + 2, 3, url)

    #User may choose to skip director list as it takes a while to grab compared to other info
    if film_directors != 0:
        outSheet.write(0, 4, "Directors")
        for index, dir in enumerate(film_directors):
            outSheet.write(index + 2, 4, dir)

    # Close excel book
    outWorkbook.close()

    print(f"Saved {len(film_names)} - check {path} for results")

########################################################

#url of movie calenders
start_time = time.time()
my_url = 'https://www.imdb.com/calendar?region=GB&ref_=rlm'

#opening connection and grabbing page html
req = requests.get(my_url).text

#parsing the html from the page so we can extract it
page_soup = soup(req, 'lxml')

#Get all the film release dates
film_names = get_film_names_info(page_soup)
film_release_dates = get_film_dates_info(page_soup)
film_urls = get_film_urls_info(page_soup)

#Global variable to want to asynchronously  right into
__directors__ = list(range(len(film_names)))

#Open all film url pages asynchronious and load director information
if __name__ == '__main__':
    asyncio.run(main(film_urls))

#Write all the film data into a .xlsx file at the specified path
write_data_to_file(film_names, film_release_dates, film_urls, __directors__)

print(f"Took {time.time() - start_time} to load {len(film_names)} films.")
