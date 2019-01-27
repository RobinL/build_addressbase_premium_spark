# Will want to execute this in docker so we don't have to include BeautifulSoup in Spark.
from bs4 import BeautifulSoup
from dataengineeringutils.s3 import s3_path_to_bytes_io, upload_file_to_s3_from_path
import json

def get_abp_links(soup):

    soup.find("a")

    el = soup.find(text = "Number of Files:").parent.parent
    num = el.text.replace("Number of Files:", "").strip()
    numfiles = int(num)

    my_links = set()
    for a in soup.findAll("a"):
        if "href" in a.attrs:
            if "AB76DL" in a["href"]:  # This is a bit of trial and error, but it turned out that the download links all contain "AB76DL" as part of the URL
                my_links.add(a["href"])
    my_links = list(my_links)

    # Check we've found the right number of links
    if (len(my_links) != numfiles):
        raise Exception

    return my_links

def s3_to_soup(s3_location):

    bio = s3_path_to_bytes_io(s3_location)
    return BeautifulSoup(bio.read(), "html.parser")

def abp_links_to_json(s3_location):
    soup = s3_to_soup(s3_location)
    links = get_abp_links(soup)
    with open("abp_file_links.json", "w") as f:
        json.dump(links, f)

    upload_file_to_s3_from_path("abp_file_links.json", "alpha-everyone", "addressbase_premium/json/links.json")

