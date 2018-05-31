import requests
from bs4 import BeautifulSoup
from mpi4py import MPI
import csv
import time

def read_domain_from_csv(file_name, rank, size):
    web = "https://fortiguard.com/webfilter?q="
    with open(file_name, 'r') as f:
        for i, line in enumerate(f):
            # skip first line
            if i > 0:
                if i % size == rank:
                    url = line.split(',')[0]
                    # delete http or https
                    if url.startswith('http://'):
                        url = url[7:]
                    elif url.startswith('https://'):
                        url = url[8:]
                    # delet ending '/'
                    if url.endswith('/'):
                        url = url.strip('/')
                    search_url = web + url
                    try:
                        category = get_category(search_url)
                    except:
                        time.sleep(3)
                        print("Sleep 3 seconds")
                        category = get_category(search_url)
                        pass
                    print(url, category)
                    save_as_csv(line, category)

def get_category(search_url):

    r = requests.get(search_url)
    soup = BeautifulSoup(r.content, "lxml")
    result = soup.find_all(class_="info_title")

    for elem in result:
        if "Category:" in elem.text:
            category = elem.text[10:]
            return category
        else:
            return "Not Detection"

def save_as_csv(line, category):
    file_names = ['url', 'status_code', 'CMS', 'category']
    url = line.split(',')[0]
    status_code = line.split(',')[1]
    url_cms = line.split(',')[2]
    url_cms = url_cms[:-1]
    with open("final_with_category.csv", 'a') as f2:
        writer = csv.DictWriter(f2, fieldnames=file_names)
        writer.writerow({'url': url, 'status_code': status_code, 'CMS': url_cms, 'category': category})
        f2.close()



if __name__=="__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Initial a file to write data to
    file_names = ['url', 'status_code', 'CMS', 'category']
    with open("final_with_category.csv", 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=file_names)
        writer.writeheader()
        csv_file.close()

    read_domain_from_csv("Final_result.csv", rank, size)