import requests
from bs4 import BeautifulSoup as soup
from datetime import datetime
import concurrent.futures
import time
import string
import pandas as pd
import logging as log

header = {'Origin': 'https://www.1mg.com',
 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
}

def clean_script(soup_obj):
    remove_tags = ['head','script','meta','style','blockquote']
    for x in soup_obj.findAll(remove_tags):
        x.decompose()
    return soup_obj

def call_scrapper(base_url,char):
    links = []
    index = 0
    while True:
        base_url_copy = base_url
        if index != 0 and char != 'a':
            base_url_copy += f'?page={index}&label={char}'   
        elif index !=0 and char == 'a':
            base_url_copy += f'?page={index}'

        time.sleep(3)
        base_html = requests.get(url=base_url_copy,headers=header)

        # print(f"Page: {index+1}")
        if base_html.status_code == 200:
            base_bs_obj = soup(base_html.text,'html.parser')
            base_clean_obj = clean_script(base_bs_obj)
            temp = list()        
            [temp.append(a['href']) for a in base_clean_obj.findAll('a',{'class':'button-text','target':'_blank'}) if 'drug' in a['href']]

            if temp:
                [links.append('https://www.1mg.com/drugs/'+x) for x in temp]
            else:
                print("----- No data available in page.... skipping to next search character -----")
                break
            index += 1
        else:
            print(f"---- Status code : {base_html.status_code} ----")
            print(f"\n Error URL: {base_url}")
            break
    return links

if __name__ == "__main__":
    start = datetime.now()
    base_url = 'https://www.1mg.com/drugs-all-medicines'
    alpha = string.ascii_lowercase
    max_workers = 50

    response = list()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(call_scrapper,base_url,char) for char in alpha}
        for future in concurrent.futures.as_completed(futures):
            response.append(future.result())
    
    final_links = list()
    if response:
        [[final_links.append(x) for x in lst] for lst in response if lst]
        
    df = pd.DataFrame({"links":final_links})
    df["links"] = df["links"].apply(lambda x:x.replace('https://www.1mg.com/drugs//','https://www.1mg.com/'))
    df.to_csv('data/med_card_links.csv',index=False)
    end = datetime.now()

    log.info(" Time Taken: %s",(end-start))