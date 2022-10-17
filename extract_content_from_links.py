from turtle import pd
import requests
from bs4 import BeautifulSoup as soup
from datetime import datetime
import concurrent.futures
import time
import pandas as pd
import re
import string
import json
import csv
import os


def write_to_file(dir,file,data):
    if not os.path.exists(dir):
        os.mkdir(dir)
    
    prev_data = list()
    if os.path.exists(os.path.join(dir,file)):
        prev_data = json.load(open(os.path.join(dir,file)))
        # print(prev_data)

    prev_data.append(data)
    with open(os.path.join(dir,file),'w') as f:
        json.dump(prev_data,f,indent=4)

def write_to_csv(file,headers,row):
    file_exists = os.path.isfile(file)
    with open (file, 'a',encoding='UTF8') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n',fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def clean_script(soup_obj):
    remove_tags = ['head','script','meta','style','blockquote']
    for x in soup_obj.findAll(remove_tags):
        x.decompose()
    return soup_obj

def process_batch_urls(count,urls):
    batch_res = []
    print(f"--- Processing batch: {count} ----")
    for ct in range(0,len(urls)):
        # time.sleep(1)
        data = fetch_page_content(urls[ct],ct)
        batch_res.append(data)
        write_to_csv("data/med_data_complete_v2.csv",list(data.keys()),data)
    return batch_res


def fetch_page_content(url,ct):    
    print(f"--- Url Count: {ct} ----")
    page_data = {}
    time.sleep(1)
    base_html = requests.get(url=url,headers=header)
    
    if base_html.status_code == 200:
        print(f"----- Fetching : {url} ------")
        page_data['url'] = url
        base_bs_obj = soup(base_html.text,'html.parser')
        base_clean_obj = clean_script(base_bs_obj)
        try:
            # main page
            content = base_clean_obj.find('div',{'id':'drug-main-header'})
            
            # header
            header_content = content.find('div',{'id':'drug_header'})
            med_name = header_content.find(re.compile('h\d'),{'class':re.compile('^DrugHeader__title')}).text
            page_data['med_name'] = med_name
            links = header_content.findAll('a')
            # print(links)
            for x in links[:-1]:                
                try:
                    # print(x) 
                    if 'manufacturer' in x['href']:
                        page_data['manufacturer_link'] = "https://www.1mg.com" + x['href']
                        page_data['manufacturer'] = x.getText()
                except Exception as e:
                    print(e)
                    # print("No href in manufacture anchor tag")
                    page_data['manufacturer_link'] = ""
                    page_data['manufacturer'] = ""
                
                try:
                    # print(x) 
                    if 'generics' in x['href']:
                        page_data['salt_composition_link'] = "https://www.1mg.com" + x['href']
                        page_data['salt_composition'] = x.getText()
                        page_data['salt_composition_content'] = extract_salt_composition_data(page_data['salt_composition_link'])
                except Exception as e:
                    print(e)
                    # print("No href in salt composition anchor tag")
                    page_data['salt_composition_link'] = ""
                    page_data['salt_composition'] = ""
                    page_data['salt_composition_content'] = ""
            
            # overview / product info
            info_content = content.find('div',{'id':'overview'}).find('div',{'class':re.compile('^DrugOverview__content')})
            page_data['product_information'] = info_content.get_text()

            # uses_and_benefits
            uses_and_benefits_content = content.find('div',{'id':'uses_and_benefits'}).findAll('div',{'class':re.compile('^DrugOverview__container')})
            for div in uses_and_benefits_content:
                if 'uses' in div.find(re.compile('h\d')).text.lower():
                    ctx = div.find('div',{'class':re.compile('^DrugOverview__content')}).findAll('li')
                    for x in ctx:
                        if x.find('a'):
                            page_data['uses_link'] = x.find('a')['href']
                        else:
                            page_data['uses_link'] = ""
                        page_data['uses'] = x.text
                        # print(x.find('a').text)
                
                if 'benefits' in div.find(re.compile('h\d')).text.lower():
                    ctx = div.find('div',{'class':re.compile('^DrugOverview__content')}).find(re.compile('h\d')).findNext('div')
                    # print(ctx.text)
                    page_data['benefits'] = ctx.text

            # side_effects
            side_effects_text = ''
            side_effects_content = content.find('div',{'id':'side_effects'}).find('div',{'class':re.compile('^DrugOverview__container')})
            for div in side_effects_content.findAll('div',{'class':re.compile('^DrugOverview__content')}):
                if div.find(re.compile('h\d')):
                    side_effects_text += div.find(re.compile('h\d')).text + ': \n'
                    for li in div.findAll('li'):
                        side_effects_text += "- " + li.text + '\n'
                        # print(li.text)
                else:
                    if not div.find('ul'):
                        # print(div.text)
                        side_effects_text += div.text + '. \n'
            page_data['side_effects'] = side_effects_text        

            # how_to_use
            how_to_use_content = content.find('div',{'id':'how_to_use'}).find('div',{'class':re.compile('^DrugOverview__content')})
            page_data['how_to_use'] = how_to_use_content.get_text()

            # how_drug_works
            how_drug_works_content = content.find('div',{'id':'how_drug_works'}).find('div',{'class':re.compile('^DrugOverview__content')})
            page_data['how_drug_works'] = how_drug_works_content.get_text()
            
            # safety_advice
            safety_advice_text = ''
            safety_advice_content = content.find('div',{'id':'safety_advice'}).find('div',{'class':re.compile('^DrugOverview__content')})
            for div in safety_advice_content.findChildren('div'):
                match = re.compile('^DrugOverview__content')
                if div['class']:
                    if match.search(div['class'][0]):
                        # print(div['class'][0])
                        # print("- ",div.text)
                        safety_advice_text += "- " + div.text + '\n'
            page_data['safety_advice'] = safety_advice_text
        except Exception as e:
            print(e)
            page_data = {}

    else:
        print(f"---- Status code : {base_html.status_code} ----")
        print(f"Error URL: {url}")
    return page_data

def extract_salt_composition_data(url):
    time.sleep(0.5)
    salt_data = ""
    base_html = requests.get(url=url,headers=header)
    
    if base_html.status_code == 200:
        # print(f"----- Fetching : {url} ------")
        base_bs_obj = soup(base_html.text,'html.parser')
        base_clean_obj = clean_script(base_bs_obj)
        try:
            x = base_clean_obj.find('div',{'class':re.compile('^GenericSaltStyle__row')}).find('div',{'class':re.compile('^GenericSaltStyle__col')})
            for blc in x.findAll(re.compile('h\d')):
                blc.decompose()
            for y in x.findAll('div',{'class':re.compile('GenericSaltStyle__fCol')}):
                salt_data += y.text + "\n"
        except Exception as e:
            print("Salt Extract: ")
            print(e)
            salt_data = ""
    else:
        print(f"---- Status code : {base_html.status_code} ----")
        print(f"\n Error URL: {url}")
    
    return salt_data

if __name__ == "__main__":
    
    header = {'Origin': 'https://www.1mg.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
    }

    # med_data = pd.read_csv('data/med_card_links.csv')
    # print("Total links: ",len(med_data['links']))

    med_links = list()
    with open("data/links_data.json",'r') as f:
        med_links = json.load(f)
    
    print("Total sublists: ",len(med_links))
    print("Total links ( approx ): ",(len(med_links)*len(med_links[0]))-(len(med_links[0])-len(med_links[-1])))

    for count in range(0,len(med_links)):
        process_batch_urls(count=count,urls=med_links[count])
    





      