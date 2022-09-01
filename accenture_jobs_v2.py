import os
import re
import time
import json
import logging
import requests
from math import floor, ceil
from datetime import date
from threading import Lock
from bs4 import BeautifulSoup
from unidecode import unidecode
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor
from job_meta_upload_script_v2 import JobsMeta

config_rdr=ConfigParser()
config_rdr.read('db_config.ini')
DEV_MAIL=config_rdr.get('dev_mails','dev_mail')
POST_AUTHOR=config_rdr.get('post_author_no','Jhansi')

class Accenture:
    '''Creating Wipro class containing all the methods.'''
    def __init__(self,company):
        logging.basicConfig(filename= f'{company}_logs_{date.today().strftime("%d_%m_%Y")}.log', format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',level = logging.INFO)
        self.company=company
        self.logger_obj=logging.getLogger()
        self.threadlock = Lock()
        self.dbObj = JobsMeta(self.company,self.logger_obj)
        self.page_size = 15
        self.session = requests.Session()
        self.regex = r'\<\s\d{1,2}\s[y,Y][e,E][a,A][r,R][s,S]|\>\s\d{1,2}\s[y,Y][e,E][a,A][r,R][s,S]|\d{1,2}\-\d{1,' \
                     r'2}\s[y,Y][e,E][a,A][r,R][s,S]|\d{1,2}\s\-\s\d{1,2}\s[y,Y][e,E][a,A][r,R][s,S]|\d{1,2}\-\d{1,' \
                     r'2}\s[y,Y][r,R][s,S]|\d{1,2}\s\-\s\d{1,2}\s[y,Y][r,R][s,S]|\d{1,2}\-\d{1,2}\s[m,M][o,O][n,N][t,' \
                     r'T][h,H][s,S]|\d{1,2}\s\-\s\d{1,2}\s[m,M][o,O][n,N][t,T][h,H][s,S]|\<\s\d{1,2}\s[m,M][o,O][n,' \
                     r'N][t,T][h,H][s,S]|\>\s\d{1,2}\s[m,M][o,O][n,N][t,T][h,H][s,S]|\<\s\d{1,2}\s[y,Y][r,R][s,' \
                     r'S]|\>\s\d{1,2}\s[y,Y][r,R][s,S]|\d{1,2}\+\s[y,Y][e,E][a,A][r,R][s,S]|\d{1,2}\+\s[m,M][o,O][n,' \
                     r'N][t,T][h,H][s,S] '

        self.base_url = "https://www.accenture.com/api/sitecore/JobSearch/FindJobs"
        # initilize total_count
        self.total_count = 0
        self.fetch_job_links("total_count")
        
    def fetch_job_links(self, key, params = {"first": 1, "size": 0}):
        # key can be either `jobs`` or `total_count``
        # for total_count `params` will not be passed
        # for jobs `params` have to be passed
        payload = {
            "f": params["first"],
            "s": params["size"],
            "k": "",
            "lang": "en",
            "cs": "in-en",
            "df": "[{\"metadatafieldname\":\"skill\",\"items\":[]},{\"metadatafieldname\":\"location\",\"items\":[]},{\"metadatafieldname\":\"postedDate\",\"items\":[]},{\"metadatafieldname\":\"travelPercentage\",\"items\":[]},{\"metadatafieldname\":\"jobTypeDescription\",\"items\":[]},{\"metadatafieldname\":\"businessArea\",\"items\":[]},{\"metadatafieldname\":\"specialization\",\"items\":[]},{\"metadatafieldname\":\"workforceEntity\",\"items\":[]},{\"metadatafieldname\":\"yearsOfExperience\",\"items\":[]}]",
            "c": "India",
            "sf": 1,
            "syn": False,
            "isPk": False,
            "wordDistance": 0,
            "userId": ""
        }

        try: 
            response =  self.session.post(self.base_url,json = payload)
            json_data = response.json()
            if key == "total_count":
                self.total_count = json_data['total']
            elif key == "jobs":
                page_url = "https://www.accenture.com/in-en/careers/jobsearch?jk=&sb=1&pg={}&vw=1&is_rj=0".format(ceil(params["first"]))
                jobs = json_data["documents"]
                for job in jobs:
                    job_url = unidecode(job["jobDetailUrl"].replace('/{0}', ""))
                    self.threadlock.acquire()
                    self.dbObj.link_insertion(page_url,job_url)
                    self.threadlock.release()

        except Exception as resp_err:
            self.logger_obj.critical(f'Error while requesting and getting json data from {self.base_url} : {resp_err}')
            self.dbObj.exit_fun()
    
    def insertJobLinks(self):
        pages = list(range(floor(self.total_count/self.page_size))) 
        pager = []
        for page in pages:
            pager.append({
                "first": page*self.page_size+1,
                "size": self.page_size
            })
        with ThreadPoolExecutor() as fetch_links_executor:
            fetch_links_executor.map(self.fetch_job_links, ["jobs"]*len(pager), pager)

        print("Inserted into status table")

    def scrape_job_and_insert(self, page_job_urls):
        page_url = page_job_urls[0]
        job_url = page_job_urls[1]

        search_page_no = int(page_url.split("pg=")[1].split("&vw")[0])
        try:
          response = self.session.get(job_url)
          soup = BeautifulSoup(response.text, 'lxml')

          apply_url = soup.find('a', {'class': "apply-job-btn reinvent-job-apply"}).get('href')
          allScripts = soup.find_all('script') # find all scripts
          # find the script with text `hiringOrganization`
          for script in allScripts:
              script_text = script.text
              if 'hiringOrganization' in script_text:
                  req_script_text = script_text
                  break
          
          details_json = json.loads(req_script_text)[0]
          companyname = 'Accenture'
          posttitle = unidecode(details_json['title'])
          location = unidecode(details_json['jobLocation'][0]["address"]["addressLocality"])
          qualification = unidecode(details_json['qualifications']) 
          jobtype = unidecode(details_json['employmentType'])
          jobId = unidecode(details_json['identifier']['value'])
          jobDescription = unidecode(details_json['description'])
          postcontent = ''
          experience = ''
          skills = ''
          salary = 'Not Disclosed'
          imp_info = ''

          descriptionSoup = BeautifulSoup(jobDescription, 'lxml')
          
          detailsList = descriptionSoup.find('ul').find_all('li')
          for detail in detailsList:
              text = detail.text
              if 'Project Role Description' in text:
                  postcontent = text.split(':')[1].strip()
              elif 'Work Experience' in text:
                  experience = text.split(':')[1].strip()
              elif 'Job Requirements' not in text:
                  if 'Key Responsibilities' in text:
                      imp_info = ".".join(text.split(':')[1:]).strip()
                  elif 'Technical Experience' in text:
                      skills = skills + ".".join(text.split(':')[1:]).strip()
                  elif 'Professional Attributes' in text:
                      skills = skills + ".".join(text.split(':')[1:]).strip()
                  elif 'Educational Qualification' in text:
                      qualification = ".".join(text.split(':')[1:]).strip()
          self.threadlock.acquire()
          self.dbObj.upload_job_meta_upd(POST_AUTHOR, postcontent, posttitle, companyname, location, jobtype, 
                              search_page_no, apply_url, qualification, skills, experience, salary, 
                              imp_info)
          self.dbObj.change_status(job_url)
          self.threadlock.release()
        
        except Exception as resp_err:
            self.logger_obj.critical(f'Error while scraping data from {job_url} and inserting data: {resp_err}')
            # self.dbObj.exit_fun()

    def scrape_jobs_multi_thread(self):
      self.dbObj.create_trial_job_meta_tb()
      unscrapedJobs = obj.dbObj.not_scraped_urls()
      with ThreadPoolExecutor() as scrape_jobs_executor:
          scrape_jobs_executor.map(self.scrape_job_and_insert, unscrapedJobs)
      # for item in unscrapedJobs:
        # self.scrape_job_and_insert(item)
        
if __name__ == '__main__':
    t1=time.time()
    obj = Accenture('accenture')
    obj.dbObj.create_sc_stat_tb()
    obj.insertJobLinks()
    obj.scrape_jobs_multi_thread()
    print(f'Time taken to complete scraping all {obj.total_count} is : {time.time()-t1}s')
    if os.stat(f'{obj.company}_logs_{date.today().strftime("%d_%m_%Y")}.log').st_size!=0:
        obj.dbObj.mail_log_file()
        print('Log file mailed')
    else:
        print('Log file is empty')
        logging.shutdown()
        os.remove(f'{obj.company}_logs_{date.today().strftime("%d_%m_%Y")}.log')
        if not os.path.exists(f'{obj.company}_logs_{date.today().strftime("%d_%m_%Y")}.log'):
            print('Log File deleted')

