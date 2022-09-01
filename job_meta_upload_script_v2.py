'''Importing all the necessary libraries.'''
import hashlib
import os
import smtplib
import time
from os.path import basename
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from configparser import ConfigParser
from datetime import date,datetime
import requests
from mysql import connector

#Get the required credentials to connect to Database
config_rdr=ConfigParser()#object to read .ini file
config_rdr.read('db_config.ini')

DB_HOST=config_rdr.get('jinternal_db','db_host')
DB_USR=config_rdr.get('jinternal_db','db_usr')
DB_PWD=config_rdr.get('jinternal_db','db_pwd')
DB_NAME=config_rdr.get('jinternal_db','db_name')
DEV_MAIL=config_rdr.get('dev_mails','dev_mail')

class JobsMeta:
    '''Contains methods for performing database operations'''
    def __init__(self, company,logger_obj):
        '''Constructor for initial configuration for database connection'''
        self.logger_objt=logger_obj

        try:
            con_stat=internet_connection() # waits for 5/10 min
            if not con_stat[0]:
                self.logger_objt.critical(f'Internet Connectivity Issue : {con_stat[1]}')
                self.exit_fun()
            self.con = connector.connect(host=DB_HOST,
                                        user=DB_USR,
                                        password=DB_PWD,
                                        database=DB_NAME)
            self.cur = self.con.cursor(buffered=True)
            print('Connection Created\n')
        except Exception as con_err:
            self.logger_objt.critical(f'Error in connection to database {DB_NAME} : {con_err}')
            self.exit_fun()
        else:
            self.company = company

    def create_sc_stat_tb(self):
        '''Creating temporary table for job_url scraping status'''
        try:
            if not self.con.is_connected():
                if not self.db_reconnection():
                    self.exit_fun()
            st_tb=self.company.replace(' ','_')+'_job_sc_stat'
            query = f"CREATE TABLE IF NOT EXISTS {st_tb} \
                    (page_url VARCHAR(1000), job_url VARCHAR(1000), sc_stat VARCHAR(2)) ;"
            self.cur.execute(query)
            print(f'{self.company}_job_sc_stat Created \n')
        except Exception as crt_err:
            self.logger_objt.error(f'Error while creating table \
{self.company}_job_sc_stat : {crt_err}')

    def exit_fun(self):
        '''Stops the execution in case of critical error and sends the log file
        to respective developer'''
        self.mail_log_file()
        self.cur.close()
        self.con.close()
        raise Exception("Check log file")

    def db_reconnection(self):
        '''Reconnects to database'''
        try:
            #storing status instead of calling function in if condition as I
            # need error part and for that again calling function would add sleep for 10min more
            con_stat=internet_connection()#wait for 5/10 min
            if not con_stat[0]:
                self.logger_objt.critical(f'Internet Connectivity Issue : {con_stat[1]}')
                self.exit_fun()
            self.con = connector.connect(host=DB_HOST,
                            user=DB_USR,
                            password=DB_PWD,
                            database=DB_NAME)
            self.cur = self.con.cursor(buffered=True)
        except Exception as rec_err:
            self.logger_objt.critical(f'Error while reconnecting to {DB_NAME} database : {rec_err}')
            return False
        return True

    def not_scraped_urls(self):
        '''Returns the list of Not Scraped(NS) job and page urls'''
        try:
            if not self.con.is_connected():
                if not self.db_reconnection():
                    self.exit_fun()
            s_query=f"SELECT page_url,job_url FROM {self.company}_job_sc_stat WHERE sc_stat='NS'"
            self.cur.execute(s_query)
            ns_rec=self.cur.fetchall()
        except Exception as ns_err:
            self.logger_objt.critical(f'Error while selecting Not Scraped URLs from table {self.company}_job_sc_stat : {ns_err}')
            self.exit_fun()
        ns_rec=[list(i) for i in ns_rec]
        return ns_rec

    def link_insertion(self,page_url,job_url):
        '''Inserts page and job url in the Scraping Status Table'''
        try:
            if not self.con.is_connected():
                if not self.db_reconnection():
                    self.exit_fun()
            query = f"SELECT job_url FROM {self.company}_job_sc_stat WHERE job_url='{job_url}'"
            self.cur.execute(query)
            existing_job = self.cur.fetchall()
        except Exception as fth_err:
            self.logger_objt.error(f'Error while checking \
if the job url {job_url} from page {page_url} already \
exists in status table {self.company}_job_sc_stat : {fth_err}')
        else:
            if len(existing_job)==0:#job_url does not exist in Scraping Status table
                try:
                    if not self.con.is_connected():
                        if not self.db_reconnection():
                            self.exit_fun()
                    query=f"INSERT INTO \
{self.company}_job_sc_stat VALUES('{page_url}','{job_url}','NS')"
                    self.cur.execute(query)
                    self.con.commit()
                    # print(f'{self.company}_job_sc_stat inserted new link')
                except Exception as st_ins_err:
                    self.logger_objt.error(f'Error while inserting \
the job url {job_url} from page {page_url} in status table \
{self.company}_job_sc_stat : {st_ins_err}')

    def change_status(self,j_url):
        '''Changes the scraping status of the job url to S(Scraped)
        in Scraping Status Table'''
        try:
            if not self.con.is_connected():
                if not self.db_reconnection():
                    self.exit_fun()
            up_qu=f"UPDATE {self.company}_job_sc_stat SET sc_stat='S' WHERE job_url='{j_url}'"
            self.cur.execute(up_qu)
            self.con.commit()
        except Exception as stat_chn_err:
            self.logger_objt.error(f'Error while changing status \
for job url {j_url} in status table {self.company}_job_sc_stat \
: {stat_chn_err}')

    def create_trial_job_meta_tb(self):
        try:
            if not self.con.is_connected():
                if not self.db_reconnection():
                    self.exit_fun(self.dev_mail)
            # st_tb=self.company.replace(' ','_')+'_job_sc_stat'
            query = f"CREATE TABLE IF NOT EXISTS job_meta_2(serial int not null auto_increment primary key, md5_chksum varchar(255), postauth int, postcontent varchar(1000), posttitle	varchar(200), companyname varchar(200), location varchar(200), jobtype varchar(200), job_url varchar(200), qualification varchar(1000), skills varchar(1000), experience varchar(1000), salary varchar(200), imp_info varchar(1000), company_website varchar(200), company_tagline varchar(200), company_video varchar(200),company_twitter varchar(200), job_logo tinyint(1), localFilePath varchar(100), dated varchar(50));"
            self.cur.execute(query)
            print('Trial Job Meta 2 table created')
        except Exception as crt_err:
            self.logger_objt.error(f'Error while creating table Trial Job Meta 2: {crt_err}')

    def upload_job_meta_upd(self, postauth=18, postcontent='Work for Full Time',
    posttitle='Developer', companyname='XYZ', location='India', jobtype='Full Time',
    search_page_no='-1',job_url='https://www.xyz.in/apply-for-job/',
    qualification='Graduation, Post Graduation', skills='Related to Job Title',
    experience='0-2 years', salary='Not Disclosed',
    imp_info='Candidate should be passionate about their work',
    company_website='https://www.accenture.com/in-en', company_tagline='Together, we deliver on the promise of technology and human ingenuity. Let there be change.',
    company_video='', company_twitter='https://twitter.com/Accenture', job_logo=False,
    localFilePath=''):
        '''Uploads the passed data in job data table'''
        try:
            md5_chksum = hashlib.md5((f"{postauth}, {postcontent}, {posttitle},\
{companyname},{location}, {jobtype}, {search_page_no},{job_url}, {qualification}, {skills},\
{experience},{salary}, {imp_info},{company_website}, {company_tagline},\
{company_video}, {company_twitter}, {job_logo},\
{localFilePath}").encode("utf-8")).hexdigest()

            if job_logo:
                job_logo = 1
            elif not job_logo:
                job_logo = 0
            if not self.con.is_connected():
                if not self.db_reconnection():
                    self.exit_fun()

            query=f"SELECT md5_chksum FROM job_meta_2 WHERE md5_chksum='{md5_chksum}'"
            self.cur.execute(query)
            existing_job = self.cur.fetchall()
        except Exception as slc_err:
            self.logger_objt.error(f'Error while selecting \
md5 checksum from job_meta_2 table : {slc_err}')
        else:
            if len(existing_job) > 1: # Deleting multiple copies of pre-existing jobs.
                if not self.con.is_connected():
                    if not self.db_reconnection():
                        self.exit_fun()
                try:
                    query = f"""
                                DELETE FROM job_meta_2 WHERE job_url="{job_url}"
                            """
                    self.cur.execute(query)
                    self.con.commit()
                    print(f'{job_url} deleted due to duplication')
                except Exception as del_dup_err:
                    self.logger_objt.error(f'Error while deleting \
duplicate job {job_url} from job_meta_2 : {del_dup_err}')
                else:
                    try:
                        if not self.con.is_connected():
                            if not self.db_reconnection():
                                self.exit_fun()
                        time_stamp=datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
                        query = f"""
                                INSERT INTO job_meta_2(md5_chksum, postauth, postcontent, posttitle, companyname, location, jobtype, search_page_no, job_url, qualification, skills, experience, salary, imp_info, company_website, company_tagline, company_video, company_twitter, job_logo, localFilePath,timestamp)
                                VALUES("{md5_chksum}",{postauth}, "{postcontent}", "{posttitle}",
                                "{companyname}", "{location}", "{jobtype}", "{search_page_no}","{job_url}",
                                "{qualification}", "{skills}", "{experience}", "{salary}",
                                "{imp_info}", "{company_website}", "{company_tagline}",
                                "{company_video}", "{company_twitter}", {job_logo}, 
                                "{localFilePath}",{time_stamp})
                            """
                        self.cur.execute(query)
                        self.con.commit()
                        print(f'{job_url} inserted updated one')
                    except Exception as job_ins_err:
                        self.logger_objt.error(f'Error while inserting job \
{job_url} in job_meta_2 : {job_ins_err}')
            elif len(existing_job) == 0:#new md5 checksum so either we need to insert or update
                if not self.con.is_connected():
                    if not self.db_reconnection():
                        self.exit_fun()
                try:
                    # Checking whether the job url is already present or
                    # not as md5 checksum is changed
                    sel_qry=f"SELECT job_url FROM job_meta_2 WHERE job_url='{job_url}'"
                    self.cur.execute(sel_qry)
                    exist_job=self.cur.fetchall()
                except Exception as sel_err:
                    self.logger_objt.error(f'Error while selecting apply url {job_url} : {sel_err}')
                else:
                    if len(exist_job)!=0:#job_url already exists so update the data
                        if not self.con.is_connected():
                            if not self.db_reconnection():
                                self.exit_fun()
                        try:
                            up_query=f"""
                                UPDATE job_meta_2 SET md5_chksum="{md5_chksum}",
                                postauth="{postauth}",postcontent="{postcontent}",posttitle="{posttitle}",
                                companyname="{companyname}",location="{location}",jobtype="{jobtype}",
                                search_page_no="{search_page_no}", qualification="{qualification}",skills="{skills}",
                                experience="{experience}",salary="{salary}",imp_info="{imp_info}",
                                company_website="{company_website}",company_tagline="{company_tagline}",
                                company_video="{company_video}",company_twitter="{company_twitter}",
                                job_logo="{job_logo}",localFilePath="{localFilePath}" WHERE job_url="{job_url}"
                            """
                            self.cur.execute(up_query)
                            self.con.commit()
                            # print(f"{job_url} updated data")
                        except Exception as up_err:
                            self.logger_objt.error(f'Error while updating in \
job_meta_2 for job_url {job_url} : {up_err}')
                    else: # job_url is new so insert
                        if not self.con.is_connected():
                            if not self.db_reconnection():
                                self.exit_fun()
                        try:
                            query = f"""
                                        INSERT INTO job_meta_2(md5_chksum, postauth, postcontent, posttitle, companyname, location, jobtype, search_page_no, job_url, qualification, skills, experience, salary, imp_info, company_website, company_tagline, company_video, company_twitter, job_logo, localFilePath)
                                        VALUES("{md5_chksum}",{postauth}, "{postcontent}", "{posttitle}",
                                        "{companyname}", "{location}", "{jobtype}", "{search_page_no}","{job_url}",
                                        "{qualification}", "{skills}", "{experience}", "{salary}",
                                        "{imp_info}", "{company_website}", "{company_tagline}",
                                        "{company_video}", "{company_twitter}", {job_logo}, 
                                        "{localFilePath}")
                                    """
                            self.cur.execute(query)
                            self.con.commit()
                            # print(f'{job_url} inserted new')
                        except Exception as new_ins_err:
                            self.logger_objt.error(f'Error while inserting in job_meta_2 \
    for job_url {job_url} : {new_ins_err}')

    def del_not_existing(self,j_url):
        '''Deletes the job url from Scraping status table that are
        not existing while scraping'''
        try:
            if not self.con.is_connected():
                if not self.db_reconnection():
                    self.exit_fun()
            del_query=f"DELETE FROM {self.company}_job_sc_stat WHERE job_url='{j_url}'"
            self.cur.execute(del_query)
            self.con.commit()
            self.cur.close()
            self.con.close()
        except Exception as del_st_err:
            self.logger_objt.error(f'Error while deleting \
not existing link from status table for {j_url} : {del_st_err}')

    def check_different(self, companyname):
        '''Deleting jobs from job_meta_table that are also deleted
        from job site itself of same company.'''
        if not self.con.is_connected():
            if not self.db_reconnection():
                self.exit_fun()

        try:
            query = f"""
                        DELETE job_meta_2.*
                        FROM job_meta_2
                        LEFT OUTER JOIN {self.company}_job_sc_stat
                        ON job_meta_2.job_url = {self.company}_job_sc_stat.job_url
                        WHERE {self.company}_job_sc_stat.job_url IS NULL
                        AND job_meta_2.companyname = '{companyname}'
                    """
            self.cur.execute(query)
            self.con.commit()
        except Exception as del_err:
            self.logger_objt.error(f'Error while deleting jobs deleted from the site : {del_err}')

    def delete_temp_table(self):
        '''Drops the scraping status table once all job links
        have S as scraping status'''
        try:
            if not self.con.is_connected():
                if not self.db_reconnection():
                    self.exit_fun()
            query=f"SELECT page_url,job_url FROM {self.company}_job_sc_stat WHERE sc_stat='NS'"
            self.cur.execute(query)
        except Exception as ns_err:
            self.logger_objt.critical(f'Error while selecting \
Not Scraped URLs from table {self.company}_job_sc_stat \
: {ns_err}')
            self.exit_fun()
        else:
            ns_link=[]
            if len(self.cur.fetchall())==0:
                try:
                    if not self.con.is_connected():
                        if not self.db_reconnection():
                            self.exit_fun()
                    del_query=f"DROP TABLE IF EXISTS {self.company}_job_sc_stat"
                    self.cur.execute(del_query)
                    self.con.commit()
                except Exception as del_st_tb_err:
                    self.logger_objt.critical(f'Error while \
deleting status table {self.company}_job_sc_stat \
: {del_st_tb_err}')
                    self.exit_fun()
            else:
                try:
                    if not self.con.is_connected():
                        if not self.db_reconnection():
                            self.exit_fun()
                    self.cur.execute(query)
                    ns_link=self.cur.fetchall()
                    ns_link=[list(i) for i in ns_link]
                except Exception as fth_err:
                    self.logger_objt.critical(f'Error while \
fetching Not Scraped URLs from table {self.company}_job_sc_stat : {fth_err}')
                    self.exit_fun()
        return ns_link

    def mail_log_file(self):
        '''To mail the log file to respective developer'''
        #Close all connections to database as
        # mail will be sent only if execution is completed
        # or in case of critical error
        self.cur.close()
        self.con.close()

        # from_addr = os.environ.get('RIV_EMAIL')
        from_addr = DEV_MAIL
        to_addr = DEV_MAIL
        subject = f'Log File for {self.company} Job Portal'
        content = f'Please see the attached Log File for {self.company} Job Portal'

        con_stat=internet_connection()#wait for 2/10 min
        if not con_stat[0]:
            self.logger_objt.critical(f'Internet Connectivity Issue : {con_stat[1]}')
            self.exit_fun()
        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = to_addr
        msg['Subject'] = subject
        body = MIMEText(content, 'plain')
        msg.attach(body)

        filename = f'{self.company}_logs_{date.today().strftime("%d_%m_%Y")}.log'

        try:
            with open(filename, 'r',encoding="utf-8") as l_f:
                part = MIMEApplication(l_f.read(), Name=basename(filename))
                part['Content-Disposition'] = f'attachment; filename="{basename(filename)}"'
            msg.attach(part)
            l_f.close()
        except Exception as log_open_err:
            print(f'Error while reading log file {filename}: {log_open_err}')
            self.logger_objt.error(f'Error while reading log file {filename}: {log_open_err}')
        else:
            try:
                con_stat=internet_connection()#wait for 2/10 min
                if not con_stat[0]:
                    self.logger_objt.critical(f'Internet Connectivity Issue : {con_stat[1]}')
                    self.exit_fun()
                server = smtplib.SMTP('smtp.dreamhost.com', 587)
                # server.login(from_addr, os.environ.get('RIV_PWD'))
                server.login(from_addr, 'CxYxceaD')
                server.send_message(msg, from_addr=from_addr, to_addrs=[to_addr])
            except Exception as em_log_err:
                print(f'Error while logging in to server and sending mail : {em_log_err}')
                self.logger_objt.error(f'Error while logging in and sending mail : {em_log_err}')
            server.quit()

def internet_connection():
    '''Check for internet connection and waits for
    minimum 2min and 10min max in case of connectivity issue'''
    trial=0
    int_cnct_err=''
    url = "https://www.google.com"
    time_out = 10
    while trial!=5:#checks connection for maximum 5 times
        try:
            trial_req = requests.get(url, timeout=time_out)
            return [True]
        except (requests.ConnectionError, requests.Timeout) as int_con_err:
            time.sleep(120)#sleep for 2min and check again
            trial+=1
            int_cnct_err=int_con_err
    return [False,int_cnct_err]
