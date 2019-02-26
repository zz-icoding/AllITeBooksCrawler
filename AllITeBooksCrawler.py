# !/usr/bin/env python 3.7

'''
http://www.allitebooks.com crawler

Created on Feb 2019

@author: zz-icoding
'''

__copyright__ = '''
This script was complied for computer science exercise ONLY, and its downloading
and using are strictly limited for personal usage. 
Any material downloaded by this script, and also the copyright, belongs to its
original owner. Anyone who download any material is responsible for his behavior.
'''

# User Manual
#(1) When first running, you should use "AllITeBooksCrawler(updating = False)",
#    in order to build your own database, i.e. 'allitebooks.xlsx' and other text 
#    files.
#    If there is any interruption during the first running, you should run again
#    with "AllITeBooksCrawler(updating = False)".
#(2) When your have download all the pages for the first time, then you might
#    want to update your inventory and database regularly, then you should run
#    with "AllITeBooksCrawler(updating = True)".

# DEBUG
#(1) When downloading to another disk, Disk C:, i.e. the System Disk, was fulfilled 
#    almost at the same time, when I used default Python IDLE on one of my computer.
#    (Note this didn't happen on another PC of mine.)
#    It was found that all the downloaded information was simultaneouly saved in
#    both the destination disk and the System Disk under the following path,
#    "C:\Windows\SysWOW64"
#(2) It happened that the file counts and size in the property of the "books"
#    directory didn't matched with the files seen in the directory at all. For a 
#    file I checked with 'os.path.exists()' which returned True, however I couldn't 
#    locate it in the windows Explorer.
#    It was found due to some errors in the file index, which can be remedied by
#    shell command "CHKDSK volume: /F".
#(3) 'HTTP Error 400' normally happens with unquoted url. It can be rectified by
#    quote the url before request.

import os
import string
import socket
import pandas
import urllib.request
import html.parser
from numpy import nan
from shutil import rmtree
from time import sleep, ctime
from threading import Thread, Lock
from queue import Queue

def FORMAT_TO_LINK(s):
    # format string: "Practical PHP 7, MySQL 8" >> "practical-php-7-mysql-8"
    for char in s:
        if (char in string.punctuation) or (char in string.whitespace):
            s = s.replace(char, '-')
        for i in range(len(s)):
            if s[i : i + 2] == '--':
                s = s[ : i + 1] + s[i + 2 : ]
    return s.lower()

def FORMAT_TO_FILENAME(s):
    # format string: "practical-php-7-mysql-8" >> "Practical Php 7 Mysql 8"
    s = s.replace('-', ' ')
    return string.capwords(s)

DB_PATH = 'allitebooks.xlsx'
if os.path.exists(DB_PATH):
    DB = pandas.read_excel(DB_PATH, index_col = 0, dtype = {'link-bookpage' : str, 
                                             'link-pdf' : str, 
                                             'link-epub' : str})
    # drop duplicates in 'link-bookpage'
    DB.drop_duplicates(subset = 'link-bookpage', inplace = True)
    # repair nan index
    if nan in DB.index:
        temp_index = []
        for i in range(DB.shape[0]):
            if DB.index[i] is nan:
                if DB.ix[i, 'link-pdf'] is not nan:
                    url = DB.ix[i, 'link-pdf']
                elif DB.ix[-10, 'link-epub'] is not nan:
                    url = DB.ix[i, 'link-epub']
                else:
                    url = DB.ix[i, 'link-bookpage']
                name = FORMAT_TO_FILENAME(os.path.splitext(os.path.split(url.strip('/'))[1])[0])
                temp_index.append(name)
            else:
                temp_index.append(DB.index[i])
        DB.index = temp_index
    # raise if duplicates
    if DB.index.value_counts()[DB.index.value_counts() > 1].any():
        raise Exception('**************Duplicated rows exists: %s'
                        % list(DB.index.value_counts()[DB.index.value_counts() > 1].index))
else:
    DB = pandas.DataFrame()
# repair index name
if not DB.index.name:
    DB.index.name = 'name'

BUG_DBFILE = 'bug.xlsx'
BUG_DB = pandas.DataFrame()
BUG_DB.index.name = 'name'

BUG_FILE = 'bug.txt'
if not os.path.exists(BUG_FILE):
    f = open(BUG_FILE, 'wt')
    f.close()

DOWNLOADED_COUNT = 0
ERROR_COUNT = 0

START_PAGE_FILE = 'start_page.txt'

socket.setdefaulttimeout(60) #  set socket level default timeout as 60s

class firstParser(html.parser.HTMLParser):
    '''parse links like "http://www.allitebooks.com/page/2/"
get page link
'''
    def __init__(self):
        self.anchorlist = []
        super().__init__()

    def __find_Anchorlist__(self, attrs):
        # attrs: [(key1, val1), (key2, val2), ...]
        attrs = dict(attrs)
        if attrs.get('rel') == 'bookmark':
            if attrs.get('href') not in self.anchorlist:
                self.anchorlist.append(attrs['href'])
        
    def handle_startendtag(self, tag, attrs):
        self.__find_Anchorlist__(attrs)

    def handle_starttag(self, tag, attrs):
        self.__find_Anchorlist__(attrs)

class secondParser(html.parser.HTMLParser):
    '''parse links like "http://www.allitebooks.com/elixir-in-action-2nd-edition/"
get the real book link, and log it in a pandas.Series instance
'''
    def __init__(self, link):
        self.anchor = [] #may be both .pdf and .epub links
        self.in_dt = False
        self.in_dd = False
        self.in_book_description = False
        self.title = None
        self.content = None
        self.data = pandas.Series()
        self.common_bookname = None #temp storage for book name shared between 2 anchors(pdf, epub)
        self.data['link-bookpage'] = link
        self.data['downloaded (PDF)?'] = False
        self.data['downloaded (ePub)?'] = False
        self.data['Book Description'] = ''
        super().__init__()

    def __find_Anchor__(self, attrs):
        # attrs: [(key1, val1), (key2, val2), ...]
        global FORMAT_TO_LINK, FORMAT_TO_FILENAME
        attrs = dict(attrs)
        if attrs.get('target') == '_blank':
            for ext in ('pdf', 'epub'):
                if attrs['href'].endswith(ext):
                    self.data['link-%s' % ext] = attrs['href']
                    if FORMAT_TO_LINK(os.path.splitext(os.path.split(attrs['href'])[1])[0]) \
                            not in self.data['link-bookpage']:
                        book_name = '.'.join(
                                [FORMAT_TO_FILENAME(os.path.split(self.data['link-bookpage'].strip('/'))[1]), 
                                 ext])
                    else:
                        book_name = os.path.split(attrs['href'])[1]
                        self.common_bookname = os.path.splitext(book_name)[0]
                        self.data.name = self.common_bookname #use book name as the index of DataFrame rows.
                    book_link = '&&&'.join([book_name, attrs['href']])
                    self.anchor.append(book_link) #format: "name&&&link"
        
    def handle_startendtag(self, tag, attrs):
        self.__find_Anchor__(attrs)

    def handle_starttag(self, tag, attrs):
        self.__find_Anchor__(attrs)
        if tag == 'dt':
            self.in_dt = True
        elif tag == 'dd':
            self.in_dd = True
        elif tag == 'div':
            attrs = dict(attrs)
            if attrs.get('class') == 'entry-content':
                self.in_book_description = True

    def handle_endtag(self, tag):
        if self.in_dt and (tag == 'dt'):
            self.in_dt = False
        elif self.in_dd and (tag == 'dd'):
            self.in_dd = False
        elif self.in_book_description and (tag == 'div'):
            self.in_book_description = False
        
    def handle_data(self, data):
        if self.in_dt:
            self.title = data.strip(':')
        elif self.in_dd and data.strip():
            if self.title != 'Category':
                self.data[self.title] = data.strip()
            else:
                self.data['CATG***' + data.strip()] = True #book categories
        elif self.in_book_description:
            if data.strip() and (data.strip(':') != 'Book Description'):
                self.data['Book Description'] += (data.strip() + '\n')

    def close(self):
        if self.common_bookname:
            for i in range(len(self.anchor)):
                self.anchor[i] = self.common_bookname + \
                    os.path.splitext(self.anchor[i].split('&&&')[0])[1] + \
                    '&&&' + self.anchor[i].split('&&&')[1]
        #finally ,if anchor exists, but no anchor with book_name same as the parent link.
        if (not self.data.name) and self.anchor: 
            self.data.name = os.path.splitext(self.anchor[0].split('&&&')[0])[0] 

class LinkProducer(Thread):
    '''a producer, which generates real .pdf link to be dowloaded, and puts it into queue.
'''
    link_count = 0
    
    def __init__(self, headers, queue, parallels, lock, 
                 name = 'LinkProducer', updating = False, start_page = None):
        super().__init__(name = name)
        self.base_url = 'http://www.allitebooks.com/page'
        self.headers = headers
        self.pdf_links = queue
        self.parallels = parallels
        self.lock = lock
        self.updating = updating
        self.start_page = start_page

    def __check_DB__(self):
        '''check database (loaded from allitebooks.xlsx) before parsing
'''
        global DB, BUG_FILE, FORMAT_TO_LINK
        print('checking database...')
        with open(BUG_FILE) as f:
            err_log = f.read()
        for bookname in DB.index:
            if not DB.loc[bookname, 'File format']:
                s = FORMAT_TO_LINK(bookname)
                link = '/'.join([self.base_url, s])
                if link not in err_log:
                    self.__parse_link__(link)
            elif ('pdf' in DB.loc[bookname, 'File format'].lower()) and \
               (not DB.loc[bookname, 'downloaded (PDF)?']):
                if DB.loc[bookname, 'link-pdf'] not in err_log:
                    self.__parse_link__(DB.loc[bookname, 'link-bookpage'])
            elif ('epub' in DB.loc[bookname, 'File format'].lower()) and \
               (not DB.loc[bookname, 'downloaded (ePub)?']):
                if DB.loc[bookname, 'link-epub'] not in err_log:
                    self.__parse_link__(DB.loc[bookname, 'link-bookpage'])
        print('database checked.\n', '=' * 80)                
    
    def __parse_link__(self, link): #generate real book .pdf link
        '''link, links like "http://www.allitebooks.com/elixir-in-action-2nd-edition/"
'''
        global DB, BUG_FILE, ERROR_COUNT
        while True:
            try:
                req_2 = urllib.request.Request(link, headers=self.headers)
                retval_2 = urllib.request.urlopen(req_2)
                parser_2 = secondParser(link)
                parser_2.feed(retval_2.read().decode(encoding='utf-8', errors='ignore'))
                parser_2.close()
                retval_2.close() #close response timely
                break
            except (ConnectionResetError, TimeoutError) as e:
                print('\n************************%s: %s\n\sleep a while and restart parsing...\n'
                      % (link, e))
                try:
                    retval_2.close() #close response timely, if necessary
                except:
                    pass
                sleep(3) #pause for each page processing, if Exception happens.
            except Exception as e:
                if ('timed out' in str(e)) or ('[Errno 11004] getaddrinfo failed' in str(e)):
                    print('\n************************%s: %s\nsleep a while and restart parsing...\n' 
                          % (link, e))
                    try:
                        retval_2.close() #close response timely, if necessary
                    except:
                        pass
                    sleep(3) #pause for each page processing, if Exception happens.
                else:
                    raise
        #logging info
        if self.lock.acquire():
            if not parser_2.anchor: # in case no links retrieved by parser_2
                ERROR_COUNT += 1
                msg = '\n(ERROR-%d) %s************************No pdf/epub anchor for [%s]\n' % (
                        ERROR_COUNT, ctime(), link)
                print(msg)
                with open(BUG_FILE, 'at') as f:
                    f.write(msg)
            else:
                if parser_2.data.name not in DB.index:
                    DB = DB.append(parser_2.data)
                else:
                    DB.loc[parser_2.data.name] = parser_2.data
            self.lock.release()
        #put .pdf link into queue
        for file_link in parser_2.anchor:
            self.pdf_links.put(file_link)
            self.__class__.link_count += 1
            print('~~link-%d~~ %s >> [%s] downloading...' 
                  % (self.__class__.link_count, ctime(), file_link.split('&&&')[0]))

    def run(self):
        '''get book links
'''
        global START_PAGE_FILE, DB, DB_PATH
        if self.start_page:
            page = self.start_page
        elif self.updating:
            print('regular updating @ %s' % ctime())
            page = 1
        else:
            print('first time running @ %s' % ctime())
            self.__check_DB__() # DB check first
            if os.path.exists(START_PAGE_FILE):
                with open(START_PAGE_FILE, 'rt') as f:
                    page = int(f.read())
            else:
                page = 1
        all_downloaded = False
        while not all_downloaded:
            print('\n\n%sProcessing Page-%d%s\n' % ('+' * 30, page, '+' * 30))
            try:
                url = '/'.join([self.base_url, str(page)])
                req_1 = urllib.request.Request(url, headers = self.headers)
                retval_1 = urllib.request.urlopen(req_1)
                parser_1 = firstParser()
                parser_1.feed(retval_1.read().decode(encoding = 'utf-8', errors = 'ignore'))
                parser_1.close()
                retval_1.close() #close response timely, if necessary
                updated = False
                for link in parser_1.anchorlist:
                    if 'link-bookpage' not in DB.columns:
                        self.__parse_link__(link)
                        updated = True
                    elif link not in list(DB['link-bookpage']): #'list' should be used here
                        self.__parse_link__(link)
                        updated = True
                    else:
                        #continue #first updating
                        if self.updating and (not self.start_page): #regular updating
                            all_downloaded = True
                            break
                if updated and self.lock.acquire():
                    while True:
                        try:
                            DB.to_excel(DB_PATH)
                            self.lock.release()
                            break
                        except:
                            pass
                page += 1
                if not self.updating:
                    with open(START_PAGE_FILE, 'wt') as f:
                        f.write(str(page))
                sleep(3) #regular pause after each page parsing.
            except (ConnectionResetError, TimeoutError) as e:
                print('\n************************%s: %s\nsleep a while and restart parsing...\n' 
                      % (url, e))
                try:
                    retval_1.close() #close response timely, if necessary
                except:
                    pass
                sleep(3) #pause for each page processing, if Exception happens.
            except Exception as e:
                if ('timed out' in str(e)) or ('[Errno 11004] getaddrinfo failed' in str(e)):
                    print('\n************************%s: %s\nsleep a while and restart parsing...\n' 
                          % (url, e))
                    try:
                        retval_1.close() #close response timely, if necessary
                    except:
                        pass
                    sleep(3) #pause for each page processing, if Exception happens.
                else:
                    print('\n************************%s: %s' % (url, e))
                    print('All pages (1~%d) have been parsed.' % (page - 1))
                    try:
                        retval_1.close() #close response timely, if necessary
                    except:
                        pass
                    all_downloaded = True
                    break
        for i in range(self.parallels): #produce signature to finish all downloaders.
            self.pdf_links.put('finished')
    
class Downloader(Thread):
    '''a consumer, which downloads file which linked to .pdf link from queue.
'''
    def __init__(self, headers, queue, book_dirname, lock, name = 'Downloader', err_links = False):
        super().__init__(name = name)
        self.headers = headers
        self.pdf_links = queue
        self.book_dirname = book_dirname
        self.lock = lock
        self.err_links = err_links # indicate error links downloader

    def __download_file__(self, file_link): #download single file
        '''file_link, like "filename&&&link''
'''
        global DB, DB_PATH, DOWNLOADED_COUNT, BUG_FILE, ERROR_COUNT
        if DOWNLOADED_COUNT % 100 == 0: #DEBUG-(1)
            temp_dir = os.path.join(r'C:\Windows\SysWOW64', self.book_dirname)
            if os.path.exists(temp_dir):
                rmtree(temp_dir)
        filename, link = file_link.split('&&&')
        filepath = os.path.join(self.book_dirname, filename)
        if os.path.exists(filepath) and os.path.getsize(filepath) != 0:
            if self.lock.acquire():
                if filename.endswith('.pdf'):
                    try:
                        DB.loc[os.path.splitext(filename)[0], 'downloaded (PDF)?'] = True
                    except:
                        print(filename)
                        print(DB.loc[os.path.splitext(filename)[0], 'downloaded (PDF)?'])
                elif filename.endswith('.epub'):
                    DB.loc[os.path.splitext(filename)[0], 'downloaded (ePub)?'] = True
                self.lock.release()
            return '\n[%s] already downloaded.' % filename
        else:
            while True:
                try:
                    with open(filepath, 'wb') as f:
                        try:
                            req = urllib.request.Request(link, headers=self.headers)
                            retval = urllib.request.urlopen(req)
                        except: # handle "HTTP Error 400: Bad Request"
                            alt_link = urllib.parse.quote(link, safe='/:')
                            req = urllib.request.Request(alt_link, headers=self.headers)
                            retval = urllib.request.urlopen(req)
                        f.write(retval .read())
                        retval.close() #close response timely
                    if self.lock.acquire():
                        if filename.endswith('.pdf'):
                            DB.loc[os.path.splitext(filename)[0], 'downloaded (PDF)?'] = True
                        elif filename.endswith('.epub'):
                            DB.loc[os.path.splitext(filename)[0], 'downloaded (ePub)?'] = True
                        DOWNLOADED_COUNT += 1
                        msg = '\n@@dowloaded-%d@@ %s :: %s >> [%s] downloaded' % (
                            DOWNLOADED_COUNT, ctime(), self.name, filename)
                        self.lock.release()
                    sleep(3) #regular pause after each downloading.
                    return msg
                except (ConnectionResetError, TimeoutError) as e:
                    print('\n************************%s >> %s: %s\nsleep a while and restart downloading...\n'
                          % (self.name, link, e))
                    try:
                        retval.close() #close response timely, if necessary
                    except:
                        pass
                    sleep(3) #pause a little while, if Exception happens.
                except Exception as e:
                    msg = '************************%s >> [%s] download failed: %s\n' % (
                            self.name, link, e)
                    if ('timed out' in msg) or ('IncompleteRead' in msg):
                        print('%ssleep a while and restart downloading...\n' % msg)
                        try:
                            retval.close() #close response timely, if necessary
                        except:
                            pass
                        sleep(3) #pause a little while, if Exception happens.
                    else:
                        try:
                            os.remove(filepath) #remove void file before close(), avoiding error closing.
                            retval.close() #close response timely, if necessary
                        except:
                            pass
                        ERROR_COUNT += 1
                        msg = '\n(ERROR-%d) %s%s' % (ERROR_COUNT, ctime(), msg)
                        if not self.err_links:
                            with open(BUG_FILE, 'at') as f:
                                f.write(msg)
                        return msg
    
    def run(self):
        '''download books
'''
        global BUG_DB
        while True:
            file_link = self.pdf_links.get()
            if file_link == 'finished':
                break
            else:
                msg = self.__download_file__(file_link)
                print(msg)
                if self.err_links:
                    if 'download failed' in msg:
                        name = os.path.splitext(file_link.split('&&&')[0])[0]
                        if self.lock.acquire():
                            BUG_DB.loc[name, 'Error Msg.'] = msg.split('download failed:')[1].strip()
                            self.lock.release()

class AllITeBooksCrawler(object):
    '''manage entire crawling process
'''
    def __init__(self, book_dirname = 'books', updating = False, start_page = None):
        self.h = {'User-Agent':'Mozilla/5.0'} 
        self.parallels = 8 # parallel downloading allowed
        self.q = Queue(maxsize = self.parallels) #.pdf / .epub links
        self.err_q = Queue() # error links to reparse
        self.book_dirname = book_dirname
        if not os.path.exists(self.book_dirname):
            os.makedirs(self.book_dirname)
        self.lock = Lock()
        self.updating = updating
        self.start_page = start_page #crawl from this page, in case any interrupt

    def go(self): # process links in queue
        producer_name = 'LinkUpdater' if self.updating else 'LinkProducer'
        producer = LinkProducer(self.h, self.q, self.parallels, self.lock, 
                                producer_name, self.updating, self.start_page)
        consumers = []
        for i in range(self.parallels):
            consumers.append(Downloader(self.h, self.q, self.book_dirname, self.lock, 
                                        name = 'Downloader-%d' % (i + 1)))

        producer.start()
        for i in range(self.parallels):
            consumers[i].start()

        producer.join()
        for i in range(self.parallels):
            consumers[i].join()
        print('All books downloaded!')
        self.reparse_Errors() #only reparse 'bug.txt' and 'wrong_books' if necessary
        self.statistic_DB()

    def reparse_Errors(self):
        # reparse all error links in BUG_FILE and fault files downloaded, 
        # to remedy or confirm, and build error database
        global BUG_FILE, DB, DB_PATH, BUG_DB, BUG_DBFILE, FORMAT_TO_FILENAME            
        url_db = DB[['link-pdf', 'link-epub']].reset_index()

        print('%s\nreparsing BUG_FILE...' % ('=' * 60))
        with open(BUG_FILE) as f:
            for line in f:
                if line.strip():
                    url = line.split('[')[1].split(']')[0]
                    unquoted_link = urllib.parse.unquote(url)
                    bug_item = pandas.Series()
                    if 'No pdf/epub anchor' in line:
                        name = FORMAT_TO_FILENAME(os.path.split(unquoted_link.strip('/'))[1])
                        bug_item = pandas.Series(name = name)
                        bug_item['Link']  = url
                        bug_item['Error Msg.'] = 'No pdf/epub anchor'
                    else:
                        for ext in ('pdf', 'epub'):
                            if ext in url:
                                try:
                                    name = url_db.set_index('link-%s' % ext).loc[url, 'name']
                                except:
                                    name = os.path.splitext(os.path.split(unquoted_link)[1])[0]
                                    print(name)
                                file = '.'.join([name, ext])
                                if os.path.exists(os.path.join(self.book_dirname, file)):
                                    print('[%s] already downloaded.' % file)
                                    break
                                else:
                                    bug_item = pandas.Series(name = name)
                                    bug_item['Link']  = url
                                    bug_item['Error Msg.'] = line.split('download failed:')[1].strip()
                                    #if ('HTTP Error 400' in line) or ('IncompleteRead' in line): #reparse some errors
                                    file_link = '&&&'.join([file, url]) #reparse all errors with .pdf / .epub links
                                    self.err_q.put(file_link)
                                    break
                    if bug_item.any():
                        if bug_item.name in BUG_DB.index:
                            BUG_DB.loc[bug_item.name] = bug_item
                        else:
                            BUG_DB = BUG_DB.append(bug_item)
            for i in range(self.parallels):
                self.err_q.put('finished')

            consumers = []
            for i in range(self.parallels):
                consumers.append(Downloader(self.h, self.err_q, self.book_dirname, self.lock,
                                        name = 'Downloader-%d(err_links)' % (i + 1), err_links = True))
            for i in range(self.parallels):
                consumers[i].start()
            for i in range(self.parallels):
                consumers[i].join()
            print('BUG_FILE reparsed.')

        print('Please check your directory (default "books\"), if there is any incorrect files that \
can NOT be opened, move it to another directory (default "wrong_books\")')
        while True:
            try:
                if input('If you finished, please confirm by [y]: ').strip()[0].lower() == 'y':
                    break
            except:
                pass
        if os.path.exists('wrong_books'):
            print('%s\nreparseing wrong books...' % ('=' * 60))
            for file in os.listdir('wrong_books'):
                print('Incorrect file: %s' % file)
                bug_item = pandas.Series(name = os.path.splitext(file)[0])
                ext = os.path.splitext(file)[1].strip('.')
                bug_item['Link'] = DB.loc[bug_item.name, 'link-%s' % ext]
                bug_item['Error Msg.'] = 'Bad file, cannot be opened'
                if bug_item.name in BUG_DB.index:
                    BUG_DB.loc[bug_item.name] = bug_item
                else:
                    BUG_DB = BUG_DB.append(bug_item)
            print('Wrong books reparsed.')

        # other errors manually found
        url = 'http://www.allitebooks.com/mysql-cookbook-3rd-edition/'
        bug_item = pandas.Series(name = FORMAT_TO_FILENAME(os.path.split(url.strip('/'))[1]))
        bug_item['Link']  = url
        bug_item['Error Msg.'] = 'Incorrect pdf links in the page'
        BUG_DB = BUG_DB.append(bug_item)

        BUG_DB = BUG_DB.sort_values('Error Msg.')
        BUG_DB.to_excel(BUG_DBFILE)
        DB.to_excel(DB_PATH)
        print('All error links in BUG_FILE and fault files downloaded successfully parsed.')

    def statistic_DB(self):
        # statistics of all kinds of books
        global DB
        catg_counts = pandas.Series(name = 'Books Counts')
        for idx in DB.columns:
            if idx.startswith('CATG'):
                catg_counts[idx.split('***')[1]] = pandas.value_counts(DB[idx]).iloc[0]
        catg_counts.sort_values(ascending = False, inplace = True)

        annual_sum = pandas.Series(name = 'Summary') # year sum
        for i in range(DB.shape[0]):
            year = DB.ix[i, 'Year']
            if ',' in year: # 'January 20, 2014'
                DB.ix[i, 'Year'] = year.split(',')[1].strip()
            elif '-' in year: # '2014-02-24'
                DB.ix[i, 'Year'] = year.split('-')[0].strip()
            elif '.' in year: # '20 Jun. 2009'
                DB.ix[i, 'Year'] = year.split('.')[1].strip()
        year_counts = pandas.value_counts(DB['Year'])
        for y in range(1900, 2030):
            if str(y) in year_counts.index:
                annual_sum[str(y)] = year_counts[str(y)]
        temp_cats = []
        final_cats = []
        for idx in DB.columns:
            if idx.startswith('CATG'):
                temp_cats.append(idx)
                final_cats.append(idx.split('***')[1])
        annuals = DB.groupby('Year').count().loc[annual_sum.index, temp_cats]
        annuals.columns = final_cats
        # tbd. cat sum row needed, and sort the dataframe by the sum row.
        # take care of the sum of the "annual_sum" column at last.
        annuals = annuals.join(annual_sum)

        writer = pandas.ExcelWriter('counts.xlsx')
        catg_counts.to_excel(writer, sheet_name = 'categories')
        annuals.to_excel(writer, sheet_name = 'annually summary')
        writer.save()

def main():
#    return  #debug
#    robot = AllITeBooksCrawler(updating = False) #first runing
#    robot.go()
    robot = AllITeBooksCrawler(updating = True, start_page = None) #regular updating
    robot.go()

if __name__ == '__main__':
    main()
