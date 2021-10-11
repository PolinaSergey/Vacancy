import requests
import psycopg2
import configparser
from bs4 import BeautifulSoup

link = "https://by.trud.com/employment_f/postoyannaya/"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36'}

INDUSTRIES_DB_NAME = 'industries'

ADD_INDUSTRY_RAW_SQL = '''
    INSERT INTO industries(name, link)
    VALUES(%s, %s); 
'''

ADD_SALARY_RAW_SQL = '''
    INSERT INTO salaries(industry_id, salary)
    VALUES(%s, %s); 
'''

GET_INDUSTRIES_LINKS_AND_IDS = '''
    SELECT id, link
    FROM industries
'''

DELETE_DATA_FROM_INDUSTRIES = '''
    DELETE FROM industries;
'''
DELETE_DATA_FROM_SALARIES = '''
    DELETE FROM salaries;
'''



def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    full_page = requests.get(link, headers=headers).text
    soup = BeautifulSoup(full_page, "html.parser")
    industry_block = soup.find_all('ul', {'data-gatrackname': 'similarOtherCategoriesJobClick'})
    industries = industry_block[0].find_all('a')

    connection = psycopg2.connect(
        host=config['DATABASE']['host'],
        database=config['DATABASE']['database'],
        user=config['DATABASE']['user'],
        password=config['DATABASE']['password']
    )

    cursor = connection.cursor()

    #TODO getting industries into func

    # cursor.execute(DELETE_DATA_FROM_INDUSTRIES)
    # # cursor.execute(DELETE_DATA_FROM_SALARIES)
    # for industry in industries:
    #     cursor.execute(ADD_INDUSTRY_RAW_SQL, (industry.get_text(), industry['href'],))
    # connection.commit()

    cursor.execute(GET_INDUSTRIES_LINKS_AND_IDS)

    #TODO parallelize scraping??
    for ind, lnk in cursor:
        i = 1
        salary_list = []
        while i<2:
            response = requests.get(lnk + '/page/' + str(i), headers=headers)
            if response.history:
                break
            i += 1
            #print(i)
            soup = BeautifulSoup(response.text, "html.parser")
            salary_blocks = soup.find_all('span',{'class': 'link-glyph salary'})
            salary_list = salary_list + list(map(lambda a: a.get_text(), salary_blocks))

        cursor_salary = connection.cursor()
        for salary in salary_list:
            #TODO getting amount func (values '100-500 Br')
            amount = salary.split()[0].split('-')[0]
            cursor_salary.execute(ADD_SALARY_RAW_SQL, (ind, amount,))
        connection.commit()
        cursor_salary.close()
        #print(salary_list)

    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()