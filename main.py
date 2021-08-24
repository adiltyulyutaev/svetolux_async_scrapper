import requests
from bs4 import BeautifulSoup
import asyncio
import aiohttp
from mysql.connector import connect



CONNECT = connect()

count_all = 0
count_existed = 0

def get_product_id(product_title):
    select_query = '''select ID from wpd98_posts where post_title = '{product_title}'  and post_type = 'product' '''.format(product_title=product_title)
    cursor = CONNECT.cursor()
    try:
        cursor.execute(select_query)
    except Exception as e:
        print(e)
    rows = cursor.fetchall()
    return rows





def update_regular_price(price, product_id):
    select_query = '''
    UPDATE

        wpd98_postmeta

    SET

        meta_value = {price} -- новая цена

    WHERE

        meta_key = '_regular_price'
    and
        post_id = {product_id};'''.format(price=price, product_id=product_id)
    cursor = CONNECT.cursor()
    try:
        cursor.execute(select_query)
    except Exception as e:
        print(e)
    CONNECT.commit()
    print('_regular_price updated set ' + str(price))


def update_price(price, product_id):
    select_query = '''
    UPDATE

        wpd98_postmeta

    SET

        meta_value = {price} -- новая цена

    WHERE

        meta_key = '_price'
    and
        post_id = {product_id};'''.format(price=price, product_id=product_id)
    cursor = CONNECT.cursor()
    try:
        cursor.execute(select_query)
    except Exception as e:
        print(e)
    CONNECT.commit()
    print('_price updated set '+ str(price))


def get_all_pages():
    pages = []
    while True:
        try:
            response = requests.get('https://svetolux.kz/catalog')
            soup = BeautifulSoup(response.content, 'lxml')
            last_page = int(soup.find(class_='nums').find_all('a')[-1].text)
            pages = ['https://svetolux.kz/catalog/?PAGEN_1=' + str(i) for i in range(1, last_page)]
        except Exception as e:
            global CONNECT
            CONNECT.close()
            CONNECT = connect(host='185.113.134.81', user='lighthome_kz_655df', password='3K1q7E9u', database='lighthome_kz')
            print(e)
            continue
        break
    return pages



async def parse(pages, session):
    for page in pages:
        while True:
            try:
                async with session.get(page) as response:
                    text = await response.read()
                    soup = BeautifulSoup(text, 'lxml')
                    items = soup.find_all(class_='inner_wrap')
                    for item in items:
                        global count_all, count_existed
                        title = item.find(class_='item-title').a.text
                        try:
                            price = int(item.find(class_='price_value').text.replace(' ', '')) - 500
                        except:
                            price = None
                        if not price:
                            continue
                        product_id = get_product_id(title)
                        count_all+=1
                        if len(product_id) > 0:
                            count_existed+=1
                            for pr_id in product_id:
                                id_pr = list(pr_id)[0]
                                update_regular_price(price, id_pr)
                                update_price(price, id_pr)
                                print(id_pr, title, count_existed, count_all)
                        else:
                            # print('id not found', title, count_all)
                            continue

            except Exception as e:
                print(e)
                continue
            break

async def main():
    pages = get_all_pages()
    tasks = []
    async with aiohttp.ClientSession() as session:
        for i in range(5):
                task = asyncio.create_task(parse(pages[i::5], session))
                tasks.append(task)
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
