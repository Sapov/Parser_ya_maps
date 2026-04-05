import json
import re
import time

import aiohttp
import asyncio
from bs4 import BeautifulSoup as bs

from core.adb import AsyncDB
from core.db import DB


class ParseSite:
    def __init__(self, list_link_site: list):
        self.list_link_site = list_link_site

    async def get_page(self, itemd: dict) -> dict:
        async with aiohttp.ClientSession() as session:
            try:
                url = f"http://{itemd['site']}"
                item = {} | itemd
                async with session.get(url, ssl=False) as result:
                    try:
                        if result.status == 200:
                            page = await result.text()
                            print(f"---- Обработка страницы: {url} ----")
                            item["mail"] = self.search_mail(page)
                            item["whatsapp"] = self.search_wa_me(page)
                            item["telegram"] = self.search_telega(page)
                            itm = AsyncDB()
                            await itm.insert_data(item)
                        return item
                    except Exception as e:
                        print(f'Нет данных" {e}')
            except Exception as e:
                print(f"Сайт не ответил {e}")

    def search_mail(self, page):
        soup = bs(page, "lxml")
        emails = set()
        for index, link in enumerate(
                soup.find_all("a", attrs={"href": re.compile("^mailto:")})
        ):
            # Достаём email-адреса из тегов с mailto
            email = link.get("href").replace("mailto:", "")
            emails.add(email)
        to_mail = self.search_mail_in_text(soup.text)
        emails.add(to_mail)
        print("Нашлись адреса почты:", emails)
        print("=" * 30)
        return ', '.join(list(emails)) if len(emails) != 0 else ''

    def search_mail_in_text(self, text):
        mail = re.compile(r"[a-zA-Z0-9._%+-]+@+[a-zA-Z0-9._%+-]+(\.[a-zA-Z]{2,4})")
        mo = mail.search(text)
        print(mo.group() if mo else "")
        return mo.group() if mo else ""

    def search_wa_me(self, page):
        whatsapp = re.compile(r"https:\/\/wa.me\/\d{11}")
        whatsapp_2 = re.compile(r"https:\/\/wa.clck.bar\/\w+")
        mo = whatsapp.findall(page)
        mo1 = whatsapp_2.findall(page)
        lst = list(set(mo + mo1))
        print("НОМЕР WHATSSAPP", lst)
        return ' '.join(lst)

    def search_telega(self, page):
        telega = re.compile(r"https:\/\/t.me\/\w+")
        mo = telega.findall(page)
        lst = list(set(mo))
        print("НОМЕР TETELGRAMM", lst, "\n")
        return ' '.join(lst)

    def get_table_list(self):
        lst_old = DB().get_all_sites()
        return lst_old

    async def main(self):
        start = time.time()

        requests = [self.get_page(i) for i in self.list_link_site if i['site'] != '']
        lst = await asyncio.gather(*requests)
        print('Время выполнения: ', time.time() - start)

        return lst



def run():
    from core.db import DB

    item = DB()
    lst_old = item.get_all_sites()
    lst = asyncio.run(ParseSite(lst_old).main())
    return lst


if __name__ == '__main__':
    run()
