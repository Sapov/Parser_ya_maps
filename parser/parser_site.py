import json
import re
import time

import aiohttp
import asyncio
from bs4 import BeautifulSoup as bs


def run(lst:list):
    start = time.time()

    asyncio.run(ParseSite(lst).main())
    print('Время выполнения: ', time.time() - start)


class ParseSite:
    def __init__(self, list_link_site:list):

        self.list_link_site = list_link_site

    async def get_page(self, site: str) -> dict:
        async with aiohttp.ClientSession() as session:
            print(site)
            try:
                url = f"http://{site}"
                item= {}
                async with session.get(url, ssl=False) as result:
                    try:
                        if result.status == 200:
                            page = await result.text()
                            print(f"Почта со страницы {url}")
                            item["mail"] = self.search_mail(page)
                            item["whatsapp"] = self.search_wa_me(page)
                            item["telegram"] = self.search_telega(page)

                        return item
                    except:
                        print("Нет данных")
            except:
                print("Сайт не ответил")

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
        print("Почта со страницы", emails)

        print("=" * 30)
        return list(emails) if len(emails) != 0 else []

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
        return lst

    def search_telega(self, page):
        telega = re.compile(r"https:\/\/t.me\/\w+")
        mo = telega.findall(page)
        lst = list(set(mo))
        print("НОМЕР TETELGRAMM", lst, "\n")
        return lst

    async def main(self):
        requests = [self.get_page(i['site']) for i in self.list_link_site if i['site'] !='']
        print(requests)
        lst = await asyncio.gather(*requests)
        print(lst)
        return lst

    def add_in_base(self):
        pass