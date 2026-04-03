import asyncio

from core.db import DB
from parser.parser_card import ParserCard
from parser.parser_site import ParseSite, save_data
from parser.parser_ya_page import ParserPage


def start_parsing(category, city, quantity):
    search = ParserCard(category, city, quantity)
    search.run()
    ParserPage().run()
    item = DB()
    lst_old = item.get_all_sites()
    lst = asyncio.run(ParseSite(lst_old).main())
    save_data(lst)
    return lst
