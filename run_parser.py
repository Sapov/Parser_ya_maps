import logging

from parser.parser_card import runing_parser


def run_parser(category: str, city: str, quantity:int):

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # search = ParserCard(category, city, quantity)
    # search.run()
    runing_parser(category, city, quantity)
    return 'wait'

if __name__ == '__main__':


    try:
        run_parser('Агентство недвижимости', "Тамбов", 2000)
    except Exception as e:
        print(f'Error {e}')