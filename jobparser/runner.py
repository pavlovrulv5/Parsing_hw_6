# runner создается для того, чтобы можно было в режиме отладки отсматривать работу паука (искать ошибки, проверять работу и т.д.)

"""
HW_LESSON_6_Scrapy
1) Доработать паука в имеющемся проекте, чтобы он формировал item по структуре:
*Наименование вакансии
*Зарплата от
*Зарплата до
*Ссылку на саму вакансию
*Сайт откуда собрана вакансия
И складывал все записи в БД(любую)
2) Создать в имеющемся проекте второго паука по сбору вакансий с сайта superjob.
Паук должен формировать item'ы по аналогичной структуре и складывать данные также в БД
"""


# Импорты из Скрапи:
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

# Импорты из нашего скрапи-проекта jobparser:
from jobparser import settings
from jobparser.spiders.hhru import HhruSpider
from jobparser.spiders.sjru import SjruSpider

if __name__ == '__main__':      # чтобы вызывать нижепрописанный процесс только из этого файла runner.py
    crawler_settings = Settings()
    crawler_settings.setmodule(settings)

    process = CrawlerProcess(settings=crawler_settings)
    process.crawl(HhruSpider)
    process.crawl(SjruSpider)

    process.start()