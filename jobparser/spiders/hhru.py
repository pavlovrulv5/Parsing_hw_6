import scrapy
from scrapy.http import HtmlResponse
from jobparser.items import JobparserItem

class HhruSpider(scrapy.Spider):
    name = 'hhru'
    allowed_domains = ['hh.ru']
    start_urls = ['https://hh.ru/search/vacancy?area=1&fromSearchLine=true&st=searchVacancy&text=python']

    def parse(self,response: HtmlResponse):
        vacancy_links = response.xpath('//div[contains(@class, "vacancy-serp-item")]//a[contains(@class, "HH-LinkModifier")]/@href').extract()
        next_page = response.xpath('//a[contains(@class, "-Pager-Controls-Next")]/@href').get()

        for vacancy_link in vacancy_links:
            yield response.follow(vacancy_link, callback=self.parse_vacancies)

        if next_page:
            yield response.follow(next_page, callback=self.parse)
        # print()

    def parse_vacancies(self, response: HtmlResponse):
        # обработку сделаем в piplines)
        title = response.xpath("//h1//text()").get()
        salary = response.xpath("//p[contains(@class, 'vacancy-salary')]//span/text()").getall()
        link = response.url
        yield JobparserItem(title=title, salary=salary, link = link) # создается экземпляр класса JobparserItem
        # print()