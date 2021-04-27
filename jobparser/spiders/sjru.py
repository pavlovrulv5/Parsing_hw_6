import scrapy
from scrapy.http import HtmlResponse
from jobparser.items import JobparserItem


class SjruSpider(scrapy.Spider):
    name = 'sjru'
    allowed_domains = ['superjob.ru']
    start_urls = ['https://www.superjob.ru/vacancy/search/?keywords=Python']

    def parse(self, response: HtmlResponse):
        vacancy_links = response.xpath('//a[contains (@class, "_6AfZ9")]/@href').extract()
        next_page = response.xpath('//a[contains(@class , "f-test-link-Dalshe")]/@href').extract_first()

        for vacancy_link in vacancy_links:
            yield response.follow(vacancy_link, callback=self.parse_vacancies)
        if next_page:
            yield response.follow(next_page, callback=self.parse)
        # print()

    def parse_vacancies(self, response: HtmlResponse):
        title = response.xpath('//h1/text()').extract_first()
        salary = response.xpath('//span[@class="_1OuF_ ZON4b"]//text()').extract()
        link = response.url
        yield JobparserItem(title=title, salary=salary, link=link)
        # print()