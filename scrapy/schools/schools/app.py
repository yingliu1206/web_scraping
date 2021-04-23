import json
from klein import route, run
from scrapy import signals
from scrapy.crawler import CrawlerRunner
from spiders.scrapy_vanilla import CharterSchoolSpider

class MyCrawlerRunner(CrawlerRunner):
    def crawl(self, crawler_or_spidercls, *args, **kwargs):
        self.items = []

        crawler = self.create_crawler(crawler_or_spidercls)

        crawler.signals.connect(self.item_scraped, signals.item_scraped)

        dfd = self._crawl(crawler, *args, **kwargs)

        dfd.addCallback(self.return_items)
        return dfd
    def item_scraped(self, item, response, spider):
        self.items.append(item)
    def return_items(self, result):
        return self.items

def return_spider_output(output):
    return json.dumps([dict(item) for item in output])

@route("/")
def schedule(request):
    runner = MyCrawlerRunner()
    spider = CharterSchoolSpider()
    deferred = runner.crawl(spider)
    deferred.addCallback(return_spider_output)
    return deferred

run('localhost', 8080)
