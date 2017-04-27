# -*- coding: utf-8 -*-
import scrapy
from scrapy import Spider
from scrapy.http import Request
import os
import sys
import csv
import glob
import MySQLdb
from pprint import pprint
from time import gmtime, strftime

sys.path.insert(0, r'./spiders/config')
import config

sys.path.insert(0, r'./spiders/managers')
import quotes_s_manager

class QuotesSSpider(Spider):

    name = config.config_spider1_name

    allowed_domains = config.config_allowed_domains

    start_urls = config.config_start_urls


    def parse(self, response):
        main_url = config.config_main_url
        links = response.xpath('/html/body/div[@class="main-wrapper"]/div[@class="container"][4]/div[@class="ads-container"]/div[@class="all-ads-block"]/a[@class="item-link"]/@href').extract()

        for link in links:
            absolute_next_page_url = main_url+link
            yield scrapy.Request(absolute_next_page_url, callback=self.parse_page2)

        #for num in range(0, 1):
            #absolute_next_page_url = main_url + links[num]
            #yield scrapy.Request(absolute_next_page_url, callback=self.parse_page2)

    def parse_page2(self, response):
        curent_url=response.url
        imageURLs=response.xpath('/html/body/div[@class="main-wrapper"]/div[@class="content-panel"]/div[@class="panel-left"]/div[@class="container last"]/div[@class="content-wrapper "]/div[@class="content-right"]/div[@class="right-media"]/div[@class="mini-photos-container"]/div[@class="mini-container"]/div/@data-src').extract()

        picture=imageURLs[1]

        bread_crumb=response.xpath("/html/body/div[@class='main-wrapper']/div[@class='content-panel']/div[@class='panel-left']/div[@id='title']/div[@class='content-wrapper ']/div[@class='left']/div[@class='bread-crumb']/a/text()").extract()

        brand = bread_crumb[1]
        model = bread_crumb[2]

        price=response.xpath("/html/body/div[@class='main-wrapper']/div[@class='content-panel']/div[@class='panel-left']/div[@class='container last']/div[@class='content-wrapper ']/div[@class='content-left']/div[@class='params-block'][1]/div[@class='param'][1]/div[@class='right orange']/div[@class='price']/text()").extract()

        core_prop=response.xpath("/html/body/div[@class='main-wrapper']/div[@class='content-panel']/div[@class='panel-left']/div[@class='container last']/div[@class='content-wrapper ']/div[@class='content-left']/div[@class='params-block'][2]/div/div/text()").extract()

        colourName = "Spalva"
        colour = quotes_s_manager.extractCoreProp(colourName, core_prop)

        year = core_prop[1]
        yearSplit=year.split(' ')
        yearSplit_2 = yearSplit[0].split('/')
        carYear=yearSplit_2[0]+'-'+yearSplit_2[1]+'-00'

        gearBoxName = "Pavarų dėžė"
        gearBox = quotes_s_manager.extractCoreProp(gearBoxName, core_prop)

        runName = "Rida, km"
        runKm = quotes_s_manager.extractCoreProp(runName, core_prop)

        gasName = "Kuro tipas"
        gas = quotes_s_manager.extractCoreProp(gasName, core_prop)

        engine = core_prop[3]

        dates_info=response.xpath("/html/body/div[@class='main-wrapper']/div[@class='content-panel']/div[@class='panel-left']/div[@class='container last']/div[@class='content-wrapper ']/div[@class='content-left']/div[@class='params-block times']/div/div/text()").extract()

        publishDateName = "Įdėtas:"
        publishDate = quotes_s_manager.extractCoreProp(publishDateName, dates_info)

        yield {

            'brand': brand, #0
            'model': model, #1
            'colour': colour, #2
            'curent_url' : curent_url, #3
            'price': price, #4
            'picture': picture, #5
            'carYear': carYear, #6
            'gearBox': gearBox, #7
            'engine': engine, #8
            'runKm': runKm, #9
            'gas': gas, #10
            'publishDate':publishDate, #11

        }

    def close(self, reason):
        csv_file = max(glob.iglob('*.csv'), key=os.path.getctime)
        row_count = 0
        mydb = MySQLdb.connect(host=config.config_host, user=config.config_user, passwd=config.config_passwd, db=config.config_db)
        mydb.set_character_set('utf8')

        cursor = mydb.cursor()

        cursor.execute('SET NAMES utf8;')
        cursor.execute('SET CHARACTER SET utf8;')
        cursor.execute('SET character_set_connection=utf8;')
        createDatetimeVar=strftime("%Y-%m-%d %H:%M:%S", gmtime())
        with open(csv_file, 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                if row:
                    pprint (row)
                    if row_count != 0:
                        cursor.execute(
                            'INSERT IGNORE INTO advertisement(type, model, mark, colour, year, price, power, gear, url, picture, created_date, run, gas, publish_date) values(' + '"Lengvasis"' + ', "' +
                            row[1] + '", "' + row[0] + '", "' + row[2] + '", "' + row[6] + '", "' + row[4] + '", "' +
                            row[8] + '", "' + row[7] + '", "' + row[3] + '", "' + row[5] + '", "' + createDatetimeVar + '", "' + row[9] + '", "' + row[10] + '", "' + row[11] + '")')
                    row_count += 1
        mydb.commit()
        cursor.close()