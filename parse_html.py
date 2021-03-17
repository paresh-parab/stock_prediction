#!/usr/bin/env python
# coding: utf-8


import sys
import os
import glob
import pandas as pd
import codecs
import csv
import re

from bs4 import BeautifulSoup

fund_data_folder = "fundamental_info"
access_rights = 0o755


def list_files(dir):
    r = []
    count1 = 0
    for root, dirs, files in os.walk(dir):
        for name in files:
            filepath = root + os.sep + name
            if (name.endswith(".html") and name != "index.html" and (
                    not name.endswith(".to.html") and filepath.find("profiles/Yahoo/US/01/p") >= 0)):

                f = open(filepath, encoding="windows-1255", errors="ignore")
                soup = BeautifulSoup(f, 'html.parser')
                if soup(text=lambda
                        t: "Statistics at a Glance" in t or "Trading Information" in t or "TRADING INFORMATION" in t):
                    r.append(filepath)
                    count1 += 1
                    # print("Found file :" + str(count1))
    return r


def parse_2001(target_file, all_files):
    # Reading all the html pages into a generator

    df_from_each_file = (pd.read_html(codecs.open(f, "rb", "windows-1255", errors="ignore")) for f in all_files)

    row_count = 0
    f = csv.writer(open(target_file, "w"))  # Open the output file for writing before the loop
    f.writerow(["Ticker", "Year_low", "Year_high", "Profit_Margin", "Book_value",
                "Daily_volume"])  # Write column headers as the first line
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 1000)
    with open(target_file, 'a') as f:

        writer = csv.writer(f)
        for company in df_from_each_file:
            try:
                row_count += 1
                ticker_head = company[1][0].to_string().split(":")
                if (len(ticker_head) > 1):
                    ticker = ticker_head[1][:-1]
                    year_week_low = float('nan')
                    year_week_high = float('nan')
                    Profit_Margin = float('nan')
                    book_value = float('nan')
                    daily_volume = float('nan')

                    if (len(company) >= 13 and company[11][0][0].find("Price and Volume") >= 0):
                        if (company[12][0][0].find("Price and Volume") >= 0):
                            year_week_low = company[12][company[12][0].str.contains('52-Week Low')][1].item()
                            year_week_high = company[12][company[12][0].str.contains('52-Week High')][1].item()
                            Profit_Margin = company[13][company[13][0].str.contains('Profit Margin')][1].item()
                            book_value = company[13][company[13][0].str.contains('Book Value')][1].item()
                            if (company[14][0].str.contains('Daily Volume').any()):
                                daily_volume = company[14][company[14][0].str.contains('Daily Volume')][1].item()
                        else:
                            year_week_low = company[11][company[11][0].str.contains('52-Week Low')][1].item()
                            year_week_high = company[11][company[11][0].str.contains('52-Week High')][1].item()
                            Profit_Margin = company[12][company[12][0].str.contains('Profit Margin')][1].item()
                            book_value = company[12][company[12][0].str.contains('Book Value')][1].item()
                            if (company[13][0].str.contains('Daily Volume').any()):
                                daily_volume = company[13][company[13][0].str.contains('Daily Volume')][1].item()

                    elif (len(company) >= 13 and company[11][0][0].find("Per-Share Data") >= 0):
                        Profit_Margin = company[11][company[11][0].str.contains('Profit Margin')][1].item()
                        book_value = company[11][company[11][0].str.contains('Book Value')][1].item()
                        year_week_low = company[10][company[10][0].str.contains('52-Week Low')][1].item()
                        year_week_high = company[10][company[10][0].str.contains('52-Week High')][1].item()
                        if (company[12][0].str.contains('Daily Volume').any()):
                            daily_volume = company[12][company[12][0].str.contains('Daily Volume')][1].item()

                    # print(company[11]) #Price and Volume, Stock Performance , Share-Related Items
                    # print(year_week_low,year_week_high)
                    # print(Profit_Margin,book_value)
                    # print(daily_volume)
                    # print("Parsing : " + str(row_count))
                    writer.writerow([ticker, year_week_low[1:], year_week_high[1:], Profit_Margin[:-1], book_value[1:],
                                     daily_volume])

            except:
                continue



def parse_non2001(target_file,all_files):
    count = 0

    f = csv.writer(open(target_file, "w"))  # Open the output file for writing before the loop
    f.writerow(["Ticker", "Year_low", "Year_high", "Profit_Margin", "Book_value",
                "Daily_volume"])  # Write column headers as the first line

    with open(target_file, 'a') as f:
        writer = csv.writer(f)

        for file in all_files:
            try:
                fin = open(file, encoding="windows-1255", errors="ignore")
                soup = BeautifulSoup(fin, 'html.parser')
                count += 1
                # print("Parsing : " + str(count))

                # writer.writerow([file[::-1][5:(file[::-1].index("/"))][::-1],soup.find("td", text=re.compile("^Beta")).find_next("tr").find("td").find_next("tr").find("td").find_next("tr").find("td").find_next("tr").find("td").find_next_sibling("td").text,soup.find("td", text=re.compile("^Beta")).find_next("tr").find("td").find_next("tr").find("td").find_next("tr").find("td").find_next_sibling("td").text,soup.find("td", text="Profit Margin (ttm):").find_next_sibling("td").text,soup.find("td", text="Book Value Per Share (mrq):").find_next_sibling("td").text,soup.find("td", text="Share Statistics").find_next("tr").find("td").find_next("tr").find("td").find_next_sibling("td").text])

                # writer.writerow([file[::-1][5:(file[::-1].index("/"))][::-1],soup.find("td", text=re.compile("^Beta")).find_next("tr").find("td").find_next("tr").find("td").find_next("tr").find("td").find_next("tr").find("td").find_next_sibling("td").text,soup.find("td", text=re.compile("^Beta")).find_next("tr").find("td").find_next("tr").find("td").find_next("tr").find("td").find_next_sibling("td").text,soup.find("td", text="Profit Margin (ttm):").find_next_sibling("td").text[:-1],soup.find("td", text="Book Value Per Share (mrq):").find_next_sibling("td").text.strip(),soup.find("td", text="Share Statistics ").find_next("tr").find("td").find_next("tr").find("td").find_next_sibling("td").text])

                writer.writerow([file[::-1][5:(file[::-1].index("/"))][::-1],
                                 soup.find("td", text=re.compile("^Beta")).find_next("tr").find("td").find_next(
                                     "tr").find("td").find_next("tr").find("td").find_next("tr").find(
                                     "td").find_next_sibling("td").text,
                                 soup.find("td", text=re.compile("^Beta")).find_next("tr").find("td").find_next(
                                     "tr").find("td").find_next("tr").find("td").find_next_sibling("td").text,
                                 soup.find("td", text="Profit Margin (ttm):").find_next_sibling("td").text[:-1],
                                 soup.find("td", text="Book Value Per Share (mrq):").find_next_sibling(
                                     "td").text.strip(),
                                 soup.find("td", text=re.compile("^Share Statistics")).find_next("tr").find(
                                     "td").find_next("tr").find("td").find_next_sibling("td").text])

                # print(file[::-1][5:(file[::-1].index("/"))][::-1])
                # print(soup.find("td", text=re.compile("^Beta")).find_next("tr").find("td").find_next("tr").find("td").find_next("tr").find("td").find_next_sibling("td").text)
                # # 52-Week Low
                # print(soup.find("td", text=re.compile("^Beta")).find_next("tr").find("td").find_next("tr").find("td").find_next("tr").find("td").find_next("tr").find("td").find_next_sibling("td").text)
                # # Book value
                # print(soup.find("td", text="Book Value Per Share (mrq):").find_next_sibling("td").text.strip())
                # #Profit Margin (ttm):
                # print(soup.find("td", text="Profit Margin (ttm):").find_next_sibling("td").text[:-1])
                # #Daily volume
                # print(soup.find("td", text="Share Statistics ").find_next("tr").find("td").find_next("tr").find("td").find_next_sibling("td").text)



            except:
                continue



def scrape_htmls(year, month1, month2, write_dir):
    input_month = month1
    write_path = write_dir
    parse_month = input_month.split("/")[-1]
    parse_year = input_month.split("/")[-2]

    target_path = os.path.join(write_path , fund_data_folder)
    target_file = os.path.join(target_path, "fundamental_" + parse_year + "_" + parse_month + ".csv")

    if not os.path.exists(target_path):
        os.mkdir(target_path)

    src_dir = "./" + parse_year + "/" + parse_month + "/profiles/Yahoo/US/01/p"
    # print("Parsing the files in directory: " + src_dir)

    all_files = list_files(src_dir)

    # print("Parsing no. of files = " + str(len(all_files)))

    if int(parse_year) == 2001:
        parse_2001(target_file, all_files)
    else:
        parse_non2001(target_file,all_files)
