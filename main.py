import sys
from build_lstm import build_lstm
from parse_html import scrape_htmls
from parse_streaming import parse_streaming
from trader import print_top_trades

def executor():
    year = sys.argv[1]
    month1 = sys.argv[2]
    month2 = sys.argv[3]
    write_dir = sys.argv[4]

    pred_count = parse_streaming(year, month1, month2, write_dir)
    build_lstm(year, month1, month2, write_dir, pred_count)
    scrape_htmls(year, month1, month2, write_dir)
    print_top_trades(year, month1, month2, write_dir)

if __name__ == '__main__':
    executor()






