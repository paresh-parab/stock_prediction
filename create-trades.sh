#!/bin/sh

pip install numpy --user
pip install pandas --user
pip install matplotlib --user
pip install keras --user
pip install sklearn --user
pip install codecs --user
pip install bs4 --user

python main.py $1 $2 $3 $4