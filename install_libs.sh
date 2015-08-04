#!/bin/sh
sudo pip install python-instagram
sudo pip install cassandra-driver
sudo pip install schedule

## Install langid
git clone https://github.com/saffsd/langid.py.git
cd langid.py
sudo python3 setup.py install

cd ..
rm -rf langid.py