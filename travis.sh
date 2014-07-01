#!/bin/bash
#file   : travis.sh
#author : ning
#date   : 2014-05-10 16:54:43

#install
sudo pip install redis
sudo pip install nose
sudo pip install -e git://github.com/idning/python-memcached.git#egg=memcache
sudo apt-get install socat

#build twemproxy
CFLAGS="-ggdb3 -O0" autoreconf -fvi && ./configure --enable-debug=log && make 

#setup test-twemproxy
git clone https://github.com/idning/test-twemproxy.git

cp  src/nutcracker  test-twemproxy/_binaries/
cp `which redis-server` test-twemproxy/_binaries/
cp `which redis-cli` test-twemproxy/_binaries/
cp `which memcached` test-twemproxy/_binaries/

#run test
cd test-twemproxy/ && nosetests --nocapture  --nologcapture -v

