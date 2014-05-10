#!/bin/bash
#file   : travis.sh
#author : ning
#date   : 2014-05-10 16:54:43

DATE=`date +'%Y%m%d%H%M'`
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pip install nose
pip install redis
git clone https://github.com/idning/python-memcached
cd python-memcached && python setup.py install


CFLAGS="-ggdb3 -O0" autoreconf -fvi && ./configure --enable-debug=log && make && sudo make install

git clone git@github.com:idning/test-twemproxy.git

cp  src/nutcracker  test-twemproxy/_binaries/

cd test-twemproxy && nosetests -v

