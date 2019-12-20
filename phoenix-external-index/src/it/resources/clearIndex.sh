#! /bin/sh
indexes=$(curl http://server1:9200/_cat/indices | cut -f3 -d' ')
for index in ${indexes}
do
    echo "delete index ${index}"
    curl -XDELETE http://server1:9200/${index}
done