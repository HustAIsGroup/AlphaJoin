#!/bin/sh
/bin/systemctl restart  postgresql-10
echo 3 > /proc/sys/vm/drop_caches
echo  finish

