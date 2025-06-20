#!/bin/sh
echo "processing nodes..."
sed -n -e '2,500000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt1.csv
rm node_tmp.out && sed -n -e '500001,1000000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt2.csv
rm node_tmp.out && sed -n -e '1000001,1500000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt3.csv
rm node_tmp.out && sed -n -e '1500001,2000000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt4.csv
rm node_tmp.out && sed -n -e '2000001,2500000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt5.csv
rm node_tmp.out && sed -n -e '2500001,3000000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt6.csv
rm node_tmp.out && sed -n -e '3000001,3500000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt7.csv
rm node_tmp.out && sed -n -e '3500001,4000000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt8.csv
rm node_tmp.out && sed -n -e '4000001,4500000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt9.csv
rm node_tmp.out && sed -n -e '4500001,5000000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt10.csv
rm node_tmp.out && sed -n -e '5000001,5500000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt11.csv
rm node_tmp.out && sed -n -e '5500001,6000000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt12.csv
rm node_tmp.out && sed -n -e '6000001,6500000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt13.csv
rm node_tmp.out && sed -n -e '6500001,7000000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt14.csv
rm node_tmp.out && sed -n -e '7000001,7500000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt15.csv
rm node_tmp.out && sed -n -e '7500001,8000000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt16.csv
rm node_tmp.out && sed -n -e '8000001,8500000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt17.csv
rm node_tmp.out && sed -n -e '8500001,9000000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt18.csv
rm node_tmp.out && sed -n -e '9000001,9500000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt19.csv
rm node_tmp.out && sed -n -e '9500001,10000000p' ./common/rk-nodes.csv > node_tmp.out && cat ./common/rk-node-header-cols.csv node_tmp.out >> rk-nodes-pt20.csv
rm node_tmp.out
