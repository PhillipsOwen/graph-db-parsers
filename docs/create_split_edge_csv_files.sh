#!/bin/sh
echo "processing edges..."
sed -n -e '2,6000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt1.csv
rm edge_tmp.out && sed -n -e '6000001,12000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt2.csv
rm edge_tmp.out && sed -n -e '12000001,18000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt3.csv
rm edge_tmp.out && sed -n -e '18000001,24000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt4.csv
rm edge_tmp.out && sed -n -e '24000001,30000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt5.csv
rm edge_tmp.out && sed -n -e '30000001,36000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt6.csv
rm edge_tmp.out && sed -n -e '36000001,42000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt7.csv
rm edge_tmp.out && sed -n -e '42000001,48000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt8.csv
rm edge_tmp.out && sed -n -e '48000001,54000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt9.csv
rm edge_tmp.out && sed -n -e '54000001,60000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt10.csv
rm edge_tmp.out && sed -n -e '60000001,66000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt11.csv
rm edge_tmp.out && sed -n -e '66000001,72000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt12.csv
rm edge_tmp.out && sed -n -e '72000001,78000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt13.csv
rm edge_tmp.out && sed -n -e '78000001,84000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt14.csv
rm edge_tmp.out && sed -n -e '84000001,90000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt15.csv
rm edge_tmp.out && sed -n -e '90000001,96000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt16.csv
rm edge_tmp.out && sed -n -e '96000001,102000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt17.csv
rm edge_tmp.out && sed -n -e '102000001,108000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt18.csv
rm edge_tmp.out && sed -n -e '108000001,114000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt19.csv
rm edge_tmp.out && sed -n -e '114000001,120000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt20.csv
rm edge_tmp.out && sed -n -e '120000001,126000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt21.csv
rm edge_tmp.out && sed -n -e '126000001,132000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt22.csv
rm edge_tmp.out && sed -n -e '132000001,138000000p' ./common/rk-edges.csv > edge_tmp.out && cat ./common/rk-edge-header-cols.csv edge_tmp.out >> rk-edges-pt23.csv
rm edge_tmp.out
