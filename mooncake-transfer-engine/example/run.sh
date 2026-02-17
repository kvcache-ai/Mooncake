rm -rf port.txt
rm -rf /root/ascend/log
export HCCL_INTRA_ROCE_ENABLE=1
source /usr/local/Ascend/ascend-toolkit/latest/bin/setenv.bash
source /usr/local/Ascend/driver/bin/setenv.bash
export ASCEND_BUFFER_POOL=0:0
python3 $@
