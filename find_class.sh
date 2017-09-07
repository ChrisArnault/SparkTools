
class=$1
echo "class=${class}"
# find /data2/spark-2.0.0-bin-hadoop2.6/jars/ -name '*.jar' -exec jar tvf {} \; -print | egrep -i -e data2 -e "${class}"
find . -name '*.jar' -exec jar tvf {} \| egrep -e "${class}" \; -print

