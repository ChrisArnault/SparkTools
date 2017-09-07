
set col=%1
mongodump  --host 193.55.95.149:81 --db lsst --collection="${col}" --archive | mongorestore --host 134.158.75.222:27017  --username='lsst' --password='c.a@lal.200' --db lsst --archive | egrep -v 'egrep -v 'duplicate key error collection' >"copydb${col}".log

