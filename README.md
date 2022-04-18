# RedissonStreamTestA
A chance to Tinker with Redisson and see how it works with Redis Streams.

* Want to spawn multiple consumerGroups that each target the same Stream
* Have to ensure that all consumer groups get all the events

### To check on the Stream using redis-cli -- execute:
* XINFO STREAM X:dataUpdates
* XINFO GROUPS X:dataUpdates
* XINFO CONSUMERS X:dataUpdates group_0
* XRANGE X:dataUpdates 1650270491021-0 + COUNT 3

### To execute against a local default redis install you can use: 
``` 
mvn compile exec:java
```

### If using a different install user / password you can provide either one or 3 args:
#### Order Matters...  
#### either just URL
#### or: URL USER PASS
```
mvn compile exec:java -Dexec.args="redis://192.168.0.9:6379"

mvn compile exec:java -Dexec.args="redis://127.0.0.1:6379 fred bigpassword"
```

### if you want to use more or less groups than the default of 2 groups - you can supply an additional Argument pair:
#### CHANGEGROUPSIZE [newsize]
Examples:
```
mvn compile exec:java -Dexec.args="redis://192.168.0.9:6379 CHANGEGROUPSIZE 4"

or:

mvn compile exec:java -Dexec.args="redis://127.0.0.1:6379 fred bigpassword CHANGEGROUPSIZE 3"
```
