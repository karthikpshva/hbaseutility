# hbaseutility

## 1. Compaction Status

Compaction status can be viewed from Hbase log or Regionservers GUI But if we've huge transaction, It is tough to track compacting region status from log. GUI only shows compacting status on the same node and tedious to view in GUI If we've 'n' regionservers

>> In one place, This utility collect online compacting region status across Regionservers to a given table.
