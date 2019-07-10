### Examples

Query to get all terms and documents from a topic, where all topic terms appear
all in the documents (like e.g. for conjunctive bm25)

```
WITH [591020,720333,462570] as ids MATCH (t:Term)-[a:appearsIn]->(d:Doc) WHERE t.termid in ids WITH d, size(ids) as inputCnt, count(DISTINCT t) as cnt, ids WHERE cnt = inputCnt WITH d, ids  MATCH (t)--(d) WHERE t.termid in ids WITH t,d return t,d
```

`t` and `d` can be used for further calculation instead of returning them.
