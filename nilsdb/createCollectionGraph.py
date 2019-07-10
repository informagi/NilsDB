#!/usr/bin/env python3

from dbconnection import DBConnection
import argparse
import time 

class CreateCollectionGraph:

    def __init__(self, args):
        self.con = DBConnection(args.uri, args.user, args.password)
        self.write_prepare()
        self.write_dict(args.dict)
        self.write_docs(args.docs)
        a = time.time()
        self.write_terms(args.terms)
        print(time.time() - a)

    def write_prepare(self):
        qs = []
        qs.append("CREATE CONSTRAINT ON (t:Term) ASSERT t.termid IS UNIQUE")
        qs.append("CREATE CONSTRAINT ON (t:Term) ASSERT t.term IS UNIQUE")
        qs.append("CREATE CONSTRAINT ON (d:Doc) ASSERT d.docid IS UNIQUE")
        qs.append("CREATE CONSTRAINT ON (d:Doc) ASSERT d.collectionid IS UNIQUE")
        for q in qs:
            self.con.write(q)
        
    def write_dict(self, dict_filename):
        query = """ 
           USING PERIODIC COMMIT 10000
           LOAD CSV FROM 'file:///{}' AS line FIELDTERMINATOR '|' 
           CREATE (:Term {{ termid: toInteger(line[0]), term: line[1], df: toInteger(line[2]) }})
        """.format(dict_filename)
        self.con.write(query)

    def write_docs(self, docs_filename):
        query = """ 
           USING PERIODIC COMMIT 10000
           LOAD CSV FROM 'file:///{}' AS line FIELDTERMINATOR '|' 
           CREATE (:Doc {{ docid: toInteger(line[1]), collectionid: line[0], len: toInteger(line[2]) }})
        """.format(docs_filename)
        self.con.write(query)

    def write_terms(self, terms_filename):
        query = """ 
           USING PERIODIC COMMIT 10000
           LOAD CSV FROM 'file:///{}' AS line FIELDTERMINATOR '|' 
           MATCH (d:Doc),(t:Term)
           WHERE d.docid = toInteger(line[1]) AND t.termid = toInteger(line[0])
           CREATE (t)-[a:appearsIn {{count: toInteger(line[2])}}]->(d)
        """.format(terms_filename)
        self.con.write(query)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--uri', default='bolt://localhost:7687', help='The connection URI',)
    parser.add_argument('-u', '--user', default='neo4j', help='Username')          
    parser.add_argument('-p', '--password', required=True, help='Password')

    parser.add_argument('-di', '--dict', default='dict.csv', help='Path to dict file')
    parser.add_argument('-do', '--docs', default='docs.csv', help='Path to docs file')
    parser.add_argument('-t', '--terms', default='terms.csv', help='Path to terms file')

    args, _ = parser.parse_known_args()
    CreateCollectionGraph(args) 
