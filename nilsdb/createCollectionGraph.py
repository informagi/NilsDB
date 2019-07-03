#!/usr/bin/env python3

from dbconnection import DBConnection
import argparse

class CreateCollectionGraph:

    def __init__(self, args):
        self.con = DBConnection(args.uri, args.user, args.password)
        self.write_dict(args.dict)

    def write_dict(self, dict_filename):
        query = """ 
           LOAD CSV FROM 'file:///{}' AS line FIELDTERMINATOR '|' 
           CREATE (:Term {{ term: line[1], df: toInteger(line[2])}})
        """.format(dict_filename)
        self.con.write(query)

    def write_terms(self, terms_filename):
        return

    def write_docs(self, docs_filename):
        return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--uri', default='bolt://localhost:7687', help='The connection URI',)
    parser.add_argument('-u', '--user', default='neo4j', help='Username')          
    parser.add_argument('-p', '--password', required=True, help='Password')

    parser.add_argument('-di', '--dict', default='dict.csv', help='Path to dict file')
    parser.add_argument('-t', '--terms', default='terms.csv', help='Path to terms file')
    parser.add_argument('-do', '--docs', default='docs.csv', help='Path to docs file')

    args, _ = parser.parse_known_args()
    CreateCollectionGraph(args) 
