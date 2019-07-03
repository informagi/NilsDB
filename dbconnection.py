#!/usr/bin/env python3

from neo4j import GraphDatabase
import argparse

class DBConnection(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def test_connection_with_message(self, message):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
        return greeting

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--uri', default='bolt://localhost:7687', help='The connection URI',)
    parser.add_argument('-u', '--user', default='neo4j', help='Username')
    parser.add_argument("-p", '--password', required=True, help='Password')

    args, _ = parser.parse_known_args()

    con = DBConnection(uri=args.uri, user=args.user, password=args.password)
    print(con.test_connection_with_message("hello world"))
    con.close()
