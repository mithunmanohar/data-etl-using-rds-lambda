#!usr/bin/env python

import sys
import os
import logging
import psycopg2

logging.getLogger(__name__)

class PgDb(object):
    def __init__(self, configs):
        self.postgres_conn = psycopg2.connect(**configs)
        self._postgres_cursor = self.postgres_conn.cursor()

    def get_cursor(self):
        """returns a cursor object"""
        return self.postgres_conn.cursor()

    def write_query(self, query, fetch_result=False):
        """
        Insert or update query
        """
        self._postgres_cursor.execute(query)
        self._postgres_conn.commit()
        if fetch_result:
            return self._postgres_cursor.fetchall()

    def write_many_query(self, query, fetch_result=False):
        """
        Insert or update query
        """
        self._postgres_cursor.executemany(query)
        self._postgres_conn.commit()
        if fetch_result:
            return self._postgres_cursor.fetchall()

    def read_query(self, query):
        """
        Select query
        """
        self._postgres_cursor.execute(query)
        return self._postgres_cursor.fetchall()


    # def __del__(self):
    #     if self._postgres_conn:
    #         self._postgres_conn.close()
