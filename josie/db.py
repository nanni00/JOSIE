import logging
import time
from typing import Optional

import psycopg

from .io import ListEntry, RawTokenSet


class DBHandler:
    def __init__(
        self, db_config: dict, prepend_table_name_tag: Optional[str] = None
    ) -> None:
        self._database = db_config.pop("database")
        self._username = db_config.pop("username", "user")
        self._password = db_config.pop("password", "user")
        self._host = db_config.pop("host", "127.0.0.1")
        self._port = db_config.pop("port", 5442)
        self._conn_spec = db_config

        # default drivername for PostgreSQL
        self._drivername = "postgresql"

        self.uri = (
            f"{self._drivername}://{self._username}:{self._password}@{self._host}:{self._port}/{self._database}?"
            + " ".join(f"{key}={value}" for key, value in self._conn_spec.items())
        )

        self.jdbc_uri = (
            f"{self._drivername}://{self._host}:{self._port}/{self._database}?user={self._username}&password={self._password}"
            + " ".join(f"{key}={value}" for key, value in self._conn_spec.items())
        )

        self.conn: psycopg.Connection | None = None
        self._batch_size = 1000

        # initial cost values
        self.min_read_cost = 1000000.0
        self.read_set_cost_slope = 1253.19054300781
        self.read_set_cost_intercept = -9423326.99507381
        self.read_list_cost_slope = 1661.93366983753
        self.read_list_cost_intercept = 1007857.48225696

        self.inverted_list_table_name = f"{prepend_table_name_tag}inverted_lists"
        self.sets_table_name = f"{prepend_table_name_tag}sets"
        self.query_table_name = f"{prepend_table_name_tag}queries"
        self.read_list_cost_samples_table_name = (
            f"{prepend_table_name_tag}read_list_cost_samples"
        )
        self.read_set_cost_samples_table_name = (
            f"{prepend_table_name_tag}read_set_cost_samples"
        )

    def open(self):
        self.conn = psycopg.connect(self.uri)

    def close(self):
        if isinstance(self.conn, psycopg.Connection):
            self.conn.close()

    def _exec_query(
        self, query, stream: bool = False, params=None, prepare: bool = False
    ):
        if not isinstance(self.conn, psycopg.Connection):
            raise TypeError("Database connection not opened yet.")

        if self.conn.closed:
            raise ConnectionError("Connection is closed.")

        if self.conn.broken:
            raise ConnectionError("Connection is broken")

        rv = None

        with self.conn.cursor() as cur:
            try:
                res = cur.execute(query, params, prepare=prepare)

                if stream:

                    def stream():
                        try:
                            while True:
                                rows = res.fetchmany(self._batch_size)
                                if rows:
                                    yield rows
                                else:
                                    break
                        except Exception as e:
                            self.conn.rollback()
                            raise e

                    return stream()
                else:
                    try:
                        rv = res.fetchall()
                    except psycopg.ProgrammingError:
                        rv = res.rowcount

            except Exception as e:
                self.conn.rollback()
                raise e
            else:
                self.conn.commit()

        if rv:
            return rv

    def create_tables(self, drop_before: bool = False):
        if drop_before:
            self.drop_tables()

        q = f"""
            CREATE TABLE {self.inverted_list_table_name} (
                token                   INTEGER PRIMARY KEY,
                frequency               INTEGER,
                duplicate_group_id      INTEGER,
                duplicate_group_count   INTEGER,
                raw_token               BYTEA,
                set_ids                 INTEGER[],
                set_sizes               INTEGER[],
                match_positions         INTEGER[]
            );
        """
        self._exec_query(q)

        q = f"""
            CREATE TABLE {self.sets_table_name} (
                id INTEGER PRIMARY KEY,
                size INTEGER,
                num_non_singular_token INTEGER,
                tokens INTEGER[]
            );
        """
        self._exec_query(q)

        q = f"""
            CREATE TABLE {self.query_table_name} (
                id INTEGER PRIMARY KEY,
                tokens INTEGER[]
            );
        """
        self._exec_query(q)

        q = f"""
            CREATE TABLE {self.read_list_cost_samples_table_name} (
                token INTEGER PRIMARY KEY,
                frequency INTEGER,
                cost INTEGER
            );
        """
        self._exec_query(q)

        q = f"""
            CREATE TABLE {self.read_set_cost_samples_table_name} (
                id INTEGER PRIMARY KEY,
                size INTEGER,
                cost INTEGER
            );
        """
        self._exec_query(q)

    def drop_tables(self):
        for table_name in [
            self.inverted_list_table_name,
            self.sets_table_name,
            self.query_table_name,
            self.read_list_cost_samples_table_name,
            self.read_set_cost_samples_table_name,
        ]:
            q = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
            self._exec_query(q)

    def clear_query_table(self):
        q = f"DELETE FROM {self.query_table_name}"
        return self._exec_query(q)

    def add_queries_from_indexed_sets(self, set_ids: list[int]):
        q = f"""
            INSERT INTO {self.query_table_name} (id, tokens)
            SELECT s.id, s.tokens 
            FROM {self.sets_table_name} AS s
            WHERE s.id = ANY(%(set_ids)s);
        """
        return self._exec_query(q, params={"set_ids": set_ids}, prepare=False)

    def _add_query(self, set_id: int, tokens: list[int]):
        q = f"INSERT INTO {self.query_table_name} VALUES(%(set_id)s, %(tokens)s;"
        return self._exec_query(
            q, params={"set_id": set_id, "tokens": tokens}, prepare=True
        )

    def add_queries(self, queries: dict):
        # self.execute_write(insert(Query).values(queries), "nofetch")
        q = f"""
            INSERT INTO {self.query_table_name}
            SELECT (set_id, tokens) FROM UNNEST (%(set_ids)s, %(tokens_lists)s) AS t;
        """

        set_ids, tokens_lists = zip(*queries)

        return self._exec_query(
            q, params={"set_ids": set_ids, "tokens_lists": tokens_lists}
        )

    def count_posting_lists(self, exact: bool = True) -> int:
        if exact:
            q = f"SELECT COUNT(*) FROM {self.inverted_list_table_name};"
        else:
            q = f"SELECT reltuples::bigint AS estimate FROM pg_class where relname = '{self.inverted_list_table_name}';"

        return self._exec_query(q)[0][0]

    def count_sets(self, exact: bool = True) -> int:
        if exact:
            q = f"SELECT COUNT(*) FROM {self.sets_table_name};"
        else:
            q = f"SELECT reltuples::bigint AS estimate FROM pg_class where relname = '{self.sets_table_name}';"

        return self._exec_query(q)[0][0]

    def max_duplicate_group_id(self):
        q = f"SELECT MAX(duplicate_group_id) FROM {self.inverted_list_table_name};"
        return self._exec_query(q, prepare=True)

    def posting_lists__memproc(self):
        q = f"SELECT raw_token, token, frequency, duplicate_group_id FROM {self.inverted_list_table_name};"
        return self._exec_query(q, prepare=True)

    def posting_lists__diskproc(self, raw_token_set: RawTokenSet, ignore_self: bool):
        """
        Returns a list of posting lists to create the token table without loading all the data on memory
        """
        if ignore_self:
            q = f"""
                SELECT token, frequency - 1 AS count, duplicate_group_id
                FROM {self.inverted_list_table_name}
                WHERE token = ANY(%(tokens)s) AND frequency > 1
                ORDER BY token ASC;
            """
        else:
            q = f"""
                SELECT token, frequency - 1 AS count, duplicate_group_id
                FROM {self.inverted_list_table_name}
                WHERE token = ANY(%(tokens)s)
                ORDER BY token ASC;
            """

        rv = self._exec_query(q, params={"tokens": raw_token_set.tokens})
        if rv is None:
            # this should be the case when a token is contained only
            # in one set
            return []
        return rv

    def get_set_tokens(self, set_id: int):
        q = f"SELECT tokens FROM {self.sets_table_name} WHERE id = %(set_id)s;"
        rv = self._exec_query(q, params={"set_id": set_id})
        return rv[0][0]

    def get_set_tokens_by_suffix(self, set_id: int, start_pos: int):
        try:
            q = f"SELECT tokens[%(start_pos)s:size] FROM {self.sets_table_name} WHERE id = %(set_id)s;"
            return self._exec_query(
                q, params={"start_pos": start_pos, "set_id": set_id}, prepare=True
            )[0][0]
        except Exception as e:
            logging.error(f"Strange error in get_set_tokens_by_suffix: {type(e)} - {e}")
            raise e

    def get_set_tokens_by_prefix(self, set_id: int, end_pos: int):
        try:
            q = f"SELECT tokens[1:%(end_pos)s] FROM {self.sets_table_name} WHERE id = %(set_id)s;"
            return self._exec_query(
                q, params={"set_id": set_id, "end_pos": end_pos}, prepare=True
            )[0][0]
        except Exception as e:
            logging.error(f"Strange error in get_set_tokens_by_prefix: {type(e)} - {e}")
            raise e

    def get_set_tokens_subset(self, set_id: int, start_pos: int, end_pos: int):
        try:
            q = f"SELECT tokens[%(start_pos)s:%(end_pos)s] FROM {self.sets_table_name} WHERE id = %(set_id)s;"
            return self._exec_query(
                q,
                params={"set_id": set_id, "start_pos": start_pos, "end_pos": end_pos},
                prepare=True,
            )[0][0]
        except Exception as e:
            logging.error(f"Strange error in get_set_tokens_by_subset: {type(e)} - {e}")
            raise e

    def get_inverted_list(self, token: int) -> list[ListEntry]:
        q = f"""
            SELECT set_ids, set_sizes, match_positions
            FROM {self.inverted_list_table_name} 
            WHERE token = %(token)s;
        """
        records = self._exec_query(q, params={"token": token}, prepare=True)
        set_ids, sizes, match_positions = records[0]

        entries = []
        for i in range(len(set_ids)):
            entry = ListEntry(
                set_id=set_ids[i],
                size=int(sizes[i]),
                match_position=int(match_positions[i]),
            )
            entries.append(entry)
        return entries

    def get_query_sets(self):
        q = f"""
            SELECT id, tokens, (
                SELECT array_agg(raw_token)
                FROM {self.inverted_list_table_name}
                WHERE token = ANY(tokens)
            ) FROM {self.query_table_name}
            ORDER BY id
        """
        return self._exec_query(q, prepare=True)

    def get_raw_tokens_from_to(self, from_: int, to_: int):
        q = f"""
            SELECT token, raw_token
            FROM {self.inverted_list_table_name}
            WHERE token >= %(from_)s
            AND token < %(to_)s;
        """
        return self._exec_query(q, params={"from_": from_, "to_": to_}, prepare=True)

    def get_raw_tokens(self, tokens: list[int]):
        q = f"""
            SELECT array_agg(raw_token)
            FROM {self.inverted_list_table_name}
            WHERE token = ANY(%(tokens)s)
        """
        return self._exec_query(q, params={"tokens": tokens}, prepare=True)

    def get_queries_agg_id(self):
        q = f"SELECT array_agg(id) FROM {self.query_table_name};"
        return self._exec_query(q, prepare=True)

    def insert_read_set_cost(self, set_id: int, size: int, cost: int | float):
        q = f"""
            INSERT INTO {self.read_set_cost_samples_table_name} (id, size, cost)
            VALUES (%(id)s, %(size)s, %(cost)s);
        """
        return self._exec_query(
            q, params={"id": set_id, "size": size, "cost": cost}, prepare=True
        )

    def insert_read_list_cost(
        self, min_freq: int, max_freq: int, sample_size_per_step: int
    ):
        q = f"""
            INSERT INTO {self.read_list_cost_samples_table_name} (token, frequency)
            SELECT token, frequency
            FROM {self.inverted_list_table_name}
            WHERE frequency >= %(min_f)s AND frequency < %(max_f)s
            ORDER BY RANDOM()
            LIMIT %(limit)s;
        """
        return self._exec_query(
            q,
            params={
                "min_f": min_freq,
                "max_f": max_freq,
                "limit": sample_size_per_step,
            },
            prepare=True,
        )

    def count_token_from_read_list_cost(self, min_freq: int, max_freq: int):
        q = f"""
            SELECT COUNT(token) 
            FROM {self.read_list_cost_samples_table_name}
            WHERE frequency >= %(min_f)s AND frequency <= %(max_f)s;
        """
        res = self._exec_query(
            q, params={"min_f": min_freq, "max_f": max_freq}, prepare=True
        )
        return res[0] if res else 0

    def get_array_agg_token_read_list_cost(self):
        q = f"SELECT array_agg(token) FROM {self.read_list_cost_samples_table_name};"
        res = self._exec_query(q, prepare=True)
        return res[0][0] if res else []

    def update_read_list_cost(self, token: int, cost: int | float):
        q = f"""
            UPDATE {self.read_list_cost_samples_table_name}
            SET cost = %(cost)s
            WHERE token = %(token)s;
        """
        return self._exec_query(q, params={"cost": cost, "token": token}, prepare=True)

    def delete_cost_tables(self):
        # We can combine these or run them sequentially
        for table in [
            self.read_list_cost_samples_table_name,
            self.read_set_cost_samples_table_name,
        ]:
            q = f"DELETE FROM {table};"
            self._exec_query(q)

    def read_list_cost(self, length: int) -> float:
        cost = self.read_list_cost_slope * float(length) + self.read_list_cost_intercept
        if cost < self.min_read_cost:
            cost = self.min_read_cost
        return cost / 1000000.0

    def read_set_cost(self, size: int) -> float:
        cost = self.read_set_cost_slope * float(size) + self.read_set_cost_intercept
        if cost < self.min_read_cost:
            cost = self.min_read_cost
        return cost / 1000000.0

    def read_set_cost_reduction(self, size: int, truncation: int) -> float:
        return self.read_set_cost(size) - self.read_set_cost(size - truncation)

    def reset_cost_function_parameters(self) -> None:
        q = f"""
            SELECT regr_slope(cost, frequency), regr_intercept(cost, frequency)
            FROM {self.read_list_cost_samples_table_name};
        """
        slope, intercept = self._exec_query(q)[0]
        self.read_list_cost_slope = slope
        self.read_list_cost_intercept = intercept

        q = f"""
            SELECT regr_slope(cost, size), regr_intercept(cost, size)
            FROM {self.read_set_cost_samples_table_name};
        """
        slope, intercept = self._exec_query(q)[0]
        self.read_set_cost_slope = slope
        self.read_set_cost_intercept = intercept

    def are_costs_sampled(self):
        q = f""" 
            SELECT 
                (SELECT COUNT(*) FROM {self.read_list_cost_samples_table_name}),
                (SELECT COUNT(*) FROM {self.read_set_cost_samples_table_name});
        """
        return all(x != 0 for x in self._exec_query(q))

    def random_set_ids(self, n):
        q = f"SELECT array_agg(id) FROM {self.sets_table_name} ORDER BY random() LIMIT %(n)s;"

        # return list(chain.from_iterable(self._exec_query(q, params={"n": n})))
        return self._exec_query(q, params={"n": n})[0][0]

    def sample_costs(
        self,
        min_length: int = 0,
        max_length: int = 20_000,
        step: int = 500,
        sample_size_per_step: int = 100,
    ):
        """
        Sample costs. Pay attention to tune the parameters in order to avoid sampling with zero-variance
        or similar numeric problems, that will end with NULL values for the results of regression computation
        """
        random_ids = self.random_set_ids(100)

        for set_id in random_ids:
            start = time.time()
            s = self.get_set_tokens(set_id)
            duration = time.time() - start  # Duration in seconds
            self.insert_read_set_cost(set_id, len(s), int(duration * 1e6))

        for l in range(min_length, max_length, step):
            self.insert_read_list_cost(l, l + step, sample_size_per_step)

        sample_list_tokens = self.get_array_agg_token_read_list_cost()
        for token in sample_list_tokens:
            start = time.time()
            _ = self.get_inverted_list(token)
            duration = time.time() - start
            self.update_read_list_cost(token, int(duration * 1e6))
