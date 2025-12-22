import logging

import psycopg

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("psycopg").setLevel(logging.DEBUG)


class DBHandler:
    def __init__(self, **connection_info) -> None:
        self._database = connection_info.pop("database")
        self._username = connection_info.pop("username", "user")
        self._password = connection_info.pop("password", "user")
        self._hostname = connection_info.pop("localhost", "127.0.0.1")
        self._port = connection_info.pop("port", 5442)
        self._conn_spec = connection_info

        # default drivername for PostgreSQL
        self._drivername = "postgresql"

        self.uri = (
            f"{self._drivername}://{self._username}:{self._password}@{self._hostname}:{self._port}/{self._database}?"
            + " ".join(f"{key}={value}" for key, value in self._conn_spec.items())
        )

        self.conn: psycopg.Connection | None = None
        self._batch_size = 1000

        self.inverted_list_table_name = "inverted_lists"
        self.sets_table_name = "sets"
        self.query_table_name = "queries"

    def open(self):
        self.conn = psycopg.connect(self.uri)

    def close(self):
        if isinstance(self.conn, psycopg.Connection):
            self.conn.close()
        else:
            raise ConnectionError("Connection already closed.")

    def _exec_query(
        self, query, stream: bool = False, params=None, prepare: bool = False
    ):
        print("Inside the function!")
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

    def create_tables(self):
        q = f"""
            CREATE TABLE {self.inverted_list_table_name} (
                token INTEGER PRIMARY KEY,
                frequency INTEGER,
                duplicate_group_id INTEGER,
                duplicate_group_count INTEGER,
                raw_token BYTEA,
                set_ids INTEGER[],
                set_sizes INTEGER[],
                match_positions INTEGER[]
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

    def drop_tables(self):
        for table_name in [
            self.inverted_list_table_name,
            self.sets_table_name,
            self.query_table_name,
        ]:
            q = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
            self._exec_query(q)


if __name__ == "__main__":
    db_config = {
        "database": "testdb",
        "username": "nanni",
        "password": "nanni",
        "host": "127.0.0.1",
        "port": 5442,
    }

    dbh = DBHandler(**db_config)
    print("Opening connection...")
    dbh.open()
    print("Removing tables...")
    dbh.drop_tables()
    print("Creating new tables...")
    dbh.create_tables()
    print("Closing connection...")
    dbh.close()
