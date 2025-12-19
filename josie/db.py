import time

from sqlalchemy import (
    ARRAY,
    Column,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    create_engine,
    delete,
    func,
    insert,
    select,
    text,
    update,
)
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session

from .io import ListEntry, RawTokenSet

InvertedList: Table
Set: Table
Query: Table
ReadListCostSamples: Table
ReadSetCostSamples: Table


class DBHandler:
    def __init__(self, dbtag: str = "", **connection_info) -> None:
        self.dbtag = dbtag
        self.url = URL.create(**connection_info)
        self.username = self.url.username
        self.password = self.url.password
        self.engine = create_engine(self.url)

        # initial cost values
        self.min_read_cost = 1000000.0
        self.read_set_cost_slope = 1253.19054300781
        self.read_set_cost_intercept = -9423326.99507381
        self.read_list_cost_slope = 1661.93366983753
        self.read_list_cost_intercept = 1007857.48225696

        self.inverted_list_table_name = f"{self.dbtag}inverted_lists"
        self.sets_table_name = f"{self.dbtag}sets"
        self.query_table_name = f"{self.dbtag}queries"
        self.read_list_cost_samples_table_name = f"{self.dbtag}read_list_cost_samples"
        self.read_set_cost_samples_table_name = f"{self.dbtag}read_set_cost_samples"

    def execute_read(self, q, fetch, **kwargs):
        with Session(bind=self.engine) as session:
            results = session.execute(q, kwargs)
            match fetch:
                case "one":
                    return results.fetchone()
                case "all":
                    return results.fetchall()

    def execute_write(self, q, fetch, **kwargs):
        with Session(bind=self.engine) as session:
            results = session.execute(q, kwargs)
            match fetch:
                case "all":
                    results = results.fetchall()
                case "one":
                    results = results.fetchone()
                case "nofetch":
                    pass
            session.commit()
            session.close()
        return results

    def create_tables(self):
        global InvertedList, Set, Query, ReadListCostSamples, ReadSetCostSamples
        metadata = MetaData()

        inverted_list_table = Table(
            self.inverted_list_table_name,
            metadata,
            Column("token", Integer, primary_key=True),
            Column("frequency", Integer),
            Column("duplicate_group_id", Integer),
            Column("duplicate_group_count", Integer),
            Column("raw_token", LargeBinary),
            Column("set_ids", ARRAY(Integer)),
            Column("set_sizes", ARRAY(Integer)),
            Column("match_positions", ARRAY(Integer)),
        )

        set_table = Table(
            self.sets_table_name,
            metadata,
            Column("id", Integer, primary_key=True, index=True),
            Column("size", Integer),
            Column("num_non_singular_token", Integer),
            Column("tokens", ARRAY(Integer)),
        )

        query_table = Table(
            self.query_table_name,
            metadata,
            Column("id", Integer, primary_key=True, index=True),
            Column("tokens", ARRAY(Integer)),
        )

        read_list_cost_samples_table = Table(
            self.read_list_cost_samples_table_name,
            metadata,
            Column("token", Integer, primary_key=True, index=True),
            Column("frequency", Integer),
            Column("cost", Integer),
        )

        read_set_cost_samples_table = Table(
            self.read_set_cost_samples_table_name,
            metadata,
            Column("id", Integer, primary_key=True, index=True),
            Column("size", Integer),
            Column("cost", Integer),
        )

        Set = set_table
        Query = query_table
        InvertedList = inverted_list_table
        ReadListCostSamples = read_list_cost_samples_table
        ReadSetCostSamples = read_set_cost_samples_table

        metadata.reflect(self.engine)
        metadata.create_all(self.engine, checkfirst=True)

        for table_class in [
            InvertedList,
            Set,
            Query,
            ReadListCostSamples,
            ReadSetCostSamples,
        ]:
            try:
                self.execute_write(delete(table_class), "nofetch")
            except Exception as e:
                print(f"Failed to drop table {table_class}: {e}")
                continue

    def drop_tables(self):
        metadata = MetaData()
        metadata.reflect(self.engine)

        for _, table_cls in metadata.tables.items():
            self.execute_write(delete(table_cls), "nofetch")

    def load_tables(self):
        global InvertedList, Set, Query, ReadListCostSamples, ReadSetCostSamples
        metadata = MetaData()
        metadata.reflect(bind=self.engine)

        Set = metadata.tables[self.sets_table_name]
        Query = metadata.tables[self.query_table_name]
        InvertedList = metadata.tables[self.inverted_list_table_name]
        ReadListCostSamples = metadata.tables[self.read_list_cost_samples_table_name]
        ReadSetCostSamples = metadata.tables[self.read_set_cost_samples_table_name]

    def clear_query_table(self):
        with Session(self.engine) as session:
            session.query(Query).delete()
            session.commit()

    def add_queries_from_indexed_sets(self, table_ids: list[int]):
        self.execute_write(
            insert(Query).from_select(
                ["id", "tokens"],
                select(Set.c.id, Set.c.tokens).where(Set.c.id.in_(table_ids)),
            ),
            "nofetch",
        )

    def add_queries(self, queries: dict):
        self.execute_write(insert(Query).values(queries), "nofetch")

    def add_query(self, set_id: int, tokens: list[int]):
        values = [{"id": set_id, "tokens": tokens}]
        self.execute_write(insert(Query).values(values), "nofetch")

    def close(self):
        self.engine.dispose()

    def count_posting_lists(self) -> int:
        return self.execute_read(select(func.count()).select_from(InvertedList), "one")[
            0
        ]

    def count_sets(self) -> int:
        return int(self.execute_read(select(func.count(Set.c.id)), "one")[0])

    def max_duplicate_group_id(self):
        return self.execute_read(
            select(func.max(InvertedList.c.duplicate_group_id)), "one"
        )[0]

    def posting_lists__memproc(self):
        return self.execute_read(
            select(
                InvertedList.c.raw_token,
                InvertedList.c.token,
                InvertedList.c.frequency,
                InvertedList.c.duplicate_group_id,
            ),
            "all",
        )

    def posting_lists__diskproc(self, raw_token_set: RawTokenSet, ignore_self: bool):
        """
        Returns a list of posting lists to create the token table without loading all the data on memory
        """
        if ignore_self:
            q = f"""
                SELECT token, frequency - 1 AS count, duplicate_group_id
                FROM {InvertedList.name}
                WHERE token = ANY(:tokens) AND frequency > 1
                ORDER BY token ASC;
            """
        else:
            q = f"""
                SELECT token, frequency - 1 AS count, duplicate_group_id
                FROM {InvertedList.name}
                WHERE token = ANY(:tokens)
                ORDER BY token ASC;
            """
        return self.execute_read(text(q), "all", tokens=raw_token_set.tokens)

    def get_set_tokens(self, set_id):
        return self.execute_read(
            select(Set.c.tokens).filter(Set.c.id == set_id), "one"
        )[0]

    def get_set_tokens_by_suffix(self, set_id, start_pos):
        with Session(self.engine) as session:
            try:
                return session.execute(
                    select(Set.c.tokens[start_pos:1e9]).filter(Set.c.id == set_id)
                ).fetchone()[0]
            except Exception:
                return [
                    row
                    for i, row in enumerate(
                        session.execute(
                            select(Set.c.tokens).where(Set.c.id == set_id)
                        ).fetchone()[0]
                    )
                    if i >= start_pos
                ]

    # These two functions are used by JOSIE, but were defined in the original code
    # def get_set_tokens_by_prefix(self, set_id, end_pos):
    #     with Session(self.engine) as session:
    #         try:
    #             return session.execute(select(Set.c.tokens[1:end_pos]).filter(Set.c.id == set_id)).fetchone()[0]
    #         except:
    #             return (
    #                 row
    #                 for i, row in enumerate(session.execute(select(Set.c.tokens).filter(Set.c.id == set_id)).all())
    #                 if i <= end_pos
    #             )
    # def get_set_tokens_subset(self, set_id, start_pos, end_pos):
    #     with Session(self.engine) as session:
    #         try:
    #             return session.execute(select(Set.c.tokens[start_pos:end_pos]).filter(Set.c.id == set_id)).fetchone()[0]
    #         except:
    #             return (
    #                     row
    #                     for i, row in enumerate(session.execute(select(Set.c.tokens).filter(Set.c.id == set_id)).all())
    #                     if start_pos <= i <= end_pos
    #                 )

    def get_inverted_list(self, token: int) -> list[ListEntry]:
        results = self.execute_read(
            select(
                InvertedList.c.set_ids,
                InvertedList.c.set_sizes,
                InvertedList.c.match_positions,
            ).filter(InvertedList.c.token == token),
            "one",
        )
        set_ids, sizes, match_positions = results

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
        # TODO: translate this into sqlalchemy code
        q = f"""
        SELECT id, tokens, (
			SELECT array_agg(raw_token)
			FROM {self.inverted_list_table_name}
			WHERE token = any(tokens)
		) FROM {self.query_table_name}
        ORDER BY id
        """
        return self.execute_read(text(q), "all")

    def get_raw_tokens_from_to(self, from_: int, to_: int):
        return self.execute_read(
            select(InvertedList.c.token, InvertedList.c.raw_token).where(
                from_ <= InvertedList.c.token, InvertedList.c.token < to_
            ),
            "all",
        )

    def get_raw_tokens(self, tokens):
        q = f"""
            SELECT array_agg(raw_token)
            FROM {self.inverted_list_table_name}
            WHERE token = ANY(:tokens)
        """
        return [r[0] for r in self.execute_read(text(q), "all", tokens=tokens)][0]

    def get_queries_agg_id(self):
        return self.execute_read(select(func.array_agg(Query.c.id)), "one")[0]

    def insert_read_set_cost(self, set_id, size, cost):
        self.execute_write(
            insert(ReadSetCostSamples).values(id=set_id, size=size, cost=cost),
            "nofetch",
        )

    def insert_read_list_cost(self, min_freq, max_freq, sample_size_per_step):
        subq = (
            select(InvertedList.c.token, InvertedList.c.frequency)
            .where(
                min_freq <= InvertedList.c.frequency,
                InvertedList.c.frequency < max_freq,
            )
            .order_by(func.random())
            .limit(sample_size_per_step)
        )
        q = insert(ReadListCostSamples).from_select(["token", "frequency"], subq)
        self.execute_write(q, "nofetch")

    def count_token_from_read_list_cost(self, min_freq, max_freq):
        return self.execute_read(
            select(func.count(ReadListCostSamples.c.token)).where(
                min_freq <= ReadListCostSamples.c.frequency,
                ReadListCostSamples.c.frequency <= max_freq,
            ),
            "one",
        )[0]

    def get_array_agg_token_read_list_cost(self):
        return self.execute_read(
            select(func.array_agg(ReadListCostSamples.c.token)), "one"
        )[0]

    def update_read_list_cost(self, token, cost):
        self.execute_write(
            update(ReadListCostSamples)
            .values(cost=cost)
            .where(ReadListCostSamples.c.token == token),
            "nofetch",
        )

    def delete_cost_tables(self):
        self.execute_write(delete(ReadListCostSamples), "nofetch")
        self.execute_write(delete(ReadSetCostSamples), "nofetch")

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
        with Session(self.engine) as session:
            q = f"""SELECT regr_slope(cost, frequency), regr_intercept(cost, frequency)
                FROM {ReadListCostSamples.name};"""
            slope, intercept = session.execute(text(q)).fetchone()
            self.read_list_cost_slope = slope
            self.read_list_cost_intercept = intercept

            q = f"""SELECT regr_slope(cost, size), regr_intercept(cost, size)
                    FROM {ReadSetCostSamples.name};"""
            slope, intercept = session.execute(text(q)).fetchone()
            self.read_set_cost_slope = slope
            self.read_set_cost_intercept = intercept

    def are_costs_sampled(self):
        q = f""" 
            SELECT 
                (SELECT COUNT(*) FROM {ReadListCostSamples.name}),
                (SELECT COUNT(*) FROM {ReadSetCostSamples.name});"""
        return all(x != 0 for x in self.execute_read(text(q), "one"))

    def random_set_ids(self, n):
        q = select(Set.c.id).order_by(func.random()).limit(n)
        return self.execute_read(q, "all")

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
        # sample_set_ids = self.get_queries_agg_id()
        random_ids = list(self.random_set_ids(100))
        sample_set_ids = [row[0] for row in random_ids]

        for set_id in sample_set_ids:
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
