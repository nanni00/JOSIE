import binascii
import logging
from pathlib import Path
from time import time
from typing import Optional

import mmh3
import numpy as np
import pyspark.storagelevel
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from tqdm import tqdm

from .core import JOSIEAlgorithm
from .db import DBHandler
from .io import RawTokenSet
from .log import init_logger
from .tokentable import TokenTableDisk, TokenTableMem
from .util import get_result_ids, get_result_overlaps


class JOSIE:
    def __init__(
        self,
        db_connection_info: dict,
        logfile: Optional[Path] = None,
        log_on_stdout: bool = True,
    ) -> None:
        self.connection_info = db_connection_info

        # Create the database handler
        self.db = DBHandler(**self.connection_info)

        init_logger(logfile, log_on_stdout)

    def index(self, sets_path: Path, spark_config: dict):
        logger = logging.getLogger("JOSIE")
        logger.info("Creating integer sets and inverted index tables...")
        self.db.create_tables(drop_before=True)

        conf = SparkConf().setAll(list(spark_config.items()))
        sc = SparkContext.getOrCreate(conf)
        spark = SparkSession(sparkContext=sc)
        assert hasattr(spark, "sparkContext"), (
            "Missing sparkContext attribute on sparkSession."
        )

        token_sets = (
            spark.sparkContext.textFile(sets_path.as_uri())
            .map(lambda line: line.strip().split(" "))
            .flatMap(
                lambda table_terms: [
                    (int(term), int(table_terms[0])) for term in table_terms[1:] if term
                ]
            )
        )

        posting_lists_sorted = (
            token_sets.groupByKey()
            .map(
                # (token, [set_idA, set_idB, set_idC, ...]) -> (token, [set_id1, set_id2, set_id3, ..., set_idZ])
                lambda t: (t[0], sorted(list(t[1])))
            )
            .map(
                # (token, set_ids) -> (token, set_ids, set_ids_hash)
                lambda t: (t[0], t[1], mmh3.hash_bytes(np.array(t[1]).tobytes()))
            )
            .sortBy(
                # t: (token, setIDs, hash)
                lambda t: (len(t[1]), t[2], t[1])
            )
            .zipWithIndex()
            .map(
                # t: ((rawToken, sids, hash), tokenIndex) -> (token_id, (raw_token, set_ids, set_ids_hash))
                lambda t: (t[1], t[0])
            )
            .persist(pyspark.StorageLevel.MEMORY_ONLY)
        )

        # create the duplicate groups
        duplicate_group_ids = (
            posting_lists_sorted.map(
                # t: (tokenIndex, (rawToken, sids, hash)) -> (token_index, (sids, hash))
                lambda t: (t[0] + 1, (t[1][1], t[1][2]))
            )
            .join(posting_lists_sorted)
            .map(
                lambda t: -1
                if t[1][0][1] == t[1][1][2] and t[1][0][0] == t[1][1][1]
                else t[0]
            )
            .filter(lambda i: i > 0)
            .union(spark.sparkContext.parallelize([0, posting_lists_sorted.count()]))
            .sortBy(lambda i: i)
            .zipWithIndex()
            .map(
                # returns a mapping from group ID to the
                # starting index of the group
                # (startingIndex, GroupID) -> (GroupID, startingIndex)
                lambda t: (t[1], t[0])
            )
        )

        # generating all token indexes of each group
        token_group_ids = duplicate_group_ids.join(  # (GroupIDLower, startingIndexLower) JOIN (GroupIDUpper, startingIndexUpper)
            duplicate_group_ids.map(
                # (GroupID, startingIndexUpper) -> (GroupID, startingIndexUpper)
                lambda t: (t[0] - 1, t[1])
            )
        )

        token_group_ids = token_group_ids.flatMap(
            # GroupID, (startingIndexLower, startingIndexUpper) -> (tokenIndex, groupID)
            lambda t: [(i, t[0]) for i in range(t[1][0], t[1][1])]
        ).persist(pyspark.StorageLevel.MEMORY_ONLY)

        # join posting lists with their duplicate group IDs
        posting_lists_with_group_ids = posting_lists_sorted.join(token_group_ids).map(
            # (tokenIndex, ((rawToken, sids, _), gid)) -> (token_index, (group_id, raw_token, sids))
            lambda t: (t[0], (t[1][1], t[1][0][0], t[1][0][1]))
        )

        # STAGE 2: CREATE INTEGER SETS
        # Create sets and replace text tokens with token index
        integer_sets = (
            posting_lists_with_group_ids.flatMap(
                # (tokenIndex, (group_id, raw_token, sids))
                lambda t: [(sid, t[0]) for sid in t[1][2]]
            )
            .groupByKey()
            .map(
                # (sid, tokenIndexes)
                lambda t: (t[0], sorted(t[1]))
            )
        )

        # STAGE 3: CREATE THE FINAL POSTING LISTS
        # Create new posting lists and join the previous inverted
        # lists to obtain the final posting lists with all the information
        posting_lists = (
            integer_sets.flatMap(
                lambda t: [
                    (token, (t[0], len(t[1]), pos)) for pos, token in enumerate(t[1])
                ]
            )
            .groupByKey()
            .map(
                # (token, sets)
                lambda t: (t[0], sorted(t[1], key=lambda s: s[0]))
            )
            .join(posting_lists_with_group_ids)
            .map(
                # (token, (sets, (gid, rawToken, _))) -> (token, rawToken, gid, sets)
                lambda t: (t[0], t[1][1][1], t[1][1][0], t[1][0])
            )
        )

        # STAGE 4: SAVE INTEGER SETS AND FINAL POSTING LISTS
        n_posting_lists = posting_lists.count()
        n_sets = integer_sets.count()
        logger.info(f"Total posting lists: {n_posting_lists}")
        logger.info(f"Total integer sets: {n_sets}")

        def _integer_set_format(t):
            sid, indices = t
            return (sid, len(indices), len(indices), indices)

        def _postinglist_format(t):
            token, raw_token, gid, sets = t
            byteatoken = binascii.hexlify(str(raw_token).encode("utf-8"))
            set_ids, set_sizes, set_pos = zip(*sets)

            set_ids = list(map(int, set_ids))
            set_sizes = list(map(int, set_sizes))
            set_pos = list(map(int, set_pos))

            return (
                int(token),
                len(sets),
                int(gid),
                1,
                byteatoken,
                set_ids,
                set_sizes,
                set_pos,
            )

        uri = self.db.jdbc_uri
        logger.info(f"Placing data at jdbc:{uri}")

        properties: dict[str, str] = {
            "user": self.db._username,
            "password": self.db._password,
            "driver": "org.postgresql.Driver",
        }

        (
            integer_sets.map(lambda t: _integer_set_format(t))
            .toDF(schema=["id", "size", "num_non_singular_token", "tokens"])
            .write.jdbc(
                f"jdbc:{uri}", self.db.sets_table_name, "append", properties=properties
            )
        )

        logger.info("Integer sets saved")

        (
            posting_lists.map(lambda t: _postinglist_format(t))
            .toDF(
                schema=[
                    "token",
                    "frequency",
                    "duplicate_group_id",
                    "duplicate_group_count",
                    "raw_token",
                    "set_ids",
                    "set_sizes",
                    "match_positions",
                ]
            )
            .write.jdbc(
                f"jdbc:{uri}",
                self.db.inverted_list_table_name,
                "append",
                properties=properties,
            )
        )

        logger.info("Posting lists saved")

        spark.stop()
        logger.info("Spark session stopped")

        logger.info("Sets indexing completed")

    def query(
        self,
        queries: list[dict] | list[int],
        k: int,
        reset_cost_function_parameters: bool = False,
        force_sampling_cost: bool = False,
        token_table_on_memory: bool = False,
        show_query_progress: bool = False,
    ):
        logger = logging.getLogger("JOSIE")

        logger.info("Clearing query table...")
        self.db.clear_query_table()

        logger.info(f"Adding {len(queries)} queries...")

        if isinstance(queries[0], int):
            logger.info("Using indexed sets with given IDs as query sets")
            self.db.add_queries_from_indexed_sets(queries)

            queries_rts = [
                RawTokenSet(
                    query_id,
                    self.db.get_set_tokens(query_id),
                    self.db.get_raw_tokens(self.db.get_set_tokens(query_id)),
                )
                for query_id in queries
            ]

        elif isinstance(queries[0], dict):
            logger.info("Using user's sets as query sets")
            self.db.add_queries(queries)

            queries_rts = [
                RawTokenSet(
                    query["id"],
                    query["tokens"],
                    self.db.get_raw_tokens(query["tokens"]),
                )
                for query in queries
            ]

        if not self.db.are_costs_sampled() or force_sampling_cost:
            logger.info("Deleting old cost tables values...")
            self.db.delete_cost_tables()
            logger.info("Sampling costs...")
            self.db.sample_costs()

        # reset the cost function parameters used by JOSIE
        # for the cost estimation
        if reset_cost_function_parameters:
            logger.info("Resetting cost function parameters...")
            self.db.reset_cost_function_parameters()

        # create the token table, on memory or on disk
        logger.info(
            f"Creating token table on {'memory' if token_table_on_memory else 'disk'}..."
        )
        tb = (
            TokenTableMem(self.db, True)
            if token_table_on_memory
            else TokenTableDisk(self.db, True)
        )

        logger.info(f"Begin experiment for {k=}...")

        perfs = []
        start = time()

        # execute the JOSIE algorithm for each query
        for q in tqdm(queries_rts):
            perfs.append(
                JOSIEAlgorithm(
                    self.db,
                    tb,
                    q,
                    k,
                    ignore_self=True,
                    verbose=show_query_progress,
                )
            )

        logger.info(
            f"Finished experiment for {k=} in {round((time() - start) / 60, 3)} minutes"
        )

        # create a list of tuples <dataset_id, overlap_size>
        # from JOSIE results. This is a simple and ready-to-use
        # version of them, which are more verbose.
        results = [
            [
                p[1].query_id,
                list(
                    zip(
                        eval(get_result_ids(p[1].results)),
                        eval(get_result_overlaps(p[1].results)),
                    )
                ),
            ]
            for p in perfs
        ]

        return results, perfs

    def clean(self):
        self.db.drop_tables()

    def open(self):
        self.db.open()

    def close(self):
        self.db.close()
