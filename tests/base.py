import os
from pathlib import Path
import random
import sys

sys.path.append("..")

from josie import JOSIE

random.seed(0)


def generate_sets(
    num_sets: int, min_size: int, max_size: int, universe_size: int
) -> list:
    return [
        (
            set_id,
            sorted(
                random.choices(
                    range(universe_size), k=random.randint(min_size, max_size)
                )
            ),
        )
        for set_id in range(num_sets)
    ]


def save_sets(sets: list, sets_path: Path):
    with open(sets_path, "w") as file:
        file.writelines(
            [
                str(_set_id) + " " + " ".join(map(str, _set)) + "\n"
                for _set_id, _set in sets
            ]
        )


def read_sets(sets_path: Path):
    with open(sets_path, "r") as file:
        sets = [list(map(int, line.split())) for line in file.readlines()]

        return [(s[0], s[1:]) for s in sets]


def test(num_sets: int, min_size: int, max_size: int, universe_size: int):
    sets = generate_sets(num_sets, min_size, max_size, universe_size)
    sets_path = Path(__file__).parent.joinpath("sets.txt")

    save_sets(sets, sets_path)

    db_config = {
        "database": "josie-testing",
        "username": "nanni",
        "password": "nanni",
        "host": "127.0.0.1",
        "port": 5442,
    }

    temp_spark_path = Path(__file__).parent.joinpath("spark_tmp")
    temp_spark_path.mkdir(parents=True, exist_ok=True)

    spark_config = {
        "spark.app.name": "JOSIE Testing",
        "spark.log.level": "ERROR",
        "spark.master": "local[4]",
        "spark.executor.memory": "100g",
        "spark.driver.memory": "20g",
        "spark.local.dir": temp_spark_path,
        "spark.driver.maxResultSize": "12g",
        "spark.jars.packages": "org.postgresql:postgresql:42.7.4",
        "spark.driver.extraClassPath": f"{os.environ['HOME']}/.ivy2/jars/org.postgresql_postgresql-42.7.3.jar",
    }

    index = JOSIE(db_config, None, True)
    index.open()
    index.index(sets_path, spark_config)

    print(index.db.count_sets())
    print(index.db.count_posting_lists())

    queries = [set_id for set_id, _ in sets][:100]
    _results = index.query(
        queries, k=10, force_sampling_cost=True, reset_cost_function_parameters=True
    )
    index.close()


def t1():
    print("\n" + " TEST 1 (small)".center(100, "=") + "\n")
    test(100, 10, 100, int(1e4))


def t2():
    print("\n" + " TEST 2 (small)".center(100, "=") + "\n")
    test(1000, 10, 100, int(1e5))


def t3():
    print("\n" + " TEST 3 (small-medium)".center(100, "=") + "\n")
    test(1000, 10, 1000, int(1e5))


if __name__ == "__main__":
    # t1()
    # t2()
    t3()
