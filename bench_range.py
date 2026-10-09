import psycopg
from psycopg.types.range import Int8Range, Range
from psycopg.types.numeric import Int8
from time import sleep, monotonic
import statistics as st
from psycopg.types.range import Int8RangeBinaryDumper

conn = psycopg.connect('dbname=postgres user=postgres password=mysecretpassword host=localhost port=5432')
conn.cursor().execute('CREATE TEMPORARY TABLE range_test (id serial PRIMARY KEY, r int8range)')

def measure(func, args=None, kwargs=None, msg=None, repeat=10, relax=1.0):
    if repeat < 5:
        raise Exception('repeat must be greater than 4')
    durations = []
    for _ in range(repeat):
        start = monotonic()
        func(*(args or []), **(kwargs or {}))
        end = monotonic()
        durations.append(end - start)
        sleep(relax)
    mean = st.mean(durations)
    median = st.median(durations)
    sdev = st.stdev(durations)
    cv = sdev / mean
    msg = f"{func.__name__} {msg}" if msg else func.__name__
    print(f"{msg}: {round(mean, 5)} ±{round(sdev, 5)} (cv: {round(cv, 3)}, repeat: {repeat})")
    return {
        'raw': durations,
        'mean': mean,
        'median': median,
        'sdev': sdev,
        'cv': cv
    }


range_data = [(i, i+10) for i in range(1, 10000)]


def bench_range_dumping():
    dump = Int8RangeBinaryDumper(None).dump
    #return [dump(Int8Range(Int8(left), Int8(right))) for left, right in range_data]
    return [dump(Int8Range(left, right)) for left, right in range_data]
#measure(bench_range_dumping, repeat=50, relax=0)


def bench_range_db():
    range_data = [(i, i+10) for i in range(1, 10000)]
    with conn.cursor() as cur:
        with conn.transaction():
            with cur.copy(b'COPY "range_test" (r) FROM STDIN (FORMAT BINARY)') as copy:
                copy.set_types(['int8range'])
                for left, right in range_data:
                    #copy.write_row([Int8Range(Int8(left), Int8(right))])
                    copy.write_row([Int8Range(left, right)])
measure(bench_range_db, repeat=10, relax=0)
