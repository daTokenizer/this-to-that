import os
import tempfile
import threading
import time
import pytest
from sources.file_source import FileSource

def test_file_source_non_continuous():
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tf:
        tf.write('line1\nline2\nline3\n')
        tf.flush()
        tf_name = tf.name
    try:
        src = FileSource()
        src.initialize({'filepath': tf_name})
        entries = src.get_entries()
        assert entries == ['line1', 'line2', 'line3']
        src.close()
    finally:
        os.remove(tf_name)

def test_file_source_continuous():
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tf:
        tf.write('start\n')
        tf.flush()
        tf_name = tf.name
    results = []
    def reader():
        src = FileSource()
        src.initialize({'filepath': tf_name, 'continuous': True, 'poll_interval': 0.1})
        gen = src.get_entries()
        for _ in range(3):
            results.append(next(gen))
        src.close()
    t = threading.Thread(target=reader)
    t.start()
    time.sleep(0.2)
    with open(tf_name, 'a') as tf:
        tf.write('next1\n')
        tf.flush()
        time.sleep(0.2)
        tf.write('next2\n')
        tf.flush()
    t.join(timeout=2)
    assert results == ['start', 'next1', 'next2']
    os.remove(tf_name)

