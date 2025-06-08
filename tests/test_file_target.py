import os
import tempfile
from targets.file_target import FileTarget

def test_file_target_write_multiple_entries():
    with tempfile.NamedTemporaryFile(mode='r+', delete=False) as tf:
        tf_name = tf.name
    try:
        tgt = FileTarget()
        tgt.initialize({'filepath': tf_name})
        entries = ['one', 'two', 'three']
        tgt.create_entries(entries)
        tgt.close()
        with open(tf_name, 'r') as f:
            lines = [line.rstrip('\n') for line in f]
        assert lines == entries
    finally:
        os.remove(tf_name)

def test_file_target_write_single_entry():
    with tempfile.NamedTemporaryFile(mode='r+', delete=False) as tf:
        tf_name = tf.name
    try:
        tgt = FileTarget()
        tgt.initialize({'filepath': tf_name})
        tgt.create_entries('single')
        tgt.close()
        with open(tf_name, 'r') as f:
            lines = [line.rstrip('\n') for line in f]
        assert lines == ['single']
    finally:
        os.remove(tf_name)

