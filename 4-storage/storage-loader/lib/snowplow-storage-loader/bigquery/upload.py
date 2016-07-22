import glob
import json
import os
import subprocess
import gzip
from optparse import OptionParser

BQ_CMD = 'bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON --schema=%s %s.%s %s'

def upload(in_dir, schema, dataset):
    in_glob = os.path.join(in_dir, '*.gzip')

    for in_file in list(glob.glob(in_glob)):
        table = os.path.basename(in_file).split('.')[0]
        table = 'table_' + table.replace('-', '')
        cmd = BQ_CMD % (schema, dataset, table, in_file)
        print cmd
        
        print 'Uploading', in_file
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)
        
        while p.poll() is None:
            l = p.stdout.readline()
            print l
        print p.stdout.read()


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--in", dest="in_dir")
    parser.add_option("-s", "--schema", dest="schema")
    parser.add_option("-d", "--dataset", dest="dataset")

    (options, args) = parser.parse_args()
    upload(in_dir=options.in_dir, schema=options.schema, dataset=options.dataset)
