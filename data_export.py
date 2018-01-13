import ujson as json
import glob
import os


SRC_DIR = './data'
DEST_DIR = './data/documents'
MEGABYTE = 1000 * 1000 * 1000

def filenames(data_dir):
    for fn in glob.glob("%s/*.jsonl" % data_dir):
        yield fn


def records(filename):
    with open(filename, 'r') as f:
        for line in f.readlines(MEGABYTE):
            yield json.loads(line)


def doc_filepath(dest_dir, tid, depth=2):
    basename = str(tid).zfill(10)
    dirname = os.path.join(dest_dir, *[basename[::-1][p:p + 2] for p in range(0, depth * 2, 2)])
    if not os.path.exists(dirname ):
        os.makedirs(dirname)
    return os.path.join(dirname, "%s.html" % basename)


def htmldoc(rec):
    return '<html><title>%s</title><body>%s</body></html>' % (rec['title'], rec['body'])


def main():
    os.makedirs(DEST_DIR, exist_ok=True)
    for fn in filenames(SRC_DIR):
        print("Reading %s ..." % fn, end='')
        for rec in records(fn):
            docpath = doc_filepath(DEST_DIR, rec['id'])
            with open(docpath, 'w') as f:
                f.write(htmldoc(rec))
        print(" done!" % fn)


if __name__ == '__main__':
    main()
