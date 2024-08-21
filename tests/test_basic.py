import hashlib
import pydwarfs as pd
import pytest
import sys


class test_logger(pd.logger):
    def __init__(self, level=pd.logger.INFO):
        super().__init__(level)

    def write(self, level, msg, file, line):
        print(f"{level}: {msg} ({file}:{line})")


def make_fs(image):
    lgr = test_logger(pd.logger.VERBOSE)
    os = pd.os_access_generic()
    pm = pd.performance_monitor({"inode_reader_v2", "block_cache"})
    return pd.reader.filesystem(lgr, os, image, pm)


def is_sorted(l):
    return all(l[i] <= l[i + 1] for i in range(len(l) - 1))


@pytest.fixture
def data_fs():
    return make_fs("tests/data.dwarfs")


@pytest.fixture
def catdata_fs():
    return make_fs("tests/catdata.dwarfs")


@pytest.fixture
def data_fs_files():
    return {
        "",
        "bench.sh",
        "dev",
        "empty",
        "empty/alsoempty",
        "foo",
        "foo/1",
        "foo/1/2",
        "foo/1/2/3",
        "foo/1/2/3/4",
        "foo/1/2/3/4/5",
        "foo/1/2/3/4/5/6",
        "foo/1/2/3/4/5/6/7",
        "foo/1/2/3/4/5/6/7/8",
        "foo/1/2/3/4/5/6/7/8/9",
        "foo/1/2/3/4/5/6/7/8/9/a",
        "foo/1/2/3/4/5/6/7/8/9/b",
        "foo/1/2/3/4/5/6/7/8/9/blubb",
        "foo/1/2/3/4/5/6/7/8/9/c",
        "foo/1/2/3/4/5/6/7/8/9/d",
        "foo/1/2/3/4/5/6/7/8/9/e",
        "foo/1/2/3/4/5/6/7/8/9/f",
        "foo/1/2/3/4/5/6/7/8/9/g",
        "foo/1/2/3/4/5/6/7/8/9/h",
        "foo/1/2/3/4/5/6/7/8/9/i",
        "foo/1/2/3/4/5/6/7/8/9/j",
        "foo/1/2/3/4/5/6/7/8/9/k",
        "foo/1/2/3/4/5/6/7/8/9/l",
        "foo/1/2/3/4/5/z",
        "foo/1/2/3/4/y",
        "foo/1/2/3/copy.sh",
        "foo/1/2/3/x",
        "foo/1/2/xxx.sh",
        "foo/1/fmt.sh",
        "foo/bar",
        "foo/bla.sh",
        "foobar",
        "format.sh",
        "perl-exec.sh",
        "test.py",
        "unicode",
        "unicode/我爱你",
        "unicode/我爱你/☀️ Sun",
        "unicode/我爱你/☀️ Sun/Γειά σας",
        "unicode/我爱你/☀️ Sun/Γειά σας/مرحبًا",
        "unicode/我爱你/☀️ Sun/Γειά σας/مرحبًا/⚽️",
        "unicode/我爱你/☀️ Sun/Γειά σας/مرحبًا/⚽️/Карибського",
        "יוניקוד",
    }


@pytest.fixture
def data_fs_md5():
    return {
        "bench.sh": "c4159006a4c070a3ab4f4490f74c08ed",
        "foo/1/2/3/4/5/6/7/8/9/a": "60b725f10c9c85c70d97880dfe8191b3",
        "foo/1/2/3/4/5/6/7/8/9/b": "3b5d5c3712955042212316173ccf37be",
        "foo/1/2/3/4/5/6/7/8/9/blubb": "c4159006a4c070a3ab4f4490f74c08ed",
        "foo/1/2/3/4/5/6/7/8/9/c": "2cd6ee2c70b0bde53fbe6cac3c8b8bb1",
        "foo/1/2/3/4/5/6/7/8/9/d": "e29311f6f1bf1af907f9ef9f44b8328b",
        "foo/1/2/3/4/5/6/7/8/9/e": "9ffbf43126e33be52cd2bf7e01d627f9",
        "foo/1/2/3/4/5/6/7/8/9/f": "9a8ad92c50cae39aa2c5604fd0ab6d8c",
        "foo/1/2/3/4/5/6/7/8/9/g": "f5302386464f953ed581edac03556e55",
        "foo/1/2/3/4/5/6/7/8/9/h": "01fbdc44ef819db6273bc30965a23814",
        "foo/1/2/3/4/5/6/7/8/9/i": "372e25f23b5a8ae33c7ba203412ace30",
        "foo/1/2/3/4/5/6/7/8/9/j": "ddd5752b5fe66fd98fb9dcdee48b4473",
        "foo/1/2/3/4/5/6/7/8/9/k": "ccc87e7257869ad33a6a0bd9e28a4ae4",
        "foo/1/2/3/4/5/6/7/8/9/l": "12f54a96f64443246930da001cafda8b",
        "foo/1/2/3/4/5/z": "c4159006a4c070a3ab4f4490f74c08ed",
        "foo/1/2/3/4/y": "c4159006a4c070a3ab4f4490f74c08ed",
        "foo/1/2/3/copy.sh": "078a9b8877e67bb21a168ae86221b558",
        "foo/1/2/3/x": "c4159006a4c070a3ab4f4490f74c08ed",
        "foo/1/2/xxx.sh": "078a9b8877e67bb21a168ae86221b558",
        "foo/1/fmt.sh": "078a9b8877e67bb21a168ae86221b558",
        "foo/bar": "d41d8cd98f00b204e9800998ecf8427e",
        "foo/bla.sh": "c4159006a4c070a3ab4f4490f74c08ed",
        "format.sh": "078a9b8877e67bb21a168ae86221b558",
        "perl-exec.sh": "05a3b982d530bab882b4a60f76079993",
        "test.py": "4366c242bb8ecc3b46d81a2c94d21cfa",
        "unicode/我爱你/☀️ Sun/Γειά σας/مرحبًا/⚽️/Карибського": "9d53135f39f58186a35830ea0f58a661",
    }


def info_opts(level):
    opts = pd.reader.fsinfo_options()
    opts.features = pd.reader.fsinfo_features.for_level(level)
    opts.block_access = pd.reader.block_access_level.unrestricted
    return opts


def test_dump1(data_fs):
    with data_fs as fs:
        d = fs.dump(info_opts(1))
        print(d)
        assert d is not None
        assert d.startswith("DwarFS version 2.4")
        assert "created by: libdwarfs v0.6.2" in d
        assert "block size: 1 MiB" in d
        assert "inode count: 44" in d
        assert "options: mtime_only" in d
        assert "SECTION" not in d


def test_dump3(data_fs):
    with data_fs as fs:
        d = fs.dump(info_opts(3))
        print(d)
        assert d is not None
        assert d.startswith("DwarFS version 2.4")
        assert "SECTION num=0, type=BLOCK" in d
        assert "SECTION num=3, type=SECTION_INDEX" in d


def test_no_history(data_fs):
    with data_fs as fs:
        h = fs.get_history()
        print(h)
        assert h is None


def test_history(catdata_fs):
    with catdata_fs as fs:
        h = fs.get_history()
        print(h)
        assert h is not None
        assert len(h) == 2
        assert "arguments" in h[0]
        assert "timestamp" in h[0]


def test_metadata(data_fs):
    with data_fs as fs:
        m = fs.get_metadata()
        print(m)
        assert m is not None
        assert "chunk_table" in m
        assert "chunks" in m
        assert "dir_entries" in m


def test_walk(data_fs, data_fs_files):
    with data_fs as fs:
        paths = set()
        fs.walk(lambda dev: paths.add(dev.unix_path()))
        print(paths)
        assert paths == data_fs_files


def test_walk_data_order(catdata_fs):
    with catdata_fs as fs:
        paths = []
        fs.walk(lambda dev: paths.append(dev.unix_path()))
        print(paths)

        paths_data_order = []
        fs.walk_data_order(lambda dev: paths_data_order.append(dev.unix_path()))
        print(paths_data_order)

        assert paths != paths_data_order
        assert set(paths) == set(paths_data_order)

        def get_first_blocks(ps):
            fb = []
            for p in ps:
                dev = fs.find(p)
                assert dev is not None
                iv = dev.inode()
                if iv.is_regular_file():
                    info = fs.get_inode_info(iv)
                    assert info is not None
                    fb.append(info["chunks"][0]["block"])
            return fb

        fb_paths = get_first_blocks(paths)
        print(fb_paths)
        assert not is_sorted(fb_paths)

        fb_paths_data_order = get_first_blocks(paths_data_order)
        print(fb_paths_data_order)
        assert is_sorted(fb_paths_data_order)


def test_directory_iterator(data_fs):
    paths = []

    with data_fs as fs:
        dev = fs.find("foo/1")
        dv = fs.opendir(dev.inode())
        for dev in dv:
            print(dev.unix_path())
            paths.append(dev.unix_path())

    assert paths == ["foo/1/2", "foo/1/fmt.sh"]


def test_generator_dfs(data_fs, data_fs_files):
    paths = set()

    with data_fs as fs:
        for dev in fs.entries_dfs():
            paths.add(dev.unix_path())

    print(paths)
    assert paths == data_fs_files


def test_read(data_fs, data_fs_md5):
    hashes = {}

    with data_fs as fs:
        for dev in fs.entries_dfs():
            iv = dev.inode()
            if iv.is_regular_file():
                fh = fs.open(iv)
                data = fs.read(fh)
                md5 = hashlib.md5(data).hexdigest()
                print(f"{dev.unix_path()} -> {md5}")
                hashes[dev.unix_path()] = md5

    assert hashes == data_fs_md5


def test_readv(data_fs, data_fs_md5):
    hashes = {}

    with data_fs as fs:
        for dev in fs.entries_dfs():
            iv = dev.inode()
            if iv.is_regular_file():
                fh = fs.open(iv)
                md5 = hashlib.md5()
                futures = fs.readv(fh)
                for f in futures:
                    md5.update(f.get())
                print(f"{dev.unix_path()} -> {md5.hexdigest()}")
                hashes[dev.unix_path()] = md5.hexdigest()

    assert hashes == data_fs_md5
