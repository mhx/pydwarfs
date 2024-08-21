from _pydwarfs.reader import *


class filesystem(filesystem_v2):
    def entries_dfs(self, dev=None, depth=0):
        if dev is None:
            dev = self.root()
        yield dev
        iv = dev.inode()
        if iv.is_directory():
            dv = self.opendir(iv)
            for dev2 in dv:
                yield from self.entries_dfs(dev2, depth + 1)
