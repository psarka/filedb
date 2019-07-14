from pathlib import Path

# TODO LRU logic


class LocalCache:
    def __init__(self, root_path, size):
        self.root_path = Path(root_path)
        self.size = size

    def path(self, *args):
        path_ = self.root_path.joinpath(*args)
        path_.parent.mkdir(parents=True, exist_ok=True)

        return path_
