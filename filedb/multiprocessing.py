import abc
from contextlib import contextmanager


class MultiprocessingMixin:
    _connected = False

    @abc.abstractmethod
    def _setup_connection(self):
        ...

    @abc.abstractmethod
    def _teardown_connection(self):
        ...

    @contextmanager
    def stay_connected(self):
        if not self._connected:
            self._setup_connection()
            self._connected = True
            try:
                yield
            finally:
                self._teardown_connection()
                self._connected = False
        else:
            yield

    def connect(self):
        self._setup_connection()
        self._connected = True

    def disconnect(self):
        self._teardown_connection()
        self._connected = False
