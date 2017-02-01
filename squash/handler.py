from squash.exceptions import InvalidCommand
from squash.storage.bisect_storage import BisectStorageEngine


def bisect_storage_factory():
    return BisectStorageEngine()


class CommandHandler:
    VALID_COMMANDS = ('',)

    def __init__(self, storage_factory=bisect_storage_factory):
        self._storage = storage_factory()

    def handle_command(self, cmd, key, *args):
        if cmd not in self.VALID_COMMANDS:
            raise InvalidCommand(cmd)
