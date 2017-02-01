class BaseError(Exception):
    def __init__(self, message):
        self.message = message


class InvalidCommand(Exception):
    def __init__(self, cmd):
        super().__init__("Invalid command {}".format(cmd))
