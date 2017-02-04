class BaseError(Exception):
    def __init__(self, message):
        self.message = message


class InvalidArguments(BaseError):
    def __init__(self, cmd, args):
        super().__init__("Invalid arguments {} for command {}"
                         "".format(args, cmd))
