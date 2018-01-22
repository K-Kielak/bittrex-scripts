

class InvalidTicksException(Exception):
    """Exception for errors raised by malformed or wrong ticks"""
    def __init__(self, msg=None):
        if msg is None:
            msg = "Error occured due to malformed or wrong ticks"

        super(InvalidTicksException, self).__init__(msg)