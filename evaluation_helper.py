def log_value(tag: str = "logger"):
    """
    Method to create a custom map function that logs data tuples.
    """

    def log_value_sub(value):
        print(tag + ": " + str(value))
        return value

    return log_value_sub
