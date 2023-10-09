from src.Conditions.polluter_condition import PolluterCondition


class DefaultCondition(PolluterCondition):
    """
    Condition that always evaluates to true.
    """

    def condition(self, value) -> bool:
        return True
