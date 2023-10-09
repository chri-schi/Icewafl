from src.Conditions.polluter_temporal_condition import TimeDependentPolluterCondition


class DefaultTimeDependentPolluterCondition(TimeDependentPolluterCondition):
    """
    Condition that always evaluates to true.
    """

    def condition(self, timestamp_ms: float) -> bool:
        return True
