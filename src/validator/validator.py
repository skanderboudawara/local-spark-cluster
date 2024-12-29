from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class Validator:
    def __init__(self, dataframe: DataFrame) -> None:
        self._dataframe = dataframe

    def __getattr__(self, name: str) -> Any:
        """
        Delegate attribute access to the underlying DataFrame.
        """
        return getattr(self._dataframe, name)

    @staticmethod
    def _check_msg_strategy(msg: str, strategy: str) -> None:
        """
        This method is used to check the message and strategy arguments.
        """
        if not isinstance(msg, str):
            raise ValueError("Argument 'msg' must be a string.")
        if not isinstance(strategy, str):
            raise ValueError("Argument 'strategy' must be a string.")
        if strategy not in {"fail", "warn"}:
            raise ValueError("Argument 'strategy' must be either 'fail' or 'warn'.")

    @staticmethod
    def _print_or_raise(msg: str, strategy: str) -> None:
        """
        This method is used to print or raise an error based on the strategy.
        """
        if strategy == "fail":
            raise ValueError(msg)
        if strategy == "warn":
            pass
        else:
            raise ValueError("Invalid strategy.")

    def _check_failure(self, df: DataFrame, msg: str, strategy: str) -> None:
        """
        This method is used to check if the DataFrame is empty.
        """
        if not isinstance(df, DataFrame):
            raise ValueError("Argument 'df' must be a DataFrame.")
        self._check_msg_strategy(msg, strategy)
        if not df.isEmpty():
            self._print_or_raise(msg, strategy)

    def _condition_failure(self, condition: bool, msg: str, strategy: str) -> None:
        if not isinstance(condition, bool):
            raise ValueError("Argument 'condition' must be a boolean.")
        self._check_msg_strategy(msg, strategy)
        if condition:
            self._print_or_raise(msg, strategy)

    def checkPrimaryKey(self, strategy: str, *args: Any) -> "Validator":
        """
        Checks if the given columns form a valid primary key (no duplicates or null values).
        """
        # Ensure at least one column is provided
        if len(args) == 0:
            raise ValueError("At least one column must be provided for primary key validation.")

        # Check for duplicates based on the combination of columns
        invalid_count = self._dataframe.groupBy(*args).count().filter(col("count") > 1)
        self._check_failure(
            invalid_count,
            f"Columns {', '.join(args)} have duplicate values.",
            strategy,
        )
        return self

    def checkRegexCol(self, colB: str, pattern: str, strategy: str)-> "Validator":
        """
        Checks if all values in the given column match the provided regex pattern.
        """
        invalid_count = self._dataframe.filter(~col(colB).rlike(pattern))
        self._check_failure(
            invalid_count,
            f"Column '{colB}' has values that do not match the regex pattern '{pattern}'.",
            strategy,
        )

        return self

    def checkEqualityCol(self, colA: str, colB: str, strategy: str) -> "Validator":
        """
        Checks if all values in the given column match the provided regex pattern.
        """
        invalid_count = self._dataframe.filter(~(col(colB) == col(colA)))
        self._check_failure(
            invalid_count,
            f"Column '{colA}' does not equal '{colB}'",
            strategy,
        )
        return self

    def checkCount(self, row_count: int, strategy: str) -> "Validator":
        """
        Checks if all values in the given column match the provided regex pattern.
        """
        invalid_count = self._dataframe.count()
        self._condition_failure(
            invalid_count != row_count,
            f"Row count does not equal {row_count}",
            strategy,
        )
        return self


# Adding a property to PySpark DataFrame to enable the validator
def validator(self: DataFrame) -> Validator:
    return Validator(self)


# Add the validator property to the DataFrame class
DataFrame.validator = property(validator)
