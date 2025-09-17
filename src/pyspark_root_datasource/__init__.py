from .datasource import UprootDataSource

def register(spark):
    """
    Convenience helper:
        >>> from pyspark_root_datasource import register
        >>> register(spark)
    """
    # Primary path (Spark 4 current)
    try:
        spark.dataSource.register(UprootDataSource)
    except TypeError:
        # Fallback for variants that require (name, cls)
        spark.dataSource.register(UprootDataSource.name(), UprootDataSource)

__all__ = ["UprootDataSource", "register"]
