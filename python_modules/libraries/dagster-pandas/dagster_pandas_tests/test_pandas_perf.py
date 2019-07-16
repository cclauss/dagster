# import pandas as pd
# from dagster import execute_pipeline, lambda_solid, pipeline


# @lambda_solid
# def pandas_solid():
#     return pd.DataFrame(
#         columns=[str(i) for i in range(1000)], index=range(90)  # 1000 columns  # 90 rows
#     )


# @pipeline
# def pandas_pipeline():
#     return pandas_solid()


# def test_pandas_pipeline_execution():
#     import cProfile, pstats, io
#     from pstats import SortKey

#     pr = cProfile.Profile()
#     pr.enable()
#     execute_pipeline(pandas_pipeline)

#     pr.disable()
#     s = io.StringIO()
#     sortby = SortKey.CUMULATIVE
#     ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
#     ps.print_stats()
#     print(s.getvalue())
