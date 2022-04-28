This is a quick tool I made to average dataframes and write them to .csv's continuously. The rationale is that I have a Monte-Carlo simulation where the results may be too large to keep in memory, so I want to continously write to files as to not lose any data.

Currently, the code assumes that the rows in your frame are ordered between runs.

The rationale for this code is as follows: If you have an experiment outputting amounts of data that are too large to
keep in memory, and you need to average this data (for example, it could be a Monte Carlo simulation) then you can use
this code.

As of now, it is very niche to my use case. The code is formally untested and assumes that your dataframes will always
have its rows in the same order. Thus, it can be haphazard to use.

Install through:

pip install -i https://test.pypi.org/simple/ dataframe-aggregator-MaxOliveberg
