This is a quick tool I made to average dataframes and write them to .csv's continuously. The rationale is that I have a
Monte-Carlo simulation where the results may be too large to keep in memory, so I want to continuously write to file as
to not lose any data.

As of now, it is very niche to my use case. The code is formally untested and assumes that your dataframes will always
have its rows in the same order. Thus, it can be haphazard to use.

Install through:

pip install -i https://test.pypi.org/simple/ dataframe-aggregator-MaxOliveberg
