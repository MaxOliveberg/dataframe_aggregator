This is a quick tool for averaging .csv files into one dataframe.

The rationale for this code is as follows: If you have an experiment outputting amounts of data that are too large to
keep in memory, and you need to average this data (for example, it could be a Monte Carlo simulation) then you can use
this code.

As of now, it is very niche to my use case. The code is formally untested and assumes that your dataframes will always
have its rows in the same order. Thus, it can be haphazard to use.