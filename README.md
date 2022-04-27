This is a quick tool I made to average dataframes and write them to .csv's continuously. The rationale is that I have a Monte-Carlo simulation where the results may be too large to keep in memory, so I want to continously write to files as to not lose any data.

Currently, the code assumes that the rows in your frame are ordered between runs.
