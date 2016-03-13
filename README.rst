Simple IQFeed plugin to access historical data from DTN servers.

It's a slight modification to
https://www.quantstart.com/articles/Downloading-Historical-Intraday-US-Equities-From-DTN-IQFeed-with-Python
by Michael Halls-Moore

Basically will cache data to the cache folder set and try to run updates instead of full re-downloads.
Also slight difference is that instead of f.write the data to a csv and re-read, I'm directly converting
COM socket data to pandas dataframe by utilizing numpy.

Not a programmer by trade and only learned python late last year, so feel free to give suggestions and improvements.
I would love to have your feedback and would love people contributing better code.

This comes with no guarantees or warranties. Use it at your own risk.
