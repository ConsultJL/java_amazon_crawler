# Java Amazon Crawler

I wanted a project to start playing around with Java and Jauntium so I created a very simple, single product, amazon crawler that sends all crawled data to AWS Kinesis for later processing.

If you want to play around with it on your system update the chromedriver path on line 24 of AmazonCrawl.java as well as the name of the Kinesis stream on line 58 of the same file. Build it with gradle and run it
