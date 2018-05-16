# Twitter Sentiment Analysis
## Installations
### Pyspark
* Go to Anaconda and run the following command
conda install -c conda-forge pyspark <br>
Note: Pyspark only works with latest Python-3.5 Version and will not work work 3.6 version.

### ElasticSearch
* Follow the installation here <br>
https://www.elastic.co/guide/en/elasticsearch/reference/current/windows.html
<br> ElasticSearch generally opens up in Port 9001.
<br>Note: Be careful to install same versions of Elastic Search and Kibana.

### Kibana
* Follow the the installation from here <br>
https://www.elastic.co/guide/en/kibana/current/windows.html
<br> Kibana generally starts on Port 5601.

## Before running the scripts
### Start the ElasticSearch server
* Go to the bin folder of ES installation folder and run <br>
\elasticsearch.exe

### Start Kibana Server
* Run bin\kibana.bat from installation folder

## Running
* Run the stream.py in a terminal with the command <br>
python stream.py Trump(or any other hashtag)
* Run spark.py within IDE or in a separate terminal.
* Open 5601 Kibana port. 
* We should be able to see the incoming data there.

