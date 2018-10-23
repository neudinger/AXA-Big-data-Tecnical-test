import json
from decimal import Decimal
from pyspark.sql.functions import unix_timestamp, from_unixtime

raw_input = """
"Identifiant";"Date";"Montant";"Telephone";"Json"
"1";"12/11/2015 23:22:26";"15.12";"06.23.15.48.29";"{'id': 'X999_Y999','from': 'X12','message': 'Looking forward to 2010!'}"
"2";"01/02/2013 08:29:45";"18.03";"07.26.95.15.48";"{'id': 'X923_Y999','from': 'Y12','message': 'Looking forward to 2010!'}"
"3";"26/09/2014 15:36:35";"2215.06";"06.98.65.12.32";"{'id': 'X995_Y999','from': 'X12','message': 'Looking forward to 2010!'}"
"4";"19/12/2014 13:28:26";"6.29";"01.26 58 69.84";"{'id': 'X999_Y959','from': 'A12','message': 'Looking forward to 2010!'}"
"5";"12/11/2015 23:22:41";"48.256";"04.68.87.34.10";"{'id': 'X913_Y969','from': 'Y12','message': 'Looking forward to 2010!'}"
"6";"01/02/2013 08:29:59";"84.2";"07 26 89 15 26";"{'id': 'X959_Y959','from': 'X12','message': 'Looking forward to 2010!'}"
"7";"26/09/2014 15:36:35";"165";"0625689515";"{'id': 'X999_Y912','from': 'X12','message': 'Looking forward to 2010!'}"
"8";"19/12/2014 13:28:28";"14.02";"07.21.36.35.48";"{'id': 'X029_Y999','from': 'Y12','message': 'Looking forward to 2010!'}"
"9";"12/11/2015 23:22:39";"95.35";"0156248628";"{'id': 'X999_Y129','from': 'X12','message': 'Looking forward to 2010!'}"
"10";"01/02/2013 08:29:01";"23";"04.26.58.95.12";"{'id': 'X159_Y329','from': 'A12','message': 'Looking forward to 2010!'}"
"""

raw_rdd = sc.parallelize(raw_input.split('\n')[2:-1]).map(lambda line: line.replace('\"', '').split(";"))


def schema_func(line=None):
    if line is None:
        return
    if len(line) < 5:
        return
    trs = str(line[4].replace('\'', '\"')).encode('utf-8')
    json_data = json.loads(trs)
    return [line[0], line[1], format(Decimal(line[2]), ".2f"), line[3].replace('.', '').replace(' ', ''),
            json_data["id"], json_data["from"], json_data["message"]]


raw_rdd = raw_rdd.map(schema_func)
df = raw_rdd.toDF(['Identifiant', "Date", "Montant", "Telephone", "id", "from", "message"])
df = df.select('Identifiant', from_unixtime(unix_timestamp('Date', 'dd/MM/yyyy HH:mm:ss')).alias('Date'), "Montant",
               "Telephone", "id", "from", "message")
df.show()
df.printSchema()
