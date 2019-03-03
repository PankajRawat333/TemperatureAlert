# TemperatureAlert

## Architecture
![alt text](https://github.com/PankajRawat333/TemperatureAlert/blob/master/TemperatureAlert%20(1).jpg)

## Temperature Event Simulator
This is simple console application to send temperature breach alert in eventHub in every minutes.

https://github.com/PankajRawat333/TemperatureAlert/tree/master/Code/AlertFunction/AlertEventhubSimulator

## Azure Function
This function listen to service bus and push data into cosmos db and same time create csv file in blob (yyyy-mm-dd HH-mm) storage. Azure Stream analytics update reference data in every minute. I'm using this file as reference data for device last alert.

https://github.com/PankajRawat333/TemperatureAlert/tree/master/Code/AlertFunction/AlertFunction

   
## Test Data for simulator
https://github.com/PankajRawat333/TemperatureAlert/blob/master/TemperatureAlert.json

## Reference Data
## 1. Device List CSV file for Referece data
https://github.com/PankajRawat333/TemperatureAlert/blob/master/DeviceThersholdLimit.csv
## 2. Device last alert CSV file for Reference data 
https://github.com/PankajRawat333/TemperatureAlert/blob/master/temperature-alerts.csv
Before starting job create blank file in blob, later Azure function will create as per Alert data.

## Stream Analytics Query
https://github.com/PankajRawat333/TemperatureAlert/blob/master/TemperatureAlertQuery.txt

