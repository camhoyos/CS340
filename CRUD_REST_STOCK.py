import json
from bson import json_util
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import bottle
from bottle import route, run, request, abort


connection = MongoClient('localhost', 27017)
db = connection['market']
collection = db['stocks']

#CREATE
def insert_document(document):
  
  try:
    collection.insert_one(json.loads(document))
    return True
  
  except Exception as e:
    print str(e)
    
  except ValidationError as ve:
    abort(400, str(ve))
    
  return False

#READ
def read_document(document):
  
  try:
    response = collection.find_one(json.loads(document))
    
    if not document:
      abort(404, 'No document with %s' % document)  
      
  except PyMongoError as e:
    print str(e)
    
  return json.loads(json.dumps(response, indent=4, default=json_util.default))
    
#UPDATE
def update_document(lookup, update):
  if int(update) <= 0:
    abort(400, 'Volume is less than or equal to 0. Passed value = %s' % update)
    
  try:
    query = { "Ticker" : lookup}
    newval = { "$set" : { "Volume" : str(update) }}
    response = collection.update_one(query, newval)
    
    if not response:
      abort(404, 'No document with %s' % lookup)
      
  except PyMongoError as e:
    print str(e)
    
  return json.loads(json.dumps(lookup, indent=4, default=json_util.default))

#DELETE
def delete_document(document):
  
  try:
    response = collection.delete_one(json.loads(document))
    
    if not response:
      abort(404, 'No document with %s' % document)
      
  except PyMongoError as e:
    print str(e)
    
  return json.loads(json.dumps(document, indent=4, default=json_util.default))

#Create MONGODB
#http://localhost:8080/stocks/api/v1.0/createStock/AA
#curl -H "Content-Type: application/json" -X POST -d '{"Ticker":"TEST","Volume":12345}' http://localhost:8080/stocks/api/v1.0/createStock/TEST
@route('/stocks/api/v1.0/createStock/<ticker>', method='POST')
def post_inspections(ticker):
  #TODO investigate int create versus string update
  try:
    postdata = json.loads(request.body.read())
    postdata["Ticker"] = ticker
    insert_document(json.dumps(postdata))
    
  except ValueError:
    abort(404, 'Incorrect data type')
    
  except KeyError:
    abort(404, 'Insufficient parameters')
    
  return json.loads(json.dumps(postdata, indent=4, default=json_util.default))

#Read MONGODB
#http://localhost:8080/stocks/api/v1.0/getStock/AA
#curl http://localhost:8080/stocks/api/v1.0/getStock/TEST
@route('/stocks/api/v1.0/getStock/<ticker>', method='GET')
def get_inspections(ticker):
  
  try:
    response = read_document('{"Ticker":\"'+ticker+'\"}')
    
  except ValueError:
    abort(404, 'Incorrect data type')
    
  return response

#Update MONGODB
#http://localhost:8080/stocks/api/v1.0/updateStock/AA
#curl -H "Content-Type: application/json" -X PUT -d '{"Ticker":"TEST","Volume":98765}' http://localhost:8080/stocks/api/v1.0/updateStock/TEST
@route('/stocks/api/v1.0/updateStock/<ticker>', method='PUT')
def put_inspections(ticker):
    
  try:
    putdata = json.loads(request.body.read()) #value pair stream in JSON notation
    putdata["Ticker"] = ticker
    response = update_document(putdata["Ticker"], putdata["Volume"])
    
  except ValueError:
    abort(404, 'Incorrect data type')
    
  return json.loads(json.dumps(response, indent=4, default=json_util.default))

#Delete MONGODB
#http://localhost:8080/stocks/api/v1.0/deleteStock/AA
#curl -X DELETE http://localhost:8080/stocks/api/v1.0/deleteStock/TEST
@route('/stocks/api/v1.0/deleteStock/<ticker>', method='DELETE')
def delete_inspections(ticker):
  
  try:
    response = delete_document('{"Ticker":\"'+ticker+'\"}')
  
  except ValueError:
    abort(404, 'Incorrect data type')
    
  return json.loads(json.dumps(response, indent=4, default=json_util.default))

#Aggregate MONGODB
#http://localhost:8080/stocks/api/v1.0/stockReport
#curl -H "Content-Type: application/json" -X POST -d '[AA,BA,T]' http://localhost:8080/stocks/api/v1.0/stockReport
@route('/stocks/api/v1.0/stockReport', method='POST')
def aggregate_stockReport():
  postdata = request.body.read().strip('[').strip(']').split(",")
  response = ""
  
  try:
    for x in postdata:
      response += json.dumps(get_inspections(x), indent=4, default=json_util.default)
      
  except ValueError:
    abort(404, 'Incorrect data type')
  
  return response

#Aggregate MONGODB
#http://localhost:8080/stocks/api/v1.0/industryReport/AA
#curl http://localhost:8080/stocks/api/v1.0/industryRpoert/Lodging
@route('/stocks/api/v1.0/industryReport/<industry>', method='GET')
def aggregate_industryReport(industry):
  response = ""
  
  try:
    #response = json.dumps(find_ticker_industry(industry), indent=4, default=json_util.default)
    for x in find_ticker_industry(industry).sort("Market Cap",-1).limit(5):
      response += json.dumps(x, indent=4, default=json_util.default)
    
  except PyMongoError as e:
    print str(e)
    
  return response
  
#Aggregate MONGODB
#http://localhost:8080/stocks/api/v1.0/portfolio/AA
#curl http://localhost:8080/stocks/api/v1.0/portfolio/Vanguard%20Value%20ETF
@route('/stocks/api/v1.0/portfolio/<companyName>', method='GET')
def aggregate_portfolio(companyName):
  try:
    response = read_document('{"Company":\"'+companyName+'\"}')
    
  except ValueError:
    abort(404, 'Incorrect data type')
    
  return response

def count_document_SMA(low, high):
  counter = 0
  if low >= high:
    raise Exception("Check submitted variables")
    
  response = '{"50-Day Simple Moving Average" : {"$gt" : ' + str(low) + ', "$lt" : ' + str(high) + '}}'
  try:
    response = collection.find(json.loads(response))
    
    for x in response:
      counter+=1
      
  except PyMongoError as e:
    print str(e)
    
  return counter

def find_ticker_industry(industry):
  response = '{"Industry" : \"' + str(industry) +'\"}'
  try:
    response = collection.find(json.loads(response))
    
  except PyMongoError as e:
    print str(e)
    
  return response

def sector_aggregate_shares(sector):
  response = [{"$match":{"Sector":sector}},{"$group":{"_id":"$Industry","Total Outstanding Shares":{"$sum":"$Shares Outstanding"}}}]
  try:
    response = collection.aggregate(response)
    
  except PyMongoError as e:
    print str(e)
    
  return response
    
def main():
  run(host='localhost', port=8080, debug=True)
  
  #tests count_document_SMA, verified with a direct mongoDB query
  print "50-Day Simple Moving Average between -1 and 5 : " + str(count_document_SMA(-1, 5))
  
  #tests find_ticker_industry, verified with a direct mongoDB query
  for x in find_ticker_industry("Medical Laboratories & Research"):
    print "Ticker Match: " + str(x["Ticker"])
  
  #test sector_aggregate_shares, verified with a direct mongoDB query
  #aggregate ({"$match":{"Sector":"Healthcare"}},{"$group":{"_id":"$Industry","Total Outstanding Shares":{"$sum":"$Shares Outstanding"}}})
  for x in sector_aggregate_shares("Healthcare"):
    print "Industry: " + x["_id"] + " Outstanding Shares : " + str(x["Total Outstanding Shares"])
  
main()