from datetime import datetime
import time
from dateutil.relativedelta import relativedelta
import pandas
import psycopg2


def getPostgreSQLConnection():
      conn = psycopg2.connect(database="postgres", user="postgres", password="123456", host="localhost", port="5432", options='-c search_path=project1') # project1 is schema
      return conn

def importData(sourcePath,maxRecord):
    csvReader = pandas.read_csv(sourcePath)
    conn = getPostgreSQLConnection()
    counter = 0
    for index,row in csvReader.iterrows():
        if counter >= maxRecord:
            break

        startTime = time.perf_counter()
        insertPiece(row, conn)
        endTime = time.perf_counter()
        print(str(counter) +"\t"+ str((endTime - startTime) * 1000))
        counter = counter + 1


def insertPiece(piece, conn):
    itemName = piece['Item Name']
    itemType = piece['Item Type']
    itemPrice = piece['Item Price']
    retrievalCity = piece['Retrieval City']
    retrievalTime = piece['Retrieval Start Time']
    retrievalCourier = piece['Retrieval Courier']
    retrievalCourierGender = piece['Retrieval Courier Gender']
    retrievalCourierPhone = piece['Retrieval Courier Phone Number']
    retrievalCourierAge = piece['Retrieval Courier Age']
    deliveryTime = piece['Delivery Finished Time']
    deliveryCity = piece['Delivery City']
    deliveryCourier = piece['Delivery Courier']
    deliveryCourierGender = piece['Delivery Courier Gender']
    deliveryCourierPhone = piece['Delivery Courier Phone Number']
    deliveryCourierAge = piece['Delivery Courier Age']
    exportCity = piece['Item Export City']
    exportTax = piece['Item Export Tax']
    exportTime = piece['Item Export Time']
    importCity = piece['Item Import City']
    importTax = piece['Item Import Tax']
    importTime = piece['Item Import Time']
    containerCode = piece['Container Code']
    containerType = piece['Container Type']
    shipName = piece['Ship Name']
    companyName = piece['Company Name']
    logTime = piece['Log Time']

    cursor = conn.cursor()

    cursor.execute("insert into company (name) values (%s)  on conflict do nothing;" , (companyName, ))
    cursor.execute("insert into portcity (name) values (%s)  on conflict do nothing;" , (exportCity, ))
    cursor.execute("insert into portcity (name) values (%s)  on conflict do nothing;" , (importCity, ) )
    cursor.execute("insert into city (name) values (%s)  on conflict do nothing;" ,(retrievalCity, ))
    cursor.execute("insert into city (name) values (%s)  on conflict do nothing;", (deliveryCity,) )

    cursor.execute("insert into courier (name, gender, birthday, phone_number, company, port_city) values (%s,%s,%s,%s,%s,%s)  on conflict do nothing;", (retrievalCourier, retrievalCourierGender, getBirthday(retrievalTime, retrievalCourierAge), retrievalCourierPhone, companyName, exportCity))

    cursor.execute("insert into shipment (item_name, item_price, item_type, from_city, to_city, export_city, import_city, company, log_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)  on conflict do nothing;", (itemName, itemPrice, itemType, retrievalCity, deliveryCity, exportCity, importCity, companyName, getDatetime(logTime)))

    cursor.execute("insert into delivery_retrieval (item_name,type,courier,date, city) values (%s,%s,%s,%s,%s)  on conflict do nothing;", (itemName, 'retrieval', retrievalCourier, getDate(retrievalTime), retrievalCity))


    if not pandas.isnull(deliveryCourier):
        cursor.execute("insert into courier (name, gender, birthday, phone_number, company, port_city) values (%s,%s,%s,%s,%s,%s)  on conflict do nothing;", (deliveryCourier, deliveryCourierGender, getBirthday(deliveryTime, deliveryCourierAge), deliveryCourierPhone, companyName, importCity))
        cursor.execute("insert into delivery_retrieval (item_name,type,courier,date, city) values (%s,%s,%s,%s,%s)  on conflict do nothing;", (itemName, 'delivery', deliveryCourier, getDate(deliveryTime), deliveryCity))

    if not pandas.isnull(exportTime):
        cursor.execute("insert into container (code, type) values (%s, %s)  on conflict do nothing;" , (containerCode, containerType))
        cursor.execute("insert into ship (name, company) values (%s, %s)  on conflict do nothing;" , (shipName, companyName))
        cursor.execute("insert into import_export_detail (item_name,type, item_type, port_city, tax, date) values (%s,%s,%s,%s,%s,%s)  on conflict do nothing;", (itemName, "export", itemType , exportCity, exportTax, getDate(exportTime)))
        cursor.execute("insert into shipping (item_name, ship, container) values (%s,%s,%s)  on conflict do nothing;", (itemName, shipName, containerCode))

    
    if not pandas.isnull(importTime):
        cursor.execute("insert into import_export_detail (item_name,type, item_type, port_city, tax, date) values (%s,%s,%s,%s,%s,%s)  on conflict do nothing;", (itemName, "import", itemType , importCity, importTax, getDate(importTime)))
        
    
    # finished import
    conn.commit()



    

def getBirthday(dateString, age):
    date = datetime.strptime(dateString, '%Y-%m-%d')
    date = date - relativedelta(years=age)
    return date

def getDate(dateString):
    return datetime.strptime(dateString, '%Y-%m-%d')

def getDatetime(dateString):
    return datetime.strptime(dateString, '%Y-%m-%d  %H:%M:%S')

if __name__ == "__main__":
    importData("../data/shipment_records.csv", 500000)
