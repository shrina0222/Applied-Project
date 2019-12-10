import mysql.connector
import pandas as pd
import csv
import pymysql
import glob
import os
import sqlalchemy
from sqlalchemy import create_engine

engine = create_engine('mysql+mysqlconnector://root:password@localhost/MyDB')
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="password",
    db= "MyDB",
    charset='utf8'
)
mycursor = mydb.cursor()
# mycursor.execute('set max_allowed_packet=67108864')
file_open = os.path.expanduser('~/Desktop/ASU DOCS/Applied Project/Dataset')
print(file_open+'*.csv')
# print('all csv files in data directory:', glob.glob(file_open+'/*.csv'))
a = glob.glob(file_open+'/*.csv')

def LoginDumpster(b,engine,mycursor):
    sql = "DROP TABLE IF EXISTS Login"
    mycursor.execute(sql)
    for file in b:
        with open(file, "rt", encoding='ISO-8859-1') as f:

            reader = csv.reader(f)
            i = next(reader)
        c = []
        try:
            mycursor.execute("CREATE TABLE Login (Username VARCHAR(255), EmailID VARCHAR(255),Password VARCHAR(255), Sourcefile VARCHAR(255))")
        except:
            pass
        for j in i:

            if j == 'Password' or j == 'Username' or j == 'EmailID' or j == 'SourceFile':
                c.append(j)

        print(c)
        print("Filename:" + file)
        data = pd.read_csv(file)[c]
        print(data)

        data.to_sql(name='Login', con=engine, if_exists='append', index=False)

def AitypeheadDumpster(d,engine,mycursor):

    for file in d:
        with open(file, "rt", encoding='ISO-8859-1') as f:
            reader = csv.reader(f)
            i = next(reader)
        c = []
        sql = "DROP TABLE IF EXISTS Aitypehead"
        print(i)
        mycursor.execute(sql)

        mycursor.execute("CREATE TABLE Aitypehead (IPaddress VARCHAR(255), City VARCHAR(255),Country VARCHAR(255), updatedDaysAgo VARCHAR(255), deviceBrand VARCHAR(255), deviceId VARCHAR(255), clientVersion VARCHAR(255), device VARCHAR(255), id_pid VARCHAR(255), aitypeVersion VARCHAR(255), androidVersion VARCHAR(255), installedDaysAgo VARCHAR(255), org VARCHAR(255), packageName VARCHAR(255), Region VARCHAR(255), deviceModel VARCHAR(255), Username VARCHAR(255), Location VARCHAR(255), gcmId VARCHAR(255), userLanguages LONGTEXT, Sourcefile VARCHAR(255))")
        for j in i:

            if j == 'IPaddress' or j == 'City' or j == 'Country' or j == 'updatedDaysAgo' or j == 'deviceBrand' or j == 'deviceId' or j == 'clientversion' or j == 'device' or j == 'id_pid' or j == 'aitypeVersion' or j == 'androidVersion' or j == 'installedDaysAgo' or j == 'org' or j == 'packageName' or j == 'Region' or j == 'deviceModel' or j == 'Username' or j == 'Location' or j == 'gcmID' or j == 'userLanguages' or j == 'SourceFile':
                c.append(j)
        print("Filename:" + file)
        data = pd.read_csv(file)[i]
        print(data)

        data.to_sql(name='Aitypehead', con=engine,chunksize=20000, if_exists='append', index=False)

def epsbotnetDumpster(h,engine,mycursor):

    for file in h:
        with open(file, "rt", encoding='ISO-8859-1') as f:
            reader = csv.reader(f)
            i = next(reader)
        c = []
        sql = "DROP TABLE IF EXISTS epsbotnet"
        print(i)
        mycursor.execute(sql)

        mycursor.execute("CREATE TABLE epsbotnet (Domainkeyword VARCHAR(255), Usercredentials VARCHAR(255),	PartnerIDkeyword VARCHAR(255), IPaddress VARCHAR(255), Count VARCHAR(255), SourceFile VARCHAR(255))")
        for j in i:
             if j == 'Domainkeyword' or j == 'Usercredentials' or j == 'PartnerIDkeyword' or j == 'IPaddress' or j == 'Count' or j == 'SourceFile':
                c.append(j)
        print("Filename:" + file)
        data = pd.read_csv(file)[c]
        print(data)

        data.to_sql(name='epsbotnet', con=engine,chunksize=20000, if_exists='append', index=False)

def MilliononeDumpster(g,engine,mycursor):
    for file in g:
        with open(file, "rt", encoding='ISO-8859-1') as f:
            reader = csv.reader(f)
            i = next(reader)
        c = []
        sql = "DROP TABLE IF EXISTS Millionone"
        print(i)
        mycursor.execute(sql)

        mycursor.execute("CREATE TABLE Millionone (EmailID VARCHAR(255), Password VARCHAR(255),	Price VARCHAR(255),	AvailablePoints VARCHAR(255), Username VARCHAR(255), LoginCredentials VARCHAR(255),	RenewalCode VARCHAR(255), Expires VARCHAR(255),	AutoRenewal VARCHAR(255), SecondPassword VARCHAR(255), IPaddress VARCHAR(255), Hostname VARCHAR(355), NotificationOverview VARCHAR(255), Status VARCHAR(255), BusinessOrganization VARCHAR(255), SocialMedia VARCHAR(255), Membership VARCHAR(255), Country VARCHAR(255), SourceFile VARCHAR(255))")
        for j in i:

            if j == 'EmailID' or j == 'Password' or j == 'Price' or j == 'AvailablePoints' or j == 'Username' or j == 'LoginCredentials' or j == 'RenewalCode' or j == 'Expires' or j == 'AutoRenewal' or j == 'SecondPassword' or j == 'IPaddress' or j == 'Hostname' or j == 'NotificationOverview' or j == 'Status' or j == 'BusinessOrganization' or j == 'SocialMedia' or j ==	'Membership' or j == 'Country' or j == 'SourceFile':
                c.append(j)
        print("Filename:" + file)
        data = pd.read_csv(file)[c]
        print(data)
        data.to_sql(name='Millionone', con=engine, chunksize=20000, if_exists='append', index=False)

def MilliontwoDumpster(k,engine,mycursor):
    for file in k:
        with open(file, "rt", encoding='ISO-8859-1') as f:
            reader = csv.reader(f)
            i = next(reader)
        c = []
        sql = "DROP TABLE IF EXISTS Milliontwo"
        mycursor.execute(sql)
        try:
            mycursor.execute("CREATE TABLE Milliontwo (EmailID VARCHAR(255), Password VARCHAR(255),	FirstName VARCHAR(255),	LastName VARCHAR(255), Initials VARCHAR(255), id VARCHAR(255), UserName VARCHAR(255), PoliticalParty VARCHAR(255), County VARCHAR(255),	District VARCHAR(255), Street VARCHAR(255),	City VARCHAR(255), State VARCHAR(255), PostalCode VARCHAR(255),	PhoneNumber VARCHAR(255), RoomNo VARCHAR(255), Fax VARCHAR(255), SourceFile VARCHAR(255))")
        except:
            pass
        for j in i:
            if j == 'EmailID' or j == 'Password' or j == 'FirstName' or j == 'LastName' or j ==	'Initials' or j == 'id' or j == 'UserName' or j == 'PoliticalParty' or j ==	'County' or j == 'District' or j ==	'Street' or j == 'City' or j ==	'State' or j ==	'PostalCode' or j == 'PhoneNumber' or j == 'RoomNo' or j == 'Fax' or j == 'SourceFile':
                c.append(j)
        print(c)
        print("Filename:" + file)
        data = pd.read_csv(file)[c]
        print(data)

        data.to_sql(name='Milliontwo', con=engine, chunksize=20000, if_exists='append', index=False)


def MillionthreeDumpster(e,engine,mycursor):
    for file in e:
        with open(file, "rt", encoding='ISO-8859-1') as f:
            reader = csv.reader(f)
            i = next(reader)
        c = []
        sql = "DROP TABLE IF EXISTS Millionthree"
        mycursor.execute(sql)
        try:
            mycursor.execute("CREATE TABLE Millionthree (id VARCHAR(255), Username VARCHAR(255), Password VARCHAR(255),	gender VARCHAR(255), prefix VARCHAR(255), prefix_other VARCHAR(255), thai_firstname VARCHAR(255), thai_lastname VARCHAR(255), eng_firstname VARCHAR(255), eng_lastname VARCHAR(255), birthday VARCHAR(255), Date VARCHAR(255), nationality VARCHAR(255), SourceFile VARCHAR(255))")
        except:
            pass
        for j in i:
            if j == 'id' or j == 'Username' or j ==	'Password' or j == 'gender' or j ==	'prefix' or j == 'prefix_other' or j ==	'thai_firstname' or j == 'thai_lastname' or j == 'eng_firstname' or j == 'eng_lastname' or j ==	'birthday' or j == 'Date' or j == 'nationality' or j ==	'SourceFile':
                c.append(j)
        print(c)
        print("Filename:" + file)
        data = pd.read_csv(file)
        print(data)

        data.to_sql(name='Millionthree', con=engine, chunksize=20000, if_exists='append', index=False)

def JapansessionDumpster(m,engine,mycursor):
    for file in m:
        with open(file, "rt", encoding='ISO-8859-1') as f:
            reader = csv.reader(f)
            i = next(reader)
        c = []
        sql = "DROP TABLE IF EXISTS Japansession"
        mycursor.execute(sql)
        try:
            mycursor.execute("CREATE TABLE Japansession (EmailID VARCHAR(255), Username VARCHAR(255), Amplifier VARCHAR(255), Date & Time VARCHAR(255),	UniProKB VARCHAR(255), Ref # VARCHAR(255), Country VARCHAR(255), FirstName VARCHAR(255), LastName VARCHAR(255),	TypeofPayment VARCHAR(255),	CardNumber VARCHAR(255), Year VARCHAR(255),	Line1 VARCHAR(255), Line2 VARCHAR(255),	Line3 VARCHAR(255),	City VARCHAR(255), District VARCHAR(255), PostalCode VARCHAR(255), PhoneNumber VARCHAR(255), IPaddress VARCHAR(255), CVV VARCHAR(255), PaymentReceipt VARCHAR(255),	Sourcefile VARCHAR(255))")
        except:
            pass
        for j in i:
            if j == 'EmailID' or j == 'Username' or j == 'Amplifier' or j == 'Date & Time' or j == 'UniProKB' or j == 'Ref #' or j == 'Country' or j ==	'FirstName' or j ==	'LastName' or j == 'TypeofPayment' or j == 'CardNumber' or j ==	'Year' or j == 'Line1' or j == 'Line2' or j == 'Line3' or j == 'City' or j == 'District' or j == 'PostalCode' or j == 'PhoneNumber' or j ==	'IPaddress' or j == 'CVV' or j == 'PaymentReceipt' or j == 'Sourcefile':
                c.append(j)
        print(c)
        print("Filename:" + file)
        data = pd.read_csv(file)[c]
        print(data)

        data.to_sql(name='Japansession', con=engine, chunksize=20000, if_exists='append', index=False)

def MissouriDumpster(n,engine,mycursor):
    for file in n:
        with open(file, "rt", encoding='ISO-8859-1') as f:
            reader = csv.reader(f)
            i = next(reader)
        c = []
        sql = "DROP TABLE IF EXISTS Missouri"
        mycursor.execute(sql)
        try:
            mycursor.execute("CREATE TABLE Missouri (Name MEDIUMTEXT ,SSN VARCHAR(255), EmailID VARCHAR(255), Line1 VARCHAR(255), Line2 VARCHAR(255), Phone VARCHAR(255), Username VARCHAR(255), Password VARCHAR(255), SourceFile VARCHAR(255))")
        except:
            print("Error")
        for j in i:
            if j == 'Name' or j == 'SSN' or j == 'EmailID' or j == 'Line1' or j == 'Line2' or j == 'Phone' or j == 'Username' or j == 'Password' or j == 'SourceFile':
                c.append(j)
        print(c)
        print("Filename:" + file)
        data = pd.read_csv(file)
        print(data)

        data.to_sql(name='Missouri', con=engine, chunksize=20000, if_exists='append', index=False)


b = ["private_net_japan.csv","wowtgc2.csv", "wowtgc1.csv"]
d = ["aitype_head.csv"]
e = ["74Million3.csv"]
g = ["74Million1.csv"]
h = ["epsbotnet.csv"]
k = ["74Million2.csv"]
m = ["Japan2019session.csv"]
n = ["Missouri.csv"]


LoginDumpster(b, engine, mycursor)
print("Data stored successfully into Database")
AitypeheadDumpster(d, engine, mycursor)
print("Aitypedata Data stored successfully")
MillionthreeDumpster(e, engine, mycursor)
print("74Million3 Data Stored Successfully")
MilliononeDumpster(g, engine, mycursor)
print("74Million1 Data Stored Successfully")
epsbotnetDumpster(h, engine, mycursor)
print("epsbotnet Data Stored Successfully")
MilliontwoDumpster(k, engine, mycursor)
print("74Million2 Data Stored Successfully")
JapansessionDumpster(m, engine, mycursor)
print("Japansession Data Stored Successfully")
MissouriDumpster(n, engine, mycursor)
print("Missouri Data Stored Successfully")
