import sqlalchemy as db
import pyodbc
import csv
from datetime import datetime, timedelta

def BuildExportQueries(databaseName, numberOfDaysEport):
    datesinChunk = numberOfDaysEport
    dataCutoffDate = datetime.today() - timedelta(days=datesinChunk)
    stringTimeStamp = dataCutoffDate.strftime("%Y-%m-%d")
    print(stringTimeStamp)
    engine = db.create_engine(f"mssql+pyodbc://sqlhawkstg/{databaseName}?driver=SQL+Server+Native+Client+11.0")
    connection = engine.connect()
    result = connection.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES")
    dict = { "MenuItemRoles": "",
             "HazardousMaterials": "c:Created",
             "PalletPositions": "q:pp INNER JOIN pallets p ON pp.PalletId = p.id WHERE p.CreatedDate > '{}' ",
             "RolePermissions":"",
             "PalletsHazardousMaterials":"q:ph INNER JOIN pallets p ON ph.PalletId = p.id WHERE p.CreatedDate > '{}'",
             "TemperatureAlertHistories":"",
             "UserPreferenceAircraft":"",
             "UserPreferenceFlightExports":"",
             "UserRoles":"",
             "UserAircraft":"c:CreateDate",
             "AircraftLayouts":"",
             "AircraftZonePositionCodes":"",
             "AircraftZonePositions":"",
             "AircraftZones":"",
             "Alerts":"",
             "Filters":"",
             "FilterValues":""
    }
    sqllist = []
    tablenames =[]
    for row in result:
        print("table: ", row['TABLE_NAME'])
        tablenames.append(row['TABLE_NAME'])
        queryEndPart = ""
        if row['TABLE_NAME'] in dict.keys():
            queryEndPart = dict[row['TABLE_NAME']]
            if queryEndPart.startswith("c:"):
                columnName = queryEndPart.strip("c:")
                queryEndPart = f"where {columnName}> '{stringTimeStamp}'"
            elif queryEndPart.startswith("q:"):
                queryEndPart = queryEndPart.strip("q:").format(stringTimeStamp)
        else : queryEndPart = f"where CreatedDate > '{stringTimeStamp}'"
        query: str = f"select * from {row['TABLE_NAME']} {queryEndPart}"
        sqllist.append(query)
        print(query)
    connection.close()
    return sqllist, tablenames

databaseName = "TenantAtlas"
queries, tablenames = BuildExportQueries(databaseName,2)

index = 0
engine = db.create_engine(f"mssql+pyodbc://sqlhawkstg/{databaseName}?driver=SQL+Server+Native+Client+11.0")
connection = engine.connect()
while index < len(tablenames) :
    q = queries[index]
    tablename = tablenames[index]
    print(q)
    outfile = open(f'{databaseName}.dbo.{tablename}.csv', 'w')
    outcsv = csv.writer(outfile)
    columnnames = connection.execute(f"select column_name from information_schema.columns where table_name = '{tablename}'")
    columns = []
    for c in columnnames:
        columns.append(c[0].strip("'").strip("'"))
    cursor = connection.execute(q)
    rows = cursor.fetchall();
    # dump column titles (optional)
    "select column_name from information_schema.columns where table_name = 'Your_table_name'"
    outcsv.writerow(columns)
    # dump rows
    outcsv.writerows(rows)
    index = index + 1
