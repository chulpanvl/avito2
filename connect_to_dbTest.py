import pandas as pd
import pyodbc

date = '2020-08-17'
df = pd.read_csv("/Users/chulpan/Desktop/UDG/avito_parser/rent_office/{}/avito_rentOffice{}_merged.csv".format(date, date),
                 delimiter = ' ')

# Connecting to server
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=[];'
                      'Database=[];'
                      'UID=[];'
                      'PWD=[];')

cursor = conn.cursor()

# Create a table if necessary
# cursor.execute('''

#                CREATE TABLE tablename
#                (
#                title nvarchar(MAX),
#                date smalldatetime,
#                link nvarchar(MAX),
#                price decimal,
#                publication_date smalldatetime,
#                address nvarchar(MAX),
#                closest_metro nvarchar(MAX),
#                district nvarchar(MAX),
#                metro_distance_km float,
#                description nvarchar(MAX),
#                parameters nvarchar(MAX),
#                page int,
#                latitude float,
#                longitude float,
#                area float,
#                class nvarchar(MAX),
#                views int,
#                views_dynamics int,
#                profile_name nvarchar(MAX),
#                profile_type nvarchar(MAX),
#                on_avito_since smalldatetime
#                )

#                ''')

# conn.commit()

# Insert df to existing table in bd
for row in df.itertuples():
    cursor.execute('''
                set language english; INSERT INTO tablename (title, date, link, price,\
                publication_date, address, closest_metro, district, metro_distance_km,\
                description, parameters, page, latitude, longitude, area, class,\
                views, views_dynamics, profile_name, profile_type, on_avito_since)
                VALUES (?, CAST(? AS DateTime),?,?, CAST(? AS DateTime),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CAST(? AS DateTime))
                ''',
                    row.title,
                    row.date,
                    row.link,
                    row.ppm,
                    row.publication_date,
                    row.address,
                    row.closest_metro,
                    row.district,
                    row.metro_distance_km,
                    row.description,
                    row.parameters,
                    row.page,
                    row.latitude,
                    row.longitude,
                    row.area,
                    row.class_,
                    row.views_total,
                    row.views_dynamics,
                    row.profile_name,
                    row.profile_type,
                    row.on_avito_since_converted
    )
conn.commit()

# Check the table
sql_query = pd.read_sql_query('SELECT * FROM tablename',conn)
print (sql_query.shape)