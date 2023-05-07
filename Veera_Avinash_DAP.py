import certifi
import psycopg2

from pymongo.mongo_client import MongoClient
# Replace the placeholder with your Atlas connection string
uri = "mongodb+srv://veeraavinashp1:KyMs9TEDqfhDMuZt@cluster0.qu9pmrb.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri, tlsCAFile=certifi.where())
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client["dataset"]

conn = psycopg2.connect(
    host='localhost',
    database='postgres',
    user='postgres',
    password='avinash@97'
)

if conn :
    print('connected')
else:
    print('false')

print(conn)


cur = conn.cursor()
cur.execute("""
        CREATE TABLE IF NOT EXISTS landslide (
event_id VARCHAR(255),
event_date VARCHAR(255),
event_title VARCHAR(255),
location_accuracy VARCHAR(255),
landslide_category VARCHAR(255),
landslide_trigger VARCHAR(255),
landslide_size VARCHAR(255),
landslide_setting VARCHAR(255),
fatality_count VARCHAR(255),
event_import_source VARCHAR(255),
country_name VARCHAR(255),
country_code VARCHAR(255),
admin_division_name  VARCHAR(255),
admin_division_population VARCHAR(255),
gazeteer_closest_point VARCHAR(255),
gazeteer_distance  VARCHAR(255),
longitude VARCHAR(255),
latitude VARCHAR(255),
year VARCHAR(255),
month VARCHAR(255)
    )
""")
conn.commit()

cur.execute("""
    TRUNCATE TABLE landslide
""")
conn.commit()


for mongo_doc in db.Processed_Landslide.find():
    postgresql_data = (
        mongo_doc.get('event_id'),
        mongo_doc.get('event_date'),
        mongo_doc.get('event_title'),
        mongo_doc.get('location_accuracy'),
        mongo_doc.get('landslide_category'),
        mongo_doc.get('landslide_trigger'),
        mongo_doc.get('landslide_size'),
        mongo_doc.get('landslide_setting'),
        mongo_doc.get('fatality_count'),
        mongo_doc.get('event_import_source'),
        mongo_doc.get('country_name'),  # convert dict to string
        mongo_doc.get('country_code'),
        mongo_doc.get('admin_division_name'),
        mongo_doc.get('admin_division_population'),
        mongo_doc.get('gazeteer_closest_point'),
        mongo_doc.get('gazeteer_distance'),
        mongo_doc.get('longitude'),
        mongo_doc.get('latitude'),
        mongo_doc.get('year'),
        mongo_doc.get('month')


    )
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO landslide 
        (
        
event_id,
event_date,
event_title,
location_accuracy,
landslide_category,
landslide_trigger,
landslide_size,
landslide_setting,
fatality_count,
event_import_source,
country_name,
country_code,
admin_division_name,
admin_division_population,
gazeteer_closest_point,
gazeteer_distance,
longitude,
latitude,
year,
month
 
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        """, postgresql_data)
    conn.commit()