import certifi
import psycopg2

from pymongo.mongo_client import MongoClient
# Replace the placeholder with your Atlas connection string
uri = "mongodb+srv://dalvesneha:n7htUS3upPrqWY8k@cluster0.ny116ig.mongodb.net/?retryWrites=true&w=majority"
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
    password='root'
)

if conn :
    print('connected')
else:
    print('false')

print(conn)


cur = conn.cursor()
cur.execute("""
        CREATE TABLE IF NOT EXISTS landslide (
Landslide VARCHAR(255),
Aspect VARCHAR(255),
Curvature VARCHAR(255),
Earthquake VARCHAR(255),
Elevation VARCHAR(255),
Flow VARCHAR(255),
Lithology VARCHAR(255),
NDVI VARCHAR(255),
NDWI VARCHAR(255),
Plan VARCHAR(255),
Precipitation VARCHAR(255),
Profile VARCHAR(255),
Slope VARCHAR(255)
    )
""")
conn.commit()

cur.execute("""
    TRUNCATE TABLE new_land
""")
conn.commit()


for val_doc in db.landslide.find():
    postgresql_data = (
        val_doc.get('Landslide'),
        val_doc.get('Aspect'),
        val_doc.get('Curvature'),
        val_doc.get('Earthquake'),
        val_doc.get('Elevation'),
        val_doc.get('Flow'),
        val_doc.get('Lithology'),
        val_doc.get('NDVI'),
        val_doc.get('NDWI'),
        val_doc.get('Plan'),
        val_doc.get('Precipitation'),  # convert dict to string
        val_doc.get('Profile'),
        val_doc.get('Slope')


    )
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO new_land
        (
        
Landslide,
Aspect,
Curvature,
Earthquake,
Elevation,
Flow,
Lithology,
NDVI,
NDWI,
Plan,
Precipitation,
Profile,
Slope
 
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
            %s
            
        )
        """, postgresql_data)
    conn.commit()