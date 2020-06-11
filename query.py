import pymysql
import pandas as pd

def db_query(db, sql, params): 
    # Connect to MySQL
    conn = pymysql.connect(host='*ipname*', user='*userid*', password='*userpasswd*', charset='utf8', db=db,
                           cursorclass=pymysql.cursors.DictCursor)
    try: 
        # create Dictionary Cursor 
        with conn.cursor() as cursor: 
            sql_query = sql 
            # excute SQL 
            cursor.execute(sql_query, params) 
            rows = cursor.fetchall()
            return rows
        conn.commit()
        
    finally: 
        conn.close()
        
# Execute query and save the result as Dataframe format
def query_executor(sql):
    query = db_query(db='*dbname*', sql=sql, params=None)
    query_df = pd.DataFrame(query)

    return query_df
    
# Convert list to str for the purpose of query input format
def item_in(list): 
    items = "', '".join(map(str, list)) # where * IN ( 'items' )
    return items

def item_regexp(list):
    items = "|".join(map(str, list)) # where * regexp 'items'
    return items
    
def code_extractor(df, cdnm, param):
    cd = df[cdnm].dropna() # drop NaN in dataframe
    cd_item = item_in(cd) if param == 0 else item_regexp(cd) # item_in = 0, item_regexp = 1
    return cd_item

def unique_sort(df, colnm, boolean, uniquenm, keep_param):
    df = df.sort_values(colnm, ascending=boolean)
    unique_df = df.drop_duplicates([uniquenm], keep=keep_param)
    return unique_df


# SQL function - Sample usage
def sample(items, odcd):
    samdb = "SELECT * FROM SAMPLE where customID IN ('" + str(items) + "') AND ODCD REGEXP '" + str(odcd) + "'"
    print(samdb)
    samdb_df = qy.query_executor(samdb)
        
    print("Extraction done.")
    print("SAMPLE DB: " + str(samdb_df.shape))
    
    return samdb_df

items = item_in(samplefile["seq"].tolist())
odcd = item_regexp(samplefile["odcd"].tolist())

sample_sql = sample(items, odcd)
