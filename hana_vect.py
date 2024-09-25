import os

from hana_ml.dataframe import create_dataframe_from_pandas
from gen_ai_hub.proxy.langchain.openai import OpenAIEmbeddings

from gen_ai_hub.proxy.native.openai import embeddings

class hana_vect:
    def __init__(self):
        pass        
    
    #################################
    
    def delta_capture(cc, filenames):

        # Check if the main table exists

        cursor = cc.connection.cursor()
        sql_check_if_table_exists = f'''SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = '{os.environ.get("HANA_SCHEMA")}' AND TABLE_NAME = '{os.environ.get("HANA_MAIN_TABLE")}';'''
        check_if_table_exists = cc.sql(sql_check_if_table_exists)
        check_if_table_exists = check_if_table_exists.collect()
        last_id = 0

        if len(check_if_table_exists) == 1:
            
            # If the main table exists, then check if new filename in AWS3 for the respective doc_type exists in the table
            
            for file in filenames:
                    
                #cursor = cc.connection.cursor()
                sql_check_if_file_exists = f'''SELECT COUNT(*) FROM "{os.environ.get("HANA_MAIN_TABLE")}" WHERE "SOURCE"='{file}';'''
                check_if_file_exists = cc.sql(sql_check_if_file_exists)
                check_if_file_exists = check_if_file_exists.collect()
                
                # If filename already exists, then delete it from db
                        
                if check_if_file_exists.iloc[0,0] > 0:
                    #cursor = cc.connection.cursor()
                    sql_delete_file = f'''DELETE FROM "{os.environ.get("HANA_MAIN_TABLE")}" WHERE "SOURCE"='{file}';'''
                    cursor.execute(sql_delete_file)
                    print(f"{file} was successfully deleted from SAP HANA DB")
                    
                    #cursor = cc.connection.cursor()
                    sql_check_last_id = f'''SELECT MAX(CAST(ID AS INT)) FROM "{os.environ.get("HANA_MAIN_TABLE")}";'''
                    check_last_id = cc.sql(sql_check_last_id)
                    check_last_id = check_last_id.collect()
                    if check_last_id.iloc[0,0] is None:
                        last_id = 0
                    else:
                        last_id = check_last_id.iloc[0,0]
                    
                else:
                    sql_check_last_id = f'''SELECT MAX(CAST(ID AS INT)) FROM "{os.environ.get("HANA_MAIN_TABLE")}";'''
                    check_last_id = cc.sql(sql_check_last_id)
                    check_last_id = check_last_id.collect()
                    if check_last_id.iloc[0,0] is None:
                        last_id = 0
                    else:
                        last_id = check_last_id.iloc[0,0]

        # If the main table does not exists, create one 

        else:
            sql_create_table_command = f'''CREATE TABLE "{os.environ.get("HANA_MAIN_TABLE")}" (
                        "ID" NVARCHAR(5000),
                        "SOURCE" NVARCHAR(5000),
                        "REQ_CODE" NVARCHAR(5000),
                        "DATE" NVARCHAR(5000),
                        "TEXT" NVARCHAR(5000))'''
            cursor.execute(sql_create_table_command)
            cursor.close()
            print("Table successfully created")
        
        cursor.close()
        return last_id

    #################################
        
    def load_data(cc, df):

        # Drop table if already exists

        cursor = cc.connection.cursor()
        check_if_exists = f'''SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = '{os.environ.get("HANA_SCHEMA")}' AND TABLE_NAME = '{os.environ.get("HANA_STAGING_TABLE")}';'''
        check = cc.sql(check_if_exists)
        check = check.collect()
        
        cc.connection.setautocommit(False)

        if len(check) == 1:

            sql_drop_table_command = f'''DROP TABLE "{os.environ.get("HANA_STAGING_TABLE")}"'''
            cursor.execute(sql_drop_table_command)
            
        else:
            pass

        # Create table to store data

        sql_create_table_command = f'''CREATE TABLE "{os.environ.get("HANA_STAGING_TABLE")}" (
                        "ID" NVARCHAR(5000),
                        "SOURCE" NVARCHAR(5000),
                        "REQ_CODE" NVARCHAR(5000),
                        "DATE" NVARCHAR(5000),
                        "TEXT" NVARCHAR(5000))'''

        cursor.execute(sql_create_table_command)
        cc.connection.setautocommit(True)
        cursor.close()

        # import dataframe into hana table
        create_dataframe_from_pandas(
            connection_context=cc,
            pandas_df=df,
            table_name=f"""{os.environ.get("HANA_STAGING_TABLE")}""", 
            allow_bigint=True, 
            append=True,
            force=False)
        
    #################################
        
    def prepare_table(cc, schema_name, table_name, text_column):
        
        ### Prepare the staging table by adding the vector column
        
        cursor = cc.connection.cursor()
        sql_add_vector_column = f'''ALTER TABLE "{schema_name}"."{table_name}" ADD ("{text_column}_VECTOR" REAL_VECTOR(1536))'''
        cursor.execute(sql_add_vector_column)
        cursor.close()
            
    #################################

    def read_docs(cc, schema_name, table_name, key_column, text_column, batch_size_read):
        
        ### Collect documents from SAP HANA staging table, which does not have a vector yet
        
        sql_select_texts = f'''SELECT "{key_column}", "{text_column}" FROM "{schema_name}"."{table_name}"
        WHERE "{text_column}_VECTOR" IS NULL AND "{text_column}" IS NOT NULL AND LENGTH("{text_column}") > 2
        LIMIT {batch_size_read} '''
        hdf = cc.sql(sql_select_texts)
        return hdf.collect()
    
    #################################
        
    def get_embedding(input, model="text-embedding-ada-002") -> str:
        response = embeddings.create(
          model_name=model,
          input=input
        )
        return [data.embedding for data in response.data]
    
    #################################
    
    def vectorize_docs(data, key_column, text_column, batch_size_vector):

        ### Vectorize data by using generatrive-ai-hub and OpenAIEmbeddings from langchain
        
        # Create tuples (text + id)
        text = [(row[text_column], row[key_column]) for _, row in data.iterrows()]
        # Create list of descriptions
        text_list = [e[0][:30000] for e in text]
        embedding = OpenAIEmbeddings(proxy_model_name='text-embedding-ada-002',
                                     deployment_id = os.environ.get("AICORE_EMBED_DEPLOYMENT_ID"),
                                     chunk_size=batch_size_vector, max_retries=10)
        
        # Return list of lists (list of vectors, where each vector is a list of n dimensions)
        return embedding.embed_documents(text_list)
    
    #################################
    
    def store_vectors(cc, schema_name, table_name, key_column, text_column, data, vector_list):
        
        ### Store vectors in SAP HANA staging table
        
        # Create tuples (text + id) 
        text = [(row[text_column], row[key_column]) for _, row in data.iterrows()]
        # Create tuples (vector + id) 
        ## Each tuple is cmposed by 2 strings -> vector + id
        rows = [(str(e), text[idx][1]) for idx, e in enumerate(vector_list)]

        cc.connection.setautocommit(False)

        try:
            curr = cc.connection.cursor()
            # Update SAP HANA staging table with respective values from tuple (vector for id)
            sql_command_store_vector = f'''UPDATE "{schema_name}"."{table_name}" SET "{text_column}_VECTOR" = TO_REAL_VECTOR(?) WHERE "{key_column}" = ?'''
            curr.executemany(sql_command_store_vector, rows)
            cc.connection.commit()
        except Exception as e:
            cc.connection.rollback()
        finally:
            if curr != None:
                curr.close()
                pass
            pass
        cc.connection.setautocommit(True)
        pass
    
    #################################

    def read_embed_store_documents(cc, schema_name, table_name, key_column, text_column, batch_size_read, batch_size_vector):
        try:
            ### Prepare the staging table by adding the vector column
            hana_vect.prepare_table(cc = cc, schema_name=schema_name, table_name=table_name, text_column=text_column)
        except:
            pass
        while True:
            vector_list = []
            number_of_new_docs = 0
            ### Collect documents from SAP HANA staging table, which does not have a vector yet
            df_docs = hana_vect.read_docs(cc = cc, schema_name=schema_name, table_name=table_name, key_column=key_column, text_column=text_column, batch_size_read=batch_size_read)
            number_of_new_docs = len(df_docs)
            if number_of_new_docs == 0:
                print('All docs embedded.')
                break
            else:
                print('Fetched {n} new docs.'.format(n=number_of_new_docs))
                try:
                    print('Embedding {n} documents, using batch size {batch_size_vector}...'.format(n=number_of_new_docs, batch_size_vector=batch_size_vector))
                    ### Vectorize data by using generatrive-ai-hub and OpenAIEmbeddings from langchain
                    vector_list = hana_vect.vectorize_docs(data=df_docs, key_column=key_column, text_column=text_column, batch_size_vector=batch_size_vector)
                    print('Done. Storing vectors in HANA...')
                    ### Store vectors in SAP HANA staging table
                    hana_vect.store_vectors(cc = cc, schema_name=schema_name, table_name=table_name, key_column=key_column, text_column=text_column, data=df_docs, vector_list=vector_list)
                    print('Done.')
                finally:
                    pass
        print('Done')
        
    #################################
    
    def insert_main(cc, schema_name, main_table_name, staging_table_name):
        cursor = cc.connection.cursor()
        sql_insert_into_main_table = f'''INSERT INTO "{schema_name}"."{main_table_name}" SELECT * FROM "{schema_name}"."{staging_table_name}"'''
        cursor.execute(sql_insert_into_main_table)
        cursor.close()
    
    #################################