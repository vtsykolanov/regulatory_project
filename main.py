import os
from flask import Flask, jsonify, request
from flask_uploads import UploadSet, configure_uploads, DOCUMENTS
from werkzeug.utils import secure_filename

####################################

from dotenv import load_dotenv
load_dotenv('./environment.env')
####################################

from hana_vect import hana_vect
from ask import ask
from create_df import create_df

# Create instance of the Flask and assign it to app variable
app = Flask(__name__)
port = int(os.environ.get('PORT', 3000))

# Configuration for file uploads
app.config['UPLOADED_FILES_DEST'] = './files/'  # directory where files will be stored
pdfs = UploadSet('files', DOCUMENTS)
configure_uploads(app, pdfs)

from hana_ml.dataframe import ConnectionContext

cc = ConnectionContext(
    address=os.environ.get("HANA_ADDRESS"),
    port=os.environ.get("HANA_PORT"), 
    user=os.environ.get("HANA_USER"), 
    password=os.environ.get("HANA_PASSWORD"),
    encrypt=True
    )

####################################
####################################
####################################

@app.route('/upload', methods = ['POST'])
def generate_vectors():
    
    try:

    ####################################
    # Read files locally

        #doc_type = request.headers.get('doc_type')
        
        #if not doc_type:
        #    return "No document type specified", 400
        
        #doc_type = secure_filename(doc_type)
        
        #doc_type_p = doc_type + "/"

        pdfs_directory = os.path.join(app.config['UPLOADED_FILES_DEST'], 'contracts')
        if not os.path.exists(pdfs_directory):
            os.makedirs(pdfs_directory)

        if 'pdfs' not in request.files:
            return "No file part", 400
        
        file_names = []
        files = request.files.getlist("pdfs")
        for file in files:
            if file and file.filename.endswith('.pdf'):
                # Save files
                file_path = os.path.join(pdfs_directory,file.filename)
                file.save(file_path)
                file_names.append(file.filename)
            else:
                return "Invalid file format", 400

        #doc_type = "contracts"

        ####################################
        # Check if files exist in db
        last_id = hana_vect.delta_capture(cc, file_names)

        ####################################
        # Create df with text partitions and respective metadata on files
        df = create_df.create_df(pdfs_directory, last_id)

        ####################################
        # Load data into HANA db
        hana_vect.load_data(cc, df)

        ####################################
        # Insert from staging into main table

        hana_vect.insert_main(cc, os.environ.get("HANA_SCHEMA"), os.environ.get("HANA_MAIN_TABLE"), os.environ.get("HANA_STAGING_TABLE"))
        return jsonify({"message": "Files successfully uploaded into SAP HANA", "files": file_names}), 200

    except Exception as e:
        app.logger.error(f"An error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
@app.route('/fetch_regulation', methods = ['POST'])
def fetch_contract():

    try:

        data = request.get_json()

        if not data:
            return jsonify({"error": "Request body must be JSON"}), 400

        regulation = data.get("regulation")

        if not regulation:
            return jsonify("Not all data was provided!"), 400
        
        context = ask.fetch_regulation(cc, regulation)
        
        return jsonify({"Regulation": context}), 200

    except Exception as e:
        app.logger.error(f"An error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
@app.route('/get_regulation_info', methods = ['POST'])
def get_regulation_info():

    try:

        data = request.get_json()

        if not data:
            return jsonify({"error": "Request body must be JSON"}), 400

        regulation = data.get("regulation")
        query = data.get("query")

        if not regulation:
            return jsonify("Not all data was provided!"), 400
        
        if not query:
            return jsonify("Not all data was provided!"), 400
        
        context = ask.get_regulation_info(cc, query, regulation)
        
        return jsonify({"Info": context}), 200

    except Exception as e:
        app.logger.error(f"An error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
@app.route('/compare_regulation', methods = ['POST'])
def compare_regulation():

    try:

        data = request.get_json()

        if not data:
            return jsonify({"error": "Request body must be JSON"}), 400

        regulation = data.get("regulation")
        query = data.get("query")

        if not regulation:
            return jsonify("Not all data was provided!"), 400
        
        if not query:
            return jsonify("Not all data was provided!"), 400
        
        context = ask.compare_regulation(cc, query, regulation)
        
        return jsonify({"Results of comparing": context}), 200

    except Exception as e:
        app.logger.error(f"An error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
    # Run the app
if __name__ == '__main__':
    app.run('0.0.0.0', port)