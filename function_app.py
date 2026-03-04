import azure.functions as func
import logging
import csv
import io
import json
import base64

app = func.FunctionApp()

@app.route(route="process_csv", auth_level=func.AuthLevel.ANONYMOUS)
def process_csv(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing CSV file.')

    try:
        file_content = None
        # Try to get filename from header or query param first
        filename = req.headers.get('x-filename') or req.params.get('filename') or "processed.csv"
        
        # Check if file was uploaded as multipart/form-data
        if req.files and 'file' in req.files:
            file_item = req.files['file']
            file_content = file_item.read()
            if not req.headers.get('x-filename') and not req.params.get('filename') and file_item.filename:
                filename = file_item.filename
        else:
            # Fallback to reading the body (could be JSON with Base64 or raw text)
            try:
                req_json = req.get_json()
                # Check for Power Automate file content signature
                if isinstance(req_json, dict):
                    if '$content' in req_json:
                        # Sometimes base64 decoding needs padding fixes, but usually it's fine
                        file_content = base64.b64decode(req_json['$content'])
                    elif 'file' in req_json and isinstance(req_json['file'], dict) and '$content' in req_json['file']:
                        file_content = base64.b64decode(req_json['file']['$content'])
                        if 'filename' in req_json:
                            filename = req_json['filename']
            except ValueError:
                pass # Not JSON

            if not file_content:
                file_content = req.get_body()

        if not file_content:
            return func.HttpResponse(
                "Please pass a CSV file in the request body, JSON structure, or as a form data 'file' field.",
                status_code=400
            )

        # Decode the file content (handling potential UTF-8 BOM commonly placed by Excel/Windows)
        try:
            text_content = file_content.decode('utf-8-sig')
        except UnicodeDecodeError:
            text_content = file_content.decode('latin-1')

        # Use csv.Sniffer to detect the delimiter
        sample_size = min(1024, len(text_content))
        sample = text_content[:sample_size]
        
        try:
            dialect = csv.Sniffer().sniff(sample)
            delimiter = dialect.delimiter
            logging.info(f"Detected delimiter: {repr(delimiter)}")
        except csv.Error:
            logging.warning("Could not automatically detect delimiter. Defaulting to comma (,).")
            delimiter = ','

        # Read the CSV Data
        f_in = io.StringIO(text_content)
        reader = csv.reader(f_in, delimiter=delimiter)

        # Write the CSV Data with pipe delimiter
        f_out = io.StringIO()
        writer = csv.writer(f_out, delimiter='|')
        
        for row in reader:
            writer.writerow(row)

        processed_csv = f_out.getvalue()

        # Return the processed file directly
        return func.HttpResponse(
            body=processed_csv,
            mimetype="text/csv",
            status_code=200,
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        return func.HttpResponse(
            f"An error occurred: {str(e)}",
            status_code=500
        )