import azure.functions as func
import logging
import csv
import io
import json
import base64

app = func.FunctionApp()

@app.route(route="process_csv", auth_level=func.AuthLevel.ANONYMOUS)
def process_csv(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('--- NEW REQUEST RECEIVED ---')
    
    # Log Incoming Headers
    headers_dict = dict(req.headers)
    logging.info(f"Incoming Headers: {json.dumps(headers_dict, indent=2)}")

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
            logging.info("Input found via multipart/form-data")
        else:
            # Fallback to reading the body (could be JSON with Base64 or raw text)
            body_bytes = req.get_body()
            logging.info(f"Raw body length: {len(body_bytes)} bytes")
            
            if body_bytes:
                try:
                    # Manually parse JSON since Power Automate might not send 'application/json' Content-Type
                    body_str = body_bytes.decode('utf-8')
                    req_json = json.loads(body_str)
                    
                    if isinstance(req_json, dict):
                        if '$content' in req_json:
                            file_content = base64.b64decode(req_json['$content'])
                            logging.info("Input decoded from JSON Base64 wrapper ($content)")
                        elif 'file' in req_json and isinstance(req_json['file'], dict) and '$content' in req_json['file']:
                            file_content = base64.b64decode(req_json['file']['$content'])
                            logging.info("Input decoded from JSON Base64 wrapper (file.$content)")
                            if 'filename' in req_json:
                                filename = req_json['filename']
                except Exception:
                    # If it's not JSON or decoding fails, we treat it as raw text/CSV
                    pass
                
                # If we couldn't extract base64 from a JSON wrapper, assume the body IS the file content
                if not file_content:
                    file_content = body_bytes
                    logging.info("Input taken as raw byte stream fallback")

        if not file_content:
            headers_str = str(dict(req.headers))
            body_len = len(body_bytes) if 'body_bytes' in locals() and body_bytes else 0
            debug_info = f"DEBUG INFO: Headers received: {headers_str} | Body length: {body_len} bytes. "
            if body_len > 0:
                debug_info += f"Body snippet: {body_bytes[:100]}... "
            logging.error(f"Failed to find file content. Debug: {debug_info}")
            return func.HttpResponse(
                debug_info + "Please pass a CSV file in the request body, JSON structure, or as a form data 'file' field.",
                status_code=400
            )

        # Log an excerpt of the gathered file content
        logging.info(f"Gathered File Content Length: {len(file_content)} bytes")
        logging.info(f"Input Excerpt (first 100 chars): {file_content[:100]}")

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
        
        logging.info(f"Processed CSV Length: {len(processed_csv)} bytes")
        logging.info(f"Output Excerpt (first 100 chars): {processed_csv[:100]}")
        logging.info("--- REQUEST COMPLETED SUCCESSFULLY ---")

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