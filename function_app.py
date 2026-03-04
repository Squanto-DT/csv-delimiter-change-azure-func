import azure.functions as func
import logging
import csv
import io

app = func.FunctionApp()

@app.route(route="process_csv", auth_level=func.AuthLevel.ANONYMOUS)
def process_csv(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing CSV file.')

    try:
        file_content = None
        filename = "processed.csv"
        
        # Check if file was uploaded as multipart/form-data
        if req.files and 'file' in req.files:
            file_item = req.files['file']
            file_content = file_item.read()
            if file_item.filename:
                filename = file_item.filename
        else:
            # Fallback to reading the raw body
            file_content = req.get_body()

        if not file_content:
            return func.HttpResponse(
                "Please pass a CSV file in the request body or as a form data 'file' field.",
                status_code=400
            )

        # Decode the file content
        text_content = file_content.decode('utf-8')

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