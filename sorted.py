import duckdb
import openai
import re

# Set your OpenAI GPT-3 API key
openai.api_key = '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'

# Set the database file path
db_file_path = 'emails.db'

# Connect to the DuckDB database
conn = duckdb.connect(database=db_file_path)
cursor = conn.cursor()

# Set the transaction mode to read-write
cursor.execute('BEGIN TRANSACTION;')

# Create a new table to store extracted information
cursor.execute('CREATE TABLE IF NOT EXISTS extracted_data (email_body STRING, cleaned_text STRING, context_and_links STRING, extracted_links STRING)')

# Fetch email bodies from the database
cursor.execute('SELECT body FROM emails')
emails = cursor.fetchdf()['body'].tolist()

# Process each email individually
for email in emails:
    try:
        # Split email content into chunks
        chunk_size = 300  # You can adjust the chunk size based on your requirements
        email_chunks = [email[i:i + chunk_size] for i in range(0, len(email), chunk_size)]

        cleaned_text_chunks = []

        # Process each chunk using ChatGPT to get useful content and links
        for chunk in email_chunks:
            cleaned_text_chunk = openai.Completion.create(
                model="text-davinci-003",
                prompt=f"Extract useful content and links: {chunk}",
                temperature=0.7,
                max_tokens=300
            )['choices'][0]['text']

            cleaned_text_chunks.append(cleaned_text_chunk)

        # Combine cleaned text chunks into a single string
        cleaned_text = ''.join(cleaned_text_chunks)

        # Use ChatGPT to get context and links from the cleaned text
        context_and_links = openai.Completion.create(
            model="text-davinci-003",
            prompt=f"Extract context and links: {cleaned_text}",
            temperature=0.7,
            max_tokens=300
        )['choices'][0]['text']

        # Extract links from the context_and_links using regex
        extracted_links = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', context_and_links)

        # Insert the extracted information into the new table
        cursor.execute('INSERT INTO extracted_data VALUES (?, ?, ?, ?)', (email, cleaned_text, context_and_links, ','.join(extracted_links)))

    except openai.error.OpenAIError as e:
        # Handle token limitation errors (skip the email)
        print(f"Error processing email: {e}")
        continue

# Commit changes and close the database connection
cursor.execute('COMMIT;')
conn.close()
