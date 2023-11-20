import os
from simplegmail import Gmail
import duckdb

# Set the database file path
db_file_path = 'emails.db'

# Check if the database file exists
if not os.path.exists(db_file_path):
    # If it doesn't exist, create the DuckDB connection and the table
    conn = duckdb.connect(database=db_file_path, read_only=False)
    cursor = conn.cursor()

    # Create a table to store email data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS emails (
            recipient TEXT,
            sender TEXT,
            subject TEXT,
            date TEXT,
            snippet TEXT,
            body TEXT,
            label TEXT
        )
    ''')

    # Commit the changes to the database
    conn.commit()
else:
    # If the database file already exists, use it
    conn = duckdb.connect(database=db_file_path, read_only=False)
    cursor = conn.cursor()

# Initialize Gmail
gmail = Gmail()

# Fetch unread messages in your inbox and sort them by labels
sorted_messages = sorted(gmail.get_unread_inbox(), key=lambda x: ', '.join(label.name for label in x.label_ids))

# Loop through the sorted messages and insert them into the database
for message in sorted_messages:
    recipient = message.recipient
    sender = message.sender
    subject = message.subject
    date = message.date
    snippet = message.snippet
    body = message.plain
    label_ids = ', '.join(label.name for label in message.label_ids)  # Extract label names into a comma-separated string

    # Insert the data into the DuckDB database
    cursor.execute('''
        INSERT INTO emails (recipient, sender, subject, date, snippet, body, label)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (recipient, sender, subject, date, snippet, body, label_ids))

    # Print out the email information
    print("To: " + recipient)
    print("From: " + sender)
    print("Subject: " + subject)
    print("Date: " + date)
    print("Preview: " + snippet)
    print("Body: " + body)
    print("Label IDs: " + label_ids)
    print("\n")

# Commit the changes to the database
conn.commit()

# Close the database connection
conn.close()
#'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'