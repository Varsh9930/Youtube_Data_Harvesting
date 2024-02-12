### YouTube Data Harvesting and Warehousing

This Python script enables users to collect data from YouTube channels using the YouTube Data API, store it in a MongoDB database, and then migrate it to a SQL database for further analysis. The collected data includes information about channels, playlists, videos, and comments. Additionally, it provides functionalities to visualize the data and execute SQL queries for insightful analysis.

#### Features:

1. **Data Collection and Storage:**
   - Collects channel information such as name, ID, subscribers, total views, total videos, and description.
   - Retrieves playlist details including ID, title, channel ID, channel name, published date, and the number of videos.
   - Gathers video information like ID, title, tags, thumbnail, description, published date, duration, views, likes, comments, etc.
   - Collects comment details such as ID, video ID, comment text, author name, and published date.

2. **Data Storage:**
   - Stores the collected data in a MongoDB database.

3. **Migrate to SQL:**
   - Migrates the collected data from MongoDB to a SQL database.
   - Creates separate tables for channels, playlists, videos, and comments in the SQL database.

4. **Visualization:**
   - Provides functionalities to view the stored data in tables using Streamlit.
   - Allows users to select and view data from channels, playlists, videos, and comments tables.

5. **SQL Queries:**
   - Executes SQL queries to analyze the data and derive insights.
   - Offers pre-defined queries for various analyses such as most viewed videos, channels with the most number of videos, videos with the highest number of comments, etc.

#### How to Use:

1. **Installation:**
   - Install the required Python packages listed in the `requirements.txt` file.
   - Make sure you have MongoDB and PostgreSQL installed and running on your system.

2. **API Key:**
   - Replace the placeholder API key with your own YouTube Data API key.

3. **Run the Script:**
   - Run the Python script to start the data collection and storage process.
   - Input the channel ID to collect data from the desired YouTube channel.
   - Click the "Collect and Store Data" button to initiate data collection and storage.

4. **Migrate to SQL:**
   - Once data collection is complete, click the "Migrate to SQL" button to migrate data to the SQL database.

5. **Visualization and Analysis:**
   - Use the Streamlit interface to visualize the stored data in tables.
   - Select the desired table (channels, playlists, videos, comments) to view the data.

6. **Execute SQL Queries:**
   - Choose from the pre-defined SQL queries provided in the dropdown menu to analyze the data.
   - View the query results and derive insights from the data.

#### Requirements:

- Python 3.x
- MongoDB
- PostgreSQL
- Google API Key
- Streamlit
- psycopg2
- pymongo
- pandas

#### Note:

- Ensure proper API key authentication and database configurations before running the script.
- Make sure to handle sensitive information securely, such as API keys and database credentials.
- Customize the script as per your requirements for additional functionalities or modifications.
