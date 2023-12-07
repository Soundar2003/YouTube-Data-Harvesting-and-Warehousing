# YouTube Data Harvesting and Warehousing using SQL, MongoDB, and Streamlit: Documentation

**1. Introduction**

This project revolves around building a system for harvesting and warehousing data from YouTube channels. It leverages the power of three key technologies:

* **YouTube Data API:** Retrieves data like channel information, video details, comments, etc.
* **MongoDB:** Acts as a flexible data lake for storing the raw, unstructured YouTube data.
* **SQL:** Provides a structured data warehouse for efficient querying and analysis.
* **Streamlit:** Enables building a user-friendly interface to interact with the system.

**2. System Architecture**

The project follows a three-step architecture:

1. **Data Harvesting:**
    * Users input YouTube channel IDs through the Streamlit interface.
    * Python script utilizing the YouTube Data API fetches relevant data.
    * Extracted data is stored in MongoDB as JSON documents.

2. **Data Warehousing:**
    * Users select channels to migrate data from MongoDB to the SQL warehouse.
    * Python script transforms and structures the JSON data into relational tables.
    * Tables are populated in the SQL database (e.g., MySQL, PostgreSQL).

3. **Data Analysis & Visualization:**
    * Users can query the SQL warehouse through the Streamlit interface.
    * Queries can filter, join, and aggregate data across channels and videos.
    * Streamlit displays interactive visualizations of the queried data.

**3. Key Functionalities:**

* **Harvest data from multiple YouTube channels.**
* **Store raw data in a scalable and flexible MongoDB data lake.**
* **Migrate data to a structured SQL warehouse for efficient analysis.**
* **Query the warehouse with user-defined filters and joins.**
* **Visualize data insights through interactive dashboards built with Streamlit.**

**4. Technologies Used:**

* **Python:** Scripting language for data harvesting, transformation, and warehousing.
* **Google API Client Library for Python:** Interacts with the YouTube Data API.
* **PyMongo:** Driver for interacting with MongoDB.
* **SQLalchemy:** Python library for connecting and manipulating SQL databases.
* **Streamlit:** Framework for building data applications with interactive UI.

**5. Conclusion:**

This project demonstrates a powerful approach to harvesting, warehousing, and analyzing YouTube data using a combination of best-suited technologies. It empowers users to gain valuable insights from the vast world of YouTube content, paving the way for further exploration and innovation.



