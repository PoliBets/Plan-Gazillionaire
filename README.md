# PoliBets Backend

## Description
This is the backend for the Plan-Gazillionaire project, designed to provide arbitrage opportunities for betting in politics, financial markets, and other events. This backend connects to a MySQL database to manage and analyze betting data, including arbitrage opportunities, bet details, and outcomes.

For the frontend code, please visit our frontend github repo: https://github.com/RinnaZhang/Plan-Gazillionaire-Frontend.

## Installation and Setup
1. **Clone the Repository**
   Open your terminal or command prompt and run the following command:
   ```bash
   git clone https://github.com/phat-do-nyu/Plan-Gazillionaire
   ```

2. **Navigate to the Project Directory**
   Change into the project directory:
   ```bash
   cd Plan-Gazillionaire
   ```

3. **Create a Virtual Environment (recommended)**
   ```bash
   python -m venv .venv
   ```
   * **Windows**: `.\.venv\Scripts\activate`
   * **Mac/Linux**: `source .venv/bin/activate`

4. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Configure Environment Variables**
   * Create a `.env` file in the project root with your MySQL connection details:
   ```
   DB_HOST=your_mysql_host
   DB_USER=your_mysql_username
   DB_PASS=your_mysql_password
   DB_NAME=your_database_name
   ```
6. **Run the Application**
   ```bash
   python main.py
   python app.py
   ```
   
## API Endpoints
### GET /api/v1/arbitrage
* **Description**: Retrieves a list of all arbitrage opportunities.
* **URL**: http://localhost:9000/api/v1/arbitrage

### GET /api/v1/arbitrage/{arb_id}
* **Description**: Retrieves details of a specific arbitrage opportunity by ID.
* **URL Parameters**: arb_id (integer): ID of the arbitrage opportunity.

## Error Handling
This backend includes basic error handling for the following cases:
* **404 Not Found**: Returned if a specific resource (like an arbitrage opportunity) is not found.
* **500 Internal Server Error**: Returned for unexpected errors, such as database connectivity issues.

In the frontend, error messages are shown to the user whenever an error is encountered.

## User Interactions in the Frontend
This backend supports user interactions through API calls, allowing the frontend to:
1. **Fetch Data**: On loading the page, the frontend sends a GET request to retrieve all arbitrage opportunities, displaying them in a list.
2. **View Bet Details**: Users can click on an arbitrage opportunity to see more details, which triggers another GET request for that specific opportunity.
