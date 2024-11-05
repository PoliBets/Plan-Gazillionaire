# PoliBets Front End

## Description
Brief description of the web app. Include what the app does and any important information users need to know.

## Installation and Launch Instructions

Follow these steps to clone and run the web app on your local machine.

### Prerequisites
- **Node.js**: Ensure that Node.js is installed on your system. You can download it from [nodejs.org](https://nodejs.org/).
- **Git**: Ensure Git is installed for cloning the repository. Download it from [git-scm.com](https://git-scm.com/).

### Steps to Launch

1. **Clone the Repository**
   Open your terminal or command prompt and run the following command:
   ```bash
   git clone <https://github.com/RinnaZhang/Plan-Gazillionaire-Frontend.git>
   ```

2. **Navigate to the Project Directory**
   Change into the project directory:
   ```bash
   cd <project-folder-name>
   ```
   Replace `<project-folder-name>` with the name of the cloned project folder.

3. **Install Dependencies**
   Install the project dependencies by running:
   ```bash
   npm install
   ```

4. **Start the Development Server**
   Launch the development server with:
   ```bash
   npm start
   ```

5. **Access the Web App**
   Once the server is running, open your web browser and navigate to:
   ```
   http://localhost:3000
   ```
   The web app should now be running locally on your machine.

### Troubleshooting
- If you encounter any issues with missing dependencies or errors, make sure that Node.js and npm are up to date.
- Run `npm audit fix` or `npm install` again if you face dependency warnings.

### Additional Notes
- For production builds, use:
  ```bash
  npm run build
  ```
  This will create an optimized build in the `build` directory.