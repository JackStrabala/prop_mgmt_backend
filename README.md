# Property Management App — Backend

This repository contains the starter backend for the Property Management App individual project. By the end of this setup, you will have a live FastAPI application running on Google Cloud Run, connected to a BigQuery database pre-loaded with sample data.

## What's in this repo

| File | Description |
|---|---|
| `main.py` | Starter FastAPI application with one working endpoint |
| `pyproject.toml` | Poetry dependency configuration |
| `Dockerfile` | Container build instructions for Cloud Run |
| `create_tables.sql` | BigQuery DDL — creates the three database tables |
| `populate_db.sql` | BigQuery DML — loads sample data into the tables |

## Overview

1. [Set Up the Database in BigQuery](#part-1-set-up-the-database-in-bigquery)
   - [Step 1: Select Your Google Cloud Project](#step-1-select-your-google-cloud-project)
   - [Step 2: Create a BigQuery Dataset](#step-2-create-a-bigquery-dataset)
   - [Step 3: Create the Tables](#step-3-create-the-tables)
   - [Step 4: Load the Sample Data](#step-4-load-the-sample-data)
2. [Fork and Clone the Repository](#part-2-fork-and-clone-the-repository)
   - [Step 5: Fork This Repository](#step-5-fork-this-repository)
   - [Step 6: Clone Your Fork in Cloud Shell](#step-6-clone-your-fork-in-cloud-shell)
3. [Configure and Run the API](#part-3-configure-and-run-the-api)
   - [Step 7: Open the Project in Cloud Shell Editor](#step-7-open-the-project-in-cloud-shell-editor)
   - [Step 8: Install Dependencies](#step-8-install-dependencies)
   - [Step 9: Update Your Project ID](#step-9-update-your-project-id)
   - [Step 10: Test Locally](#step-10-test-locally)
   - [Step 11: Connect Cloud Run to GitHub](#step-11-connect-cloud-run-to-github)
   - [Step 12: Push to GitHub and Deploy](#step-12-push-to-github-and-deploy)
   - [Step 13: Add Your Service Account to Cloud Run](#step-13-add-your-service-account-to-cloud-run)
4. [What's Next](#whats-next)

---

## Part 1: Set Up the Database in BigQuery

### Step 1: Select Your Google Cloud Project

1. Go to the [Google Cloud Console](https://console.cloud.google.com)
2. In the project selector at the top of the page, confirm that your course project is selected

---

### Step 2: Create a BigQuery Dataset

1. Search for **BigQuery** in the top search bar and open it
2. In the **Explorer** pane on the left, click the three dots (`⋮`) next to your Project ID and select **Create dataset**
3. Fill in the following fields:
   - **Dataset ID**: `property_mgmt`
   - **Location type**: `us-central1`
4. Click **Create Dataset**

---

### Step 3: Create the Tables

1. At the top of the BigQuery interface, click the **Untitled query** tab
2. Open `create_tables.sql` from this repository and copy its entire contents
3. Paste the contents into the query tab

> [!IMPORTANT]
> Before running the query, replace every occurrence of `` `your_dataset` `` with your actual project ID and dataset, following this format:
> `` `your-project-id.property_mgmt` ``
>
> For example, if your project ID is `mgmt54500-jsmith`, the first table should read:
> ```sql
> CREATE TABLE IF NOT EXISTS `mgmt54500-jsmith.property_mgmt.properties` (
> ```

4. Click **Run**
5. Verify that three new tables appear under `property_mgmt` in the Explorer pane: `properties`, `income`, and `expenses`

---

### Step 4: Load the Sample Data

1. Open a new query tab in BigQuery
2. Open `populate_db.sql` from this repository and copy its entire contents
3. Paste the contents into the query tab

> [!IMPORTANT]
> This file does not reference your project ID — it uses table names only (e.g., `properties`, `income`, `expenses`). BigQuery will automatically use the project and dataset you selected in the Explorer pane.
>
> Before running, make sure your `property_mgmt` dataset is selected as the default in the Explorer. You can confirm this by clicking the dataset name so it is highlighted.

4. Click **Run**
5. To verify the data loaded correctly, run this quick check in a new query tab (substituting your project ID):

```sql
SELECT 'properties' AS tbl, COUNT(*) AS row_count FROM `your-project-id.property_mgmt.properties`
UNION ALL
SELECT 'income',   COUNT(*) FROM `your-project-id.property_mgmt.income`
UNION ALL
SELECT 'expenses', COUNT(*) FROM `your-project-id.property_mgmt.expenses`
```

You should see 3 properties, 111 income records, and 183 expense records.

---

## Part 2: Fork and Clone the Repository

### Step 5: Fork This Repository

Forking creates your own personal copy of this repository under your GitHub account. All of your work for this project will live in your fork.

1. Click the **Fork** button at the top right of this page
2. Leave all settings at their defaults and click **Create fork**
3. You should now be looking at `your-github-username/prop_mgmt_backend` — confirm that the URL includes your username

---

### Step 6: Clone Your Fork in Cloud Shell

1. Open [Google Cloud Shell](https://shell.cloud.google.com)
2. Click the **Code** button on your forked repository page and copy the HTTPS clone URL
3. In the Cloud Shell terminal, run:

```bash
git clone YOUR_FORK_URL
```

4. Navigate into the project directory:

```bash
cd prop_mgmt_backend
```

---

## Part 3: Configure and Run the API

### Step 7: Open the Project in Cloud Shell Editor

1. In the Cloud Shell terminal, open the project in the editor:

```bash
cloudshell edit .
```

2. You should see the following files in the Explorer pane:

```
prop_mgmt_backend/
├── main.py             ← Your FastAPI application
├── pyproject.toml      ← Poetry dependencies
├── Dockerfile          ← Container build instructions
├── create_tables.sql   ← Reference copy of the table definitions
└── populate_db.sql     ← Reference copy of the seed data
```

---

### Step 8: Install Dependencies

In the Cloud Shell terminal, run:

```bash
poetry install
```

Poetry will read `pyproject.toml` and install FastAPI, Uvicorn, and the BigQuery client library.

---

### Step 9: Update Your Project ID

1. Open `main.py` in the Cloud Shell Editor
2. Find this line near the top of the file:

```python
PROJECT_ID = "YOUR_PROJECT_ID"
```

3. Replace `YOUR_PROJECT_ID` with your actual Google Cloud project ID — the same one you used in the BigQuery steps above

> [!TIP]
> You can find your project ID in the [Google Cloud Console](https://console.cloud.google.com) project selector at the top of the page. It looks something like `mgmt54500-jsmith`.

---

### Step 10: Test Locally

Before you can test locally, Cloud Shell needs to know which Google Cloud project to use when connecting to BigQuery.

1. In the Cloud Shell terminal, set your active project:

```bash
gcloud config set project YOUR_PROJECT_ID
```

Replace `YOUR_PROJECT_ID` with your actual project ID — the same value you put in `main.py`.

2. Start the development server:

```bash
poetry run uvicorn main:app --reload --port 8080
```

3. Click the **Web Preview** button (the eye icon in the Cloud Shell toolbar) and select **Preview on port 8080**
4. In the browser tab that opens, add `/properties` to the URL

You should see a JSON array containing Alex's three rental properties. If you see an authentication error, confirm that your `PROJECT_ID` in `main.py` matches the project you set with `gcloud config set project`.

5. Press `Ctrl+C` in the terminal to stop the server

---

### Step 11: Connect Cloud Run to GitHub

This step creates the Cloud Build trigger and Cloud Run service that will automatically deploy your API whenever you push to GitHub. You only need to do this once.

1. In the [Google Cloud Console](https://console.cloud.google.com), search for **Cloud Run** and select **Services**
2. Click **+ Create Service**
3. Select **Continuously deploy from a repository (source or function)**
4. Click **Set up with Cloud Build**
5. Under **Repository provider**, select **GitHub** and click **Authenticate** — authorize Google Cloud Build to access your GitHub account when prompted
6. Under **Repository**, search for and select your forked repository (`your-github-username/prop_mgmt_backend`)
7. Click **Next**
8. Under **Branch**, leave the default (`^main$`)
9. Under **Build type**, select **Dockerfile** — leave the Dockerfile location as `/Dockerfile`
10. Click **Save**
11. Back on the service configuration screen, fill in the following:
    - **Service name**: `prop-mgmt-api`
    - **Region**: `us-central1`
    - **Authentication**: Allow unauthenticated invocations
12. Click **Create**

Cloud Build will trigger an initial deployment. This may take a few minutes. Once it completes, you will see a green checkmark and a live URL at the top of the service page.

> [!NOTE]
> If you see a build error on the first deploy, it is likely because `YOUR_PROJECT_ID` was not replaced in `main.py` before pushing. Update the file, commit, and push again — Cloud Build will automatically trigger a new deployment.

---

### Step 12: Push to GitHub and Deploy

Now that Cloud Run is connected to your repository, every push to `main` will automatically trigger a new deployment.

1. Stage your changes:

```bash
git add .
```

2. Commit with a descriptive message:

```bash
git commit -m "Configure project ID for BigQuery connection"
```

3. Push to your fork:

```bash
git push
```

4. In the [Google Cloud Console](https://console.cloud.google.com), search for **Cloud Build** and confirm that a new build has triggered
5. Once the build finishes, search for **Cloud Run** and click on **prop-mgmt-api**

> [!NOTE]
> The service is deployed, but visiting `/properties` may return a permissions error at this point. The deployed API runs under the default service account, which does not have permission to query BigQuery. Complete Step 13 to fix this, then return here to verify the live URL.

---

### Step 13: Add Your Service Account to Cloud Run

Now that your service exists in Cloud Run, assign it the service account that has permission to query BigQuery. The Cloud Run service must exist before this step can be completed — that is why it comes after the first deployment.

> [!NOTE]
> You are not creating a new service account — you are reusing the one you created in a previous assignment (`fastapi-bq-accessor`).

1. In the [Google Cloud Console](https://console.cloud.google.com), search for **Cloud Run** and select **Services**
2. Click on **prop-mgmt-api**
3. At the top of the screen, click **✏️ Edit & deploy new revision**
4. Select the **Security** tab
5. In the **Service account** dropdown, choose `fastapi-bq-accessor`
6. Click **Deploy**
7. Once the new revision has deployed, click the service URL at the top of the page and append `/properties` — you should now see the same JSON response you saw locally

---

## What's Next

Your backend is live. The `/properties` endpoint is your starting point — refer to the assignment instructions in Brightspace (IA 9) for the full list of endpoints you are responsible for building.

The interactive API documentation is available at `/docs` on both your local server and your deployed Cloud Run URL. Use it to explore and test your endpoints as you build them.
