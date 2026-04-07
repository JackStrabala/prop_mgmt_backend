from datetime import date
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel


app = FastAPI()

PROJECT_ID = "mgmt-467-94721"
DATASET = "property_mgmt"

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class PropertyCreate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: Optional[str] = None
    tenant_name: Optional[str] = None
    monthly_rent: Optional[float] = None


class PropertyUpdate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: Optional[str] = None
    tenant_name: Optional[str] = None
    monthly_rent: Optional[float] = None


class IncomeCreate(BaseModel):
    amount: float
    date: date
    description: Optional[str] = None


class ExpenseCreate(BaseModel):
    amount: float
    date: date
    category: Optional[str] = None
    description: Optional[str] = None


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def property_exists(property_id: int, bq: bigquery.Client) -> bool:
    query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )
    results = list(bq.query(query, job_config=job_config).result())
    return len(results) > 0


def get_next_id(table_name: str, id_column: str, bq: bigquery.Client) -> int:
    query = f"""
        SELECT COALESCE(MAX({id_column}), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.{table_name}`
    """
    results = list(bq.query(query).result())
    return results[0]["next_id"]


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"message": "Property Management API is running"}


@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a single property by ID.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    return dict(results[0])


@app.post("/properties", status_code=201)
def create_property(property_data: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new property.
    """
    try:
        new_property_id = get_next_id("properties", "property_id", bq)

        query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.properties`
            (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
            VALUES
            (@property_id, @name, @address, @city, @state, @postal_code, @property_type, @tenant_name, @monthly_rent)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", new_property_id),
                bigquery.ScalarQueryParameter("name", "STRING", property_data.name),
                bigquery.ScalarQueryParameter("address", "STRING", property_data.address),
                bigquery.ScalarQueryParameter("city", "STRING", property_data.city),
                bigquery.ScalarQueryParameter("state", "STRING", property_data.state),
                bigquery.ScalarQueryParameter("postal_code", "STRING", property_data.postal_code),
                bigquery.ScalarQueryParameter("property_type", "STRING", property_data.property_type),
                bigquery.ScalarQueryParameter("tenant_name", "STRING", property_data.tenant_name),
                bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property_data.monthly_rent),
            ]
        )

        bq.query(query, job_config=job_config).result()

        return {
            "property_id": new_property_id,
            **property_data.model_dump()
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )


@app.put("/properties/{property_id}")
def update_property(property_id: int, property_data: PropertyUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Updates an existing property.
    """
    if not property_exists(property_id, bq):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.properties`
        SET
            name = @name,
            address = @address,
            city = @city,
            state = @state,
            postal_code = @postal_code,
            property_type = @property_type,
            tenant_name = @tenant_name,
            monthly_rent = @monthly_rent
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("name", "STRING", property_data.name),
            bigquery.ScalarQueryParameter("address", "STRING", property_data.address),
            bigquery.ScalarQueryParameter("city", "STRING", property_data.city),
            bigquery.ScalarQueryParameter("state", "STRING", property_data.state),
            bigquery.ScalarQueryParameter("postal_code", "STRING", property_data.postal_code),
            bigquery.ScalarQueryParameter("property_type", "STRING", property_data.property_type),
            bigquery.ScalarQueryParameter("tenant_name", "STRING", property_data.tenant_name),
            bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property_data.monthly_rent),
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
        return {
            "message": f"Property {property_id} updated successfully",
            "property": {
                "property_id": property_id,
                **property_data.model_dump()
            }
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database update failed: {str(e)}"
        )


@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Deletes a property.
    """
    if not property_exists(property_id, bq):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
        return {"message": f"Property {property_id} deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database delete failed: {str(e)}"
        )


@app.get("/properties/{property_id}/summary")
def get_property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a summary for one property, including total income, total expenses, and net.
    """
    if not property_exists(property_id, bq):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    query = f"""
        SELECT
            p.property_id,
            p.name,
            COALESCE(i.total_income, 0) AS total_income,
            COALESCE(e.total_expenses, 0) AS total_expenses,
            COALESCE(i.total_income, 0) - COALESCE(e.total_expenses, 0) AS net_amount
        FROM `{PROJECT_ID}.{DATASET}.properties` p
        LEFT JOIN (
            SELECT property_id, SUM(amount) AS total_income
            FROM `{PROJECT_ID}.{DATASET}.income`
            GROUP BY property_id
        ) i
        ON p.property_id = i.property_id
        LEFT JOIN (
            SELECT property_id, SUM(amount) AS total_expenses
            FROM `{PROJECT_ID}.{DATASET}.expenses`
            GROUP BY property_id
        ) e
        ON p.property_id = e.property_id
        WHERE p.property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
        return dict(results[0])
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


# ---------------------------------------------------------------------------
# Income
# ---------------------------------------------------------------------------

@app.get("/income/{property_id}")
def get_income_for_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all income records for a specific property.
    """
    query = f"""
        SELECT
            income_id,
            property_id,
            amount,
            date,
            description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.post("/income/{property_id}", status_code=201)
def create_income(property_id: int, income_data: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new income record for a property.
    """
    if not property_exists(property_id, bq):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    try:
        new_income_id = get_next_id("income", "income_id", bq)

        query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.income`
            (income_id, property_id, amount, date, description)
            VALUES
            (@income_id, @property_id, @amount, @date, @description)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("income_id", "INT64", new_income_id),
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("amount", "FLOAT64", income_data.amount),
                bigquery.ScalarQueryParameter("date", "DATE", income_data.date),
                bigquery.ScalarQueryParameter("description", "STRING", income_data.description),
            ]
        )

        bq.query(query, job_config=job_config).result()

        return {
            "income_id": new_income_id,
            "property_id": property_id,
            **income_data.model_dump()
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )


# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------

@app.get("/expenses/{property_id}")
def get_expenses_for_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all expense records for a specific property.
    """
    query = f"""
        SELECT
            expense_id,
            property_id,
            amount,
            date,
            category,
            description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.post("/expenses/{property_id}", status_code=201)
def create_expense(property_id: int, expense_data: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new expense record for a property.
    """
    if not property_exists(property_id, bq):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    try:
        new_expense_id = get_next_id("expenses", "expense_id", bq)

        query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
            (expense_id, property_id, amount, date, category, description)
            VALUES
            (@expense_id, @property_id, @amount, @date, @category, @description)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("expense_id", "INT64", new_expense_id),
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("amount", "FLOAT64", expense_data.amount),
                bigquery.ScalarQueryParameter("date", "DATE", expense_data.date),
                bigquery.ScalarQueryParameter("category", "STRING", expense_data.category),
                bigquery.ScalarQueryParameter("description", "STRING", expense_data.description),
            ]
        )

        bq.query(query, job_config=job_config).result()

        return {
            "expense_id": new_expense_id,
            "property_id": property_id,
            **expense_data.model_dump()
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )