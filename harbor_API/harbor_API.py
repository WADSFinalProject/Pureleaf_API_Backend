from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from mysql.connector import Error
from harbor_pydantic import *
import mysql.connector

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Establishing MySQL Server Connection (Currently Local)
def get_new_connection():
    try:
        connection = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            passwd="*neoSQL01",
            database="central_database"
        )
        print("MySQL Database connection successful")
        return connection
    except Error as err:
        print(f"Error: '{err}'")
        return None

#API Endpoints
# Get all shipment
@app.get("/shipments", response_model=List[HarborCheckpoint])
def get_all_shipments():
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT hc.*, bs.status_description AS transport_status_description FROM harbor_checkpoint hc JOIN batch_status bs ON hc.transport_status = bs.status_ID")
        result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="No shipments found")
        
        # Log and validate the fetched data
        print("Fetched Data:", result)
        for row in result:
            if 'transport_status_description' not in row:
                raise HTTPException(status_code=500, detail="Missing required data: transport_status_description")

        return [HarborCheckpoint(**row) for row in result]
    except Error as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")
    finally:
        cursor.close()
        conn.close()


# Get shipment based on sent_date
@app.get("/shipments/sent_date/{date}", response_model=List[HarborCheckpoint])
def get_shipment_by_sent_date(date: datetime):
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM harbor_checkpoint WHERE sent_date = %s", (date,))
        result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Shipment not found")
        return [HarborCheckpoint(**row) for row in result]
    except Error as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")
    finally:
        cursor.close()
        conn.close()


# Get shipment based on arrival_date
@app.get("/shipments/arrival_date/{date}", response_model=List[HarborCheckpoint])
def get_shipment_by_arrival_date(date: datetime):
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM harbor_checkpoint WHERE arrival_date = %s", (date,))
        result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Shipment not found")
        return [HarborCheckpoint(**row) for row in result]
    except Error as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")
    finally:
        cursor.close()
        conn.close()


# Get shipment based on harbor_ID
@app.get("/shipment/{harbor_ID}", response_model=List[HarborCheckpoint])
def get_shipment_by_harbor(harbor_ID: int):
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)  
    try:
        cursor.execute("SELECT * FROM harbor_checkpoint WHERE harbor_ID = %s", (harbor_ID,))
        result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Shipment not found")
        return [HarborCheckpoint(**row) for row in result]
    except Error as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")
    finally:
        cursor.close()
        conn.close()


# Get shipment based on specific harbor_ID and its sent_date 
@app.get("/shipments/{harbor_id}/sent_date/{date}", response_model=List[HarborCheckpoint])
def get_shipment_by_harbor_and_sent_date(harbor_id: int, date: datetime):
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM harbor_checkpoint WHERE harbor_ID = %s AND sent_date = %s", (harbor_id, date))
        result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="No shipments found for the specified date and harbor")
        return [HarborCheckpoint(**row) for row in result]
    except Error as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")
    finally:
        cursor.close()
        conn.close()


# Get shipment based on specific harbor_ID and its arrival_date 
@app.get("/shipments/{harbor_id}/arrival_date/{date}", response_model=List[HarborCheckpoint])
def get_shipment_by_harbor_and_arrival_date(harbor_id: int, date: datetime):
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM harbor_checkpoint WHERE harbor_ID = %s AND arrival_date = %s", (harbor_id, date))
        result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="No shipments found for the specified date and harbor")
        return [HarborCheckpoint(**row) for row in result]
    except Error as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")
    finally:
        cursor.close()
        conn.close()


# Updating the status of shipment
@app.put("/updateShipment/{harbor_id}/{shipment_id}/{status}")
def update_shipment_status(harbor_id: int, shipment_id: int, status: int):
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE harbor_checkpoint SET transport_status = %s WHERE harbor_ID = %s AND checkpoint_ID = %s", (status, harbor_id, shipment_id))
        conn.commit()
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Shipment not found or status unchanged")
        return {"message": "Shipment status updated successfully"}
    except Error as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database update failed: {e}")
    finally:
        cursor.close()

# Latest shipment
@app.get("/shipments/latest", response_model=HarborCheckpoint)
def get_latest_shipment():
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT hc.*, bs.status_description AS transport_status_description" 
                       " FROM harbor_checkpoint hc JOIN batch_status bs ON hc.transport_status = bs.status_ID ORDER BY hc.sent_date DESC LIMIT 1;")
        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="No shipments found")
        return HarborCheckpoint(**result)
    except Error as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")
    finally:
        cursor.close()
        conn.close()


# Finished shipments
@app.get("/shipments/finished", response_model=List[HarborCheckpoint])
def get_shipments_with_status_3():
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT hc.*, bs.status_description AS transport_status_description "
            "FROM harbor_checkpoint hc "
            "JOIN batch_status bs ON hc.transport_status = bs.status_ID "
            "WHERE hc.transport_status = 3"
        )
        result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="No shipments found with transport status 3")
        return [HarborCheckpoint(**row) for row in result]
    except Error as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")
    finally:
        cursor.close()
        conn.close()

