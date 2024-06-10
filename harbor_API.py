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
@app.get("/shipments/{harbor_ID}/latest", response_model=HarborCheckpoint)
def get_latest_shipment(harbor_ID: int):
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT hc.*, bs.status_description AS transport_status_description
            FROM harbor_checkpoint hc
            JOIN batch_status bs ON hc.transport_status = bs.status_ID
            WHERE hc.harbor_ID = %s
            ORDER BY hc.sent_date DESC LIMIT 1
        """, (harbor_ID,))
        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="No shipments found for this harbor")
        return HarborCheckpoint(**result)
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")
    finally:
        cursor.close()
        conn.close()


# Finished shipments
@app.get("/shipments/{harbor_ID}/finished", response_model=List[HarborCheckpoint])
def get_shipments_with_status_3(harbor_ID: int):
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT hc.*, bs.status_description AS transport_status_description
            FROM harbor_checkpoint hc
            JOIN batch_status bs ON hc.transport_status = bs.status_ID
            WHERE hc.transport_status = 3 AND hc.harbor_ID = %s
        """, (harbor_ID,))
        result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="No finished shipments found for this harbor")
        return [HarborCheckpoint(**row) for row in result]
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")
    finally:
        cursor.close()
        conn.close()


# Update Shipment Information
@app.put("/update_harbor_shipment/{harbor_ID}/{checkpoint_ID}", response_model=HarborCheckpoint)
def update_harbor_shipment(harbor_ID: int, checkpoint_ID: int, update_data: HarborUpdateModel):
    conn = get_new_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(dictionary=True)
    try:
        # Building the dynamic SQL query based on provided fields
        update_fields = []
        values = []
        
        if update_data.harbor_batch_rescale is not None:
            update_fields.append("harbor_batch_rescale = %s")
            values.append(update_data.harbor_batch_rescale)
        if update_data.arrival_date is not None:
            update_fields.append("arrival_date = %s")
            values.append(update_data.arrival_date)
        if update_data.hg_user_ID is not None:
            update_fields.append("hg_user_ID = %s")
            values.append(update_data.hg_user_ID)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No valid fields provided for update")

        # Join the update fields with commas and format the SQL statement
        sql_update = ", ".join(update_fields)
        sql_query = f"UPDATE harbor_checkpoint SET {sql_update} WHERE checkpoint_ID = %s AND harbor_ID = %s"
        values.append(checkpoint_ID)
        values.append(harbor_ID)  # Add harbor_ID to the SQL parameters
        
        cursor.execute(sql_query, tuple(values))
        conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Shipment not found, no update needed, or it does not belong to this harbor")

        # Fetch the updated record to return
        cursor.execute("SELECT * FROM harbor_checkpoint WHERE checkpoint_ID = %s AND harbor_ID = %s", (checkpoint_ID, harbor_ID))
        updated_shipment = cursor.fetchone()
        if not updated_shipment:
            raise HTTPException(status_code=404, detail="Shipment not found after update in this harbor")

        return HarborCheckpoint(**updated_shipment)
    except Error as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database update failed: {e}")
    finally:
        cursor.close()
        conn.close()
