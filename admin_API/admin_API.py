from fastapi import FastAPI, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import mysql.connector
from mysql.connector import Error
from admin_pydantic import *
from typing import List
from fastapi.responses import JSONResponse

app = FastAPI()

# CORSMiddleware Config
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
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
# Get ALL batch infomation
@app.get("/batches/", response_model=List[BatchInformation])
async def get_all_batches():
    connection = get_new_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
        
    try:
        cursor = connection.cursor(dictionary=True)
        query = """
        SELECT bi.*, dl.*, wl.*, pl.*
        FROM batch_information bi
        LEFT JOIN dry_leaves dl ON bi.dry_leaves_ID = dl.dry_leaves_ID
        LEFT JOIN wet_leaves wl ON bi.wet_leaves_ID = wl.wet_leaves_ID
        LEFT JOIN powdered_leaves pl ON bi.powdered_leaves_ID = pl.powdered_leaves_ID;
        """

        cursor.execute(query)
        result = cursor.fetchall()
        formatted_result = []
        for row in result:
            formatted_row = {
                "batch_ID": row["batch_ID"],
                "batch_date": row["batch_date"],
                "dry_leaves_ID": row.get("dry_leaves_ID"),
                "wet_leaves_ID": row.get("wet_leaves_ID"),
                "powdered_leaves_ID": row.get("powdered_leaves_ID"),
                "status": row["status"],
                "dry_leaves": {
                    "dry_leaves_ID": row.get("dry_leaves_ID"),
                    "dry_weight": row.get("dry_weight"),
                    "dry_date": row.get("dry_date"),
                    "dry_image": row.get("dry_image")
                } if row.get("dry_leaves_ID") else None,
                "wet_leaves": {
                    "wet_leaves_ID": row.get("wet_leaves_ID"),
                    "wet_weight": row.get("wet_weight"),
                    "wet_date": row.get("wet_date"),
                    "wet_image": row.get("wet_image")
                } if row.get("wet_leaves_ID") else None,
                "powdered_leaves": {
                    "powdered_leaves_ID": row.get("powdered_leaves_ID"),
                    "powdered_weight": row.get("powdered_weight"),
                    "powdered_date": row.get("powdered_date"),
                    "powdered_image": row.get("powdered_image")
                } if row.get("powdered_leaves_ID") else None
            }
            formatted_result.append(formatted_row)
        return formatted_result
    except Error as err:
        raise HTTPException(status_code=500, detail=str(err))
    
# Get 1 batch infomation
@app.get("/batches/{batch_id}", response_model=BatchInformation)
async def get_batch_by_id(batch_id: int = Path(..., title="The ID of the batch to retrieve")):
    connection = get_new_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
        
    try:
        cursor = connection.cursor(dictionary=True)
        query = """
        SELECT bi.*, dl.*, wl.*, pl.*
        FROM batch_information bi
        LEFT JOIN dry_leaves dl ON bi.dry_leaves_ID = dl.dry_leaves_ID
        LEFT JOIN wet_leaves wl ON bi.wet_leaves_ID = wl.wet_leaves_ID
        LEFT JOIN powdered_leaves pl ON bi.powdered_leaves_ID = pl.powdered_leaves_ID
        WHERE bi.batch_ID = %s;
        """

        cursor.execute(query, (batch_id,))
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        
        if result is None:
            raise HTTPException(status_code=404, detail="Batch not found")
        
        formatted_result = {
            "batch_ID": result["batch_ID"],
            "batch_date": result["batch_date"],
            "dry_leaves_ID": result.get("dry_leaves_ID"),
            "wet_leaves_ID": result.get("wet_leaves_ID"),
            "powdered_leaves_ID": result.get("powdered_leaves_ID"),
            "status": result["status"],
            "dry_leaves": {
                "dry_leaves_ID": result.get("dry_leaves_ID"),
                "dry_weight": result.get("dry_weight"),
                "dry_date": result.get("dry_date"),
                "dry_image": result.get("dry_image")
            } if result.get("dry_leaves_ID") else None,
            "wet_leaves": {
                "wet_leaves_ID": result.get("wet_leaves_ID"),
                "wet_weight": result.get("wet_weight"),
                "wet_date": result.get("wet_date"),
                "wet_image": result.get("wet_image")
            } if result.get("wet_leaves_ID") else None,
            "powdered_leaves": {
                "powdered_leaves_ID": result.get("powdered_leaves_ID"),
                "powdered_weight": result.get("powdered_weight"),
                "powdered_date": result.get("powdered_date"),
                "powdered_image": result.get("powdered_image")
            } if result.get("powdered_leaves_ID") else None
        }
        return formatted_result
    except Error as err:
        raise HTTPException(status_code=500, detail=str(err))

# TODO: Needs to be adjusted to work with new status function.
# Get batch information based on status
# @app.get("/batches/status/{status}", response_model=List[BatchInformation])
# async def get_batches_by_status(status: int):
#     if status not in [0, 1]:
#         raise HTTPException(status_code=400, detail="Invalid status. Status must be 0 or 1.")
    
#     connection = get_new_connection()
#     if connection is None:
#         raise HTTPException(status_code=500, detail="Database connection failed")
        
#     try:
#         cursor = connection.cursor(dictionary=True)
#         query = """
#         SELECT bi.*, dl.*, wl.*, pl.*
#         FROM batch_information bi
#         LEFT JOIN dry_leaves dl ON bi.dry_leaves_ID = dl.dry_leaves_ID
#         LEFT JOIN wet_leaves wl ON bi.wet_leaves_ID = wl.wet_leaves_ID
#         LEFT JOIN powdered_leaves pl ON bi.powdered_leaves_ID = pl.powdered_leaves_ID
#         WHERE bi.status = %s;
#         """
#         cursor.execute(query, (status,))
#         result = cursor.fetchall()
#         cursor.close()
#         connection.close()
        
#         formatted_result = []
#         for row in result:
#             formatted_row = {
#                 "batch_ID": row["batch_ID"],
#                 "batch_date": row["batch_date"],
#                 "dry_leaves_ID": row.get("dry_leaves_ID"),
#                 "wet_leaves_ID": row.get("wet_leaves_ID"),
#                 "powdered_leaves_ID": row.get("powdered_leaves_ID"),
#                 "status": row["status"],
#                 "dry_leaves": {
#                     "dry_leaves_ID": row.get("dry_leaves_ID"),
#                     "dry_weight": row.get("dry_weight"),
#                     "dry_date": row.get("dry_date"),
#                     "dry_image": row.get("dry_image")
#                 } if row.get("dry_leaves_ID") else None,
#                 "wet_leaves": {
#                     "wet_leaves_ID": row.get("wet_leaves_ID"),
#                     "wet_weight": row.get("wet_weight"),
#                     "wet_date": row.get("wet_date"),
#                     "wet_image": row.get("wet_image")
#                 } if row.get("wet_leaves_ID") else None,
#                 "powdered_leaves": {
#                     "powdered_leaves_ID": row.get("powdered_leaves_ID"),
#                     "powdered_weight": row.get("powdered_weight"),
#                     "powdered_date": row.get("powdered_date"),
#                     "powdered_image": row.get("powdered_image")
#                 } if row.get("powdered_leaves_ID") else None
#             }
#             formatted_result.append(formatted_row)
#         return formatted_result
#     except Error as err:
#         raise HTTPException(status_code=500, detail=str(err))

# TODO: This piece seems redundant for Admin, since Admin is only for viewing data
# Update order as finished
# @app.put("/toggle-batch-status/{batch_id}")
# def toggle_batch_status(batch_id: int = Path(..., description="The ID of the batch to toggle the status")):
#     connection = get_new_connection()
#     if connection is None:
#         raise HTTPException(status_code=500, detail="Database connection failed")
    
#     try:
#         cursor = connection.cursor()
#         toggle_status_query = "UPDATE batch_information SET status = NOT status WHERE batch_ID = %s"
#         cursor.execute(toggle_status_query, (batch_id,))
#         connection.commit()
#         cursor.close()
#         return JSONResponse(content={"message": "Batch status updated successfully"}, status_code=200)
#     except mysql.connector.Error as err:
#         print("Error occurred:", err)
#         return JSONResponse(content={"message": "Failed to update batch status"}, status_code=500)
    
# Get all shipment information with harbor guard user IDs
@app.get("/shipments/", response_model=List[HarborCheckpoint])
def get_all_shipments():
    connection = get_new_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = connection.cursor(dictionary=True)
        query = """
        SELECT hc.*, hgu.user_ID
        FROM harbor_checkpoint hc
        LEFT JOIN harbor_guard_user hgu ON hc.hg_user_ID = hgu.hg_user_ID
        """
        cursor.execute(query)
        shipments = cursor.fetchall()
        print(shipments)  # Debug: Print the raw results
    except Error as err:
        raise HTTPException(status_code=500, detail=str(err))
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    formatted_result = []
    for row in shipments:
        formatted_row = {
            "checkpoint_ID": row["checkpoint_ID"],
            "harbor_batch_rescale": row["harbor_batch_rescale"],
            "sent_date": row["sent_date"],
            "arrival_date": row["arrival_date"],
            "transport_status": row["transport_status"],
            "batch_ID": row["batch_ID"],
            "hg_user_ID": row["hg_user_ID"],
            "harbor_ID": row["harbor_ID"],
            "user_ID": row.get("user_ID") 
        }
        formatted_result.append(formatted_row)
    return formatted_result


# Get 1 shipment information
@app.get("/shipments/{shipment_id}", response_model=HarborCheckpoint)
def get_shipment_by_id(shipment_id: int = Path(..., title="The ID of the shipment to retrieve")):
    connection = get_new_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = connection.cursor(dictionary=True)
        query = """
       SELECT hc.*, hgu.user_ID
        FROM harbor_checkpoint hc
        LEFT JOIN harbor_guard_user hgu ON hc.hg_user_ID = hgu.hg_user_ID
        WHERE hc.checkpoint_ID = %s
        """
        cursor.execute(query, (shipment_id,))
        shipment = cursor.fetchone()
        cursor.close()
        connection.close()
        
        if shipment is None:
            raise HTTPException(status_code=404, detail="Shipment not found")
        
        return shipment
    except Error as err:
        connection.close()
        raise HTTPException(status_code=500, detail=str(err))

    
# Delete a shipment
@app.delete("/delete-shipment/{checkpoint_id}")
def delete_shipment(checkpoint_id: int = Path(..., description="The ID of the shipment to delete")):
    connection = get_new_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM harbor_checkpoint WHERE checkpoint_id = %s", (checkpoint_id,))
        connection.commit()
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Shipment not found")
        cursor.close()
        connection.close()
        return JSONResponse(content={"message": "Shipment deleted successfully"}, status_code=200)
    except Error as err:
        connection.close()
        raise HTTPException(status_code=500, detail=str(err))

# Delete a batch order
@app.delete("/delete-batch/{batch_id}")
def delete_batch(batch_id: int = Path(..., description="The ID of the batch order to delete")):
    connection = get_new_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM batch_information WHERE batch_ID = %s", (batch_id,))
        connection.commit()
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Batch order not found")
        cursor.close()
        connection.close()
        return JSONResponse(content={"message": "Batch order deleted successfully"}, status_code=200)
    except Error as err:
        connection.close()
        raise HTTPException(status_code=500, detail=str(err))
    