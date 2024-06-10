from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import mysql.connector
import datetime
import random

from centra_pydantic import *

connect = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='*neoSQL01',
    database='central_database'
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

def generateID():
    newid = ""
    for _ in range(20):
        newid += "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[random.randint(0, 61)]
    return newid

# API Endpoints
# Get all orders
@app.get("/get_all_orders/{centra_id}", response_model=List[BatchInformation])
async def get_batches(centra_id: int):
    cursor = connect.cursor(dictionary=True)
    query = """
        SELECT bi.*, dl.*, wl.*, pl.* 
        FROM batch_information bi 
        LEFT JOIN dry_leaves dl ON bi.dry_leaves_ID = dl.dry_leaves_ID 
        LEFT JOIN wet_leaves wl ON bi.wet_leaves_ID = wl.wet_leaves_ID 
        LEFT JOIN powdered_leaves pl ON bi.powdered_leaves_ID = pl.powdered_leaves_ID 
        JOIN centra_user ON bi.centra_user_ID = centra_user.centra_user_ID 
        WHERE centra_user.centra_id = %s 
        ORDER BY batch_date DESC;
    """
    cursor.execute(query, (centra_id,))
    records = cursor.fetchall()
    batches = []
    for record in records:
        batch = BatchInformation(
            batch_ID=record['batch_ID'],
            batch_date=record['batch_date'],
            dry_leaves_ID=record['dry_leaves_ID'],
            wet_leaves_ID=record['wet_leaves_ID'],
            powdered_leaves_ID=record['powdered_leaves_ID'],
            status=record['status'],
            dry_leaves=DryLeaves(**record) if record['dry_leaves_ID'] else None,
            wet_leaves=WetLeaves(**record) if record['wet_leaves_ID'] else None,
            powdered_leaves=PowderedLeaves(**record) if record['powdered_leaves_ID'] else None
        )
        batches.append(batch)
    return batches


@app.get("/get_ongoing_orders/{centra_id}", response_model=List[BatchInformation])
def get_ongoing_orders(centra_id):
    db = connect.cursor()
    db.execute("""
            SELECT bi.*, dl.*, wl.*, pl.* 
            FROM batch_information bi 
            LEFT JOIN dry_leaves dl ON bi.dry_leaves_ID = dl.dry_leaves_ID 
            LEFT JOIN wet_leaves wl ON bi.wet_leaves_ID = wl.wet_leaves_ID 
            LEFT JOIN powdered_leaves pl ON bi.powdered_leaves_ID = pl.powdered_leaves_ID 
            JOIN centra_user ON bi.centra_user_ID = centra_user.centra_user_ID 
            WHERE centra_user.centra_id = %s 
            AND bi.status < 4
            ORDER BY bi.batch_date DESC;
        """, (centra_id,))
    return [
                {
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
            for row in db.fetchall()]


@app.post("/create_new_order/{centra_id}/{uid}")
def create_new_orders(centra_id, uid):
    db = connect.cursor()
    db.execute("SELECT centra_user_ID from centra_user "
               "WHERE centra_id = %s "
               "AND centra_user_ID = %s;", (centra_id, uid))
    res = db.fetchone()
    if len(res) == 1:
        uid = res[0]
        batch_id = generateID()
    else:
        return "User ID not found"

    db.execute("INSERT INTO batch_information "
               "VALUES (%s, %s, %s, %s, %s, %s, %s)",
               (batch_id, datetime.date.today(), None, None, None, uid, 0))
    connect.commit()
    return batch_id


# create system to add leaves data
@app.post("/set_wet_leaves/{batch_id}/{weight}")
def set_wet_leaves(batch_id, weight):
    db = connect.cursor()


@app.post("/create_new_shipment/{harbor_guard_id}/{batch_id}/{uid}")
def create_new_shipment(harbor_guard_id, batch_id, uid):
    db = connect.cursor()
    db.execute("SELECT batch_id from batch_information "
               "WHERE batch_id = %s "
               "AND centra_user_ID = %s;", (batch_id, uid))
    res = db.fetchone()
    if len(res) == 1:
        batch_id = res[0]
        checkpoint_id = generateID()
    else:
        return "batch not found"
    db.execute("UPDATE batch_information "
               "SET status = 4 "
               "WHERE batch_id = %s", (batch_id,))
    db.execute("INSERT INTO harbor_checkpoint "
               "VALUES (%s, %s, %s, %s, %s, %s, %s)",
               (checkpoint_id, None, None, None, "default status", batch_id, harbor_guard_id))   # change this
    connect.commit()