#!/usr/bin/env python3
"""Local web dashboard for browsing discovered places."""

import math
import os

from flask import Flask, jsonify, render_template, request

from config import CATEGORIES, VALID_CATEGORIES
from pipeline import db

app = Flask(__name__)


@app.route("/")
def index():
    conn = db.get_connection()
    try:
        cities = conn.execute("SELECT * FROM cities ORDER BY name").fetchall()
        city_id = request.args.get("city_id", type=int)
        city_name = None
        stats = None
        places = []
        place_types = set()
        place_categories = []

        if not city_id and cities:
            city_id = cities[0]["id"]

        page = max(1, request.args.get("page", 1, type=int))
        per_page = 50
        total_places = 0
        total_pages = 0

        category_filter = request.args.get("category", "")
        if category_filter and category_filter not in VALID_CATEGORIES:
            category_filter = ""

        if city_id:
            city_row = conn.execute("SELECT * FROM cities WHERE id = ?", (city_id,)).fetchone()
            if city_row:
                city_name = city_row["name"]
                stats = db.get_city_stats(conn, city_id)
                places, total_places = db.get_places_page(
                    conn,
                    city_id,
                    page,
                    per_page,
                    category=category_filter or None,
                )
                total_pages = math.ceil(total_places / per_page) if total_places else 0
                place_types = sorted(
                    r["type"]
                    for r in conn.execute(
                        "SELECT DISTINCT type FROM places WHERE city_id = ?",
                        (city_id,),
                    ).fetchall()
                )
                place_categories = sorted(
                    r["category"]
                    for r in conn.execute(
                        "SELECT DISTINCT category FROM places WHERE city_id = ? AND category IS NOT NULL",
                        (city_id,),
                    ).fetchall()
                )

        type_filter = request.args.get("type", "")
        trap_filter = request.args.get("trap", "no")
        search = request.args.get("search", "")
    finally:
        conn.close()

    return render_template(
        "dashboard.html",
        cities=cities,
        city_id=city_id,
        city_name=city_name,
        stats=stats,
        places=places,
        place_types=place_types,
        place_categories=place_categories,
        category_labels=CATEGORIES,
        type_filter=type_filter,
        trap_filter=trap_filter,
        category_filter=category_filter,
        search=search,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
    )


@app.route("/api/places")
def api_places():
    """JSON endpoint for places data."""
    conn = db.get_connection()
    try:
        city_id = request.args.get("city_id", type=int)
        if not city_id:
            return jsonify([])

        page = max(1, request.args.get("page", 1, type=int))
        per_page = min(500, max(1, request.args.get("per_page", 50, type=int)))
        category = request.args.get("category", "") or None

        places, total = db.get_places_page(conn, city_id, page, per_page, category=category)
        result = [dict(p) for p in places]
        return jsonify({"places": result, "total": total, "page": page, "per_page": per_page})
    finally:
        conn.close()


# Initialize database schema once at startup
with db.get_connection() as _conn:
    db.init_db(_conn)

if __name__ == "__main__":
    print("Starting Atlasi Dashboard at http://localhost:5555")
    app.run(host="127.0.0.1", port=5555, debug=os.getenv("FLASK_DEBUG", "false").lower() == "true")
