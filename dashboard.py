#!/usr/bin/env python3
"""Local web dashboard for browsing discovered places."""

import math
import os

from flask import Flask, jsonify, render_template, request
from pipeline import db

app = Flask(__name__)


@app.route("/")
def index():
    conn = db.get_connection()
    try:
        db.init_db(conn)

        cities = conn.execute("SELECT * FROM cities ORDER BY name").fetchall()
        city_id = request.args.get("city_id", type=int)
        city_name = None
        stats = None
        places = []
        place_types = set()

        if not city_id and cities:
            city_id = cities[0]["id"]

        page = request.args.get("page", 1, type=int)
        per_page = 50
        total_places = 0
        total_pages = 0

        if city_id:
            city_row = conn.execute("SELECT * FROM cities WHERE id = ?", (city_id,)).fetchone()
            if city_row:
                city_name = city_row["name"]
                stats = db.get_city_stats(conn, city_id)
                places, total_places = db.get_places_page(conn, city_id, page, per_page)
                total_pages = math.ceil(total_places / per_page) if total_places else 0
                place_types = sorted(
                    r["type"] for r in conn.execute(
                        "SELECT DISTINCT type FROM places WHERE city_id = ?", (city_id,),
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
        type_filter=type_filter,
        trap_filter=trap_filter,
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
        db.init_db(conn)

        city_id = request.args.get("city_id", type=int)
        if not city_id:
            return jsonify([])

        places = db.get_all_places(conn, city_id)
        result = [dict(p) for p in places]
    finally:
        conn.close()
    return jsonify(result)


if __name__ == "__main__":
    print("Starting Atlasi Dashboard at http://localhost:5555")
    app.run(host="127.0.0.1", port=5555, debug=os.getenv("FLASK_DEBUG", "false").lower() == "true")
