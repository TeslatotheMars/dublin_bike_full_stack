from flask import Blueprint, request, jsonify
from werkzeug.security import check_password_hash, generate_password_hash
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
from api.db import get_db_conn
import pymysql

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/signup", methods=["POST"])
def signup():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    hashed_password = generate_password_hash(password)
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO users (username, email, password_hash, balance) VALUES (%s, %s, %s, %s)",
                (username, username, hashed_password, 0.00)
            )
        conn.commit()
        return jsonify({"message": "User registered successfully"}), 201
    except pymysql.err.IntegrityError as e:
        if e.args[0] == 1062:
            return jsonify({"error": "Username already exists"}), 409
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@auth_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT password_hash FROM users WHERE username = %s", (username,)
            )
            user = cursor.fetchone()
    except Exception as e:
        return jsonify({"error": "Database error"}), 500
    finally:
        conn.close()

    if user and check_password_hash(user['password_hash'], password):
        access_token = create_access_token(identity=username)
        return jsonify(access_token=access_token), 200

    return jsonify({"error": "Invalid username or password"}), 401


@auth_bp.route("/profile", methods=["GET"])
@jwt_required()
def profile():
    username = get_jwt_identity()
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT username, email, balance, created_at FROM users WHERE username = %s",
                (username,)
            )
            user = cursor.fetchone()
        if not user:
            return jsonify({"error": "User not found"}), 404
        return jsonify({
            "username":     user["username"],
            "email":        user["email"] or user["username"],
            "balance":      float(user["balance"]),
            "member_since": user["created_at"].strftime("%B %Y") if user["created_at"] else "—"
        }), 200
    finally:
        conn.close()