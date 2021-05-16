from app import app, db, bcrypt
from flask import render_template, flash, redirect, url_for
from app.models import User
from flask import request, abort, session
from datetime import datetime
from flask import jsonify
from flask_jwt_extended import create_access_token



@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
def index():
    return render_template("index.html", token="secret")


@app.route('/api/register', methods=['POST'])
def register():
    username = request.json.get("username")
    password = request.json.get("password")
    hashed_password = bcrypt.generate_password_hash(password).decode('utf')
    email = request.json.get('email')
    print("==================EMAIL=============")
    print(username, password)
    print(email)
    last_seen = datetime.utcnow()
    # return jsonify({'status': 'error'}), 400
    if User.query.filter_by(username=username).first() is not None:
        abort(400)
    user = User(username=username, password_hash=hashed_password, email=email, last_seen=last_seen)
    db.session.add(user)
    db.session.commit()
    return jsonify(status= "OK")


@app.route('/auth', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        if session.get('username'):
            username = session['username']
            return jsonify(status='OK',username=username)
        return jsonify(status='invalid session')
    if request.method == 'POST':
        username = request.json.get("username")
        password = request.json.get("password")
        user = User.query.filter_by(username=username).first()
        if user:
            if bcrypt.check_password_hash(user.password_hash, password):
                access_token = create_access_token(identity={'username': user.username})
                user.last_seen = datetime.utcnow()
            session['username'] = username
            session.permanent = False
            return jsonify({"token" : access_token})
        return jsonify(status='Invalid username')
    return jsonify(status='error')


@app.route('/api/signout', methods=['POST'])
def logout():
    session['username'] = None
    return redirect(url_for('index'))
