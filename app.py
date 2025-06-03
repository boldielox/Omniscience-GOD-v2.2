import os
import io
import csv
import zipfile
import tempfile
import pandas as pd
import numpy as np
from datetime import datetime
from flask import Flask, request, jsonify, render_template_string
from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'omniscient-divine-key')
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///omniscience.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
db = SQLAlchemy(app)

class Omniscience(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=True)
    swings_competitive = db.Column(db.Integer)
    percent_swings_competitive = db.Column(db.Float)
    contact = db.Column(db.Integer)
    avg_bat_speed = db.Column(db.Float)
    hard_swing_rate = db.Column(db.Float)
    squared_up_per_bat_contact = db.Column(db.Float)
    squared_up_per_swing = db.Column(db.Float)
    blast_per_bat_contact = db.Column(db.Float)
    blast_per_swing = db.Column(db.Float)
    swing_length = db.Column(db.Float)
    swords = db.Column(db.Integer)
    batter_run_value = db.Column(db.Float)
    whiffs = db.Column(db.Integer)
    whiff_per_swing = db.Column(db.Float)
    batted_ball_events = db.Column(db.Integer)
    batted_ball_event_per_swing = db.Column(db.Float)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    delta_bat_speed = db.Column(db.Float)
    oscillator_bat_speed = db.Column(db.Float)
    cashout_signal = db.Column(db.Boolean, default=False)
    pick_tracked = db.Column(db.Boolean, default=False)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

def add_delta_and_oscillator(df, col, window=5):
    if col not in df.columns:
        return df
    df[col] = pd.to_numeric(df[col], errors='coerce')
    df[f'delta_{col}'] = df[col].diff()
    df[f'oscillator_{col}'] = (df[col] - df[col].rolling(window).mean()) / (df[col].rolling(window).std() + 1e-6)
    return df

def engineer_features(df):
    for col in ['avg_bat_speed']:
        df = add_delta_and_oscillator(df, col)
    if 'oscillator_avg_bat_speed' in df.columns:
        df['cashout_signal'] = df['oscillator_avg_bat_speed'] < -2
    df['pick_tracked'] = True
    return df

def is_csv_corrupted(file_obj):
    try:
        pos = file_obj.tell()
        reader = csv.reader(io.TextIOWrapper(file_obj, encoding='utf-8'))
        header = next(reader, None)
        first_row = next(reader, None)
        file_obj.seek(pos)
        if not header or not first_row:
            return True, "Empty or missing data"
        if len(header) != len(first_row):
            return True, f"Header has {len(header)} columns, row has {len(first_row)}"
        return False, None
    except Exception as e:
        file_obj.seek(0)
        return True, str(e)

def is_zip_corrupted(file_obj):
    try:
        pos = file_obj.tell()
        with zipfile.ZipFile(file_obj, 'r') as zipf:
            file_list = zipf.namelist()
            if not file_list:
                return True, "ZIP is empty"
            for filename in file_list:
                try:
                    zipf.getinfo(filename)
                except:
                    return True, f"Cannot read file info for {filename}"
        file_obj.seek(pos)
        return False, None
    except zipfile.BadZipFile as e:
        file_obj.seek(0)
        return True, f"Bad ZIP file: {str(e)}"
    except Exception as e:
        file_obj.seek(0)
        return True, str(e)

@app.route('/upload_stats', methods=['POST'])
def upload_stats():
    if 'files' not in request.files:
        return jsonify({'error': 'No files provided'}), 400
    files = request.files.getlist('files')
    alerts = []
    results = []
    try:
        for file_storage in files:
            if file_storage.filename == '':
                continue
            filename = secure_filename(file_storage.filename)
            file_obj = file_storage
            if filename.endswith('.zip'):
                corrupted, error_msg = is_zip_corrupted(file_obj)
                if corrupted:
                    alerts.append(f"Corrupted ZIP: {filename} - {error_msg}")
                    continue
                file_obj.seek(0)
                try:
                    with zipfile.ZipFile(file_obj, 'r') as zipf:
                        for zipinfo in zipf.infolist():
                            if zipinfo.filename.endswith('.csv'):
                                try:
                                    with zipf.open(zipinfo) as csvfile:
                                        corrupted, error_msg = is_csv_corrupted(csvfile)
                                        if corrupted:
                                            alerts.append(f"Corrupted CSV in ZIP: {zipinfo.filename} - {error_msg}")
                                            continue
                                        csvfile.seek(0)
                                        results.extend(process_csv(csvfile, zipinfo.filename))
                                except Exception as e:
                                    alerts.append(f"Error processing {zipinfo.filename}: {str(e)}")
                except Exception as e:
                    alerts.append(f"Error processing ZIP {filename}: {str(e)}")
            elif filename.endswith('.csv'):
                corrupted, error_msg = is_csv_corrupted(file_obj)
                if corrupted:
                    alerts.append(f"Corrupted CSV: {filename} - {error_msg}")
                    continue
                file_obj.seek(0)
                results.extend(process_csv(file_obj, filename))
            else:
                alerts.append(f"Unsupported file type: {filename}")
        db.session.commit()
        return jsonify({
            'status': 'INGESTION COMPLETE',
            'alerts': alerts,
            'results': results,
            'files_processed': len(results)
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Server error: {str(e)}'}), 500

def process_csv(file_obj, filename):
    results = []
    try:
        df = pd.read_csv(file_obj)
        df = engineer_features(df)
        for _, row in df.iterrows():
            data = {col: row.get(col) for col in Omniscience.__table__.columns.keys() if col in row and not pd.isna(row[col])}
            data['timestamp'] = datetime.utcnow()
            omni = Omniscience(**data)
            db.session.add(omni)
            results.append({
                'id': row.get('id', 'unknown'),
                'name': row.get('name', 'unknown'),
                'avg_bat_speed': row.get('avg_bat_speed'),
                'cashout_signal': bool(row.get('cashout_signal', False)),
                'oscillator_bat_speed': row.get('oscillator_avg_bat_speed')
            })
    except Exception as e:
        raise Exception(f"Error processing {filename}: {str(e)}")
    return results

@app.route('/api/omniscience_stats', methods=['GET'])
def omniscience_stats():
    stats = Omniscience.query.order_by(Omniscience.timestamp.desc()).limit(100).all()
    return jsonify([s.to_dict() for s in stats])

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
