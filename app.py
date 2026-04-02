import os
import re
import csv
import io
from datetime import datetime, timezone
from functools import wraps

import boto3
from botocore.exceptions import ClientError
from flask import (Flask, render_template, redirect, url_for, request,
                   session as flask_session, make_response, flash, abort)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, case

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-me')

# --- Database ---
database_url = os.environ.get('DATABASE_URL', 'sqlite:///ratings.db')
# Neon/Heroku use "postgres://" but SQLAlchemy 2.x requires "postgresql://"
if database_url.startswith('postgres://'):
    database_url = database_url.replace('postgres://', 'postgresql://', 1)
app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- AWS / App config ---
S3_BUCKET = os.environ.get('S3_BUCKET', 'ethz-otree-whisper')
S3_REGION = os.environ.get('S3_REGION', 'eu-north-1')
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'changeme')
PRESIGNED_EXPIRY = 3600  # seconds


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Recording(db.Model):
    __tablename__ = 'recordings'
    id = db.Column(db.Integer, primary_key=True)
    s3_key = db.Column(db.String(500), unique=True, nullable=False)
    session_code = db.Column(db.String(50), nullable=False)
    participant_code = db.Column(db.String(50), nullable=False)
    app_name = db.Column(db.String(50), nullable=False)   # 'seating' or 'discount'
    round_num = db.Column(db.Integer, nullable=False)
    discovered_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    ratings = db.relationship('Rating', backref='recording', lazy=True,
                               cascade='all, delete-orphan')


class Rating(db.Model):
    __tablename__ = 'ratings'
    id = db.Column(db.Integer, primary_key=True)
    recording_id = db.Column(db.Integer, db.ForeignKey('recordings.id'), nullable=False)
    rater = db.Column(db.String(100), nullable=False)
    rule_compliant = db.Column(db.Boolean, nullable=False)
    noise_ok = db.Column(db.Boolean, nullable=False)
    turntaking_ok = db.Column(db.Boolean, nullable=False)
    audio_ok = db.Column(db.Boolean, nullable=False)
    intelligible = db.Column(db.Boolean, nullable=False)
    on_topic = db.Column(db.Boolean, nullable=False)
    usable = db.Column(db.Boolean, nullable=False)
    comment = db.Column(db.Text, nullable=True)
    rated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


with app.app_context():
    db.create_all()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_s3_client():
    return boto3.client(
        's3',
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


# Matches: {session}_{participant}_{seating|discount}_r{n}.webm
# Optional leading path prefix (e.g. "folder/file.webm")
FILENAME_RE = re.compile(
    r'^(?:.*?/)?(?P<session>[a-z0-9]+)_(?P<participant>[a-z0-9]+)'
    r'_(?P<app>seating|discount)_r(?P<round>\d+)\.webm$',
    re.IGNORECASE,
)


def parse_s3_key(key):
    """Return (session_code, participant_code, app_name, round_num) or None."""
    m = FILENAME_RE.match(key)
    if not m:
        return None
    return (m.group('session'), m.group('participant'),
            m.group('app').lower(), int(m.group('round')))


def get_presigned_url(s3_key):
    return get_s3_client().generate_presigned_url(
        'get_object',
        Params={'Bucket': S3_BUCKET, 'Key': s3_key},
        ExpiresIn=PRESIGNED_EXPIRY,
    )


def recordings_for_participant(participant_code):
    """Return recordings ordered: seating rounds first, discount rounds second."""
    app_order = case(
        (Recording.app_name == 'seating', 0),
        (Recording.app_name == 'discount', 1),
        else_=2,
    )
    return (Recording.query
            .filter_by(participant_code=participant_code)
            .order_by(app_order, Recording.round_num)
            .all())


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not flask_session.get('logged_in'):
            return redirect(url_for('login', next=request.path))
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        rater = request.form.get('rater', '').strip()
        password = request.form.get('password', '')
        if password == ADMIN_PASSWORD and rater:
            flask_session['logged_in'] = True
            flask_session['rater'] = rater
            return redirect(request.args.get('next') or url_for('index'))
        flash('Wrong password or missing name.', 'danger')
    return render_template('login.html')


@app.route('/logout')
def logout():
    flask_session.clear()
    return redirect(url_for('login'))


@app.route('/')
@login_required
def index():
    rater = flask_session['rater']

    rows = (db.session.query(
                Recording.participant_code,
                Recording.session_code,
                func.count(Recording.id).label('total'),
            )
            .group_by(Recording.participant_code, Recording.session_code)
            .order_by(Recording.session_code, Recording.participant_code)
            .all())

    participants = []
    for row in rows:
        rated = (db.session.query(func.count(Rating.id))
                 .join(Recording, Rating.recording_id == Recording.id)
                 .filter(Recording.participant_code == row.participant_code,
                         Rating.rater == rater)
                 .scalar() or 0)
        participants.append({
            'participant_code': row.participant_code,
            'session_code': row.session_code,
            'total': row.total,
            'rated': rated,
        })

    total_recordings = sum(p['total'] for p in participants)
    total_rated = sum(p['rated'] for p in participants)
    return render_template('index.html', participants=participants,
                           rater=rater, total_recordings=total_recordings,
                           total_rated=total_rated)


@app.route('/participant/<participant_code>')
@login_required
def participant(participant_code):
    recordings = recordings_for_participant(participant_code)
    if not recordings:
        abort(404)
    rater = flask_session['rater']
    for rec in recordings:
        rec.my_rating = Rating.query.filter_by(
            recording_id=rec.id, rater=rater).first()
    return render_template('participant.html',
                           participant_code=participant_code,
                           session_code=recordings[0].session_code,
                           recordings=recordings,
                           rater=rater)


@app.route('/rate/<int:recording_id>', methods=['GET', 'POST'])
@login_required
def rate(recording_id):
    recording = db.get_or_404(Recording, recording_id)
    rater = flask_session['rater']
    existing = Rating.query.filter_by(recording_id=recording_id, rater=rater).first()

    if request.method == 'POST':
        fields = dict(
            rule_compliant='rule_compliant' in request.form,
            noise_ok='noise_ok' in request.form,
            turntaking_ok='turntaking_ok' in request.form,
            audio_ok='audio_ok' in request.form,
            intelligible='intelligible' in request.form,
            on_topic='on_topic' in request.form,
            usable='usable' in request.form,
            comment=request.form.get('comment', '').strip() or None,
        )
        if existing:
            for k, v in fields.items():
                setattr(existing, k, v)
            existing.rated_at = datetime.now(timezone.utc)
        else:
            db.session.add(Rating(recording_id=recording_id, rater=rater, **fields))
        db.session.commit()

        # Advance to next recording in sequence for this participant
        all_recs = recordings_for_participant(recording.participant_code)
        ids = [r.id for r in all_recs]
        idx = ids.index(recording_id)
        if idx + 1 < len(ids):
            return redirect(url_for('rate', recording_id=ids[idx + 1]))
        flash(f'All recordings for {recording.participant_code} rated!', 'success')
        return redirect(url_for('participant',
                                participant_code=recording.participant_code))

    # GET — generate a fresh presigned URL
    try:
        audio_url = get_presigned_url(recording.s3_key)
    except ClientError:
        audio_url = None

    all_recs = recordings_for_participant(recording.participant_code)
    ids = [r.id for r in all_recs]
    idx = ids.index(recording_id)

    return render_template('rate.html',
                           recording=recording,
                           audio_url=audio_url,
                           existing=existing,
                           idx=idx,
                           total=len(all_recs),
                           prev_id=ids[idx - 1] if idx > 0 else None,
                           next_id=ids[idx + 1] if idx + 1 < len(ids) else None)


@app.route('/sync', methods=['POST'])
@login_required
def sync():
    """Scan S3 bucket and register any new .webm recordings in the DB."""
    client = get_s3_client()
    paginator = client.get_paginator('list_objects_v2')
    added = 0
    for page in paginator.paginate(Bucket=S3_BUCKET):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if Recording.query.filter_by(s3_key=key).first():
                continue
            parsed = parse_s3_key(key)
            if not parsed:
                continue
            session_code, participant_code, app_name, round_num = parsed
            db.session.add(Recording(
                s3_key=key,
                session_code=session_code,
                participant_code=participant_code,
                app_name=app_name,
                round_num=round_num,
            ))
            added += 1
    db.session.commit()
    flash(f'Sync complete — {added} new recording(s) added.', 'success')
    return redirect(url_for('index'))


@app.route('/export')
@login_required
def export():
    """Download all ratings as a CSV file."""
    rows = (db.session.query(Rating, Recording)
            .join(Recording, Rating.recording_id == Recording.id)
            .order_by(Recording.session_code, Recording.participant_code,
                      Recording.app_name, Recording.round_num)
            .all())

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['session_code', 'participant_code', 'app', 'round', 's3_key',
                'rater', 'rule_compliant', 'noise_ok', 'turntaking_ok',
                'audio_ok', 'intelligible', 'on_topic', 'usable',
                'comment', 'rated_at'])
    for rating, rec in rows:
        w.writerow([
            rec.session_code, rec.participant_code, rec.app_name, rec.round_num,
            rec.s3_key, rating.rater,
            int(rating.rule_compliant), int(rating.noise_ok),
            int(rating.turntaking_ok), int(rating.audio_ok),
            int(rating.intelligible), int(rating.on_topic), int(rating.usable),
            rating.comment or '', rating.rated_at.isoformat(),
        ])

    resp = make_response(out.getvalue())
    resp.headers['Content-Type'] = 'text/csv; charset=utf-8'
    fname = f'ratings_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    resp.headers['Content-Disposition'] = f'attachment; filename={fname}'
    return resp


if __name__ == '__main__':
    app.run(debug=True)
