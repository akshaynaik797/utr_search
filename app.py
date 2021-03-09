from flask import Flask, request, jsonify, url_for
from flask_cors import CORS

from apscheduler.schedulers.background import BackgroundScheduler

from last_mails import last_mails_function

app = Flask(__name__)
cors = CORS(app)


app.config['CORS_HEADERS'] = 'Content-Type'
app.config['referrer_url'] = None


@app.route("/")
def index():
    return url_for('index', _external=True)

@app.route("/lastmail", methods=["POST"])
def lastmail():
    data = request.form.to_dict()
    for i in ['hospital']:
        if i not in data:
            return jsonify({"error": f"pass {i} parameter"})
    return jsonify(last_mails_function(data['hospital'], ''))
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=9983)
