# Import the dependencies.
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
import datetime as dt
import pandas as pd

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()  

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)


#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route('/')
def homepage():
    return (f"""
        <html>
            <head>
                <title>Climate API<</title>
            </head>
            <body>
                <h1>Welcome to Climate API<!</h1>
                <h2>Available Routes:</h2>
                <ul>
                    <li><a href="/api/v1.0/precipitation">precipitation</a></li>
                    <li><a href="/api/v1.0/stations">stations</a></li>
                    <li><a href="/api/v1.0/tobs">tobs</a></li>
                    <li><a href="/api/v1.0/start/<start>">start</a></li>
                    <li><a href="/api/v1.0/start_end/<start>/<end>">start/end</a></li>
                </ul>
            </body>
        </html>
    """)


@app.route('/api/v1.0/precipitation')
def precipitation():
    latest_date = session.query(func.max(Measurement.date)).scalar()
    latest_date = dt.datetime.strptime(latest_date, '%Y-%m-%d')
    last_year = latest_date - dt.timedelta(days=365)
    
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= last_year).all()
    last_year_df = pd.DataFrame(results, columns=['date', 'precipitation'])
    last_year_df = last_year_df.sort_values('date')
    precipitation_dict = {date: prcp for date, prcp in results}
    return jsonify(precipitation_dict)


@app.route('/api/v1.0/stations')
def stations():
    results = session.query(Station.station, Station.name).all()
    stations_list = [{'station': station, 'name': name} for station, name in results]
    return jsonify(stations_list)


@app.route('/api/v1.0/tobs')
def tobs():
    latest_date = session.query(func.max(Measurement.date)).scalar()
    latest_date = dt.datetime.strptime(latest_date, '%Y-%m-%d')
    last_year = latest_date - dt.timedelta(days=365)
    
    most_active_station = session.query(Measurement.station).group_by(Measurement.station).order_by(func.count().desc()).first()[0]
    results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == most_active_station, Measurement.date >= last_year).all()
    tobs_list = [{'date': date, 'temperature': tobs} for date, tobs in results]
    return jsonify(tobs_list)


############

def get_start(start):
    # Create session
    session = Session()

    # Find the most recent date in the database
    latest_date = session.query(func.max(Measurement.date)).scalar()
    latest_date = dt.datetime.strptime(latest_date, "%Y-%m-%d").date()
    
    try:
        # Convert start date to datetime object with proper format validation
        start_date = dt.datetime.strptime(start, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
    

    # Consult query
    temp_query_result = session.query(
        func.min(Measurement.tobs),
        func.avg(Measurement.tobs),
        func.max(Measurement.tobs)
    ).filter(Measurement.date >= start_date, Measurement.date <= latest_date).all()

    # Formatar os resultados
    tobs_data = []
    for min_temp, avg_temp, max_temp in temp_query_result:
        tobs_dict = {
            "Min_Temp": min_temp,
            "Avg_Temp": avg_temp,
            "Max_Temp": max_temp
        }
        tobs_data.append(tobs_dict)
    
    session.close()
    
    return jsonify(tobs_data)

@app.route("/api/v1.0/start_end/<start>/<end>")
def get_start_end(start, end):
    try:
        # Converter as datas de início e fim para objetos datetime
        start_date = dt.datetime.strptime(start, "%Y-%m-%d").date()
        end_date = dt.datetime.strptime(end, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

    # Criar uma sessão
    session = Session()

    # Consultar os dados de temperatura para o intervalo fornecido
    temp_query_result = session.query(
        func.min(Measurement.tobs),
        func.avg(Measurement.tobs),
        func.max(Measurement.tobs)
    ).filter(Measurement.date >= start_date, Measurement.date <= end_date).all()

    session.close()

    # Formatar os resultados
    tobs_data = []
    for min_temp, avg_temp, max_temp in temp_query_result:
        tobs_dict = {
            "Min_Temp": min_temp,
            "Avg_Temp": avg_temp,
            "Max_Temp": max_temp
        }
        tobs_data.append(tobs_dict)

    return jsonify(tobs_data)

if __name__ == '__main__':
    app.run(debug=True)